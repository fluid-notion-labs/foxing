use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use crossbeam::queue::SegQueue;
use std::time::Instant;
use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use tracing::warn;

static BATCH_TOKENS: AtomicU64 = AtomicU64::new(1000);
const MAX_WRITERS_BEFORE_BARRIER: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOpKind {
    Write,
    Rename,
    Truncate,
    Metadata,
    Barrier,
}

#[derive(Debug)]
pub struct WalEntry {
    pub id: u64,
    pub inode: u64,
    pub kind: WalOpKind,
    pub seq: u64,
    pub timestamp: Instant,
    pub completed: AtomicBool,
}

struct InodeState {
    active_writers: AtomicUsize,
    pending_barriers: AtomicUsize,
    exclusive_locked: AtomicBool,
    _last_seq: AtomicU64,
}

pub struct InMemoryWal {
    log_queue: SegQueue<Arc<WalEntry>>,
    inode_states: DashMap<u64, Arc<InodeState>>,
    global_seq: AtomicU64,
}

impl InMemoryWal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            log_queue: SegQueue::new(),
            inode_states: DashMap::new(),
            global_seq: AtomicU64::new(0),
        })
    }

    pub fn get_next_seq(&self) -> u64 {
        let seq = self.global_seq.fetch_add(1, Ordering::SeqCst);
        if seq >= u64::MAX - 10000 {
            warn!("WAL sequence approaching overflow. Resetting.");
            self.global_seq.store(1, Ordering::SeqCst);
            return 1;
        }
        seq
    }

    pub async fn acquire_barrier(&self, inode: u64, kind: WalOpKind) -> WalGuard<'_> {
        let state = self.inode_states.entry(inode).or_insert_with(|| Arc::new(InodeState {
            active_writers: AtomicUsize::new(0),
            pending_barriers: AtomicUsize::new(0),
            exclusive_locked: AtomicBool::new(false),
            _last_seq: AtomicU64::new(0),
        })).clone();

        let backoff = crossbeam::utils::Backoff::new();

        loop {
            // Check if exclusive locked OR if a barrier is pending (writer starvation prevention)
            if state.exclusive_locked.load(Ordering::Acquire) {
                backoff.snooze();
                if backoff.is_completed() {
                    tokio::task::yield_now().await;
                    backoff.reset();
                }
                continue;
            }

            match kind {
                WalOpKind::Write => {
                    // Critical Fix: Writers must wait if a barrier/rename is pending
                    let pending = state.pending_barriers.load(Ordering::Acquire);
                    if pending > 0 {
                        // Anti-starvation check: If too many writers are active, respect the barrier
                        let active = state.active_writers.load(Ordering::Acquire);
                        if active >= MAX_WRITERS_BEFORE_BARRIER {
                            backoff.snooze();
                            if backoff.is_completed() {
                                tokio::task::yield_now().await;
                                backoff.reset();
                            }
                            continue;
                        }
                    }
                    
                    state.active_writers.fetch_add(1, Ordering::SeqCst);
                    // Double check if exclusive lock was grabbed in the interim
                    if state.exclusive_locked.load(Ordering::Acquire) {
                        state.active_writers.fetch_sub(1, Ordering::SeqCst);
                        continue;
                    }
                    break;
                },
                WalOpKind::Rename | WalOpKind::Truncate | WalOpKind::Barrier => {
                    // Increment pending barriers count to signal writers to back off
                    state.pending_barriers.fetch_add(1, Ordering::SeqCst);
                    
                    loop {
                        // Wait for all active writers to drain
                        if state.active_writers.load(Ordering::Acquire) == 0 {
                            // Try to grab exclusive lock
                            if state.exclusive_locked.compare_exchange(
                                false, true, Ordering::SeqCst, Ordering::SeqCst
                            ).is_ok() {
                                break;
                            }
                        }
                        
                        backoff.snooze();
                        if backoff.is_completed() {
                            // Yield inside the inner loop while holding pending_barriers count high
                            // This effectively blocks new writers, draining the pool
                            tokio::task::yield_now().await;
                            backoff.reset();
                        }
                    }
                    // Decrement pending count once lock is acquired
                    state.pending_barriers.fetch_sub(1, Ordering::SeqCst);
                    break;
                },
                _ => break,
            }
        }

        let id = self.get_next_seq();
        let entry = Arc::new(WalEntry {
            id,
            inode,
            kind,
            seq: 0,
            timestamp: Instant::now(),
            completed: AtomicBool::new(false),
        });
        
        self.log_queue.push(entry.clone());
        self.trigger_batch_check();

        WalGuard {
            wal: self,
            inode,
            kind,
            entry,
        }
    }

    fn trigger_batch_check(&self) -> bool {
        let tokens = BATCH_TOKENS.fetch_sub(1, Ordering::SeqCst);
        if tokens == 0 {
            BATCH_TOKENS.store(1000, Ordering::SeqCst);
            return true;
        }
        false
    }

    fn release(&self, inode: u64, kind: WalOpKind, entry: &Arc<WalEntry>) {
        entry.completed.store(true, Ordering::Release);
        if let Some(state) = self.inode_states.get(&inode) {
            match kind {
                WalOpKind::Write => {
                    state.active_writers.fetch_sub(1, Ordering::SeqCst);
                },
                WalOpKind::Rename | WalOpKind::Truncate | WalOpKind::Barrier => {
                    state.exclusive_locked.store(false, Ordering::SeqCst);
                },
                _ => {}
            }
        }
    }
}

pub struct WalGuard<'a> {
    wal: &'a InMemoryWal,
    inode: u64,
    kind: WalOpKind,
    entry: Arc<WalEntry>,
}

impl<'a> Drop for WalGuard<'a> {
    fn drop(&mut self) {
        self.wal.release(self.inode, self.kind, &self.entry);
    }
}
