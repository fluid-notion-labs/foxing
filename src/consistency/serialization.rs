use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Write,
    Rename,
}

pub struct OpGuard {
    engine: Arc<SerializationEngine>,
    inode: u64,
    kind: OpKind,
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        self.engine.complete_op(self.inode, self.kind);
    }
}

struct InodeState {
    active_count: usize,
    exclusive_active: bool,
    queue: VecDeque<(OpKind, Arc<Notify>)>,
}

impl InodeState {
    fn new() -> Self {
        Self {
            active_count: 0,
            exclusive_active: false,
            queue: VecDeque::new(),
        }
    }
}

pub struct SerializationEngine {
    state: std::sync::Mutex<HashMap<u64, InodeState>>,
}

impl SerializationEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(HashMap::new()),
        })
    }

    pub fn is_active(&self, inode: u64) -> bool {
        let state = self.state.lock().unwrap();
        state.contains_key(&inode)
    }

    pub async fn acquire_barrier(self: &Arc<Self>, inode: u64, kind: OpKind) -> OpGuard {
        let notify = {
            let mut state_map = self.state.lock().unwrap();
            let inode_state = state_map.entry(inode).or_insert_with(InodeState::new);
            
            if inode_state.queue.is_empty() && self.can_run(inode_state, kind) {
                self.mark_running(inode_state, kind);
                return OpGuard {
                    engine: self.clone(),
                    inode,
                    kind,
                };
            }
            
            let notify = Arc::new(Notify::new());
            inode_state.queue.push_back((kind, notify.clone()));
            notify
        };

        notify.notified().await;

        OpGuard {
            engine: self.clone(),
            inode,
            kind,
        }
    }

    fn can_run(&self, state: &InodeState, kind: OpKind) -> bool {
        if state.exclusive_active { return false; }
        match kind {
            OpKind::Write => true,
            OpKind::Rename => state.active_count == 0,
        }
    }

    fn mark_running(&self, state: &mut InodeState, kind: OpKind) {
        match kind {
            OpKind::Write => state.active_count += 1,
            OpKind::Rename => state.exclusive_active = true,
        }
    }

    fn complete_op(&self, inode: u64, kind: OpKind) {
        let mut state_map = self.state.lock().unwrap();
        
        if let Some(inode_state) = state_map.get_mut(&inode) {
            match kind {
                OpKind::Write => if inode_state.active_count > 0 { inode_state.active_count -= 1; },
                OpKind::Rename => inode_state.exclusive_active = false,
            }

            // Fix #12: Only wake ONE waiter to prevent Thundering Herd
            if let Some((next_kind, _)) = inode_state.queue.front() {
                if self.can_run(inode_state, *next_kind) {
                    let (kind_to_run, _) = *inode_state.queue.front().unwrap();
                    self.mark_running(inode_state, kind_to_run);
                    let (_, notify) = inode_state.queue.pop_front().unwrap();
                    notify.notify_one();
                }
            }

            if inode_state.active_count == 0 && !inode_state.exclusive_active && inode_state.queue.is_empty() {
                state_map.remove(&inode);
            }
        }
    }
}
