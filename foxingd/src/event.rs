use crate::metrics;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use serde::{Serialize, Deserialize};
use std::time::Instant;
use std::sync::atomic::Ordering;

pub const RENAME_NOREPLACE: u32 = 1 << 0;
pub const RENAME_EXCHANGE: u32 = 1 << 1;
pub const RENAME_WHITEOUT: u32 = 1 << 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EventType {
    Write=1, WriteRange=2, SetXattr=3, RemoveXattr=4, Rmdir=5, Fsync=6, Rename=7,
    Create=8, Unlink=9, Mkdir=10, Truncate=11, Link=12, Chmod=13, Chown=14,
    Barrier=15, Mknod=16, Symlink=17, Fallocate=18, Utimes=19,
    SetFlags=20,
    Lock=21,
    Flock=22,
    RenameIncomplete=23,
    Clone=24,
    SequenceGap=255, Unknown=0
}

impl From<u8> for EventType {
    fn from(v: u8) -> Self {
        match v {
             1=>Self::Write, 2=>Self::WriteRange, 3=>Self::SetXattr, 4=>Self::RemoveXattr,
            5=>Self::Rmdir, 6=>Self::Fsync, 7=>Self::Rename, 8=>Self::Create,
            9=>Self::Unlink, 10=>Self::Mkdir, 11=>Self::Truncate, 12=>Self::Link,
            13=>Self::Chmod, 14=>Self::Chown, 15=>Self::Barrier, 16=>Self::Mknod,
            17=>Self::Symlink, 18=>Self::Fallocate, 19=>Self::Utimes,
            20=>Self::SetFlags,
            21=>Self::Lock,
            22=>Self::Flock,
            23=>Self::RenameIncomplete,
            24=>Self::Clone,
            255=>Self::SequenceGap,
            _=>Self::Unknown
        }
    }
}

/// CAKE-inspired priority tin classification for events.
/// Events are classified into 4 tins with independent queues.
/// Higher-priority tins are always drained before lower ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventTin {
    /// Ordering barriers, sequence gaps, fsync — must always flow
    Control = 0,
    /// Create, Mkdir, Rename, Unlink — filesystem structure, never drop
    Structural = 1,
    /// Chmod, Chown, Utimes, xattr — metadata, lower urgency
    Metadata = 2,
    /// Write, WriteRange, Clone, Truncate — bulk data, coalesced, droppable
    Bulk = 3,
}

impl EventTin {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Structural => "structural",
            Self::Metadata => "metadata",
            Self::Bulk => "bulk",
        }
    }
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Write => "write", Self::WriteRange => "writerange", Self::SetXattr => "setxattr",
            Self::RemoveXattr => "removexattr", Self::Rmdir => "rmdir", Self::Fsync => "fsync",
            Self::Rename => "rename", Self::Create => "create", Self::Unlink => "unlink",
            Self::Mkdir => "mkdir", Self::Truncate => "truncate", Self::Link => "link",
            Self::Chmod => "chmod", Self::Chown => "chown", Self::Barrier => "barrier",
            Self::Mknod => "mknod", Self::Symlink => "symlink", Self::Fallocate => "fallocate",
            Self::Utimes => "utimes", Self::SetFlags => "setflags",
            Self::Lock => "lock", Self::Flock => "flock",
            Self::RenameIncomplete => "rename_incomplete",
            Self::Clone => "clone",
            Self::SequenceGap => "gap",
            Self::Unknown => "unknown"
        }
    }

    /// Classify event into a CAKE-style priority tin.
    pub fn tin(&self) -> EventTin {
        match self {
            Self::Barrier | Self::SequenceGap | Self::Fsync => EventTin::Control,
            Self::Create | Self::Mkdir | Self::Rename | Self::Unlink
            | Self::Rmdir | Self::Link | Self::Symlink | Self::Mknod
            | Self::RenameIncomplete => EventTin::Structural,
            Self::Chmod | Self::Chown | Self::Utimes | Self::SetXattr
            | Self::RemoveXattr | Self::SetFlags | Self::Lock | Self::Flock => EventTin::Metadata,
            _ => EventTin::Bulk,
        }
    }

    pub fn is_structural_metadata(&self) -> bool {
        matches!(self,
            Self::Mkdir | Self::Rmdir | Self::Unlink |
            Self::Link | Self::Symlink | Self::Mknod
        )
    }
    pub fn requires_global_ordering(&self) -> bool {
        matches!(self, Self::Rename | Self::Rmdir | Self::Mkdir | Self::Unlink | Self::Link | Self::RenameIncomplete)
    }
    pub fn is_control_plane(&self) -> bool {
        matches!(self,
            Self::Rename |
            Self::Create |
            Self::Mkdir |
            Self::Rmdir |
            Self::Link |
            Self::Symlink |
            Self::Unlink |
            Self::Mknod |
            Self::RenameIncomplete
        )
    }
}

/// Per-worker tinned receiver set. Workers drain tins in priority order
/// using biased select: control > structural > metadata > bulk.
pub struct TinnedReceiver {
    pub control: mpsc::UnboundedReceiver<Arc<Event>>,
    pub structural: mpsc::Receiver<Arc<Event>>,
    pub metadata: mpsc::Receiver<Arc<Event>>,
    pub bulk: mpsc::Receiver<Arc<Event>>,
}

/// CAKE-inspired multi-tin event queue. Events are classified by type
/// and routed to priority-separated channels per worker.
#[derive(Debug)]
pub struct EventQueue {
    control: Vec<mpsc::UnboundedSender<Arc<Event>>>,
    structural: Vec<mpsc::Sender<Arc<Event>>>,
    metadata: Vec<mpsc::Sender<Arc<Event>>>,
    bulk: Vec<mpsc::Sender<Arc<Event>>>,
    worker_count: usize,
}

impl EventQueue {
    pub fn push(&self, e: Arc<Event>) -> bool {
        if e.is_internal_traffic() {
            return true;
        }
        metrics::EVENTS_TOTAL.with_label_values(&[e.event_type.as_str()]).inc();
        if self.worker_count == 0 { return false; }

        let pool_size = self.worker_count;
        let target_idx = if pool_size <= 1 {
            0
        } else if e.event_type.is_control_plane() {
            0
        } else {
            let mut hasher = DefaultHasher::new();
            if e.parent_inode != 0 {
                e.parent_inode.hash(&mut hasher);
            } else {
                e.inode.hash(&mut hasher);
            }
            let hash = hasher.finish();
            let data_worker_count = pool_size - 1;
            1 + (hash as usize % data_worker_count)
        };

        let tin = e.event_type.tin();
        match tin {
            EventTin::Control => {
                // Control: unbounded, never drop
                let _ = self.control[target_idx].send(e);
                true
            }
            EventTin::Structural => {
                // Structural: try target worker, then all others, then blocking_send
                static STRUCT_PUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let sp = STRUCT_PUSH.fetch_add(1, Ordering::Relaxed);
                if sp < 20 || sp % 100 == 0 {
                    tracing::info!("TinnedQueue: Structural event #{} type={:?} name={} → worker {}",
                                   sp, e.event_type, e.name, target_idx);
                }
                match self.structural[target_idx].try_send(e.clone()) {
                    Ok(_) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        for offset in 1..pool_size {
                            let alt = (target_idx + offset) % pool_size;
                            if self.structural[alt].try_send(e.clone()).is_ok() {
                                return true;
                            }
                        }
                        // NEVER drop structural — block BPF thread briefly
                        let _ = self.structural[0].blocking_send(e);
                        true
                    }
                    Err(_) => { metrics::EVENTS_DROPPED.inc(); false }
                }
            }
            EventTin::Metadata => {
                match self.metadata[target_idx].try_send(e) {
                    Ok(_) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        metrics::EVENTS_DROPPED.inc();
                        false
                    }
                    Err(_) => { metrics::EVENTS_DROPPED.inc(); false }
                }
            }
            EventTin::Bulk => {
                match self.bulk[target_idx].try_send(e) {
                    Ok(_) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Bulk events: drop freely under pressure.
                        // Hydration scan catches missed writes.
                        metrics::EVENTS_DROPPED.inc();
                        false
                    }
                    Err(_) => { metrics::EVENTS_DROPPED.inc(); false }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub event_type: EventType,
    pub dev_id: u32,
    pub inode: u64,
    pub parent_inode: u64,
    pub new_parent_inode: u64,
    pub seq_num: u64,
    pub timestamp_ns: u64,
    pub offset: u64,
    pub length: u64,
    pub name: String,
    pub new_name: Option<String>,
    pub generation: u32,
    pub projid: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub flags: u32,
    pub nlink: u32,
    pub process_name: String,
    pub interactive: bool,
    #[serde(skip, default="Instant::now")]
    pub created_at: Instant
}

impl Event {
    pub fn is_rename_exchange(&self) -> bool {
        self.event_type == EventType::Rename && (self.flags & RENAME_EXCHANGE) != 0
    }
    pub fn is_internal_traffic(&self) -> bool {
        self.name.starts_with(".foxing") ||
        self.name.contains(".tmp.") ||
        self.name.ends_with(".swap_tmp") ||
        (self.new_name.as_ref().map(|n| n.starts_with(".foxing") || n.contains(".tmp.") || n.ends_with(".swap_tmp")).unwrap_or(false))
    }
}

/// Create a tinned fanout: per-worker 4-tin channel set.
/// Returns the EventQueue (sender side) and per-worker TinnedReceiver (receiver side).
pub fn create_fanout(_cap: usize, workers: usize) -> (EventQueue, Vec<TinnedReceiver>) {
    let actual_workers = workers.max(1);
    let mut control_txs = Vec::with_capacity(actual_workers);
    let mut structural_txs = Vec::with_capacity(actual_workers);
    let mut metadata_txs = Vec::with_capacity(actual_workers);
    let mut bulk_txs = Vec::with_capacity(actual_workers);
    let mut receivers = Vec::with_capacity(actual_workers);

    for _ in 0..actual_workers {
        let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
        let (str_tx, str_rx) = mpsc::channel(4096);
        let (met_tx, met_rx) = mpsc::channel(1024);
        let (blk_tx, blk_rx) = mpsc::channel(64);

        control_txs.push(ctl_tx);
        structural_txs.push(str_tx);
        metadata_txs.push(met_tx);
        bulk_txs.push(blk_tx);

        receivers.push(TinnedReceiver {
            control: ctl_rx,
            structural: str_rx,
            metadata: met_rx,
            bulk: blk_rx,
        });
    }

    let queue = EventQueue {
        control: control_txs,
        structural: structural_txs,
        metadata: metadata_txs,
        bulk: bulk_txs,
        worker_count: actual_workers,
    };

    (queue, receivers)
}
