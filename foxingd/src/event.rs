use crate::metrics;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use serde::{Serialize, Deserialize};
use std::time::Instant;

pub const RENAME_NOREPLACE: u32 = 1 << 0;
pub const RENAME_EXCHANGE: u32 = 1 << 1;
pub const RENAME_WHITEOUT: u32 = 1 << 2;

#[allow(dead_code)]
const S_IFMT: u32 = 0o170000;
#[allow(dead_code)]
const S_IFDIR: u32 = 0o040000;

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
            Self::Mkdir |
            Self::Rmdir |
            Self::Link |
            Self::Symlink |
            Self::Unlink |
            Self::RenameIncomplete
        )
    }
}

#[derive(Debug)]
pub struct EventQueue {
    pub senders: Vec<mpsc::Sender<Arc<Event>>>
}

impl EventQueue {
    pub fn new(senders: Vec<mpsc::Sender<Arc<Event>>>) -> Self {
        Self { senders }
    }
    pub fn push(&self, e: Arc<Event>) -> bool {
        if e.is_internal_traffic() {
            return true;
        }
        metrics::EVENTS_TOTAL.with_label_values(&[e.event_type.as_str()]).inc();
        if self.senders.is_empty() { return false; }
        let pool_size = self.senders.len();
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
        match self.senders[target_idx].try_send(e.clone()) {
            Ok(_) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Channel full — try other workers before dropping
                for offset in 1..pool_size {
                    let alt_idx = (target_idx + offset) % pool_size;
                    if self.senders[alt_idx].try_send(e.clone()).is_ok() {
                        return true;
                    }
                }
                metrics::EVENTS_DROPPED.inc();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                metrics::EVENTS_DROPPED.inc();
                false
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
    pub uid: u32, // Added for ownership preservation
    pub gid: u32, // Added for ownership preservation
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

pub fn create_fanout(cap: usize, workers: usize) -> (EventQueue, Vec<mpsc::Receiver<Arc<Event>>>) {
    let actual_workers = workers.max(1);
    let (mut txs, mut rxs) = (Vec::new(), Vec::new());
    for _ in 0..actual_workers {
        let (t, r) = mpsc::channel(cap);
        txs.push(t);
        rxs.push(r);
    }
    (EventQueue::new(txs), rxs)
}
