use crate::metrics;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventType {
    Write=1, WriteRange=2, SetXattr=3, RemoveXattr=4, Rmdir=5, Fsync=6, Rename=7,
    Create=8, Unlink=9, Mkdir=10, Truncate=11, Link=12, Chmod=13, Chown=14,
    Barrier=15, Mknod=16, Symlink=17, Fallocate=18, Utimes=19,
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
            Self::Utimes => "utimes", Self::SequenceGap => "gap", Self::Unknown => "unknown"
        }
    }

    /// Returns true if this event modifies directory topology and should be handled
    /// by the dedicated Metadata Plane to prevent starvation.
    pub fn is_structural_metadata(&self) -> bool {
        matches!(self, 
            Self::Mkdir | Self::Rmdir | Self::Rename | 
            Self::Link | Self::Symlink | Self::Mknod
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
    
    pub fn push(&self, e: Arc<Event>) {
        metrics::EVENTS_TOTAL.with_label_values(&[&e.dev_id.to_string(), e.event_type.as_str()]).inc();
        if self.senders.is_empty() { return; }
        
        let target_idx = if self.senders.len() > 1 {
            if e.event_type.is_structural_metadata() {
                // CONTROL PLANE: Always route structural changes to Worker 0
                // This ensures strict ordering of directory creation/deletion
                // and prevents them from being blocked by bulk I/O.
                0
            } else {
                // DATA PLANE: Route file IO to Workers 1..N based on inode hash.
                // This provides parallelism for heavy operations.
                // We hash the inode to ensure all writes for the same file go to the same worker
                // to preserve write ordering.
                let mut hasher = DefaultHasher::new();
                e.inode.hash(&mut hasher);
                let hash = hasher.finish();
                
                // Map to range [1, len-1]
                1 + (hash as usize % (self.senders.len() - 1))
            }
        } else {
            0 // Fallback for single-worker config
        };
        
        if self.senders[target_idx].try_send(e).is_err() {
            metrics::EVENTS_DROPPED.inc();
        }
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub event_type: EventType, pub dev_id: u32, pub inode: u64, pub parent_inode: u64,
    pub seq_num: u64, pub offset: u64, pub length: u64, pub name: String, pub new_name: Option<String>,
    pub generation: u32,
    pub projid: u32,
    pub mode: u32,
    pub flags: u32,
    pub process_name: String,
    pub interactive: bool,
    pub created_at: std::time::Instant
}

pub fn create_fanout(cap: usize, workers: usize) -> (EventQueue, Vec<mpsc::Receiver<Arc<Event>>>) {
    // Ensure at least 2 workers for Control/Data plane separation if requested
    let actual_workers = workers.max(1);
    
    let (mut txs, mut rxs) = (Vec::new(), Vec::new());
    for _ in 0..actual_workers {
        let (t, r) = mpsc::channel(cap);
        txs.push(t);
        rxs.push(r);
    }
    (EventQueue::new(txs), rxs)
}
