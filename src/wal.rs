
use std::path::PathBuf;
use std::time::SystemTime;
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use tracing::{debug, error, warn};
use std::io;
use crate::error::{FoxingError, Result};
use std::hash::Hasher;
use std::collections::hash_map::DefaultHasher;
use crate::metrics;
use std::sync::Arc;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalState {
    None,
    IntentPending,
    InProgress,
    CommitPending,
    WriteBulk,
    FsyncCommit,
    PendingRename,
    Unknown
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PersistedWalEntry {
    pub inode: u64,
    pub seq: u64,
    pub state: WalState,
    pub timestamp: u64,
    pub daemon_id: String,
    pub path: PathBuf,
    pub projid: u32,
    pub crc: u64,
}
#[derive(Debug)]
pub struct WalStateMap {
    states: DashMap<u64, PersistedWalEntry>,
}
pub struct WalGuard<'a> {
    map: &'a WalStateMap,
    inode: u64,
    entry: PersistedWalEntry,
    // Flag to indicate if the caller successfully committed the operation.
    // If true, drop() will NOT rollback. If false, drop() performs rollback.
    committed: bool,
}
impl WalStateMap {
    pub fn new() -> Self {
        Self { states: DashMap::new() }
    }
    pub fn begin_write<'a>(&'a self, inode: u64, path: PathBuf, seq: u64, daemon_id: String, projid: u32) -> Result<WalGuard<'a>> {
        if inode == 0 {
             return Err(FoxingError::Io(io::Error::new(io::ErrorKind::InvalidInput, "Inode 0 is forbidden for WAL entries.")));
        }
        let current_state = self.get_state_for_inode(inode);
        if current_state != WalState::None {
            return Err(FoxingError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("WAL Conflict for Inode {}: Found {:?}, Expected None.", inode, current_state)
            )));
        }
        let entry = self.create_wal_entry(inode, WalState::IntentPending, seq, daemon_id, path, projid);
        if self.states.contains_key(&inode) {
            // This is a double check against `get_state_for_inode`, should ideally not happen
            return Err(FoxingError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("WAL Insertion Failed for Inode {}: Key already present.", inode)
            )));
        }
        self.states.insert(inode, entry.clone());
        debug!("WAL: Inode {} -> IntentPending", inode);
        Ok(WalGuard {
            map: self,
            inode,
            entry,
            committed: false,
        })
    }
    // NEW: Public method to signal success without relying on drop() to commit.
    pub fn mark_committed(&self, guard: &mut WalGuard) {
        if let Some((_, entry)) = self.states.remove(&guard.inode) {
            debug!("WAL: Inode {} Commit OK (State was {:?}). Removed from map.", guard.inode, entry.state);
        } else {
            debug!("WAL: Inode {} Commit OK: Entry already missing (Acceptable Race).", guard.inode);
        }
        guard.committed = true;
    }
    // Updated signature: use WalGuard
    pub fn rearm_guard<'a>(&'a self, entry: PersistedWalEntry) -> Option<WalGuard<'a>> {
        if entry.state == WalState::CommitPending {
             Some(WalGuard {
                 map: self,
                 inode: entry.inode,
                 entry,
                 committed: false,
             })
        } else {
            None
        }
    }
    // Updated to handle WalGuard struct
    pub fn advance(&self, guard: &mut WalGuard, to_state: WalState) -> Result<()> {
        let inode = guard.inode;
        let expected_from = guard.entry.state.clone();
        match self.states.get_mut(&inode) {
            Some(mut dash_entry) => {
                if dash_entry.state != expected_from {
                    let found = dash_entry.state.clone();
                    metrics::WAL_COHERENCE_FAILURES.with_label_values(&["advance"]).inc();
                    error!("WAL Transition Mismatch for Inode {}: Expected {:?}, Found {:?}", inode, expected_from, found);
                    return Err(FoxingError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("WAL Transition Failed: Expected {:?}, Found {:?}", expected_from, found)
                    )));
                }
                let new_entry = self.create_wal_entry(
                    inode,
                    to_state.clone(),
                    guard.entry.seq,
                    guard.entry.daemon_id.clone(),
                    guard.entry.path.clone(),
                    guard.entry.projid,
                );
                *dash_entry = new_entry.clone();
                guard.entry = new_entry;
                debug!("WAL: Inode {} Advanced to {:?}", inode, to_state);
                Ok(())
            }
            None => {
                metrics::WAL_COHERENCE_FAILURES.with_label_values(&["missing_advance"]).inc();
                error!("WAL Transition Mismatch for Inode {}: Entry missing during advance from {:?}", inode, expected_from);
                Err(FoxingError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("WAL Entry Missing for Inode {} during advance", inode)
                )))
            }
        }
    }
    // Updated: Use mark_committed instead of this public commit function for WalGuard.
    // Kept only for non-guarded operations if necessary, but marked for removal.
    pub fn commit(&self, guard: &mut WalGuard) {
        self.mark_committed(guard);
    }
    // Updated: Do not log "already missing" noise.
    pub fn clear_wal_state(&self, inode: u64) {
        if inode == 0 { return; }
        if self.states.remove(&inode).is_some() {
            debug!("WAL: Inode {} state cleared via explicit API.", inode);
        }
    }
    pub fn get_state_for_inode(&self, inode: u64) -> WalState {
        self.states.get(&inode).map(|e| e.state.clone()).unwrap_or(WalState::None)
    }
    pub fn get_entry_for_inode(&self, inode: u64) -> Option<PersistedWalEntry> {
        self.states.get(&inode).map(|r| r.clone())
    }
    fn create_wal_entry(&self, inode: u64, state: WalState, seq: u64, daemon_id: String, path: PathBuf, projid: u32) -> PersistedWalEntry {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(seq);
        hasher.write(daemon_id.as_bytes());
        let crc = hasher.finish();
        PersistedWalEntry {
            inode,
            seq,
            state,
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs(),
            daemon_id,
            path,
            projid,
            crc,
        }
    }
    pub fn remove_stale_wal_entry(&self, inode: u64, expected_state: WalState) -> bool {
        let result = self.states.remove_if(&inode, |_, entry| {
            entry.state == expected_state
        });
        if result.is_some() {
            debug!("WAL: Inode {} state {:?} conditionally removed as stale.", inode, expected_state);
            return true;
        }
        false
    }
}
impl Drop for WalGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            metrics::WAL_COHERENCE_FAILURES.with_label_values(&["rollback_drop"]).inc();
            error!("WAL Rollback: Operation failed or guard dropped prematurely for Inode {}. State was {:?}", self.inode, self.entry.state);
            // Attempt to remove the entry if it's still present in any state
            if self.map.states.remove(&self.inode).is_some() {
                 debug!("WAL: Inode {} manually cleared during rollback.", self.inode);
            }
        }
    }
}
pub fn get_wal_state(wal_map: &Arc<WalStateMap>, inode: u64) -> Option<PersistedWalEntry> {
    wal_map.get_entry_for_inode(inode)
}
pub fn clear_wal_state_for_inode(wal_map: &Arc<WalStateMap>, inode: u64) {
    wal_map.clear_wal_state(inode);
}
