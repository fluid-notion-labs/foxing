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
    needs_commit: bool,
}
impl WalStateMap {
    // --- FIX: Restore missing constructor ---
    pub fn new() -> Self {
        Self { states: DashMap::new() }
    }
    // ----------------------------------------

    pub fn begin_write<'a>(&'a self, inode: u64, path: PathBuf, seq: u64, daemon_id: String, projid: u32) -> Result<WalGuard<'a>> {
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
            needs_commit: true,
        })
    }

    // NEW: Public method to rearm the guard for external commitment (e.g., after successful copy).
    pub fn rearm_guard<'a>(&'a self, entry: PersistedWalEntry) -> Option<WalGuard<'a>> {
        if entry.state == WalState::CommitPending {
             Some(WalGuard {
                 map: self,
                 inode: entry.inode,
                 entry,
                 needs_commit: true, // It needs a commit call to clean up state
             })
        } else {
            None
        }
    }

    pub fn advance(&self, guard: &mut WalGuard, to_state: WalState) -> Result<()> {
        let inode = guard.inode;
        let expected_from = guard.entry.state.clone();
        match self.states.get_mut(&inode) {
            Some(mut dash_entry) => {
                if dash_entry.state != expected_from {
                    let found = dash_entry.state.clone();
                    metrics::WAL_COHERENCE_FAILURES.with_label_values(&["advance"]).inc();
                    // This is a major issue: the state of the entry changed outside of the guard's control.
                    // We must fail hard, but also ensure the entry is cleaned up if it's already committed.
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
    pub fn commit(&self, guard: &mut WalGuard) {
        // Only attempt to remove and set needs_commit=false. Don't worry if it's already gone.
        if let Some((_, entry)) = self.states.remove(&guard.inode) {
            debug!("WAL: Inode {} Commit OK (State was {:?}). Removed from map.", guard.inode, entry.state);
        } else {
            // This is acceptable in a highly concurrent, race-prone system
            debug!("WAL: Inode {} Commit OK: Entry already missing (Acceptable Race).", guard.inode);
        }
        guard.needs_commit = false;
    }
    pub fn clear_wal_state(&self, inode: u64) {
        if self.states.remove(&inode).is_some() {
            debug!("WAL: Inode {} state cleared via explicit API.", inode);
        } else {
            // Change from error/warn to debug, as other threads might clear it in recovery/rename.
            debug!("WAL: Inode {} clear requested, but entry was already missing.", inode);
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
}
impl Drop for WalGuard<'_> {
    fn drop(&mut self) {
        if self.needs_commit {
            // If needs_commit is true, check the current state before rolling back.
            let current_state = self.map.get_state_for_inode(self.inode);
            if current_state == WalState::CommitPending {
                // If it reached CommitPending but the guard is being dropped prematurely (e.g., op returned error
                // before final commit call), we should still attempt a commit rather than a rollback, as the
                // data is likely fine on disk (CommitPending means write/copy succeeded).
                warn!("WAL Auto-Commit: Guard dropped at CommitPending for Inode {}. Committing instead of rolling back.", self.inode);
                self.map.commit(self);
                return;
            }
            // All other states should trigger a genuine rollback / cleanup
            metrics::WAL_COHERENCE_FAILURES.with_label_values(&["rollback_drop"]).inc();
            error!("WAL Rollback: Operation failed or guard dropped prematurely for Inode {}. State was {:?}", self.inode, self.entry.state);
            self.map.states.remove(&self.inode);
        }
    }
}
pub fn get_wal_state(wal_map: &Arc<WalStateMap>, inode: u64) -> Option<PersistedWalEntry> {
    wal_map.get_entry_for_inode(inode)
}
pub fn clear_wal_state_for_inode(wal_map: &Arc<WalStateMap>, inode: u64) {
    wal_map.clear_wal_state(inode);
}
