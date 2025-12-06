use std::path::PathBuf;
use std::time::{Instant, SystemTime};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use tracing::{debug, error, warn};
use std::io;
use crate::error::{FoxingError, Result};
use std::hash::Hasher;
use std::collections::hash_map::DefaultHasher;
use crate::metrics;

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
    // Note: Path is kept here for crash recovery logs/hydration checks
    pub path: PathBuf,
    pub crc: u64,
}

// The core in-memory, inode-based WAL state store
pub struct WalStateMap {
    // Keyed by inode (u64)
    states: DashMap<u64, PersistedWalEntry>,
}

pub struct WalGuard<'a> {
    map: &'a WalStateMap,
    inode: u64,
    // Local copy of the entry to ensure state consistency during the operation
    entry: PersistedWalEntry,
    needs_commit: bool,
}

impl WalStateMap {
    pub fn new() -> Self {
        Self { states: DashMap::new() }
    }

    /// Attempts to start a write operation and register IntentPending state.
    pub fn begin_write(&self, inode: u64, path: PathBuf, seq: u64, daemon_id: String) -> Result<WalGuard> {
        let current_state = self.get_state_for_inode(inode);

        if current_state != WalState::None {
            // This prevents concurrent writes or overwriting a stale state.
            return Err(FoxingError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("WAL Conflict for Inode {}: Found {:?}, Expected None.", inode, current_state)
            )));
        }

        let entry = self.create_wal_entry(inode, WalState::IntentPending, seq, daemon_id, path);

        match self.states.try_insert(inode, entry.clone()) {
            Ok(_) => {
                debug!("WAL: Inode {} -> IntentPending", inode);
                Ok(WalGuard {
                    map: self,
                    inode,
                    entry,
                    needs_commit: true,
                })
            }
            Err(e) => {
                // Another thread beat us to the insertion.
                Err(FoxingError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("WAL Insertion Failed for Inode {}: {:?}", inode, e.current)
                )))
            }
        }
    }

    /// Attempts to advance the WAL state for an existing guard.
    /// Fails if the current state in the map does not match the guard's expected state.
    pub fn advance(&self, guard: &mut WalGuard, to_state: WalState) -> Result<()> {
        let inode = guard.inode;
        let expected_from = guard.entry.state;

        match self.states.get_mut(&inode) {
            Some(mut dash_entry) => {
                if dash_entry.state != expected_from {
                    let found = dash_entry.state;
                    metrics::WAL_COHERENCE_FAILURES.with_label_values(&["advance"]).inc();
                    error!("WAL Transition Mismatch for Inode {}: Expected {:?}, Found {:?}", inode, expected_from, found);
                    return Err(FoxingError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("WAL Transition Failed: Expected {:?}, Found {:?}", expected_from, found)
                    )));
                }
                
                // Create the new entry and update the DashMap and the guard's local copy
                let new_entry = self.create_wal_entry(inode, to_state, guard.entry.seq, guard.entry.daemon_id.clone(), guard.entry.path.clone());
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

    /// Marks the operation as complete and removes the state from the map.
    pub fn commit(&self, guard: &mut WalGuard) {
        if let Some((_, entry)) = self.states.remove(&guard.inode) {
            debug!("WAL: Inode {} Commit OK (State was {:?}). Removed from map.", guard.inode, entry.state);
        } else {
            warn!("WAL: Inode {} Commit failed: Entry already missing.", guard.inode);
        }
        guard.needs_commit = false;
    }
    
    /// Clears the WAL state without requiring a guard (e.g., after atomic rename/self-heal/unlink).
    pub fn clear_wal_state(&self, inode: u64) {
        if self.states.remove(&inode).is_some() {
            debug!("WAL: Inode {} state cleared via explicit API.", inode);
        }
    }

    pub fn get_state_for_inode(&self, inode: u64) -> WalState {
        self.states.get(&inode).map(|e| e.state).unwrap_or(WalState::None)
    }

    pub fn get_entry_for_inode(&self, inode: u64) -> Option<PersistedWalEntry> {
        self.states.get(&inode).map(|r| r.clone())
    }
    
    fn create_wal_entry(&self, inode: u64, state: WalState, seq: u64, daemon_id: String, path: PathBuf) -> PersistedWalEntry {
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
            crc,
        }
    }
}

/// If the WalGuard is dropped without a successful commit, the incomplete state is cleaned up.
impl Drop for WalGuard<'_> {
    fn drop(&mut self) {
        if self.needs_commit {
            metrics::WAL_COHERENCE_FAILURES.with_label_values(&["rollback_drop"]).inc();
            error!("WAL Rollback: Operation failed or guard dropped prematurely for Inode {}. State was {:?}", self.inode, self.entry.state);
            self.map.states.remove(&self.inode);
        }
    }
}

// Helper function for external use (hydration/recovery)
pub fn get_wal_state(wal_map: &WalStateMap, inode: u64) -> Option<PersistedWalEntry> {
    wal_map.get_entry_for_inode(inode)
}
