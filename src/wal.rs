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
use std::sync::Arc; // Needed for WalStateMap Arc in mirror.rs compile error

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
    pub projid: u32, // <-- CONFIRMED: This is needed
    pub crc: u64,
}

// The core in-memory, inode-based WAL state store
#[derive(Debug)] // <-- CONFIRMED: This is needed
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
    pub fn begin_write(&self, inode: u64, path: PathBuf, seq: u64, daemon_id: String, projid: u32) -> Result<WalGuard> {
        let current_state = self.get_state_for_inode(inode);

        if current_state != WalState::None {
            // This prevents concurrent writes or overwriting a stale state.
            return Err(FoxingError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("WAL Conflict for Inode {}: Found {:?}, Expected None.", inode, current_state)
            )));
        }

        let entry = self.create_wal_entry(inode, WalState::IntentPending, seq, daemon_id, path, projid);

        // FIX: DashMap 5.x uses .insert(), not .try_insert()
        if self.states.contains_key(&inode) {
            // Should not happen due to the check above, but DashMap insert returns the old value
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

    /// Attempts to advance the WAL state for an existing guard.
    /// Fails if the current state in the map does not match the guard's expected state.
    pub fn advance(&self, guard: &mut WalGuard, to_state: WalState) -> Result<()> {
        let inode = guard.inode;
        // FIX: Clone the state to avoid move
        let expected_from = guard.entry.state.clone(); 

        match self.states.get_mut(&inode) {
            Some(mut dash_entry) => {
                // FIX: Clone the state to avoid move
                if dash_entry.state != expected_from {
                    let found = dash_entry.state.clone();
                    metrics::WAL_COHERENCE_FAILURES.with_label_values(&["advance"]).inc();
                    error!("WAL Transition Mismatch for Inode {}: Expected {:?}, Found {:?}", inode, expected_from, found);
                    return Err(FoxingError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("WAL Transition Failed: Expected {:?}, Found {:?}", expected_from, found)
                    )));
                }
                
                // FIX: Clone the state to avoid move
                let new_entry = self.create_wal_entry(
                    inode, 
                    to_state.clone(), 
                    guard.entry.seq, 
                    guard.entry.daemon_id.clone(), 
                    guard.entry.path.clone(),
                    guard.entry.projid, // <-- FIX: Pass existing projid
                );
                *dash_entry = new_entry.clone();
                guard.entry = new_entry;
                
                debug!("WAL: Inode {} Advanced to {:?}", inode, to_state); // FIX: Borrow here
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
        // FIX: Clone the state to avoid move
        self.states.get(&inode).map(|e| e.state.clone()).unwrap_or(WalState::None)
    }

    pub fn get_entry_for_inode(&self, inode: u64) -> Option<PersistedWalEntry> {
        self.states.get(&inode).map(|r| r.clone())
    }
    
    fn create_wal_entry(&self, inode: u64, state: WalState, seq: u64, daemon_id: String, path: PathBuf, projid: u32) -> PersistedWalEntry { // <-- FIX: Added projid
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
            projid, // <-- FIX: Set projid
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
pub fn get_wal_state(wal_map: &Arc<WalStateMap>, inode: u64) -> Option<PersistedWalEntry> { // FIX: Changed signature to take Arc for hydration.rs
    wal_map.get_entry_for_inode(inode)
}

// Helper function for external use (hydration/worker self-heal/unlink)
pub fn clear_wal_state_for_inode(wal_map: &Arc<WalStateMap>, inode: u64) {
    wal_map.clear_wal_state(inode);
}
