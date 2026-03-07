// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/consistency/serialization.rs — Binary serialization for WAL entries

//! Serialization helpers for WAL entry persistence.

use std::collections::HashMap;
use std::sync::Arc;
use crate::consistency::sequencer::{GlobalSequencer, SequenceBarrier};
use std::path::PathBuf;
use tracing::{debug, warn};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Write,
    Rename,
}

pub struct OpGuard {
    engine: Arc<SerializationEngine>,
    inode: u64,
    ticket: u64,
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        self.engine.complete_op(self.inode, self.ticket);
    }
}

pub struct PathGuard {
    engine: Arc<SerializationEngine>,
    path: PathBuf,
}

impl Drop for PathGuard {
    fn drop(&mut self) {
        self.engine.release_path(&self.path);
    }
}

struct InodeState {
    sequencer: GlobalSequencer,
    barrier: SequenceBarrier,
}

impl InodeState {
    fn new() -> Self {
        Self {
            sequencer: GlobalSequencer::new(0),
            barrier: SequenceBarrier::new(0),
        }
    }
}

pub struct SerializationEngine {
    state: std::sync::Mutex<HashMap<u64, Arc<InodeState>>>,
    path_locks: std::sync::Mutex<HashMap<PathBuf, Arc<Notify>>>,
}

impl SerializationEngine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(HashMap::new()),
            path_locks: std::sync::Mutex::new(HashMap::new()),
        })
    }

    pub fn is_active(&self, inode: u64) -> bool {
        let state = self.state.lock().unwrap();
        state.contains_key(&inode)
    }

    /// Acquires a ticket-based barrier for an inode operation.
    /// This ensures strict serialization of operations on the same inode
    /// based on the order they arrive, preventing deadlocks.
    pub async fn acquire_barrier(self: &Arc<Self>, inode: u64, _kind: OpKind) -> Result<OpGuard, std::io::Error> {
        // 1. Acquire Ticket
        let (inode_state, ticket) = {
            let mut map = self.state.lock().unwrap();
            let state = map.entry(inode).or_insert_with(|| Arc::new(InodeState::new())).clone();
            
            // Get the next ticket. 
            // Note: In this architecture, we treat Writes and Renames with the same 
            // strict ordering requirement for simplicity and deadlock prevention.
            // Writer A (100) blocks Writer B (101).
            let ticket = state.sequencer.next();
            
            (state, ticket)
        };

        // 2. Wait for previous operation to complete
        // Since tickets are 1-based, ticket 1 waits for 0 (completed by default).
        // Ticket 101 waits for 100.
        let dependency = ticket - 1;
        
        if dependency > 0 {
            // Wait for the barrier to reach the dependency state.
            // This is deadlock-free because the dependency is strictly lower than our ticket.
            inode_state.barrier.wait_for(dependency).await;
        }

        Ok(OpGuard {
            engine: self.clone(),
            inode,
            ticket,
        })
    }

    pub async fn acquire_path_barrier(self: &Arc<Self>, path: &PathBuf) -> Result<PathGuard, std::io::Error> {
        loop {
            let wait_notify = {
                let mut locks = self.path_locks.lock().unwrap();
                if let Some(notify) = locks.get(path) {
                    Some(notify.clone())
                } else {
                    locks.insert(path.clone(), Arc::new(Notify::new()));
                    None
                }
            };

            if let Some(notify) = wait_notify {
                notify.notified().await;
            } else {
                return Ok(PathGuard {
                    engine: self.clone(),
                    path: path.clone(),
                });
            }
        }
    }

    fn release_path(&self, path: &PathBuf) {
        let mut locks = self.path_locks.lock().unwrap();
        if let Some(notify) = locks.remove(path) {
            notify.notify_waiters();
        }
    }

    // Legacy sequence checking helpers - now no-ops or simple pass-throughs
    // as the ticket barrier handles ordering implicitly.
    pub fn check_sequence(&self, _inode: u64, _seq: u64) -> bool {
        true 
    }

    pub fn update_sequence(&self, _inode: u64, _seq: u64) {
        // Managed internally by sequencer
    }

    fn complete_op(&self, inode: u64, ticket: u64) {
        // We need to access the barrier to mark completion.
        let state_opt = {
            let map = self.state.lock().unwrap();
            map.get(&inode).cloned()
        };

        if let Some(state) = state_opt {
            debug!("Serialization: Completed ticket {} for inode {}", ticket, inode);
            state.barrier.complete(ticket);
            
            // Note: We don't remove the InodeState from the map aggressively here.
            // In a long-running system, we might want a cleanup task to remove 
            // InodeStates where barrier.current() == sequencer.current() and no activity.
            // For now, we rely on LRU or system restart to clean up map entries if they grow too large,
            // or the memory overhead is considered acceptable for active inodes.
        } else {
            warn!("Serialization: Attempted to complete op for unknown inode {}", inode);
        }
    }
}
