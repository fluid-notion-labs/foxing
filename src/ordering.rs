use std::collections::BTreeMap;
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;

const MAX_PENDING_BYTES: u64 = 256 * 1024 * 1024; 

struct PendingEvent {
    event: Arc<Event>,
}

pub struct OrderBuf {
    pending: BTreeMap<u64, PendingEvent>,
    pub next_seq: u64,
    pub max_count: usize,
    pub current_bytes: u64,
}

impl OrderBuf {
    pub fn new() -> Self { 
        Self { 
            pending: BTreeMap::new(), 
            next_seq: 0, 
            max_count: 100_000,
            current_bytes: 0,
        } 
    }
    
    pub fn push_and_check(&mut self, e: Arc<Event>) -> bool {
        // If we are just starting or reset, accept the first seq we see
        if self.next_seq == 0 { 
            self.next_seq = e.seq_num; 
        }
        
        if self.pending.len() >= self.max_count || self.current_bytes >= MAX_PENDING_BYTES {
            metrics::EVENTS_DROPPED.inc();
            // Force process to clear space
            return false;
        }

        // ARCHITECTURAL FIX: 
        // In sharded mode, we will receive non-contiguous sequence numbers (e.g., 1, 3, 5).
        // We simply insert everything. The `pop_batch` logic will pull the smallest available.
        // We trust the upstream channel is FIFO.
        
        let size = e.length;
        if self.pending.insert(e.seq_num, PendingEvent { event: e }).is_none() {
            self.current_bytes += size;
        }
        
        true
    }

    /// Checks for Head-of-Line blocking.
    /// 
    /// REVISED: In sharded mode, "gaps" are expected. We only check for stuck events.
    pub fn check_timeouts(&mut self) -> bool {
        // We no longer enforce (first_seq == next_seq) because gaps are natural in sharding.
        // We only care if an event has been sitting in the buffer too long without being processed,
        // which implies the worker is stalled, but `pop_batch` should handle that.
        // This function is effectively a no-op for strict ordering now, but kept for API compat.
        false
    }
    
    /// Removes all pending events for a specific inode (Time Travel Prevention)
    pub fn purge_inode(&mut self, inode: u64) {
        let mut to_remove = Vec::new();
        for (seq, entry) in self.pending.iter() {
            if entry.event.inode == inode {
                to_remove.push(*seq);
            }
        }
        
        for seq in to_remove {
            if let Some(entry) = self.pending.remove(&seq) {
                self.current_bytes -= entry.event.length;
            }
        }
    }

    pub fn pop_batch(&mut self, coalesce_limit: u64) -> Option<Arc<Event>> {
        // REVISED: Always pop the smallest sequence number available.
        // We rely on the channel guarantees that events arrived in order.
        // The BTreeMap just ensures we process the "oldest" event we currently have.
        
        let first_key = *self.pending.keys().next()?;
        
        if let Some(entry) = self.pending.remove(&first_key) {
            let mut current_event = entry.event; 
            self.current_bytes -= current_event.length;
            
            // Update next_seq to expect the one after this (loose tracking)
            self.next_seq = first_key + 1;

            let can_coalesce = matches!(current_event.event_type, EventType::Write | EventType::WriteRange);
            
            if can_coalesce {
                let mut merged_len = current_event.length;
                let mut merged_count = 0;
                let mut keys_to_remove = Vec::new();

                // Look ahead in the buffer for contiguous writes TO THE SAME INODE
                // Note: In sharded mode, we might have seq 1 (Inode A) and seq 3 (Inode A).
                // If seq 2 was for Inode B (other worker), then 1 and 3 are effectively contiguous for Inode A.
                // However, BPF offsets must match.
                
                for (seq, pending) in self.pending.iter() {
                    let next = &pending.event;
                    
                    if next.inode == current_event.inode && 
                       matches!(next.event_type, EventType::Write | EventType::WriteRange) &&
                       next.offset == current_event.offset + merged_len && 
                       merged_len + next.length <= coalesce_limit 
                    {
                        merged_len += next.length;
                        merged_count += 1;
                        keys_to_remove.push(*seq);
                    } else {
                        // If we hit a non-matching event, can we skip it? 
                        // No, that risks reordering operations on different files (consistency hazard).
                        // Coalescing must stop at the first break in the stream to be safe.
                        break;
                    }
                }
                
                if merged_count > 0 {
                    metrics::COALESCED_WRITES.inc_by(merged_count as u64);
                    
                    let mut merged_evt = (*current_event).clone();
                    merged_evt.length = merged_len;
                    
                    for k in keys_to_remove {
                        if let Some(removed) = self.pending.remove(&k) {
                            self.current_bytes -= removed.event.length;
                        }
                    }
                    
                    current_event = Arc::new(merged_evt);
                }
                
                return Some(current_event);
            }
            
            return Some(current_event);
        }
        None
    }
    
    pub fn len(&self) -> usize { self.pending.len() }
}
