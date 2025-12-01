use std::collections::BTreeMap;
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use tracing::warn;
use std::time::{Instant, Duration};

// TRIAGE 2: Reduce from 500ms to 100ms
const MAX_HOL_DELAY: Duration = Duration::from_millis(500);
const MAX_PENDING_BYTES: u64 = 256 * 1024 * 1024; // 256 MB

struct PendingEvent {
    event: Arc<Event>,
    arrival: Instant,
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
    
    /// Returns true if accepted, false if dropped/full.
    pub fn push_and_check(&mut self, e: Arc<Event>) -> bool {
        if self.next_seq == 0 { 
            self.next_seq = e.seq_num; 
        }
        
        if self.pending.len() >= self.max_count || self.current_bytes >= MAX_PENDING_BYTES {
            metrics::EVENTS_DROPPED.inc();
            let _ = self.check_timeouts();
            if self.pending.len() >= self.max_count {
                return false;
            }
        }

        if e.seq_num >= self.next_seq {
            let size = e.length;
            if self.pending.insert(e.seq_num, PendingEvent { event: e, arrival: Instant::now() }).is_none() {
                self.current_bytes += size;
            }
        } else {
            metrics::LATE_EVENTS.inc();
        }
        
        true
    }

    /// Checks for Head-of-Line blocking.
    /// Returns `true` if a gap was detected and skipped (indicating potential data loss/need for sync).
    pub fn check_timeouts(&mut self) -> bool {
        if let Some((&first_seq, first_entry)) = self.pending.iter().next() {
            if first_seq > self.next_seq {
                let gap = first_seq - self.next_seq;
                
                // FIX 4: Aggressive Gap Reset (if gap > 1000 or delayed > 500ms)
                if gap > 1000 || first_entry.arrival.elapsed() > MAX_HOL_DELAY {
                    warn!("HoL Blocking detected: Waiting for seq {} but have {} (Gap: {}). Skipping gap.", 
                          self.next_seq, first_seq, gap);
                    metrics::SEQUENCE_GAPS.with_label_values(&[&first_entry.event.dev_id.to_string()]).inc();
                    self.next_seq = first_seq;
                    return true;
                }
            }
        }
        false
    }
    
    /// Remove all pending events for a specific inode.
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
                // If we removed the HEAD of the line, advance next_seq to unblock
                if seq == self.next_seq {
                    self.next_seq += 1;
                }
            }
        }
    }

    pub fn pop_batch(&mut self, coalesce_limit: u64) -> Option<Arc<Event>> {
        if let Some(entry) = self.pending.remove(&self.next_seq) {
            let mut current_event = entry.event; 
            self.current_bytes -= current_event.length;
            
            let can_coalesce = matches!(current_event.event_type, EventType::Write | EventType::WriteRange);
            
            if can_coalesce {
                let mut merged_len = current_event.length;
                let mut merged_count = 1;
                
                while let Some(next_entry) = self.pending.get(&(self.next_seq + merged_count)) {
                    let next = &next_entry.event;
                    
                    if next.inode == current_event.inode && 
                       matches!(next.event_type, EventType::Write | EventType::WriteRange) &&
                       next.offset == current_event.offset + merged_len && 
                       merged_len + next.length <= coalesce_limit 
                    {
                        merged_len += next.length;
                        merged_count += 1;
                    } else {
                        break;
                    }
                }
                
                if merged_count > 1 {
                    metrics::COALESCED_WRITES.inc_by(merged_count - 1);
                    
                    let mut merged_evt = (*current_event).clone();
                    merged_evt.length = merged_len;
                    
                    for i in 1..merged_count {
                        if let Some(removed) = self.pending.remove(&(self.next_seq + i)) {
                            self.current_bytes -= removed.event.length;
                        }
                    }
                    
                    current_event = Arc::new(merged_evt);
                }
                
                self.next_seq += merged_count;
                return Some(current_event);
            }
            
            self.next_seq += 1;
            return Some(current_event);
        }
        None
    }
    
    pub fn len(&self) -> usize { self.pending.len() }
}
