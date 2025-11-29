// File: foxing/src/ordering.rs | Index: 9 of 24 | Function: Sequence buffering.
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use tracing::warn;
use std::sync::atomic::Ordering;

const MAX_PENDING_EVENTS: usize = 100000;

pub struct OrderBuf {
    pub pending: BTreeMap<u64, Arc<Event>>,
    pub next: u64,
    pub max_size: usize,
}

impl OrderBuf {
    pub fn new() -> Self { 
        Self { 
            pending: BTreeMap::new(), 
            next: 0, 
            max_size: MAX_PENDING_EVENTS 
        } 
    }
    
    pub fn push_and_check(&mut self, e: Arc<Event>) -> bool {
        if self.next == 0 { 
            self.next = e.seq_num; 
        }
        
        if self.pending.len() >= self.max_size {
            if let Some((&oldest_seq, _)) = self.pending.iter().next() {
                if oldest_seq > self.next {
                    warn!("Ordering buffer full and stuck at seq {}. Advancing next to {} to prevent starvation.", self.next, oldest_seq);
                    self.next = oldest_seq;
                    self.pop_batch(std::u64::MAX); 
                }
            } else {
                 metrics::EVENTS_DROPPED.inc();
                 return false;
            }
        }
        
        if self.pending.len() >= self.max_size {
            metrics::EVENTS_DROPPED.inc();
            return false;
        }

        self.pending.insert(e.seq_num, e);
        true
    }

    pub fn pop_batch(&mut self, coalesce_limit: u64) -> Option<Arc<Event>> {
        if let Some(e) = self.pending.remove(&self.next) {
            let current_event = e;
            let mut merged_len = current_event.length;
            let mut merged_count = 1;
            
            while let Some(next) = self.pending.get(&(self.next + merged_count as u64)) {
                    if next.inode == current_event.inode && 
                    (next.event_type == EventType::Write || next.event_type == EventType::WriteRange) && 
                    (current_event.event_type == EventType::Write || current_event.event_type == EventType::WriteRange) &&
                    next.offset == current_event.offset + merged_len &&
                    merged_len + next.length <= coalesce_limit { 
                        merged_len += next.length;
                        merged_count += 1;
                    } else {
                        break;
                    }
            }
            
            if merged_count > 1 {
                metrics::COALESCED_WRITES.inc_by(merged_count as u64 - 1);
                for i in 1..merged_count {
                    metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::Relaxed); 
                    self.pending.remove(&(self.next + i as u64));
                }
            }
            
            self.next += merged_count as u64;
            return Some(current_event);
        }
        None
    }
}
