use std::collections::BTreeMap;
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use tracing::warn;
use std::time::{Instant, Duration};

/// Strict Reordering Buffer for Global Ingress
/// Ensures monotonic sequence numbers before sharding.
pub struct ReorderBuffer {
    buffer: BTreeMap<u64, Arc<Event>>,
    pub next_seq: u64,
    max_pending_bytes: u64,
    current_pending_bytes: u64,
    stalled_since: Option<Instant>,
    stall_timeout: Duration,
}

impl ReorderBuffer {
    pub fn new(target_latency_ms: u64, max_pending_bytes: u64) -> Self {
        // FIX: Ordering Buffer Stall Timeout Adaptive #3
        // Adaptive timeout: Max(4 * target_latency_ms, 250ms)
        let timeout_ms = (target_latency_ms * 4).max(250);
        
        Self {
            buffer: BTreeMap::new(),
            next_seq: 0,
            max_pending_bytes,
            current_pending_bytes: 0,
            stalled_since: None,
            stall_timeout: Duration::from_millis(timeout_ms),
        }
    }

    pub fn push(&mut self, event: Arc<Event>) -> bool {
        let event_size = 256 + event.name.len() as u64;
        
        // Safety valve: if buffer is huge, we must drop or force-progress
        if self.current_pending_bytes + event_size > self.max_pending_bytes {
            return false;
        }

        // Initialize sequence on first event
        if self.next_seq == 0 && self.buffer.is_empty() {
            self.next_seq = event.seq_num;
        }

        if !self.buffer.contains_key(&event.seq_num) {
            self.current_pending_bytes += event_size;
            self.buffer.insert(event.seq_num, event);
        }
        
        metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).set((self.buffer.len() as i64) as f64);
        true
    }

    pub fn pop(&mut self) -> Option<Arc<Event>> {
        let (&seq, _) = self.buffer.iter().next()?;

        // Case 1: Gap detected
        if seq > self.next_seq {
            if let Some(time) = self.stalled_since {
                if time.elapsed() > self.stall_timeout {
                    warn!("INGRESS STALL: Jumping gap {} -> {} (after {:?}).", 
                          self.next_seq, seq, self.stall_timeout);
                    self.next_seq = seq;
                    self.stalled_since = None;
                    return self.pop();
                }
            } else {
                self.stalled_since = Some(Instant::now());
            }
            return None;
        } 
        
        // Case 2: In-order or Late event
        self.stalled_since = None;

        if seq < self.next_seq {
            // Late event (already jumped over)
            let evt = self.buffer.remove(&seq).unwrap();
            self.current_pending_bytes -= 256 + evt.name.len() as u64;
            metrics::LATE_EVENTS.inc();
            // Just emit it, don't update next_seq
            return Some(evt);
        }

        // Case 3: Correct sequence
        let evt = self.buffer.remove(&seq).unwrap();
        self.current_pending_bytes -= 256 + evt.name.len() as u64;
        self.next_seq += 1;
        
        Some(evt)
    }
}

/// Relaxed Coalescer for Workers
/// Batches compatible writes, ignores sequence gaps.
pub struct Coalescer {
    buffer: Vec<Arc<Event>>,
    scan_depth: usize,
}

impl Coalescer {
    pub fn new(scan_depth: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(128),
            scan_depth,
        }
    }

    pub fn push(&mut self, event: Arc<Event>) {
        self.buffer.push(event);
        metrics::ORDERING_BUF_SIZE.with_label_values(&["worker"]).set((self.buffer.len() as i64) as f64);
    }

    pub fn len(&self) -> usize { self.buffer.len() }
    pub fn is_empty(&self) -> bool { self.buffer.is_empty() }

    pub fn pop_batch(&mut self, coalesce_bytes_limit: u64) -> Option<Arc<Event>> {
        if self.buffer.is_empty() { return None; }
        
        let head = self.buffer.remove(0);
        
        if coalesce_bytes_limit > 0 && 
           (head.event_type == EventType::Write || head.event_type == EventType::WriteRange) 
        {
            return Some(self.try_coalesce(head, coalesce_bytes_limit));
        }
        
        Some(head)
    }

    fn try_coalesce(&mut self, head: Arc<Event>, limit: u64) -> Arc<Event> {
        let mut merged_len = head.length;
        let mut merged_count = 0;
        let mut indices_to_remove = Vec::new();
        let inode = head.inode;
        let name = &head.name;
        let mut current_end_offset = head.offset + head.length;

        // Scan ahead for contiguous writes
        for (i, evt) in self.buffer.iter().enumerate().take(self.scan_depth) {
            if merged_len >= limit { break; }

            let is_compatible = 
                (evt.event_type == EventType::Write || evt.event_type == EventType::WriteRange) &&
                evt.inode == inode &&
                evt.name == *name &&
                evt.offset == current_end_offset;

            if is_compatible {
                merged_len += evt.length;
                current_end_offset += evt.length;
                indices_to_remove.push(i);
                merged_count += 1;
            } else {
                // Break on non-contiguous event for same inode (consistency)
                if evt.inode == inode { break; }
            }
        }

        if merged_count > 0 {
            // Remove coalesced events (reverse order to keep indices valid)
            for &i in indices_to_remove.iter().rev() {
                self.buffer.remove(i);
            }
            
            metrics::COALESCED_WRITES.inc_by((merged_count as u64) as f64);
            let mut new_event = (*head).clone();
            new_event.length = merged_len;
            return Arc::new(new_event);
        }

        head
    }
}
