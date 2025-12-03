use std::collections::BTreeMap;
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use tracing::warn;
use std::time::{Instant, Duration};

pub struct OrderBuf {
    buffer: BTreeMap<u64, Arc<Event>>,
    pub next_seq: u64,
    max_pending_bytes: u64,
    current_pending_bytes: u64,
    scan_depth: usize,
    _target_latency_ms: u64,
    stalled_since: Option<Instant>,
    stall_timeout: Duration,
}

impl OrderBuf {
    pub fn new(target_latency_ms: u64, max_pending_bytes: u64, scan_depth: usize) -> Self {
        // FIX: Ordering Buffer Stall Timeout Insufficient #3
        // Adaptive timeout: 4x expected latency, minimum 250ms
        let timeout_ms = (target_latency_ms * 4).max(250);
        
        Self {
            buffer: BTreeMap::new(),
            next_seq: 0,
            max_pending_bytes,
            current_pending_bytes: 0,
            scan_depth,
            _target_latency_ms: target_latency_ms,
            stalled_since: None,
            stall_timeout: Duration::from_millis(timeout_ms),
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn peek_min_seq(&self) -> Option<u64> {
        self.buffer.keys().next().copied()
    }

    pub fn push_and_check(&mut self, event: Arc<Event>) -> bool {
        let event_size = 256 + event.name.len() as u64;
        
        if self.current_pending_bytes + event_size > self.max_pending_bytes {
            return false;
        }

        if self.next_seq == 0 && self.buffer.is_empty() {
            self.next_seq = event.seq_num;
        }

        if !self.buffer.contains_key(&event.seq_num) {
            self.current_pending_bytes += event_size;
            self.buffer.insert(event.seq_num, event);
        }
        
        metrics::ORDERING_BUF_SIZE.with_label_values(&["default"]).set((self.buffer.len() as i64) as f64);
        true
    }

    pub fn pop_batch(&mut self, coalesce_bytes_limit: u64) -> Option<Arc<Event>> {
        let (&seq, _) = self.buffer.iter().next()?;

        if seq > self.next_seq {
            if let Some(time) = self.stalled_since {
                if time.elapsed() > self.stall_timeout {
                    warn!("ORDERBUF STALL: Jumping gap {} -> {} (after {:?}) to resume processing.", 
                          self.next_seq, seq, self.stall_timeout);
                    self.next_seq = seq;
                    self.stalled_since = None;
                    return self.pop_batch(coalesce_bytes_limit);
                }
            } else {
                self.stalled_since = Some(Instant::now());
            }
            return None;
        } else {
            self.stalled_since = None;
        }

        if seq < self.next_seq {
            let evt = self.buffer.remove(&seq).unwrap();
            self.current_pending_bytes -= 256 + evt.name.len() as u64;
            
            if matches!(evt.event_type, EventType::Rename | EventType::Mkdir) {
                 warn!("ORDERBUF LATE-EXEC: forcing execution of late metadata event seq={} type={:?}", seq, evt.event_type);
                 return Some(evt);
            }
            
            metrics::LATE_EVENTS.inc();
            return self.pop_batch(coalesce_bytes_limit);
        }

        let mut head_event = self.buffer.remove(&seq).unwrap();
        self.current_pending_bytes -= 256 + head_event.name.len() as u64;
        self.next_seq += 1;

        if coalesce_bytes_limit > 0 &&
           (head_event.event_type == EventType::Write || head_event.event_type == EventType::WriteRange)
        {
            head_event = self.try_coalesce(head_event, coalesce_bytes_limit);
        }

        Some(head_event)
    }

    fn try_coalesce(&mut self, head: Arc<Event>, limit: u64) -> Arc<Event> {
        let mut merged_len = head.length;
        let mut lookahead_seq = self.next_seq;
        let mut coalesced_count = 0;
        let inode = head.inode;
        let name = &head.name;
        let mut current_end_offset = head.offset + head.length;

        while merged_len < limit && coalesced_count < self.scan_depth {
            if let Some(next_evt) = self.buffer.get(&lookahead_seq) {
                let is_compatible =
                    (next_evt.event_type == EventType::Write || next_evt.event_type == EventType::WriteRange) &&
                    next_evt.inode == inode &&
                    next_evt.name == *name &&
                    next_evt.offset == current_end_offset;

                if is_compatible {
                    merged_len += next_evt.length;
                    current_end_offset += next_evt.length;
                    
                    let removed = self.buffer.remove(&lookahead_seq).unwrap();
                    self.current_pending_bytes -= 256 + removed.name.len() as u64;
                    
                    self.next_seq += 1;
                    lookahead_seq += 1;
                    coalesced_count += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if coalesced_count > 0 {
            metrics::COALESCED_WRITES.inc_by((coalesced_count as u64) as f64);
            let mut new_event = (*head).clone();
            new_event.length = merged_len;
            return Arc::new(new_event);
        }

        head
    }
}
