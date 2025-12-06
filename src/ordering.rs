use std::collections::BTreeMap;
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use tracing::warn;
use std::time::{Instant, Duration};
use parking_lot::Mutex;
#[derive(Debug)]
pub struct ReorderBuffer {
    buffer: BTreeMap<u64, Arc<Event>>,
    pub next_seq: u64,
    max_pending_bytes: u64,
    current_pending_bytes: u64,
    stalled_since: Option<Instant>,
    base_stall_timeout: Duration,
}
impl ReorderBuffer {
    pub fn new(target_latency_ms: u64, max_pending_bytes: u64) -> Self {
        let timeout_ms = (target_latency_ms * 2).max(50).min(500);
        Self {
            buffer: BTreeMap::new(),
            next_seq: 0,
            max_pending_bytes,
            current_pending_bytes: 0,
            stalled_since: None,
            base_stall_timeout: Duration::from_millis(timeout_ms),
        }
    }
    pub fn push(&mut self, event: Arc<Event>) -> bool {
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
        metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).set((self.buffer.len() as i64) as f64);
        true
    }
    pub fn pop(&mut self) -> Option<Arc<Event>> {
        let (&seq, next_evt) = self.buffer.iter().next()?;
        if seq > self.next_seq {
            let pending_count = self.buffer.len();
            let utilization = self.current_pending_bytes as f64 / self.max_pending_bytes as f64;
            let has_structural_event = self.buffer.values().any(|evt|
                evt.event_type.is_structural_metadata() || evt.event_type == EventType::Rename
            );
            let effective_timeout = if has_structural_event {
                self.base_stall_timeout.max(Duration::from_millis(500))
            } else if pending_count > 200 || utilization > 0.8 {
                Duration::from_millis(0)
            } else if pending_count > 50 || utilization > 0.5 {
                Duration::from_millis(10)
            } else {
                self.base_stall_timeout
            };
            if let Some(time) = self.stalled_since {
                if time.elapsed() > effective_timeout {
                    warn!("INGRESS STALL: Jumping gap {} -> {} (pending: {}, timeout: {:?}).",
                          self.next_seq, seq, pending_count, effective_timeout);
                    metrics::SEQUENCE_GAPS.with_label_values(&["ingress"]).inc();
                    let gap_event = Arc::new(Event {
                        event_type: EventType::SequenceGap,
                        dev_id: next_evt.dev_id,
                        inode: 0,
                        parent_inode: 0,
                        new_parent_inode: 0,
                        seq_num: self.next_seq,
                        timestamp_ns: 0,
                        offset: 0,
                        length: 0,
                        name: format!("GAP-{}-{}", self.next_seq, seq),
                        new_name: None,
                        generation: 0,
                        projid: 0,
                        mode: 0,
                        flags: 0,
                        process_name: "GAP".into(),
                        interactive: false,
                        created_at: Instant::now(),
                    });
                    self.next_seq = seq;
                    self.stalled_since = None;
                    return Some(gap_event);
                }
            } else {
                if effective_timeout.is_zero() {
                     metrics::SEQUENCE_GAPS.with_label_values(&["ingress"]).inc();
                     let gap_event = Arc::new(Event {
                        event_type: EventType::SequenceGap,
                        dev_id: next_evt.dev_id,
                        inode: 0, parent_inode: 0, new_parent_inode: 0, seq_num: self.next_seq, timestamp_ns: 0, offset: 0, length: 0,
                        name: format!("GAP-{}-{}", self.next_seq, seq), new_name: None, generation: 0, projid: 0, mode: 0, flags: 0, process_name: "GAP".into(), interactive: false, created_at: Instant::now(),
                    });
                     self.next_seq = seq;
                     return Some(gap_event);
                }
                self.stalled_since = Some(Instant::now());
            }
            return None;
        }
        self.stalled_since = None;
        if seq < self.next_seq {
            let evt = self.buffer.remove(&seq).unwrap();
            self.current_pending_bytes -= 256 + evt.name.len() as u64;
            metrics::LATE_EVENTS.inc();
            return Some(evt);
        }
        let evt = self.buffer.remove(&seq).unwrap();
        self.current_pending_bytes -= 256 + evt.name.len() as u64;
        self.next_seq += 1;
        metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).set((self.buffer.len() as i64) as f64);
        Some(evt)
    }
}
pub struct Coalescer {
    buffer: Vec<Arc<Event>>,
    scan_depth: usize,
}
impl Coalescer {
    pub fn new(scan_depth: usize) -> Self {
        Self { buffer: Vec::with_capacity(128), scan_depth }
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
        // FIX: Structural events (Rename, Mkdir, Rmdir, Unlink) must be serialized immediately
        if head.event_type.requires_global_ordering() {
            return Some(head);
        }
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
                // Optimization: If the next event is for the same inode but not contiguous,
                // we stop checking further events for this specific coalescing chain.
                if evt.inode == inode { break; } 
            }
        }
        if merged_count > 0 {
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
