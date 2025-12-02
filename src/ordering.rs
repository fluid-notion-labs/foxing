use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use std::time::{Instant, Duration};
// Limit for total bytes buffered across all flows.
const MAX_PENDING_BYTES: u64 = 256 * 1024 * 1024;
// Limit for total concurrent flows.
const MAX_FLOWS: usize = 512;
// Target delay for bulk events (CoDel inspired).
const TARGET_DELAY: Duration = Duration::from_millis(100);

// Defines the Quality of Service class for an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum QoSClass {
    // Bulk data operations (writes, write ranges). Lowest priority.
    Bulk = 0,
    // Standard metadata updates (chmod, chown, xattr, truncate). Medium priority.
    Metadata = 1,
    // Critical structural changes (rename, unlink, mkdir, fsync, barrier). Highest priority.
    Critical = 2,
}

// Represents a sequence of events for a single file (inode/QoS combination).
struct FlowQueue {
    queue: BTreeMap<u64, Arc<Event>>,
    last_dequeue: Instant,
    total_bytes: u64,
    last_sojourn_time: Duration,
}

impl FlowQueue {
    /// Creates a new `FlowQueue` starting with the first event.
    fn new(event: Arc<Event>) -> Self {
        let mut queue = BTreeMap::new();
        let size = event.length;
        queue.insert(event.seq_num, event);
        Self {
            queue,
            last_dequeue: Instant::now(),
            total_bytes: size,
            last_sojourn_time: Duration::new(0, 0),
        }
    }
}

/// The main ordering buffer that manages event delivery sequence based on flow and QoS.
pub struct OrderBuf {
    flow_queues: HashMap<FlowKey, FlowQueue>,
    seq_to_flow: BTreeMap<u64, FlowKey>,
    pub next_seq: u64,
    pub max_count: usize,
    pub current_bytes: u64,
    pub delay_exceeded_count: u32,
}

/// Key used to group events into distinct flows.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct FlowKey {
    qos: QoSClass,
    inode: u64,
}

impl OrderBuf {
    /// Creates a new `OrderBuf`.
    pub fn new() -> Self {
        Self {
            flow_queues: HashMap::with_capacity(MAX_FLOWS),
            seq_to_flow: BTreeMap::new(),
            next_seq: 0,
            max_count: 100_000,
            current_bytes: 0,
            delay_exceeded_count: 0,
        }
    }
    
    /// Classifies an event based on its type into a QoS priority.
    fn classify(etype: EventType) -> QoSClass {
        match etype {
            EventType::Rename |
            EventType::Unlink |
            EventType::Rmdir |
            EventType::Mkdir |
            EventType::Link |
            EventType::Symlink |
            EventType::Mknod |
            EventType::Create => QoSClass::Critical,
            EventType::Chmod |
            EventType::Chown |
            EventType::SetXattr |
            EventType::RemoveXattr |
            EventType::Utimes |
            EventType::Barrier |
            EventType::Fsync |
            EventType::Truncate => QoSClass::Metadata,
            _ => QoSClass::Bulk,
        }
    }

    /// Converts QoS class to a string representation for metrics.
    fn class_name(class: QoSClass) -> &'static str {
        match class {
            QoSClass::Critical => "critical",
            QoSClass::Metadata => "metadata",
            QoSClass::Bulk => "bulk",
        }
    }
    
    /// Pushes an event into the ordering buffer. Returns `true` if accepted.
    ///
    /// # Arguments
    /// 
    /// * `e` - The event to push.
    pub fn push_and_check(&mut self, e: Arc<Event>) -> bool {
        if self.next_seq == 0 {
            self.next_seq = e.seq_num;
        }
        if self.current_bytes >= MAX_PENDING_BYTES {
            metrics::EVENTS_DROPPED.inc();
            return false;
        }
        
        // Use inode 0 for barrier/fsync events to ensure they flow quickly if needed,
        // although primary ordering is by seq_num.
        let flow_key = FlowKey { qos: Self::classify(e.event_type), inode: e.inode };
        let size = e.length;
        
        // --- Flow Control / Fair Queuing Logic ---
        if self.flow_queues.contains_key(&flow_key) {
            // Existing flow
            let flow = self.flow_queues.get_mut(&flow_key).unwrap();
            flow.queue.insert(e.seq_num, e.clone());
            flow.total_bytes += size;
        } else {
            if self.flow_queues.len() < MAX_FLOWS {
                self.flow_queues.insert(flow_key.clone(), FlowQueue::new(e.clone()));
            } else {
                metrics::EVENTS_DROPPED.inc();
                return false;
            }
        }

        self.seq_to_flow.insert(e.seq_num, flow_key);
        self.current_bytes += size;
        true
    }

    /// Checks for timeouts (gaps) in the sequence queue (stub).
    pub fn check_timeouts(&mut self) -> bool { false }

    /// Purges all events associated with a specific inode across all flows.
    /// Used when deletion events are received.
    ///
    /// # Arguments
    /// 
    /// * `inode` - The inode to purge.
    pub fn purge_inode_bulk(&mut self, inode: u64) {
        let mut to_remove_seqs = Vec::new();
        let mut flows_to_purge = HashSet::new();
        
        // Identify all sequences and flows associated with the inode
        for (seq, flow_key) in self.seq_to_flow.iter() {
            if flow_key.inode == inode {
                 to_remove_seqs.push(*seq);
                 flows_to_purge.insert(flow_key.clone());
            }
        }

        // Remove events from the flow queues and update byte count
        for flow_key in flows_to_purge.iter() {
            if let Some(flow) = self.flow_queues.get_mut(flow_key) {
                let mut bytes_freed = 0;
                let mut sequences_removed = 0;
                for seq in &to_remove_seqs {
                    if let Some(removed) = flow.queue.remove(seq) {
                        bytes_freed += removed.length;
                        sequences_removed += 1;
                    }
                }
                if sequences_removed > 0 {
                    self.current_bytes -= bytes_freed;
                    flow.total_bytes -= bytes_freed;
                }
                if flow.queue.is_empty() {
                    self.flow_queues.remove(flow_key);
                }
            }
        }

        // Remove entries from the sequence map
        for seq in to_remove_seqs {
            self.seq_to_flow.remove(&seq);
        }
    }
    
    /// Purges only bulk events associated with a specific inode (unused but retained).
    ///
    /// # Arguments
    /// 
    /// * `inode` - The inode to purge.
    pub fn purge_inode(&mut self, inode: u64) {
        let mut to_remove_seqs = Vec::new();
        let mut flows_to_purge = HashSet::new();
        for (seq, flow_key) in self.seq_to_flow.iter() {
            if flow_key.inode == inode {
                if flow_key.qos == QoSClass::Bulk {
                    to_remove_seqs.push(*seq);
                    flows_to_purge.insert(flow_key.clone());
                }
            }
        }
        for flow_key in flows_to_purge {
            if let Some(mut flow) = self.flow_queues.remove(&flow_key) {
                for seq in &to_remove_seqs {
                    if flow.queue.contains_key(seq) {
                        let removed = flow.queue.remove(seq).unwrap();
                        self.current_bytes -= removed.length;
                    }
                }
            }
        }
        for seq in to_remove_seqs {
            self.seq_to_flow.remove(&seq);
        }
    }

    /// Pops the next batch of events, prioritizing by QoS when needed.
    ///
    /// # Arguments
    /// 
    /// * `coalesce_limit` - Maximum bytes to merge into a single batch.
    pub fn pop_batch(&mut self, coalesce_limit: u64) -> Option<(Arc<Event>, bool)> {
        if self.seq_to_flow.is_empty() { return None; }
        
        let earliest_seq = *self.seq_to_flow.keys().next().unwrap();
        let mut next_flow_seq: Option<u64> = None;
        let mut best_qos = QoSClass::Bulk;
        
        // --- AGGRESSIVE QoS DEQUEUE STRATEGY ---
        // If the buffer is over 50% full, aggressively prioritize Metadata/Critical events
        // within a small lookahead window (512 events).
        let is_congested = self.current_bytes > MAX_PENDING_BYTES / 2;
        let lookahead_limit = if is_congested { earliest_seq + 512 } else { earliest_seq + 1 };
        
        for (seq, flow_key) in self.seq_to_flow.range(earliest_seq..) {
            if *seq >= lookahead_limit && !is_congested {
                // In normal mode, only check the absolute earliest event for sequence order.
                break;
            }
            
            if flow_key.qos > best_qos {
                // Found a higher priority event than the current best.
                next_flow_seq = Some(*seq);
                best_qos = flow_key.qos;
                
                if best_qos == QoSClass::Critical {
                    // Critical event found, process it immediately.
                    break;
                }
            } else if next_flow_seq.is_none() {
                // If we haven't selected anything yet, default to the earliest sequence event.
                next_flow_seq = Some(*seq);
            }

            // In congestion mode, we continue checking up to the lookahead limit
            // to pull out high-priority events, even if they are slightly out of sequential order.
            if is_congested && *seq >= lookahead_limit {
                break;
            }
        }
        
        // Default to the strictly earliest event if no priority event was selected
        let seq_to_process = next_flow_seq.unwrap_or(earliest_seq);
        
        let flow_key_to_process = self.seq_to_flow.get(&seq_to_process).unwrap().clone();
        let flow = self.flow_queues.get_mut(&flow_key_to_process).unwrap();
        
        // If we skipped ahead for priority, increment metrics counter.
        if seq_to_process > earliest_seq {
            metrics::QOS_PRIORITY_JUMPS.inc();
        }

        let initial_event = flow.queue.remove(&seq_to_process).unwrap();
        let initial_size = initial_event.length;

        flow.last_sojourn_time = Instant::now().duration_since(initial_event.created_at);
        flow.last_dequeue = Instant::now();
        self.seq_to_flow.remove(&seq_to_process);
        self.current_bytes -= initial_size;
        metrics::QOS_EVENT_CLASS.with_label_values(&[Self::class_name(flow_key_to_process.qos)]).inc();

        let mut current_event = initial_event;
        let mut coalesced = false;

        // Only coalesce Bulk events (Writes/WriteRanges)
        if flow_key_to_process.qos == QoSClass::Bulk {
            let can_coalesce = matches!(current_event.event_type, EventType::Write | EventType::WriteRange);
            if can_coalesce {
                let mut merged_len = current_event.length;
                let mut merged_count = 0;
                let mut keys_to_remove = Vec::new();

                // Look for strictly sequential and adjacent writes in the same flow queue
                for (seq, next) in flow.queue.range(seq_to_process + 1..) {
                    if next.inode == current_event.inode &&
                       matches!(next.event_type, EventType::Write | EventType::WriteRange) &&
                       next.offset == current_event.offset + merged_len &&
                       merged_len + next.length <= coalesce_limit
                    {
                        merged_len += next.length;
                        merged_count += 1;
                        keys_to_remove.push(*seq);
                    } else {
                        break;
                    }
                }

                if merged_count > 0 {
                    metrics::COALESCED_WRITES.inc_by(merged_count as u64);
                    let mut merged_evt = (*current_event).clone();
                    merged_evt.length = merged_len;

                    for k in keys_to_remove {
                        if let Some(removed) = flow.queue.remove(&k) {
                            self.current_bytes -= removed.length;
                            self.seq_to_flow.remove(&k);
                        }
                    }
                    current_event = Arc::new(merged_evt);
                    coalesced = true;
                }
            }
        }

        if flow.queue.is_empty() {
            self.flow_queues.remove(&flow_key_to_process);
        }
        (current_event, coalesced).into()
    }

    /// Checks if the BPF collector needs to be throttled due to exceeding bulk delay targets.
    pub fn should_throttle_bpf(&mut self) -> bool {
        let now = Instant::now();
        let mut delay_exceeded = false;
        
        for (flow_key, flow) in self.flow_queues.iter_mut() {
            if flow.queue.is_empty() { continue; }
            
            // Only consider Bulk events for CoDel-style delay monitoring
            if flow_key.qos == QoSClass::Bulk {
                if let Some((_, oldest_event)) = flow.queue.iter().next() {
                    let sojourn_time = now.duration_since(oldest_event.created_at);
                    if sojourn_time > TARGET_DELAY {
                        delay_exceeded = true;
                        break;
                    }
                }
            }
        }

        if delay_exceeded {
            self.delay_exceeded_count += 1;
        } else {
            self.delay_exceeded_count = 0;
        }
        
        // If delay is exceeded consistently (5 consecutive checks), signal throttle
        self.delay_exceeded_count >= 5
    }

    /// Returns the number of events currently in the buffer.
    pub fn len(&self) -> usize { self.seq_to_flow.len() }
}
