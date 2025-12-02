use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use std::time::{Instant, Duration};

const MAX_PENDING_BYTES: u64 = 256 * 1024 * 1024;
const MAX_FLOWS: usize = 2048;
const EVENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum QoSClass {
    Bulk = 0,
    Metadata = 1,
    Critical = 2,
}

// ... [FlowQueue struct and impl same as previous] ...
struct FlowQueue {
    queue: BTreeMap<u64, Arc<Event>>,
    last_dequeue: Instant,
    total_bytes: u64,
    oldest_entry: Instant,
}
impl FlowQueue {
    fn new(event: Arc<Event>) -> Self {
        let mut queue = BTreeMap::new();
        let size = event.length;
        let created = event.created_at;
        queue.insert(event.seq_num, event);
        Self {
            queue,
            last_dequeue: Instant::now(),
            total_bytes: size,
            oldest_entry: created,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct FlowKey {
    qos: QoSClass,
    inode: u64,
}

pub struct OrderBuf {
    flow_queues: HashMap<FlowKey, FlowQueue>,
    seq_to_flow: BTreeMap<u64, FlowKey>,
    inode_heads: HashMap<u64, u64>,
    pub next_seq: u64,
    pub max_count: usize,
    pub current_bytes: u64,
    pub delay_exceeded_count: u32,
    target_latency: Duration,
}

impl OrderBuf {
    pub fn new(target_latency_ms: u64) -> Self {
        Self {
            flow_queues: HashMap::with_capacity(MAX_FLOWS),
            seq_to_flow: BTreeMap::new(),
            inode_heads: HashMap::new(),
            next_seq: 0,
            max_count: 100_000,
            current_bytes: 0,
            delay_exceeded_count: 0,
            target_latency: Duration::from_millis(target_latency_ms),
        }
    }

    // ... [classify and class_name same as previous] ...
    fn classify(etype: EventType) -> QoSClass {
        match etype {
            EventType::Rename | EventType::Unlink | EventType::Rmdir | EventType::Mkdir |
            EventType::Link | EventType::Symlink | EventType::Mknod | EventType::Create => QoSClass::Critical,
            EventType::Chmod | EventType::Chown | EventType::SetXattr | EventType::RemoveXattr |
            EventType::Utimes | EventType::Barrier | EventType::Fsync | EventType::Truncate => QoSClass::Metadata,
            _ => QoSClass::Bulk,
        }
    }
    
    fn class_name(class: QoSClass) -> &'static str {
        match class {
            QoSClass::Critical => "critical",
            QoSClass::Metadata => "metadata",
            QoSClass::Bulk => "bulk",
        }
    }

    // ... [push_and_check, check_timeouts, purge_inode_bulk, purge_inode, recalculate_inode_head same as previous] ...
    // Assuming these are standard map operations, omitting full implementation for brevity to focus on pop_batch
    
    pub fn push_and_check(&mut self, e: Arc<Event>) -> bool {
        if self.next_seq == 0 { self.next_seq = e.seq_num; }
        if self.current_bytes >= MAX_PENDING_BYTES { metrics::EVENTS_DROPPED.inc(); return false; }
        
        let flow_key = FlowKey { qos: Self::classify(e.event_type), inode: e.inode };
        let size = e.length;
        let seq = e.seq_num;
        let inode = e.inode;
        
        self.inode_heads.entry(inode).and_modify(|head| *head = (*head).min(seq)).or_insert(seq);
        
        if let Some(flow) = self.flow_queues.get_mut(&flow_key) {
            flow.queue.insert(seq, e.clone());
            flow.total_bytes += size;
            if e.created_at < flow.oldest_entry { flow.oldest_entry = e.created_at; }
        } else {
            if self.flow_queues.len() < MAX_FLOWS {
                self.flow_queues.insert(flow_key.clone(), FlowQueue::new(e.clone()));
            } else {
                metrics::EVENTS_DROPPED.inc();
                return false;
            }
        }
        self.seq_to_flow.insert(seq, flow_key);
        self.current_bytes += size;
        true
    }

    // ... [Standard housekeeping methods] ...
    pub fn check_timeouts(&mut self) -> bool { false } // Dummy for brevity
    pub fn purge_inode_bulk(&mut self, _inode: u64) {} // Dummy
    pub fn purge_inode(&mut self, _inode: u64) {} // Dummy
    fn recalculate_inode_head(&mut self, _inode: u64) {} // Dummy

    pub fn pop_batch(&mut self, coalesce_limit: u64) -> Option<(Arc<Event>, bool)> {
        if self.seq_to_flow.is_empty() { return None; }
        
        let earliest_global_seq = *self.seq_to_flow.keys().next().unwrap();
        let earliest_flow_key = self.seq_to_flow.get(&earliest_global_seq).unwrap().clone();
        
        let mut seq_to_process = earliest_global_seq;
        let mut flow_key_to_process = earliest_flow_key.clone();

        // CORE LOGIC: QoS Jump
        // If the absolute head of the queue is Bulk, we look for something more important.
        if earliest_flow_key.qos == QoSClass::Bulk {
            let mut best_qos = QoSClass::Bulk;
            let mut best_seq = earliest_global_seq;

            // Scan the global sequence map (ordered by seq).
            // We stop if we find a Critical event (Highest priority).
            // Limit scan depth to prevent CPU hogging on massive queues.
            let scan_depth = 1000;
            let mut scanned = 0;

            for (seq, flow_key) in self.seq_to_flow.iter() {
                scanned += 1;
                if scanned > scan_depth { break; }

                if flow_key.qos > best_qos {
                    // Verify this event is the head of its own flow (ordering constraint)
                    // We can only process an event if it's the next one expected for that inode.
                    if let Some(head_seq) = self.inode_heads.get(&flow_key.inode) {
                        if *head_seq == *seq {
                            best_qos = flow_key.qos;
                            best_seq = *seq;
                            if best_qos == QoSClass::Critical { break; }
                        }
                    }
                }
            }

            if best_qos > QoSClass::Bulk {
                seq_to_process = best_seq;
                flow_key_to_process = self.seq_to_flow.get(&seq_to_process).unwrap().clone();
            }
        }

        if seq_to_process > earliest_global_seq {
            metrics::QOS_PRIORITY_JUMPS.inc();
        }
        
        let flow = self.flow_queues.get_mut(&flow_key_to_process).unwrap();
        let initial_event = flow.queue.remove(&seq_to_process).unwrap();
        let initial_size = initial_event.length;
        
        flow.last_dequeue = Instant::now();
        flow.total_bytes -= initial_size;
        self.seq_to_flow.remove(&seq_to_process);
        self.current_bytes = self.current_bytes.saturating_sub(initial_size);
        
        metrics::QOS_EVENT_CLASS.with_label_values(&[Self::class_name(flow_key_to_process.qos)]).inc();
        
        let mut current_event = initial_event;
        let mut coalesced = false;

        // ... [Coalescing logic same as before] ...
        if flow_key_to_process.qos == QoSClass::Bulk {
             // Standard coalescing logic
        }
        
        if flow.queue.is_empty() {
            self.flow_queues.remove(&flow_key_to_process);
        } else {
            if let Some((_, e)) = flow.queue.iter().next() {
                flow.oldest_entry = e.created_at;
            }
        }
        // Recalculate head for this inode since we popped one
        // self.recalculate_inode_head(current_event.inode); 
        
        Some((current_event, coalesced))
    }

    pub fn should_throttle_bpf(&mut self) -> bool { false } // Dummy
    pub fn len(&self) -> usize { self.seq_to_flow.len() }
}
