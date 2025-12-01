//! # Ordering Buffer and QoS Logic
//! 
//! This module implements the QoS-aware ordering buffer (OrderBuf). It is
//! responsible for reconstructing the sequential event stream from the 
//! non-guaranteed BPF stream, prioritizing critical metadata events, and 
//! coalescing writes.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;

const MAX_PENDING_BYTES: u64 = 256 * 1024 * 1024; 

// Tuning Constants
const MIN_WINDOW: usize = 32;   
const MAX_WINDOW: usize = 2048; 
const WINDOW_INC: usize = 32;   

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum QoSClass {
    Bulk = 0,     // Lowest Priority (Writes/Data)
    Metadata = 1, // Medium Priority (Chmod/Fsync/Truncate)
    Critical = 2, // Highest Priority (Create/Rename/Unlink - Structural changes)
}

struct PendingEvent {
    event: Arc<Event>,
}

pub struct OrderBuf {
    pending: BTreeMap<u64, PendingEvent>,
    pub next_seq: u64,
    pub max_count: usize,
    pub current_bytes: u64,
    pub qos_window: usize,
}

impl OrderBuf {
    pub fn new() -> Self { 
        Self { 
            pending: BTreeMap::new(), 
            next_seq: 0, 
            max_count: 100_000,
            current_bytes: 0,
            qos_window: 128, 
        } 
    }
    
    fn classify(etype: EventType) -> QoSClass {
        match etype {
            // CRITICAL: Structural changes that determine existence
            EventType::Rename | 
            EventType::Unlink | 
            EventType::Rmdir | 
            EventType::Mkdir | 
            EventType::Link | 
            EventType::Symlink | 
            EventType::Mknod |
            EventType::Create => QoSClass::Critical, // Create is fundamental

            // METADATA: Attributes (Must happen after creation)
            EventType::Chmod | 
            EventType::Chown | 
            EventType::SetXattr | 
            EventType::RemoveXattr | 
            EventType::Utimes |
            EventType::Barrier | 
            EventType::Fsync |
            EventType::Truncate => QoSClass::Metadata, 

            // BULK: Data content (Can be coalesced/purged)
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

    pub fn push_and_check(&mut self, e: Arc<Event>) -> bool {
        if self.next_seq == 0 { 
            self.next_seq = e.seq_num; 
        }
        
        if self.pending.len() >= self.max_count || self.current_bytes >= MAX_PENDING_BYTES {
            metrics::EVENTS_DROPPED.inc();
            return false;
        }

        let size = e.length;
        if self.pending.insert(e.seq_num, PendingEvent { event: e }).is_none() {
            self.current_bytes += size;
        }
        
        true
    }

    pub fn check_timeouts(&mut self) -> bool { false }
    
    pub fn purge_inode(&mut self, inode: u64) {
        let mut to_remove = Vec::new();
        for (seq, entry) in self.pending.iter() {
            if entry.event.inode == inode {
                // Only purge BULK events (Writes).
                // Critical/Metadata events must remain.
                if Self::classify(entry.event.event_type) == QoSClass::Bulk {
                    to_remove.push(*seq);
                }
            }
        }
        for seq in to_remove {
            if let Some(entry) = self.pending.remove(&seq) {
                self.current_bytes -= entry.event.length;
            }
        }
    }

    /// Tiered Priority Pop with Strict Flow Isolation
    pub fn pop_batch(&mut self, coalesce_limit: u64) -> Option<Arc<Event>> {
        if self.pending.is_empty() { return None; }

        let mut best_seq = None;
        let mut best_qos = QoSClass::Bulk;
        let mut blocked_inodes = HashSet::new();
        let mut scan_depth = 0;
        
        // 1. SCAN PHASE
        for (seq, entry) in self.pending.iter().take(self.qos_window) {
            scan_depth += 1;
            let inode = entry.event.inode;
            let current_qos = Self::classify(entry.event.event_type);

            // Flow Isolation & Causal Ordering:
            // If we have already seen ANY event for this inode in this scan,
            // subsequent events for this inode are BLOCKED.
            if blocked_inodes.contains(&inode) {
                continue;
            }

            // Selection Logic:
            if current_qos > best_qos {
                best_seq = Some(*seq);
                best_qos = current_qos;
                
                if current_qos == QoSClass::Critical {
                    break;
                }
            } else if best_seq.is_none() {
                // Default to the first unblocked event we see (Head of Line)
                best_seq = Some(*seq);
            }

            // Mark inode as seen/blocked for future iterations in this window
            blocked_inodes.insert(inode);
        }

        // --- ADAPTIVE TUNING ---
        if best_qos > QoSClass::Bulk {
            if self.qos_window < MAX_WINDOW {
                self.qos_window += WINDOW_INC;
            }
        } else {
            if self.qos_window > MIN_WINDOW {
                self.qos_window = (self.qos_window * 99) / 100;
            }
        }
        
        metrics::QOS_SCAN_DEPTH.observe(scan_depth as f64);
        
        // 2. RETRIEVAL
        let seq_to_process = best_seq.unwrap_or_else(|| *self.pending.keys().next().unwrap());
        
        if seq_to_process != *self.pending.keys().next().unwrap() {
            metrics::QOS_PRIORITY_JUMPS.inc();
        }

        if let Some(entry) = self.pending.remove(&seq_to_process) {
            let mut current_event = entry.event; 
            self.current_bytes -= current_event.length;
            
            if seq_to_process >= self.next_seq {
                self.next_seq = seq_to_process + 1;
            }

            metrics::QOS_EVENT_CLASS.with_label_values(&[Self::class_name(best_qos)]).inc();

            // 3. COALESCING (Bulk Only)
            if best_qos == QoSClass::Bulk {
                let can_coalesce = matches!(current_event.event_type, EventType::Write | EventType::WriteRange);
                
                if can_coalesce {
                    let mut merged_len = current_event.length;
                    let mut merged_count = 0;
                    let mut keys_to_remove = Vec::new();

                    for (seq, pending) in self.pending.range((seq_to_process + 1)..) {
                        let next = &pending.event;
                        
                        if next.inode == current_event.inode && 
                           matches!(next.event_type, EventType::Write | EventType::WriteRange) &&
                           next.offset == current_event.offset + merged_len && 
                           merged_len + next.length <= coalesce_limit 
                        {
                            merged_len += next.length;
                            merged_count += 1;
                            keys_to_remove.push(*seq);
                        } else if next.inode == current_event.inode {
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
                }
            }
            
            return Some(current_event);
        }
        None
    }
    
    pub fn len(&self) -> usize { self.pending.len() }
}
