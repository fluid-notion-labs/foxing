// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/ordering.rs — ReorderBuffer, Coalescer — event ordering and write merging

//! Event ordering and write coalescing for the replication pipeline.
//! ReorderBuffer delivers events in sequence; Coalescer merges adjacent writes.

use std::collections::{BTreeMap, VecDeque, HashSet, HashMap};
use std::sync::Arc;
use crate::event::{Event, EventType};
use crate::metrics;
use tracing::{warn, debug, info};
use std::time::{Instant, Duration};
use crate::columnar::EventBatch;
use fxcp_core::constants;

#[derive(Debug)]
pub struct ReorderBuffer {
    buffer: BTreeMap<u64, Arc<Event>>,
    structural_queue: VecDeque<Arc<Event>>,
    pub next_seq: u64,
    max_pending_bytes: u64,
    max_pending_count: usize,
    current_pending_bytes: u64,
    stalled_since: Option<Instant>,
    base_stall_timeout: Duration,
    inflight_renames: HashMap<u64, u64>,
}

impl ReorderBuffer {
    pub fn new(target_latency_ms: u64, max_pending_bytes: u64) -> Self {
        let timeout_ms = (target_latency_ms * 2)
            .max(constants::REORDER_SOFT_TIMEOUT_MIN_MS)
            .min(constants::REORDER_SOFT_TIMEOUT_MAX_MS);
        Self {
            buffer: BTreeMap::new(),
            structural_queue: VecDeque::new(),
            next_seq: 0,
            max_pending_bytes,
            max_pending_count: constants::REORDER_BUFFER_CAPACITY,
            current_pending_bytes: 0,
            stalled_since: None,
            base_stall_timeout: Duration::from_millis(timeout_ms),
            inflight_renames: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len() + self.structural_queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty() && self.structural_queue.is_empty()
    }

    pub fn contains_inode(&self, inode: u64) -> bool {
        if self.structural_queue.iter().any(|e| e.inode == inode) {
            return true;
        }
        self.buffer.values().any(|e| e.inode == inode)
    }

    pub fn acknowledge(&mut self, seq: u64, inode: u64) {
        if let Some(s) = self.inflight_renames.get(&inode) {
            if *s == seq {
                debug!("ReorderBuffer: Acknowledged Rename for inode {} (Seq {})", inode, seq);
                self.inflight_renames.remove(&inode);
            }
        }
    }

    pub fn push(&mut self, event: Arc<Event>) -> bool {
        let overhead = 64;
        let event_size = 256 + event.name.len() as u64 + overhead;
        let mut rejected = false;

        if self.next_seq == 0 && self.buffer.is_empty() && self.structural_queue.is_empty() {
            self.next_seq = event.seq_num;
            info!("ReorderBuffer: Initialized sequence at {}", self.next_seq);
        }

        let next_seq = self.next_seq;
        let is_wrapped = next_seq > u64::MAX - 100000 && event.seq_num < 100000;
        
        if is_wrapped {
            debug!("Sequence wraparound detected. Resetting buffer state.");
            self.buffer.clear();
            self.structural_queue.clear();
            self.next_seq = event.seq_num;
            self.current_pending_bytes = 0;
        }

        if self.current_pending_bytes + event_size > self.max_pending_bytes {
            rejected = true;
        }
        
        if self.len() >= self.max_pending_count {
            if event.seq_num > self.next_seq {
                rejected = true;
            }
        }

        if rejected {
            if self.next_seq != 0 && event.seq_num > self.next_seq {
                warn!("INGRESS OVERFLOW: Injecting gap event due to rejection. Seq {} dropped. Buffer Usage: {}/{} bytes.",
                      event.seq_num, self.current_pending_bytes, self.max_pending_bytes);
                self.inject_gap(event.dev_id, event.seq_num);
            }
            return false;
        }

        if event.event_type.requires_global_ordering() {
            self.structural_queue.push_back(event);
            metrics::ORDERING_BUF_SIZE.with_label_values(&["structural"]).inc();
        } else {
            if !self.buffer.contains_key(&event.seq_num) {
                self.current_pending_bytes += event_size;
                self.buffer.insert(event.seq_num, event);
                metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).set((self.buffer.len() as i64) as f64);
            }
        }
        true
    }

    fn inject_gap(&mut self, dev_id: u32, next_valid_seq: u64) {
        let current_seq = self.next_seq;
        metrics::SEQUENCE_GAPS.with_label_values(&["ingress"]).inc();
        let gap_event = Arc::new(Event {
            event_type: EventType::SequenceGap,
            dev_id,
            inode: 0, parent_inode: 0, new_parent_inode: 0,
            seq_num: current_seq,
            timestamp_ns: 0, offset: 0, length: 0,
            name: format!("GAP-{}-{}", current_seq, next_valid_seq),
            new_name: None, generation: 0, projid: 0,
            uid: 0, gid: 0,
            mode: 0, flags: 0, nlink: 0,
            process_name: "OVERFLOW_GAP".into(), interactive: false, created_at: Instant::now(),
        });
        self.buffer.insert(current_seq, gap_event);
    }

    pub fn pop(&mut self) -> Option<Arc<Event>> {
        let struct_head = self.structural_queue.front();
        let buffer_head = self.buffer.first_key_value();
        
        let (candidate_seq, is_structural, dev_id_hint) = match (struct_head, buffer_head) {
            (Some(s), Some((b_seq, b_evt))) => {
                if s.seq_num <= *b_seq {
                    (s.seq_num, true, s.dev_id)
                } else {
                    (*b_seq, false, b_evt.dev_id)
                }
            },
            (Some(s), None) => (s.seq_num, true, s.dev_id),
            (None, Some((b_seq, b_evt))) => (*b_seq, false, b_evt.dev_id),
            (None, None) => return None,
        };

        if candidate_seq > self.next_seq {
            return self.handle_gap(candidate_seq, dev_id_hint);
        }

        let evt = if is_structural {
            self.structural_queue.pop_front().unwrap()
        } else {
            self.buffer.remove(&candidate_seq).unwrap()
        };

        let _is_path_dependent = matches!(evt.event_type,
            EventType::Write | EventType::WriteRange | 
            EventType::Create | EventType::Unlink | EventType::Mkdir | 
            EventType::Mknod | EventType::Link | EventType::Symlink |
            EventType::Rename
        );

        // NOTE: inflight_renames tracking disabled — acknowledge() was never called
        // by any consumer, causing permanent event blocking after any rename.
        // The WAL barrier system in worker.rs handles rename ordering instead.

        if is_structural {
            metrics::ORDERING_BUF_SIZE.with_label_values(&["structural"]).dec();
        } else {
            let overhead = 64;
            self.current_pending_bytes -= 256 + evt.name.len() as u64 + overhead;
            metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).set((self.buffer.len() as i64) as f64);
        }

        self.stalled_since = None;
        if evt.seq_num == self.next_seq {
            self.next_seq += 1;
        } else {
            metrics::LATE_EVENTS.inc();
        }
        
        Some(evt)
    }

    fn handle_gap(&mut self, seq: u64, dev_id: u32) -> Option<Arc<Event>> {
        let pending_count = self.len();
        let utilization = self.current_pending_bytes as f64 / self.max_pending_bytes as f64;
        
        let effective_timeout = if pending_count > 1000 || utilization > constants::REORDER_PRESSURE_PANIC_PCT {
            Duration::from_millis(0)
        } else if pending_count > 100 || utilization > constants::REORDER_PRESSURE_WARN_PCT {
            Duration::from_millis(10)
        } else {
            self.base_stall_timeout
        };

        if let Some(time) = self.stalled_since {
            if time.elapsed() > effective_timeout {
                // FIXED: Correctly salvage gap events without infinite looping or loss
                let missed_events: Vec<_> = self.buffer.range(self.next_seq..seq)
                    .map(|(_, evt)| evt.clone())
                    .collect();

                if !missed_events.is_empty() {
                    warn!("Gap recovery: Salvaging {} events (Range {}..{})", missed_events.len(), self.next_seq, seq);
                    
                    // Return the first event and update sequence
                    if let Some(first) = missed_events.first() {
                        let first = first.clone();
                        // Important: Advance the sequence so we don't re-process the same event in a loop
                        self.next_seq = first.seq_num + 1;
                        
                        // We must remove the returned event from the buffer if it was there
                        self.buffer.remove(&first.seq_num);
                        
                        // Remaining events stay in the buffer and will be picked up by subsequent pop() calls
                        // because they are now <= next_seq (or close to it)
                        
                        return Some(first);
                    }
                }

                warn!("INGRESS STALL: Jumping gap {} -> {} (pending: {}).", self.next_seq, seq, pending_count);
                metrics::SEQUENCE_GAPS.with_label_values(&["ingress"]).inc();
                let gap_event = Arc::new(Event {
                    event_type: EventType::SequenceGap,
                    dev_id,
                    inode: 0, parent_inode: 0, new_parent_inode: 0,
                    seq_num: self.next_seq,
                    timestamp_ns: 0, offset: 0, length: 0,
                    name: format!("GAP-{}-{}", self.next_seq, seq),
                    new_name: None, generation: 0, projid: 0,
                    uid: 0, gid: 0,
                    mode: 0, flags: 0, nlink: 0,
                    process_name: "GAP".into(), interactive: false, created_at: Instant::now(),
                });
                self.next_seq = seq;
                self.stalled_since = None;
                return Some(gap_event);
            }
        } else {
            if effective_timeout.is_zero() {
                // Fast path for pressure release
                metrics::SEQUENCE_GAPS.with_label_values(&["ingress"]).inc();
                let gap_event = Arc::new(Event {
                    event_type: EventType::SequenceGap,
                    dev_id,
                    inode: 0, parent_inode: 0, new_parent_inode: 0,
                    seq_num: self.next_seq, timestamp_ns: 0, offset: 0, length: 0,
                    name: format!("GAP-{}-{}", self.next_seq, seq), new_name: None, generation: 0, projid: 0,
                    uid: 0, gid: 0,
                    mode: 0, flags: 0, nlink: 0,
                    process_name: "GAP".into(), interactive: false, created_at: Instant::now(),
                });
                self.next_seq = seq;
                return Some(gap_event);
            }
            self.stalled_since = Some(Instant::now());
        }
        None
    }
}

pub struct Coalescer {
    batch: EventBatch,
    scan_depth: usize,
    last_flush_time: Instant,
    accumulated_bytes: u64,
}

impl Coalescer {
    pub fn new(scan_depth: usize) -> Self {
        Self {
            batch: EventBatch::new(128),
            scan_depth,
            last_flush_time: Instant::now(),
            accumulated_bytes: 0,
        }
    }

    pub fn push(&mut self, event: Arc<Event>) {
        if event.event_type == EventType::Write || event.event_type == EventType::WriteRange {
            self.accumulated_bytes += event.length;
        }
        self.batch.push(event);

        // P3: Bounded frontier — IVI-inspired width limit
        // If batch exceeds threshold, apply aggressive pruning to prevent unbounded growth
        const FRONTIER_WIDTH_LIMIT: usize = 10_000;
        if self.batch.len() > FRONTIER_WIDTH_LIMIT {
            // Aggressive: prune ALL transient lifecycles (full batch, not just scan_depth)
            self.prune_transient_lifecycles(self.batch.len());
            // If still over limit after pruning, force-coalesce all contiguous writes
            if self.batch.len() > FRONTIER_WIDTH_LIMIT {
                while let Some(_) = self.batch.try_coalesce_head(self.batch.len(), u64::MAX) {
                    if self.batch.len() <= FRONTIER_WIDTH_LIMIT { break; }
                }
            }
        }

        metrics::ORDERING_BUF_SIZE.with_label_values(&["worker"]).set((self.batch.len() as i64) as f64);
    }

    pub fn len(&self) -> usize { self.batch.len() }
    
    pub fn is_empty(&self) -> bool { self.batch.is_empty() }

    pub fn collect_batch_inodes(&self) -> Vec<u64> {
        let mut inodes = Vec::new();
        let mut seen = HashSet::new();
        for i in 0..self.batch.len() {
            let t = self.batch.types[i];
            if matches!(t, EventType::Write | EventType::WriteRange | EventType::Truncate) {
                let ino = self.batch.inodes[i];
                if seen.insert(ino) {
                    inodes.push(ino);
                }
            }
        }
        inodes
    }

    pub fn pop_batch(
        &mut self,
        limit_bytes: u64,
        max_latency: Duration,
        is_busy: bool
    ) -> Option<Arc<Event>> {
        if self.batch.is_empty() { return None; }
        
        let head_type = self.batch.types.first().copied().unwrap_or(EventType::Unknown);
        let is_write = head_type == EventType::Write || head_type == EventType::WriteRange;
        
        if !is_write {
            let evt = self.batch.pop_front()?;
            self.last_flush_time = Instant::now();
            return Some(evt);
        }

        let size_threshold_met = self.accumulated_bytes >= limit_bytes;
        let time_expired = self.last_flush_time.elapsed() > max_latency;

        if is_busy && !size_threshold_met && !time_expired {
            return None;
        }

        let result = self.batch.try_coalesce_head(self.scan_depth, limit_bytes);
        if let Some(ref evt) = result {
            self.last_flush_time = Instant::now();
            self.accumulated_bytes = self.accumulated_bytes.saturating_sub(evt.length);
        }
        result
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Event>> {
        self.batch.events.iter()
    }

    pub fn contains_inode(&self, inode: u64) -> bool {
        self.batch.events.iter().any(|e| e.inode == inode)
    }

    /// Elastic look-ahead: adjusts scan depth based on buffer utilization.
    /// Higher load = deeper scan to find topological cancellations.
    pub fn try_elastic_coalesce(
        &mut self,
        base_limit: u64,
        max_capacity: usize,
    ) -> Option<Arc<Event>> {
        if self.batch.is_empty() { return None; }

        let current_len = self.batch.len();
        let util_pct = current_len as f64 / max_capacity.max(1) as f64;

        // Elastic depth: higher load = deeper scan to prune transient lifecycles
        let dynamic_m = if util_pct > 0.8 {
            self.scan_depth * 10
        } else if util_pct > 0.5 {
            self.scan_depth * 2
        } else {
            self.scan_depth
        };

        // Topological pruning: remove create→write→unlink chains for same inode
        if self.prune_transient_lifecycles(dynamic_m) {
            return self.pop_batch(base_limit, Duration::ZERO, false);
        }

        self.batch.try_coalesce_head(dynamic_m, base_limit)
    }

    /// Look ahead `m_depth` events. If a file is created and then unlinked
    /// within this window, remove ALL events for that inode from the batch.
    fn prune_transient_lifecycles(&mut self, m_depth: usize) -> bool {
        let scan_len = self.batch.len().min(m_depth);
        if scan_len == 0 { return false; }

        let mut unlinked_inodes = HashSet::new();
        for i in 0..scan_len {
            if self.batch.types[i] == EventType::Unlink {
                unlinked_inodes.insert(self.batch.inodes[i]);
            }
        }

        if unlinked_inodes.is_empty() { return false; }

        let original_len = self.batch.len();
        // Collect which indices to keep (avoids borrow conflict on self.batch)
        let inodes_snapshot: Vec<u64> = self.batch.inodes[..scan_len.min(self.batch.inodes.len())].to_vec();
        self.batch.retain(|idx| {
            idx >= inodes_snapshot.len() || !unlinked_inodes.contains(&inodes_snapshot[idx])
        });

        let pruned = original_len != self.batch.len();
        if pruned {
            tracing::debug!("Elastic coalescer: pruned {} transient events ({} inodes)",
                           original_len - self.batch.len(), unlinked_inodes.len());
        }
        pruned
    }
}
