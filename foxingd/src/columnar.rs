use crate::event::{Event, EventType};
use std::sync::Arc;
use crate::metrics;
use std::sync::atomic::{AtomicUsize, Ordering, fence};
#[derive(Debug, Default)]
pub struct EventBatch {
    pub inodes: Vec<u64>,
    pub types: Vec<EventType>,
    pub offsets: Vec<u64>,
    pub lengths: Vec<u64>,
    pub events: Vec<Arc<Event>>,
    pub next_index: AtomicUsize,
    workspace: Vec<usize>,
}
impl EventBatch {
    pub fn new(capacity: usize) -> Self {
        Self {
            inodes: Vec::with_capacity(capacity),
            types: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
            lengths: Vec::with_capacity(capacity),
            events: Vec::with_capacity(capacity),
            next_index: AtomicUsize::new(0),
            workspace: Vec::with_capacity(32),
        }
    }
    pub fn len(&self) -> usize {
        self.next_index.load(Ordering::SeqCst)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn push(&mut self, event: Arc<Event>) {
        self.inodes.push(event.inode);
        self.types.push(event.event_type);
        self.offsets.push(event.offset);
        self.lengths.push(event.length);
        self.events.push(event);
        fence(Ordering::Release);
        self.next_index.fetch_add(1, Ordering::SeqCst);
    }
    pub fn clear(&mut self) {
        self.inodes.clear();
        self.types.clear();
        self.offsets.clear();
        self.lengths.clear();
        self.events.clear();
        self.next_index.store(0, Ordering::SeqCst);
        self.workspace.clear();
    }
    pub fn pop_front(&mut self) -> Option<Arc<Event>> {
        if self.is_empty() {
            return None;
        }
        self.inodes.remove(0);
        self.types.remove(0);
        self.offsets.remove(0);
        self.lengths.remove(0);
        let evt = self.events.remove(0);
        self.next_index.fetch_sub(1, Ordering::SeqCst);
        Some(evt)
    }
    pub fn try_coalesce_head(&mut self, scan_depth: usize, limit: u64) -> Option<Arc<Event>> {
        if self.is_empty() {
            return None;
        }
        fence(Ordering::Acquire);
        let len = self.len();
        let head_idx = 0;
        let head_type = self.types[head_idx];
        if head_type != EventType::Write && head_type != EventType::WriteRange {
            return self.pop_front();
        }
        let head_inode = self.inodes[head_idx];
        let mut merged_len = self.lengths[head_idx];
        let mut current_end_offset = self.offsets[head_idx] + merged_len;
        let head_name = &self.events[head_idx].name;
        self.workspace.clear();
        let indices_to_remove = &mut self.workspace;
        let mut merged_count = 0;
        let max_scan = len.min(scan_depth);
        for i in 1..max_scan {
            if merged_len >= limit { break; }
            if self.inodes[i] != head_inode {
                continue;
            }
            let t = self.types[i];
            if t != EventType::Write && t != EventType::WriteRange {
                continue;
            }
            if self.offsets[i] != current_end_offset {
                continue;
            }
            if &self.events[i].name != head_name {
                continue;
            }
            merged_len += self.lengths[i];
            current_end_offset += self.lengths[i];
            indices_to_remove.push(i);
            merged_count += 1;
        }
        if merged_count > 0 {
            for &i in indices_to_remove.iter().rev() {
                self.inodes.remove(i);
                self.types.remove(i);
                self.offsets.remove(i);
                self.lengths.remove(i);
                self.events.remove(i);
                self.next_index.fetch_sub(1, Ordering::SeqCst);
            }
            metrics::COALESCED_WRITES.inc_by(merged_count as f64);
            let mut base_event = (*self.events[0]).clone();
            base_event.length = merged_len;
            self.pop_front();
            return Some(Arc::new(base_event));
        }
        self.pop_front()
    }
}
