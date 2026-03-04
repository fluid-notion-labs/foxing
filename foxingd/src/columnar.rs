use crate::event::{Event, EventType};
use std::sync::Arc;
use crate::metrics;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

/// SIMD-accelerated scan: find first index >= start where inodes[i] == target.
/// Returns None if no match found within range.
#[inline(always)]
fn find_inode_match(inodes: &[u64], target: u64, start: usize, end: usize) -> Option<usize> {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { find_inode_match_avx2(inodes, target, start, end) };
        }
    }
    // Scalar fallback
    for i in start..end {
        if inodes[i] == target { return Some(i); }
    }
    None
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn find_inode_match_avx2(inodes: &[u64], target: u64, start: usize, end: usize) -> Option<usize> {
    use std::arch::x86_64::*;
    let target_vec = _mm256_set1_epi64x(target as i64);
    let mut i = start;

    // Process 4 inodes per iteration
    while i + 4 <= end {
        let data = _mm256_loadu_si256(inodes[i..].as_ptr() as *const _);
        let cmp = _mm256_cmpeq_epi64(data, target_vec);
        let mask = _mm256_movemask_epi8(cmp);
        if mask != 0 {
            // Found a match — determine which lane
            let lane = mask.trailing_zeros() / 8;
            return Some(i + lane as usize);
        }
        i += 4;
    }

    // Scalar remainder
    while i < end {
        if inodes[i] == target { return Some(i); }
        i += 1;
    }
    None
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use std::arch::x86_64::*;
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
    /// Remove all elements where the predicate returns false.
    pub fn retain<F: Fn(usize) -> bool>(&mut self, keep: F) {
        let mut write = 0;
        for read in 0..self.len() {
            if keep(read) {
                if write != read {
                    self.inodes[write] = self.inodes[read];
                    self.types[write] = self.types[read];
                    self.offsets[write] = self.offsets[read];
                    self.lengths[write] = self.lengths[read];
                    self.events[write] = self.events[read].clone();
                }
                write += 1;
            }
        }
        self.inodes.truncate(write);
        self.types.truncate(write);
        self.offsets.truncate(write);
        self.lengths.truncate(write);
        self.events.truncate(write);
        self.next_index.store(write, Ordering::SeqCst);
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
        // SIMD-accelerated inode scan: skip non-matching inodes in chunks of 4
        let mut search_start = 1;
        while search_start < max_scan && merged_len < limit {
            let match_idx = match find_inode_match(&self.inodes, head_inode, search_start, max_scan) {
                Some(i) => i,
                None => break, // No more matches in scan range
            };

            let t = self.types[match_idx];
            if t != EventType::Write && t != EventType::WriteRange {
                search_start = match_idx + 1;
                continue;
            }
            if self.offsets[match_idx] != current_end_offset {
                search_start = match_idx + 1;
                continue;
            }
            if &self.events[match_idx].name != head_name {
                search_start = match_idx + 1;
                continue;
            }
            merged_len += self.lengths[match_idx];
            current_end_offset += self.lengths[match_idx];
            indices_to_remove.push(match_idx);
            merged_count += 1;
            search_start = match_idx + 1;
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
