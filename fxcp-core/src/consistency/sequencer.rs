// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/consistency/sequencer.rs — Monotonic sequence generator for event ordering

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;
use std::collections::BTreeSet;
use std::sync::Mutex;

/// A simple monotonic counter for issuing tickets.
#[derive(Debug)]
pub struct GlobalSequencer {
    counter: AtomicU64,
}

impl GlobalSequencer {
    pub fn new(start: u64) -> Self {
        Self { counter: AtomicU64::new(start) }
    }

    /// Increments and returns the *next* sequence number (1-based if started at 0).
    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }
    
    pub fn current(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}

/// A barrier that waits for a specific sequence number to be reached.
/// It handles out-of-order completions by buffering them and advancing
/// the publicly visible "finished" sequence monotonically.
#[derive(Debug)]
pub struct SequenceBarrier {
    finished: watch::Sender<u64>,
    receiver: watch::Receiver<u64>,
    pending_completions: Mutex<BTreeSet<u64>>,
}

impl SequenceBarrier {
    pub fn new(start: u64) -> Self {
        let (tx, rx) = watch::channel(start);
        Self {
            finished: tx,
            receiver: rx,
            pending_completions: Mutex::new(BTreeSet::new()),
        }
    }

    /// Wait until the barrier reaches at least `seq`.
    pub async fn wait_for(&self, seq: u64) {
        let mut rx = self.receiver.clone();
        loop {
            // Check current value first
            let current = *rx.borrow_and_update();
            if current >= seq {
                return;
            }
            // Wait for change
            if rx.changed().await.is_err() {
                // Sender dropped, we can't wait anymore. 
                // In a production system this might mean shutdown.
                return; 
            }
        }
    }

    /// Mark a specific sequence number as complete.
    /// If this completes the next expected sequence, the barrier advances.
    pub fn complete(&self, seq: u64) {
        let mut pending = self.pending_completions.lock().unwrap();
        
        let current = *self.finished.borrow();
        if seq <= current {
            return; // Already processed
        }

        pending.insert(seq);
        
        let mut next_expected = current + 1;
        let mut updated = false;
        
        // Advance monotonically as far as possible
        while pending.remove(&next_expected) {
            next_expected += 1;
            updated = true;
        }

        if updated {
            // next_expected is now 1 greater than the last processed item
            let _ = self.finished.send(next_expected - 1);
        }
    }
    
    pub fn current(&self) -> u64 {
        *self.finished.borrow()
    }
}
