use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use std::ops::Sub;
use std::path::Path;
use parking_lot::Mutex;
use lru::LruCache;
use tracing::debug;
use crate::security;
use crate::config::{MAX_FAILURE_BACKOFF, ERROR_LIMITER_SECS};

struct PoisonEntry {
    failure_count: u32,
    next_attempt: Instant,
}

pub struct PoisonCabinet {
    cache: LruCache<u64, PoisonEntry>,
}

impl PoisonCabinet {
    pub fn new() -> Self {
        Self {
            cache: LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()),
        }
    }

    pub fn check_allowed(&mut self, inode: u64) -> bool {
        if let Some(entry) = self.cache.get(&inode) {
            if Instant::now() < entry.next_attempt {
                return false;
            }
        }
        true
    }

    pub fn record_failure(&mut self, inode: u64) {
        if let Some(entry) = self.cache.get_mut(&inode) {
            entry.failure_count += 1;
            let backoff_secs = (2u64.pow(entry.failure_count.min(6))) as u64;
            entry.next_attempt = Instant::now() + Duration::from_secs(backoff_secs);
            debug!("Inode {} poisoned. Failure #{}. Backing off for {}s.", inode, entry.failure_count, backoff_secs);
        } else {
            self.cache.put(inode, PoisonEntry {
                failure_count: 1,
                next_attempt: Instant::now() + Duration::from_secs(2),
            });
        }
    }

    pub fn record_success(&mut self, inode: u64) {
        if self.cache.contains(&inode) {
            self.cache.pop(&inode);
        }
    }
}

pub struct ErrorLimiter { last: Mutex<HashMap<&'static str, Instant>> }

impl ErrorLimiter {
    pub fn new() -> Self { Self { last: Mutex::new(HashMap::new()) } }
    pub fn check(&self, key: &'static str) -> bool {
        let mut map = self.last.lock();
        let now = Instant::now();
        let entry = map.entry(key).or_insert(now.sub(Duration::from_secs(ERROR_LIMITER_SECS + 1)));
        if now.duration_since(*entry) < Duration::from_secs(ERROR_LIMITER_SECS) { return false; }
        *entry = now;
        true
    }
}

pub struct CircuitBreaker { 
    tripped: AtomicBool, 
    last_check: Mutex<Instant>, 
    interval: Duration 
}

impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self { 
            tripped: AtomicBool::new(self.tripped.load(Ordering::Relaxed)), 
            last_check: Mutex::new(*self.last_check.lock()), 
            interval: self.interval 
        }
    }
}

impl CircuitBreaker {
    pub fn new(interval_secs: u64) -> Self { 
        Self { 
            tripped: AtomicBool::new(false), 
            last_check: Mutex::new(Instant::now()), 
            interval: Duration::from_secs(interval_secs) 
        } 
    }

    pub fn can_proceed(&self, path: &Path, threshold: u64) -> bool {
        let mut last = self.last_check.lock();
        let now = Instant::now();
        if self.tripped.load(Ordering::Relaxed) {
            if now.duration_since(*last) < self.interval { return false; }
            if security::check_capacity(path, threshold) { 
                self.tripped.store(false, Ordering::Relaxed); 
                *last = now; 
                return true; 
            }
            *last = now; 
            return false;
        }
        if !security::check_capacity(path, threshold) { 
            self.tripped.store(true, Ordering::Relaxed); 
            *last = now; 
            return false; 
        }
        true
    }

    pub fn trip(&self) { self.tripped.store(true, Ordering::Relaxed); }
}

#[derive(Debug)]
pub struct FailureState { 
    is_failed: AtomicBool, 
    last_failure: Instant, 
    retry_interval: Duration, 
    pub hibernation_threshold: Duration, 
    max_backoff: Duration 
}

impl Clone for FailureState {
    fn clone(&self) -> Self { 
        Self { 
            is_failed: AtomicBool::new(self.is_failed.load(Ordering::Relaxed)), 
            last_failure: self.last_failure, 
            retry_interval: self.retry_interval, 
            hibernation_threshold: self.hibernation_threshold, 
            max_backoff: self.max_backoff 
        } 
    }
}

impl FailureState {
    pub fn new(interval_secs: u64) -> Self {
        let max_backoff_duration = Duration::from_secs(MAX_FAILURE_BACKOFF);
        Self { 
            is_failed: AtomicBool::new(false), 
            last_failure: Instant::now().sub(max_backoff_duration), 
            retry_interval: Duration::from_secs(5), 
            hibernation_threshold: Duration::from_secs(interval_secs), 
            max_backoff: max_backoff_duration 
        }
    }

    pub fn record_failure(&mut self) {
        self.is_failed.store(true, Ordering::Relaxed);
        let now = Instant::now();
        if now.duration_since(self.last_failure) > self.max_backoff { 
            self.retry_interval = Duration::from_secs(5); 
        } else { 
            self.retry_interval = (self.retry_interval * 2).min(self.max_backoff); 
        }
        self.last_failure = now;
    }

    pub fn record_success(&mut self) { 
        self.is_failed.store(false, Ordering::Relaxed); 
        self.retry_interval = Duration::from_secs(5); 
    }

    pub fn can_execute_io(&self) -> bool { 
        if !self.is_failed.load(Ordering::Relaxed) { return true; } 
        self.last_failure.elapsed() >= self.retry_interval 
    }

    pub fn check_hibernation_needed(&self) -> bool { 
        self.is_failed.load(Ordering::Relaxed) && self.last_failure.elapsed() > self.hibernation_threshold 
    }
}
