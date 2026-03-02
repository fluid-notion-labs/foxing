use std::time::{Instant, Duration};
use std::collections::HashMap;
use std::ops::Sub;
use parking_lot::Mutex;
use lru::LruCache;
use tracing::debug;
use crate::security;
// FIX: Import constant from the new location (constants.rs)
use crate::constants::ERROR_LIMITER_SECS;
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
            cache: LruCache::new(std::num::NonZeroUsize::new(65536).unwrap()),
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
pub struct ErrorLimiter {
    last: Mutex<HashMap<&'static str, Instant>>
}
impl ErrorLimiter {
    pub fn new() -> Self { Self { last: Mutex::new(HashMap::new()) } }
    pub fn check(&self, key: &'static str) -> bool {
        let mut map = self.last.lock();
        let now = Instant::now();
        // FIX: Use ERROR_LIMITER_SECS constant from constants module
        let entry = map.entry(key).or_insert(now.sub(Duration::from_secs(ERROR_LIMITER_SECS + 1)));
        if now.duration_since(*entry) < Duration::from_secs(ERROR_LIMITER_SECS) {
            return false;
        }
        *entry = now;
        true
    }
}
pub struct CircuitBreaker {
    tripped: std::sync::atomic::AtomicBool,
    last_check: Mutex<Instant>,
    interval: Duration
}
impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            tripped: std::sync::atomic::AtomicBool::new(self.tripped.load(std::sync::atomic::Ordering::Relaxed)),
            last_check: Mutex::new(*self.last_check.lock()),
            interval: self.interval
        }
    }
}
impl CircuitBreaker {
    pub fn new(interval_secs: u64) -> Self {
        Self {
            tripped: std::sync::atomic::AtomicBool::new(false),
            last_check: Mutex::new(Instant::now()),
            interval: Duration::from_secs(interval_secs)
        }
    }
    pub fn can_proceed(&self, path: &std::path::Path, threshold: u64) -> bool {
        let mut last = self.last_check.lock();
        let now = Instant::now();
        if self.tripped.load(std::sync::atomic::Ordering::Relaxed) {
            if now.duration_since(*last) < self.interval {
                return false;
            }
            if security::check_capacity(path, threshold) {
                self.tripped.store(false, std::sync::atomic::Ordering::Relaxed);
                *last = now;
                return true;
            }
            *last = now;
            return false;
        }
        if !security::check_capacity(path, threshold) {
            self.tripped.store(true, std::sync::atomic::Ordering::Relaxed);
            *last = now;
            return false;
        }
        true
    }
    pub fn trip(&self) { self.tripped.store(true, std::sync::atomic::Ordering::Relaxed); }
}
#[derive(Debug, Clone)]
pub struct FailureState {
    pub streak_start: Option<Instant>,
    current_backoff: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    hibernation_threshold: Duration,
}
impl FailureState {
    pub fn new(
        hibernation_secs: u64,
        initial_retry_ms: u64,
        max_retry_ms: u64
    ) -> Self {
        Self {
            streak_start: None,
            current_backoff: Duration::from_millis(initial_retry_ms),
            initial_backoff: Duration::from_millis(initial_retry_ms),
            max_backoff: Duration::from_millis(max_retry_ms),
            hibernation_threshold: Duration::from_secs(hibernation_secs),
        }
    }
    pub fn record_failure(&mut self) {
        let now = Instant::now();
        if self.streak_start.is_none() {
            self.streak_start = Some(now);
        }
        // Ramp up backoff (hysteresis)
        self.current_backoff = (self.current_backoff * 2).min(self.max_backoff);
    }
    pub fn record_success(&mut self) {
        self.streak_start = None;
        self.current_backoff = self.initial_backoff;
    }
    pub fn should_hibernate(&self) -> bool {
        if let Some(start) = self.streak_start {
            start.elapsed() > self.hibernation_threshold
        } else {
            false
        }
    }
    pub fn get_backoff(&self) -> Duration {
        self.current_backoff
    }
}
