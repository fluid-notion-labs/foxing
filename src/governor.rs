use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant};
use sysinfo::{System, SystemExt}; 
use parking_lot::Mutex;
use tracing::warn;
use crate::metrics;

pub struct Governor {
    system: Mutex<System>,
    last_check: Mutex<Instant>,
    max_load_avg: f64,
    min_hydration_interval: Duration,
    throttled_count: AtomicU64,
}

impl Governor {
    pub fn new(max_load: f64, hydration_delay_ms: u64) -> Self {
        let mut sys = System::new();
        sys.refresh_cpu();
        Self {
            system: Mutex::new(sys),
            last_check: Mutex::new(Instant::now()),
            max_load_avg: max_load,
            min_hydration_interval: Duration::from_millis(hydration_delay_ms),
            throttled_count: AtomicU64::new(0),
        }
    }

    pub fn is_system_stressed(&self) -> bool {
        let mut last = self.last_check.lock();
        if last.elapsed() > Duration::from_secs(5) {
            let mut sys = self.system.lock();
            sys.refresh_cpu(); 
            *last = Instant::now();
            
            // FIX: Use static method for load average
            let load = System::load_average();
            
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).set(load.one);
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["5m"]).set(load.five);
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["15m"]).set(load.fifteen);

            if load.one > self.max_load_avg {
                metrics::GOVERNOR_STRESSED.set(1);
                metrics::GOVERNOR_THROTTLED_EVENTS.inc();
                let count = self.throttled_count.fetch_add(1, Ordering::Relaxed);
                if count % 10 == 0 {
                    warn!("System under high load ({:.2} > {:.2}). Throttling.", load.one, self.max_load_avg);
                }
                return true;
            } else {
                metrics::GOVERNOR_STRESSED.set(0);
            }
        }
        metrics::GOVERNOR_STRESSED.get() == 1
    }

    pub fn pace_hydration(&self) {
        let start = Instant::now();
        let mut slept = false;
        if !self.min_hydration_interval.is_zero() {
            std::thread::sleep(self.min_hydration_interval);
            slept = true;
        }
        while self.is_system_stressed() {
            std::thread::sleep(Duration::from_millis(500));
            slept = true;
        }
        if slept {
            metrics::GOVERNOR_PACING_DURATION_MS.inc_by(start.elapsed().as_millis() as u64);
        }
    }
}
