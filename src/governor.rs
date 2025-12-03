use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::System;
use parking_lot::Mutex;
use tracing::{warn, debug};
use crate::metrics;
use std::fs;
const PSI_IO_THRESHOLD: f64 = 20.0;
const PSI_CPU_THRESHOLD: f64 = 40.0;
const HYSTERESIS_DURATION: Duration = Duration::from_secs(1); // NEW: Must be stressed for 1s
#[derive(Debug)]
struct PsiStats {
    avg10: f64,
    _avg60: f64,
    _total: u64,
}
pub struct Governor {
    system: Mutex<System>,
    last_check: Mutex<Instant>,
    max_load_avg: f64,
    min_hydration_interval: Duration,
    throttled_count: AtomicU64,
    psi_available: bool,
    // NEW: State tracking for hysteresis
    last_stressed_entry: Mutex<Option<Instant>>,
}
impl Governor {
    pub fn new(max_load: f64, hydration_delay_ms: u64) -> Self {
        let mut sys = System::new();
        sys.refresh_cpu();
        let psi_available = std::path::Path::new("/proc/pressure/io").exists();
        if psi_available {
            debug!("Governor: Linux PSI (Pressure Stall Information) detected. Using enhanced congestion control.");
        } else {
            debug!("Governor: PSI not available. Falling back to Load Average.");
        }
        Self {
            system: Mutex::new(sys),
            last_check: Mutex::new(Instant::now()),
            max_load_avg: max_load,
            min_hydration_interval: Duration::from_millis(hydration_delay_ms),
            throttled_count: AtomicU64::new(0),
            psi_available,
            last_stressed_entry: Mutex::new(None),
        }
    }
    fn read_psi(resource: &str) -> Option<PsiStats> {
        let path = format!("/proc/pressure/{}", resource);
        let content = fs::read_to_string(path).ok()?;
        for line in content.lines() {
            if line.starts_with("some ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                let avg10 = parts.get(1)?.split('=').nth(1)?.parse::<f64>().ok()?;
                let avg60 = parts.get(2)?.split('=').nth(1)?.parse::<f64>().ok()?;
                let total = parts.get(4)?.split('=').nth(1)?.parse::<u64>().ok()?;
                return Some(PsiStats { avg10, _avg60: avg60, _total: total });
            }
        }
        None
    }
    pub fn is_system_stressed(&self) -> bool {
        let mut last = self.last_check.lock();
        let now = Instant::now();
        if now.duration_since(*last) < Duration::from_millis(500) {
            // Use cached state if check interval is too fast
            return metrics::GOVERNOR_STRESSED.get() == 1;
        }
        *last = now;
        let mut current_reading_stressed = false;
        let mut reason = "";
        
        // --- 1. Check PSI (Preferred on Linux) ---
        if self.psi_available {
            if let Some(io_psi) = Self::read_psi("io") {
                if io_psi.avg10 > PSI_IO_THRESHOLD {
                    current_reading_stressed = true;
                    reason = "PSI_IO";
                }
            }
            if !current_reading_stressed {
                if let Some(cpu_psi) = Self::read_psi("cpu") {
                    if cpu_psi.avg10 > PSI_CPU_THRESHOLD {
                        current_reading_stressed = true;
                        reason = "PSI_CPU";
                    }
                }
            }
        }
        
        // --- 2. Check Load Average (Fallback) ---
        if !current_reading_stressed {
            let mut sys = self.system.lock();
            sys.refresh_cpu();
            let load = System::load_average();
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).set(load.one);
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["5m"]).set(load.five);
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["15m"]).set(load.fifteen);
            if load.one > self.max_load_avg {
                current_reading_stressed = true;
                reason = "LoadAvg";
            }
        }
        
        // --- 3. Apply Hysteresis and Set State ---
        let mut stressed = false;
        let mut last_entry = self.last_stressed_entry.lock();
        
        if current_reading_stressed {
            if last_entry.is_none() {
                // First time we saw stress in this period
                *last_entry = Some(now);
            }
            // Is stress sustained long enough?
            if now.duration_since(last_entry.unwrap()) >= HYSTERESIS_DURATION {
                stressed = true;
            }
        } else {
            // Not stressed now, reset entry
            *last_entry = None;
        }

        if stressed {
            metrics::GOVERNOR_STRESSED.set(1);
            metrics::GOVERNOR_THROTTLED_EVENTS.inc();
            let count = self.throttled_count.fetch_add(1, Ordering::Relaxed);
            if count % 100 == 0 {
                warn!("System Stressed! Reason: {}. Throttling background operations.", reason);
            }
        } else {
            metrics::GOVERNOR_STRESSED.set(0);
        }
        
        stressed
    }
    pub fn pace_hydration(&self) {
        let start = Instant::now();
        let mut slept = false;
        
        // Mandatory minimum delay (min_hydration_interval) is honored first
        if !self.min_hydration_interval.is_zero() {
            std::thread::sleep(self.min_hydration_interval);
            slept = true;
        }
        
        let mut backoff = Duration::from_millis(10);
        let max_backoff = Duration::from_millis(1000);
        
        // Only pace aggressively if the Governor is actually marked as stressed (hysteresis applied)
        while metrics::GOVERNOR_STRESSED.get() == 1 {
            std::thread::sleep(backoff);
            slept = true;
            backoff = (backoff * 2).min(max_backoff);
        }
        
        if slept {
            metrics::GOVERNOR_PACING_DURATION_MS.inc_by(start.elapsed().as_millis() as u64);
        }
    }
}
