use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::System;
use parking_lot::Mutex;
use tracing::{warn, debug};
use crate::metrics;
use std::fs;

/// Thresholds for Pressure Stall Information (PSI).
/// If the "some" (some tasks waiting) 10-second average exceeds this percentage,
/// we consider the system stressed.
const PSI_IO_THRESHOLD: f64 = 20.0; 
const PSI_CPU_THRESHOLD: f64 = 40.0;

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
}

impl Governor {
    pub fn new(max_load: f64, hydration_delay_ms: u64) -> Self {
        let mut sys = System::new();
        sys.refresh_cpu(); // Initial refresh
        
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
        }
    }

    /// Reads a specific PSI file (io, cpu, memory) and parses the 'some' line.
    /// Format: some avg10=0.00 avg60=0.00 avg300=0.00 total=0
    fn read_psi(resource: &str) -> Option<PsiStats> {
        let path = format!("/proc/pressure/{}", resource);
        let content = fs::read_to_string(path).ok()?;
        
        for line in content.lines() {
            if line.starts_with("some ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                // parts[0]="some", parts[1]="avg10=X", parts[2]="avg60=Y", ...
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
        if last.elapsed() < Duration::from_millis(500) {
            return metrics::GOVERNOR_STRESSED.get() == 1;
        }
        *last = Instant::now();

        let mut stressed = false;
        let mut reason = "";

        // 1. Check PSI (Preferred on Linux)
        if self.psi_available {
            if let Some(io_psi) = Self::read_psi("io") {
                if io_psi.avg10 > PSI_IO_THRESHOLD {
                    stressed = true;
                    reason = "PSI_IO";
                }
            }
            if !stressed {
                if let Some(cpu_psi) = Self::read_psi("cpu") {
                    if cpu_psi.avg10 > PSI_CPU_THRESHOLD {
                        stressed = true;
                        reason = "PSI_CPU";
                    }
                }
            }
        }

        // 2. Check Load Average (Fallback or Supplemental)
        if !stressed {
            let mut sys = self.system.lock();
            sys.refresh_cpu(); // Lightweight refresh
            let load = System::load_average();
            
            // Export metrics
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).set(load.one);
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["5m"]).set(load.five);
            metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["15m"]).set(load.fifteen);

            if load.one > self.max_load_avg {
                stressed = true;
                reason = "LoadAvg";
            }
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

        // Always respect the minimum delay configured by user
        if !self.min_hydration_interval.is_zero() {
            std::thread::sleep(self.min_hydration_interval);
            slept = true;
        }

        // Adaptive Backoff
        let mut backoff = Duration::from_millis(10);
        let max_backoff = Duration::from_millis(1000); // Cap at 1s to ensure we check status again

        while self.is_system_stressed() {
            std::thread::sleep(backoff);
            slept = true;
            backoff = (backoff * 2).min(max_backoff);
        }

        if slept {
            metrics::GOVERNOR_PACING_DURATION_MS.inc_by(start.elapsed().as_millis() as u64);
        }
    }
}
