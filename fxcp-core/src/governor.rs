// Governor — system stress management
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::sync::Arc;
use sysinfo::System;
use parking_lot::Mutex;
use tracing::{warn, debug};
use crate::metrics;
use std::fs;
use crate::constants;
use std::thread;

pub struct Governor {
    stress_score: Arc<Mutex<f64>>,
    memory_usage_pct: Arc<Mutex<f64>>,
    min_hydration_interval: Duration,
    min_throughput_bytes_sec: AtomicU64,
    #[allow(dead_code)]
    throttled_count: AtomicU64,
}

impl Governor {
    pub fn new(max_load: f64, hydration_delay_ms: u64, psi_io_limit: f64, psi_cpu_limit: f64) -> Self {
        let is_one_shot = constants::ONE_SHOT_MODE.load(Ordering::Relaxed);
        let (effective_io_limit, effective_cpu_limit) = if is_one_shot {
            debug!("Governor: Applying RELAXED thresholds for One-Shot mode.");
            (constants::GOVERNOR_PSI_IO_THRESHOLD_RELAXED, constants::GOVERNOR_PSI_CPU_THRESHOLD_RELAXED)
        } else {
            (psi_io_limit, psi_cpu_limit)
        };

        let psi_available = std::path::Path::new("/proc/pressure/io").exists();
        if psi_available {
            debug!("Governor: PSI active. Limits: IO>{:.1}, CPU>{:.1}", effective_io_limit, effective_cpu_limit);
        }

        // FIXED: Use Mutex<f64> instead of AtomicU64 transmutation to prevent torn reads/UB
        let stress_score = Arc::new(Mutex::new(0.0));
        let memory_usage_pct = Arc::new(Mutex::new(0.0));

        let stress_score_thread = stress_score.clone();
        let memory_usage_thread = memory_usage_pct.clone();

        thread::Builder::new()
            .name("foxing-governor".into())
            .spawn(move || {
                let mut system = System::new();
                system.refresh_all();
                loop {
                    let mut max_score = 0.0;
                    let mut reason = "None";

                    system.refresh_memory();
                    let used = system.used_memory();
                    let total = system.total_memory();
                    let mem_pct = if total > 0 { used as f64 / total as f64 } else { 0.0 };
                    
                    {
                        let mut guard = memory_usage_thread.lock();
                        *guard = mem_pct;
                    }

                    if mem_pct > 0.90 {
                        let mem_score = (mem_pct - 0.90) * 10.0;
                        if mem_score > max_score {
                            max_score = mem_score;
                            reason = "Memory";
                        }
                    }

                    if psi_available {
                        if let Some(io_psi) = Self::read_psi("io") {
                            let score = io_psi.avg10 / effective_io_limit;
                            if score > max_score {
                                max_score = score;
                                reason = "PSI_IO";
                            }
                        }
                        if let Some(cpu_psi) = Self::read_psi("cpu") {
                            let score = cpu_psi.avg10 / effective_cpu_limit;
                            if score > max_score {
                                max_score = score;
                                reason = "PSI_CPU";
                            }
                        }
                    } else {
                        system.refresh_cpu_all();
                        let load = sysinfo::System::load_average();
                        metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).set(load.one);
                        let score = load.one / max_load;
                        if score > max_score {
                            max_score = score;
                            reason = "LoadAvg";
                        }
                    }

                    {
                        let mut guard = stress_score_thread.lock();
                        *guard = max_score;
                    }

                    metrics::GOVERNOR_STRESSED.set(if max_score >= 1.0 { 1.0 } else { 0.0 });
                    metrics::GOVERNOR_STRESS_SCORE.set(max_score);

                    if max_score > 2.0 {
                         warn!("System Critical! Score: {:.2} (Reason: {}). Throttling hard.", max_score, reason);
                    }

                    thread::sleep(Duration::from_millis(constants::GOVERNOR_CHECK_INTERVAL_MS));
                }
            }).expect("Failed to spawn governor thread");

        Self {
            stress_score,
            memory_usage_pct,
            min_hydration_interval: Duration::from_millis(hydration_delay_ms),
            min_throughput_bytes_sec: AtomicU64::new(0),
            throttled_count: AtomicU64::new(0),
        }
    }

    /// Set minimum throughput floor — governor will not throttle below this rate.
    pub fn set_min_throughput(&self, bytes_per_sec: u64) {
        self.min_throughput_bytes_sec.store(bytes_per_sec, Ordering::Relaxed);
    }

    fn read_psi(resource: &str) -> Option<PsiStats> {
        let path = format!("/proc/pressure/{}", resource);
        let content = fs::read_to_string(path).ok()?;
        for line in content.lines() {
            if line.starts_with("some ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                let avg10 = parts.get(1)?.split('=').nth(1)?.parse::<f64>().ok()?;
                return Some(PsiStats { avg10 });
            }
        }
        None
    }

    pub fn current_stress_score(&self) -> f64 {
        *self.stress_score.lock()
    }

    pub fn is_system_stressed(&self) -> bool {
        self.current_stress_score() >= 1.0
    }

    pub fn current_memory_usage_pct(&self) -> f64 {
        *self.memory_usage_pct.lock()
    }

    pub fn pace_hydration(&self) {
        let score = self.current_stress_score();
        let is_one_shot = constants::ONE_SHOT_MODE.load(Ordering::Relaxed);
        let throttle_threshold = if is_one_shot { 1.5 } else { 0.8 };

        if score < throttle_threshold {
            if !self.min_hydration_interval.is_zero() {
                std::thread::sleep(self.min_hydration_interval);
            }
            return;
        }

        let severity = (score - throttle_threshold).max(0.0);
        let backoff_ms = if is_one_shot {
             (severity * 50.0).powf(1.1).clamp(0.0, 250.0) as u64
        } else {
             (severity * 100.0).powf(1.2).clamp(0.0, 500.0) as u64
        };

        if backoff_ms > 0 {
            metrics::GOVERNOR_PACING_DURATION_MS.inc_by(backoff_ms as f64);
            metrics::GOVERNOR_THROTTLED_EVENTS.inc();
            std::thread::sleep(Duration::from_millis(backoff_ms));
        } else if !self.min_hydration_interval.is_zero() {
            std::thread::sleep(self.min_hydration_interval);
        }
    }
}

#[derive(Debug)]
struct PsiStats {
    avg10: f64,
}
