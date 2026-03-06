// Governor — system stress management
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::sync::Arc;
use sysinfo::System;
use tracing::{warn, debug};
use crate::metrics;
use std::fs;
use crate::constants;
use std::thread;
use std::time::Instant;

/// Store f64 in AtomicU64 via bit transmutation (lock-free reads)
fn f64_to_u64(v: f64) -> u64 { v.to_bits() }
fn u64_to_f64(v: u64) -> f64 { f64::from_bits(v) }

/// Detect if running inside a hypervisor (KVM, VMware, Xen, Hyper-V, etc.)
pub fn detect_hypervisor() -> Option<String> {
    // Method 1: systemd-detect-virt (most reliable)
    if let Ok(output) = std::process::Command::new("systemd-detect-virt")
        .output()
    {
        let virt = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if output.status.success() && virt != "none" && !virt.is_empty() {
            return Some(virt);
        }
    }
    // Method 2: DMI vendor string
    if let Ok(vendor) = fs::read_to_string("/sys/class/dmi/id/sys_vendor") {
        let v = vendor.trim().to_lowercase();
        if v.contains("qemu") || v.contains("vmware") || v.contains("xen")
            || v.contains("microsoft") || v.contains("amazon")
            || v.contains("google") || v.contains("digitalocean") {
            return Some(v);
        }
    }
    // Method 3: hypervisor CPUID flag (Linux exposes this)
    if std::path::Path::new("/sys/hypervisor/type").exists() {
        if let Ok(t) = fs::read_to_string("/sys/hypervisor/type") {
            return Some(t.trim().to_string());
        }
    }
    None
}

pub struct Governor {
    /// System stress score — updated by background thread, read lock-free by workers
    stress_score: Arc<AtomicU64>,
    /// Memory usage percentage — updated by background thread, read lock-free
    memory_usage_pct: Arc<AtomicU64>,
    min_hydration_interval: Duration,
    min_throughput_bytes_sec: AtomicU64,
    copy_success_count: AtomicU64,
    copy_failure_count: AtomicU64,
    /// Epoch millis of failure window start — atomic, no mutex
    failure_window_start_ms: AtomicU64,
}

impl Governor {
    pub fn new(max_load: f64, hydration_delay_ms: u64, psi_io_limit: f64, psi_cpu_limit: f64) -> Self {
        let is_one_shot = constants::ONE_SHOT_MODE.load(Ordering::Relaxed);
        let in_hypervisor = detect_hypervisor();

        let (effective_io_limit, effective_cpu_limit) = if is_one_shot {
            debug!("Governor: Applying RELAXED thresholds for One-Shot mode.");
            (constants::GOVERNOR_PSI_IO_THRESHOLD_RELAXED, constants::GOVERNOR_PSI_CPU_THRESHOLD_RELAXED)
        } else if let Some(ref virt_type) = in_hypervisor {
            // Hypervisor environments have inflated PSI metrics due to
            // virtio-blk → qcow2 → host storage indirection. Apply 5x relaxation.
            let relaxed_io = (psi_io_limit * 5.0).max(50.0);
            let relaxed_cpu = (psi_cpu_limit * 5.0).max(50.0);
            warn!("Governor: Hypervisor detected ({}). Relaxing PSI thresholds: IO>{:.1}, CPU>{:.1}",
                  virt_type, relaxed_io, relaxed_cpu);
            (relaxed_io, relaxed_cpu)
        } else {
            (psi_io_limit, psi_cpu_limit)
        };

        let psi_available = std::path::Path::new("/proc/pressure/io").exists();
        if psi_available {
            debug!("Governor: PSI active. Limits: IO>{:.1}, CPU>{:.1}", effective_io_limit, effective_cpu_limit);
        }

        // Lock-free f64 storage via AtomicU64 bit transmutation
        // Safe: f64 and u64 are both 8 bytes, AtomicU64 guarantees no torn reads on x86_64
        let stress_score = Arc::new(AtomicU64::new(f64_to_u64(0.0)));
        let memory_usage_pct = Arc::new(AtomicU64::new(f64_to_u64(0.0)));

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
                    
                    memory_usage_thread.store(f64_to_u64(mem_pct), Ordering::Relaxed);

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

                    stress_score_thread.store(f64_to_u64(max_score), Ordering::Relaxed);

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
            copy_success_count: AtomicU64::new(0),
            copy_failure_count: AtomicU64::new(0),
            failure_window_start_ms: AtomicU64::new(
                Instant::now().elapsed().as_millis() as u64 // epoch-relative
            ),
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

    /// Record a copy operation result for failure-rate tracking.
    pub fn signal_copy_result(&self, success: bool) {
        if success {
            self.copy_success_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.copy_failure_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Current failure rate as a fraction [0.0, 1.0].
    pub fn failure_rate(&self) -> f64 {
        let success = self.copy_success_count.load(Ordering::Relaxed);
        let failure = self.copy_failure_count.load(Ordering::Relaxed);
        let total = success + failure;
        if total == 0 { return 0.0; }
        failure as f64 / total as f64
    }

    /// Reset failure counters (e.g. at the start of a new failure window).
    pub fn reset_failure_window(&self) {
        self.copy_success_count.store(0, Ordering::Relaxed);
        self.copy_failure_count.store(0, Ordering::Relaxed);
        self.failure_window_start_ms.store(0, Ordering::Relaxed);
    }

    /// Lock-free stress score read with failure-rate boost.
    /// Called ~320 times/second across all workers — must be contention-free.
    pub fn current_stress_score(&self) -> f64 {
        let mut score = u64_to_f64(self.stress_score.load(Ordering::Relaxed));

        // Boost stress when copy failure rate exceeds threshold
        let failure_rate = self.failure_rate();
        if failure_rate > constants::GOVERNOR_FAILURE_RATE_THRESHOLD {
            score += 0.3;
        }

        // Auto-reset failure window — lock-free, reset after enough samples
        let total_ops = self.copy_success_count.load(Ordering::Relaxed)
            + self.copy_failure_count.load(Ordering::Relaxed);
        if total_ops > 1000 {
            self.reset_failure_window();
        }

        score
    }

    pub fn is_system_stressed(&self) -> bool {
        self.current_stress_score() >= 1.0
    }

    /// Lock-free memory usage read.
    pub fn current_memory_usage_pct(&self) -> f64 {
        u64_to_f64(self.memory_usage_pct.load(Ordering::Relaxed))
    }

    pub fn pace_hydration(&self) {
        let score = self.current_stress_score();

        // Skip pacing entirely when system is not stressed — during initial
        // hydration on NVMe source there's no contention worth throttling for.
        if score < 0.1 { return; }

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
