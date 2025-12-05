use std::time::{Instant, Duration};
use std::collections::VecDeque;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use tracing::debug;
use crate::metrics;
use crate::config::{TargetConfig, TargetProfile};

pub type TunerBoard = Arc<DashMap<PathBuf, TunerState>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TunerState {
    Startup = 0,
    Drain = 1,
    ProbeBW = 2,
    Muted = 3,
    IdleReset = 4,
    SpacePressure = 6,
    CriticalDrain = 7,
    Steady = 8,
    HighLoad = 9,
}

#[derive(Debug, Clone)]
struct EwmaFilter {
    value: f64,
    alpha: f64,
    initialized: bool,
}

impl EwmaFilter {
    fn new(alpha: f64) -> Self {
        Self { value: 0.0, alpha, initialized: false }
    }

    fn update(&mut self, new_sample: f64) -> f64 {
        if !self.initialized {
            self.value = new_sample;
            self.initialized = true;
        } else {
            self.value = (self.alpha * new_sample) + ((1.0 - self.alpha) * self.value);
        }
        self.value
    }

    fn reset(&mut self) {
        self.initialized = false;
        self.value = 0.0;
    }
}

pub struct WindowedFilter<T> {
    window_duration: Duration,
    samples: VecDeque<(Instant, T)>,
    mode: FilterMode,
}

pub enum FilterMode { Min, Max }

impl<T: PartialOrd + Copy + std::fmt::Display> WindowedFilter<T> {
    pub fn new(window_secs: u64, mode: FilterMode) -> Self {
        Self {
            window_duration: Duration::from_secs(window_secs),
            samples: VecDeque::new(),
            mode,
        }
    }

    pub fn update(&mut self, val: T, now: Instant) -> T {
        while let Some((time, _)) = self.samples.front() {
            if now.duration_since(*time) > self.window_duration {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        self.samples.push_back((now, val));
        
        // Optimize: If the new value is the new best, we don't need to scan.
        // But for generic correctness and window expiry, we scan.
        // For production hot-path, a monotonic deque optimization (like standard BBR)
        // would be faster, but this is sufficient for user-space tuning frequencies.
        let mut best = val;
        for (_, v) in &self.samples {
            match self.mode {
                FilterMode::Min => if *v < best { best = *v; },
                FilterMode::Max => if *v > best { best = *v; },
            }
        }
        best
    }

    pub fn get_best(&self) -> Option<T> {
        if self.samples.is_empty() { return None; }
        // Scan needed because the 'best' might have just expired
        let mut best = self.samples[0].1;
        for (_, v) in &self.samples {
            match self.mode {
                FilterMode::Min => if *v < best { best = *v; },
                FilterMode::Max => if *v > best { best = *v; },
            }
        }
        Some(best)
    }

    /// Returns (min, max) of the current window to determine variance/stability
    pub fn get_range(&self) -> Option<(T, T)> {
        if self.samples.is_empty() { return None; }
        let mut min = self.samples[0].1;
        let mut max = self.samples[0].1;
        for (_, v) in &self.samples {
            if *v < min { min = *v; }
            if *v > max { max = *v; }
        }
        Some((min, max))
    }

    pub fn reset(&mut self) {
        self.samples.clear();
    }
}

struct BbrEvent {
    timestamp: Instant,
    _bytes: u64,
    gap_since_last: Duration,
}

struct BbrHistory {
    window: Duration,
    events: VecDeque<BbrEvent>,
    max_gap: Duration,
}

impl BbrHistory {
    fn new(window_secs: u64) -> Self {
        Self {
            window: Duration::from_secs(window_secs),
            events: VecDeque::with_capacity(100),
            max_gap: Duration::from_secs(1),
        }
    }

    fn push(&mut self, bytes: u64, gap: Duration, now: Instant) {
        while let Some(evt) = self.events.front() {
            if now.duration_since(evt.timestamp) > self.window {
                self.events.pop_front();
            } else {
                break;
            }
        }
        self.events.push_back(BbrEvent { timestamp: now, _bytes: bytes, gap_since_last: gap });
        
        let mut max = Duration::from_millis(100);
        for e in &self.events {
            if e.gap_since_last > max { max = e.gap_since_last; }
        }
        self.max_gap = max;
    }

    fn recommended_idle_timeout(&self) -> Duration {
        let derived = self.max_gap.mul_f64(1.5);
        derived.max(Duration::from_secs(5)).min(Duration::from_secs(300))
    }
}

pub struct VdoTuner {
    enabled: bool,
    active: bool,
    zero_ratio_filter: WindowedFilter<f64>,
    last_probe: Instant,
    probe_interval: Duration,
}

impl VdoTuner {
    pub fn new(cfg_enabled: bool) -> Self {
        Self {
            enabled: cfg_enabled,
            active: cfg_enabled,
            zero_ratio_filter: WindowedFilter::new(30, FilterMode::Max),
            last_probe: Instant::now(),
            probe_interval: Duration::from_secs(30),
        }
    }

    pub fn should_check_zeros(&mut self, file_size: u64) -> bool {
        if !self.enabled { return false; }
        if !self.active && file_size > 100 * 1024 * 1024 {
             self.active = true;
             return true;
        }
        if self.active { return true; }
        if self.last_probe.elapsed() > self.probe_interval {
            return true;
        }
        false
    }

    pub fn update(&mut self, total_bytes: u64, zero_bytes: u64) {
        if !self.enabled { return; }
        if total_bytes == 0 { return; }
        let ratio = zero_bytes as f64 / total_bytes as f64;
        let now = Instant::now();
        let best_ratio = self.zero_ratio_filter.update(ratio, now);

        if self.active {
            if best_ratio < 0.01 {
                self.active = false;
                self.last_probe = now;
            }
        } else {
            if ratio > 0.01 {
                self.active = true;
                self.zero_ratio_filter.reset();
            } else {
                self.last_probe = now;
            }
        }
    }
}

pub struct BbrTuner {
    pub current_batch_size: usize,
    pub current_coalesce_bytes: u64,
    pub current_flush_ms: u64,
    pub flush_multiplier: u32,
    pub hydration_debounce: Duration,
    pub state: TunerState,
    btl_bw_filter: WindowedFilter<f64>,
    rt_prop_filter: WindowedFilter<f64>,
    smoothed_bw: EwmaFilter,
    smoothed_rtt: EwmaFilter,
    batch_size_min: usize,
    batch_size_max: usize,
    min_coalesce_floor: u64,
    max_burst_coalesce_bytes: u64,
    last_cycle: Instant,
    last_data_seen: Instant,
    history: BbrHistory,
    config_flush_ms: u64,
}

impl BbrTuner {
    pub fn new(cfg: &TargetConfig) -> Self {
        let (flush_min, _) = crate::config::get_flush_multiplier_bounds(&cfg.profile);
        
        let (min_floor, max_burst, batch_min, batch_max) = match cfg.profile {
            TargetProfile::HDD => (2 * 1024 * 1024, 64 * 1024 * 1024, 4, 32),
            TargetProfile::Network => (1 * 1024 * 1024, 32 * 1024 * 1024, 8, 128),
            TargetProfile::NVMe => (256 * 1024, 16 * 1024 * 1024, 16, 256),
            TargetProfile::SSD | TargetProfile::Auto => (128 * 1024, 8 * 1024 * 1024, 8, 64),
            TargetProfile::NFS => (512 * 1024, 16 * 1024 * 1024, 8, 128)
        };

        Self {
            current_batch_size: cfg.batch_size,
            current_coalesce_bytes: min_floor,
            current_flush_ms: cfg.worker_flush_interval_ms,
            flush_multiplier: flush_min,
            hydration_debounce: Duration::from_secs(1),
            state: TunerState::Startup,
            btl_bw_filter: WindowedFilter::new(10, FilterMode::Max),
            rt_prop_filter: WindowedFilter::new(10, FilterMode::Min),
            smoothed_bw: EwmaFilter::new(0.1),
            smoothed_rtt: EwmaFilter::new(0.1),
            batch_size_min: batch_min,
            batch_size_max: batch_max,
            min_coalesce_floor: min_floor,
            max_burst_coalesce_bytes: max_burst,
            last_cycle: Instant::now(),
            last_data_seen: Instant::now(),
            history: BbrHistory::new(300),
            config_flush_ms: cfg.worker_flush_interval_ms,
        }
    }

    /// Applies hysteresis to prevent "see-sawing" of values.
    /// Returns the new value only if it differs from the current by > threshold %.
    fn stabilize_metric(&self, current: f64, target: f64, threshold_pct: f64) -> f64 {
        let diff = (current - target).abs();
        let threshold = current * threshold_pct;
        if diff > threshold {
            target
        } else {
            current
        }
    }

    pub fn tune(&mut self, elapsed_secs: f64, bytes_processed: u64, is_stressed: bool, pending_len: usize, max_pending: usize, board: &TunerBoard, path_label: &str, chunk_size: u64) -> usize {
        let now = Instant::now();
        let gap = now.duration_since(self.last_data_seen);
        
        self.history.push(bytes_processed, gap, now);
        let idle_threshold = self.history.recommended_idle_timeout();

        // Calculate Rate
        let delivery_rate = if elapsed_secs > 0.0005 {
            bytes_processed as f64 / elapsed_secs
        } else {
            0.0
        };

        // Idle Logic
        if bytes_processed == 0 {
            if gap > idle_threshold {
                if self.state != TunerState::Startup {
                    debug!("BBR: Idle for {:.1}s > {:.1}s. Resetting.", gap.as_secs_f64(), idle_threshold.as_secs_f64());
                    self.state = TunerState::IdleReset;
                    self.btl_bw_filter.reset();
                    self.rt_prop_filter.reset();
                    self.smoothed_bw.reset();
                    self.smoothed_rtt.reset();
                }
            }
            return self.current_batch_size;
        } else {
            self.last_data_seen = now;
            if self.state == TunerState::IdleReset {
                self.state = TunerState::Startup;
            }
        }

        // BBR Updates
        if delivery_rate > 0.0 {
            self.btl_bw_filter.update(delivery_rate, now);
        }
        self.rt_prop_filter.update(elapsed_secs, now);

        let raw_bw = self.btl_bw_filter.get_best().unwrap_or(1_000_000.0);
        let raw_rtt = self.rt_prop_filter.get_best().unwrap_or(0.001); // Seconds
        
        let smooth_bw = self.smoothed_bw.update(raw_bw);
        let smooth_rtt = self.smoothed_rtt.update(raw_rtt);

        // State Machine
        if is_stressed {
            self.state = TunerState::Muted;
        } else if pending_len as f64 / max_pending as f64 > 0.8 {
            self.state = TunerState::CriticalDrain;
        } else {
            match self.state {
                TunerState::Startup => {
                    if now.duration_since(self.last_cycle) > Duration::from_secs(5) {
                        self.state = TunerState::Drain;
                        self.last_cycle = now;
                    }
                },
                TunerState::Drain => {
                    if pending_len < 2 {
                        self.state = TunerState::ProbeBW;
                        self.last_cycle = now;
                    }
                },
                TunerState::ProbeBW | TunerState::Muted => {
                    if now.duration_since(self.last_cycle) > Duration::from_secs(10) {
                        self.state = TunerState::Drain;
                        self.last_cycle = now;
                    } else {
                        self.state = TunerState::ProbeBW;
                    }
                },
                TunerState::CriticalDrain => {
                    let threshold = max_pending / 2;
                    if pending_len < threshold {
                         self.state = TunerState::Drain;
                    }
                }
                _ => {}
            }
        }

        if now.duration_since(self.last_cycle) > Duration::from_secs(5) {
            self.last_cycle = now;
        }

        let effective_state = board.get(Path::new(path_label)).map_or(self.state, |r| *r.value());

        // --- Calculate Outputs ---
        let (batch_override, coalesce_override, target_inflight_bytes) = match effective_state {
            TunerState::CriticalDrain => (1, self.min_coalesce_floor, 0),
            _ => {
                let bdp_bytes = smooth_bw * smooth_rtt;
                
                let pacing_gain = match self.state {
                    TunerState::Startup => 2.89,
                    TunerState::Drain => 0.5,
                    TunerState::ProbeBW => 1.25,
                    TunerState::Muted => 0.75,
                    _ => 1.0,
                };

                let target_inflight_bytes = (bdp_bytes * pacing_gain) as u64;
                let target_op_size = (smooth_bw * 0.002) as u64; // 2ms quantum
                let calculated_coalesce = target_op_size
                    .max(self.min_coalesce_floor)
                    .min(self.max_burst_coalesce_bytes);

                let calculated_batch = (target_inflight_bytes / calculated_coalesce.max(1)) as usize;
                
                (
                    calculated_batch.max(self.batch_size_min).min(self.batch_size_max),
                    calculated_coalesce,
                    target_inflight_bytes
                )
            }
        };

        // --- Adaptive Flush Interval Calculation with Hysteresis ---
        let rtt_ms = smooth_rtt * 1000.0;
        
        let flush_target_raw = if effective_state == TunerState::CriticalDrain || effective_state == TunerState::Drain {
            (rtt_ms / 2.0).max(1.0)
        } else if effective_state == TunerState::HighLoad {
            (rtt_ms * 2.0).max(self.config_flush_ms as f64)
        } else {
            rtt_ms.max(self.config_flush_ms as f64 / 2.0)
        };

        // Apply hysteresis: only change flush interval if different by > 15%
        let stable_flush_target = self.stabilize_metric(self.current_flush_ms as f64, flush_target_raw, 0.15);
        self.current_flush_ms = (stable_flush_target as u64).max(1).min(5000);

        // Apply hysteresis: only change batch size if different by > 10%
        let stable_batch = self.stabilize_metric(self.current_batch_size as f64, batch_override as f64, 0.10);
        self.current_batch_size = stable_batch as usize;

        self.current_coalesce_bytes = coalesce_override;
        
        self.hydration_debounce = match self.state {
            TunerState::Drain | TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => {
                 if self.min_coalesce_floor >= 1024 * 1024 {
                     Duration::from_secs(5)
                 } else {
                     Duration::from_millis(500)
                 }
            },
            _ => Duration::from_millis(100),
        };

        let target_buffer_depth = if chunk_size == 0 || target_inflight_bytes == 0 {
            4
        } else {
            let depth = (target_inflight_bytes / chunk_size).max(2) as usize;
            depth.min(self.current_batch_size).min(self.batch_size_max)
        };

        board.insert(PathBuf::from(path_label), self.state);
        
        metrics::TARGET_BATCH_SIZE.with_label_values(&[&path_label]).set((self.current_batch_size as i64) as f64);
        metrics::TARGET_COALESCE_BYTES.with_label_values(&[&path_label]).set((self.current_coalesce_bytes as i64) as f64);
        metrics::TARGET_FLUSH_INTERVAL_MS.with_label_values(&[&path_label]).set(self.current_flush_ms as f64);
        metrics::TUNER_STATE.with_label_values(&[&path_label]).set((self.state as i64) as f64);
        metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_label]).set(pending_len as f64 / max_pending as f64);

        target_buffer_depth
    }

    pub fn should_defer_maintenance(&self) -> bool {
        matches!(self.state, TunerState::Startup | TunerState::Muted | TunerState::Drain | TunerState::CriticalDrain)
    }

    pub fn calculate_version_limits(&self, cfg: &TargetConfig, avail: u64, total: u64) -> (usize, u64) {
        let label = cfg.path.to_string_lossy();
        if total == 0 { return (cfg.max_versions, cfg.max_versions_size_mb); }
        
        let usage_pct = 1.0 - (avail as f64 / total as f64);
        
        let (dyn_count, dyn_mb) = if usage_pct > 0.98 { (0.0, 0.0) }
        else if usage_pct > 0.90 { (1.0, 100.0) }
        else if usage_pct > 0.75 {
            let scale = 1.0 - ((usage_pct - 0.75) / 0.15);
            let effective_scale = scale.max(0.1);
            (cfg.max_versions as f64 * effective_scale, cfg.max_versions_size_mb as f64 * effective_scale)
        } else { (cfg.max_versions as f64, cfg.max_versions_size_mb as f64) };

        let count_final = dyn_count.floor() as usize;
        let mb_final = dyn_mb.floor() as u64;

        metrics::TARGET_DYNAMIC_VERSION_LIMIT_COUNT.with_label_values(&[&label]).set((count_final as i64) as f64);
        metrics::TARGET_DYNAMIC_VERSION_LIMIT_BYTES.with_label_values(&[&label]).set((mb_final as i64) as f64);

        (count_final, mb_final)
    }

    pub fn recommended_ring_depth(&self) -> u32 {
        (self.batch_size_max * 2).min(4096) as u32
    }
}
