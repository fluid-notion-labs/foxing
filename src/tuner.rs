use std::time::{Instant, Duration};
use std::collections::VecDeque;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use tracing::debug;
use crate::metrics;
use crate::config::{TargetConfig, get_flush_multiplier_bounds};

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
        let mut best = self.samples[0].1;
        for (_, v) in &self.samples {
            match self.mode {
                FilterMode::Min => if *v < best { best = *v; },
                FilterMode::Max => if *v > best { best = *v; },
            }
        }
        Some(best)
    }

    pub fn reset(&mut self) {
        self.samples.clear();
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
             debug!("Vdo Tuner: Waking up immediately for large file ({} bytes)", file_size);
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
                debug!("Vdo Tuner: Efficiency low ({:.2}%), entering Backoff/Sleep.", best_ratio * 100.0);
                self.active = false;
                self.last_probe = now;
            }
        } else {
            if ratio > 0.01 {
                debug!("Vdo Tuner: Probe successful ({:.2}%), Waking Up.", ratio * 100.0);
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
    pub flush_multiplier: u32,
    pub hydration_debounce: Duration,
    pub state: TunerState,
    btl_bw_filter: WindowedFilter<f64>,
    rt_prop_filter: WindowedFilter<f64>,
    batch_size_min: usize,
    batch_size_max: usize,
    min_coalesce_floor: u64,
    max_burst_coalesce_bytes: u64,
    last_cycle: Instant,
    last_data_seen: Instant,
}

impl BbrTuner {
    pub fn new(cfg: &TargetConfig) -> Self {
        let (flush_min, _) = get_flush_multiplier_bounds(&cfg.profile);
        let (min_floor, max_burst, batch_min, batch_max) = match cfg.profile {
            crate::config::TargetProfile::HDD => {
                (2 * 1024 * 1024, 64 * 1024 * 1024, 4, 32)
            },
            crate::config::TargetProfile::Network => {
                (1 * 1024 * 1024, 32 * 1024 * 1024, 8, 128)
            },
            crate::config::TargetProfile::NVMe => {
                (256 * 1024, 16 * 1024 * 1024, 16, 256)
            },
            crate::config::TargetProfile::SSD | _ => {
                (128 * 1024, 8 * 1024 * 1024, 8, 64)
            },
        };
        Self {
            current_batch_size: cfg.batch_size,
            current_coalesce_bytes: min_floor,
            flush_multiplier: flush_min,
            hydration_debounce: Duration::from_secs(1),
            state: TunerState::Startup,
            btl_bw_filter: WindowedFilter::new(10, FilterMode::Max),
            rt_prop_filter: WindowedFilter::new(10, FilterMode::Min),
            batch_size_min: batch_min,
            batch_size_max: batch_max,
            min_coalesce_floor: min_floor,
            max_burst_coalesce_bytes: max_burst,
            last_cycle: Instant::now(),
            last_data_seen: Instant::now(),
        }
    }

    pub fn tune(&mut self, elapsed_secs: f64, bytes_processed: u64, is_stressed: bool, pending_len: usize, max_pending: usize, board: &TunerBoard, path_label: &str) {
        let now = Instant::now();
        let delivery_rate = if elapsed_secs > 0.0005 {
            bytes_processed as f64 / elapsed_secs
        } else {
            0.0
        };
        if bytes_processed == 0 {
            if now.duration_since(self.last_data_seen) > Duration::from_secs(30) {
                if self.state != TunerState::Startup {
                    debug!("BBR: Connection idle for 3est. Resetting to Startup/Probe mode.");
                    self.state = TunerState::IdleReset;
                    self.btl_bw_filter.reset();
                    self.rt_prop_filter.reset();
                }
            }
            return;
        } else {
            self.last_data_seen = now;
            if self.state == TunerState::IdleReset {
                self.state = TunerState::Startup;
            }
        }
        if delivery_rate > 0.0 {
            self.btl_bw_filter.update(delivery_rate, now);
        }
        self.rt_prop_filter.update(elapsed_secs, now);
        let btl_bw = self.btl_bw_filter.get_best().unwrap_or(1_000_000.0);
        let rt_prop = self.rt_prop_filter.get_best().unwrap_or(0.001);
        if is_stressed {
            self.state = TunerState::Muted;
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
                         debug!("Critical drain lifted (Buffer: {}/{}). Resuming Drain.", pending_len, max_pending);
                         self.state = TunerState::Drain;
                    }
                }
                _ => {}
            }
        }
        let effective_state = board.get(Path::new(path_label)).map_or(self.state, |r| *r.value());
        let (batch_override, coalesce_override) = match effective_state {
            TunerState::CriticalDrain => {
                (1, self.min_coalesce_floor)
            },
            _ => {
                let bdp_bytes = btl_bw * rt_prop;
                let pacing_gain = match self.state {
                    TunerState::Startup => 2.89,
                    TunerState::Drain => 0.5,
                    TunerState::ProbeBW => 1.25,
                    TunerState::Muted => 0.75,
                    _ => 1.0,
                };
                let target_inflight_bytes = (bdp_bytes * pacing_gain) as u64;
                let target_op_size = (btl_bw * 0.002) as u64;
                let calculated_coalesce = target_op_size
                    .max(self.min_coalesce_floor)
                    .min(self.max_burst_coalesce_bytes);
                let calculated_batch = (target_inflight_bytes / calculated_coalesce.max(1)) as usize;
                (
                    calculated_batch.max(self.batch_size_min).min(self.batch_size_max),
                    calculated_coalesce
                )
            }
        };
        self.current_batch_size = batch_override;
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
        board.insert(PathBuf::from(path_label), self.state);
        metrics::TARGET_BATCH_SIZE.with_label_values(&[&path_label]).set(self.current_batch_size as i64);
        metrics::TARGET_COALESCE_BYTES.with_label_values(&[&path_label]).set(self.current_coalesce_bytes as i64);
        metrics::TUNER_STATE.with_label_values(&[&path_label]).set(self.state as i64);
        metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_label]).set(pending_len as f64 / max_pending as f64);
    }

    pub fn should_defer_maintenance(&self) -> bool {
        matches!(self.state, TunerState::Startup | TunerState::Muted | TunerState::Drain | TunerState::CriticalDrain)
    }

    pub fn calculate_version_limits(&self, cfg: &TargetConfig, avail: u64, total: u64) -> (usize, u64) {
        let label = cfg.path.to_string_lossy();
        if total == 0 { return (cfg.max_versions, cfg.max_versions_size_mb); }
        let usage_pct = 1.0 - (avail as f64 / total as f64);
        let max_versions = cfg.max_versions as f64;
        let max_mb = cfg.max_versions_size_mb as f64;
        let (dyn_count, dyn_mb) = if usage_pct > 0.98 { (0.0, 0.0) }
        else if usage_pct > 0.90 { (1.0, 100.0) }
        else if usage_pct > 0.75 {
            let scale = 1.0 - ((usage_pct - 0.75) / 0.15);
            let effective_scale = scale.max(0.1);
            (max_versions * effective_scale, max_mb * effective_scale)
        } else { (max_versions, max_mb) };
        let count_final = dyn_count.floor() as usize;
        let mb_final = dyn_mb.floor() as u64;
        metrics::TARGET_DYNAMIC_VERSION_LIMIT_COUNT.with_label_values(&[&label]).set(count_final as i64);
        metrics::TARGET_DYNAMIC_VERSION_LIMIT_BYTES.with_label_values(&[&label]).set(mb_final as i64);
        (count_final, mb_final)
    }
}
