use std::time::{Instant, Duration};
use std::collections::VecDeque;
use std::sync::Arc;
use std::path::{PathBuf};
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use crate::config::{TargetConfig, TargetProfile};
use tracing::{debug, warn};
use fxcp_core::constants;
use std::sync::atomic::Ordering;
use lazy_static::lazy_static;

pub type TunerBoard = Arc<DashMap<PathBuf, TunerState>>;

lazy_static! {
    // FIXED: Use Arc<TunerOutput> to ensure atomic snapshots during iteration
    pub static ref GLOBAL_TUNER_REGISTRY: DashMap<(String, usize), Arc<TunerOutput>> = DashMap::new();
}

pub fn check_global_consistency() {
    let mut targets: std::collections::HashMap<String, Vec<(usize, TunerOutput)>> = std::collections::HashMap::new();
    for r in GLOBAL_TUNER_REGISTRY.iter() {
        let (path, worker_id) = r.key();
        let output = r.value().as_ref().clone();
        targets.entry(path.clone()).or_default().push((*worker_id, output));
    }

    for (path, workers) in targets {
        if workers.len() < 2 { continue; }
        
        let min_batch = workers.iter().map(|(_, o)| o.batch_size).min().unwrap_or(0);
        let max_batch = workers.iter().map(|(_, o)| o.batch_size).max().unwrap_or(0);
        
        if max_batch > 64 && max_batch > min_batch * 2 {
            warn!("Global Tuner Sanity Check: Significant batch size divergence for target {}. Range: {} - {}. Possible straggler worker.",
                  path, min_batch, max_batch);
        }
        
        let distinct_states: std::collections::HashSet<_> = workers.iter().map(|(_, o)| o.state).collect();
        if distinct_states.len() > 1 {
             debug!("Global Tuner Info: Mixed tuner states for target {}: {:?}", path, distinct_states);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum TunerState {
    Startup = 0,
    Drain = 1,
    ProbeBW = 2,
    Muted = 3,
    Steady = 8,
    Conservative = 99,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageClass {
    Unknown = 0,
    HDD = 1,
    SataSsd = 2,
    NVMe = 3,
    ThrottledNVMe = 4,
}

#[derive(Debug, Clone)]
pub struct TunerOutput {
    pub batch_size: usize,
    pub coalesce_bytes: u64,
    pub flush_us: u64,
    pub state: TunerState,
    pub storage_class: StorageClass,
    pub bdp_bytes: u64,
    pub estimated_bw: u64,
    pub segment_stall_timeout_secs: u64,
    pub segment_overall_timeout_secs: u64,
    pub postcopy_timeout_secs: u64,
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
}

pub struct WindowedFilter<T> {
    window_duration: Duration,
    samples: VecDeque<(Instant, T)>,
    mode: FilterMode,
}

pub enum FilterMode {
    Min,
    Max
}

impl<T: PartialOrd + Copy + std::fmt::Display> WindowedFilter<T> {
    pub fn new(window_secs: u64, mode: FilterMode) -> Self {
        Self {
            window_duration: Duration::from_secs(window_secs),
            samples: VecDeque::new(),
            mode,
        }
    }

    pub fn update(&mut self, val: T, now: Instant) -> T {
        self.prune(now);
        const MAX_SAMPLES: usize = 1000;
        while self.samples.len() >= MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back((now, val));
        self.get_best().unwrap_or(val)
    }

    pub fn prune(&mut self, now: Instant) {
        while let Some((time, _)) = self.samples.front() {
            if now.duration_since(*time) > self.window_duration {
                self.samples.pop_front();
            } else {
                break;
            }
        }
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

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

#[derive(Debug, Default)]
struct AggregatedSample {
    bytes: u64,
    ops: u64,
    duration: Duration,
}

pub struct BbrTuner {
    pub current_batch_size: usize,
    pub current_coalesce_bytes: u64,
    pub current_flush_us: u64,
    pub hydration_debounce: Duration,
    pub state: TunerState,
    pub storage_class: StorageClass,
    pub path_label: String,
    last_transition: Instant,
    last_probe: Instant,
    btl_bw_filter: WindowedFilter<f64>,
    max_iops_filter: WindowedFilter<f64>,
    rt_prop_filter: WindowedFilter<f64>,
    smoothed_bw: EwmaFilter,
    smoothed_rtt: EwmaFilter,
    short_term_latency: EwmaFilter,
    batch_size_min: usize,
    batch_size_max: usize,
    original_batch_max: usize,
    min_coalesce_floor: u64,
    max_burst_coalesce_bytes: u64,
    last_cycle: Instant,
    last_data_seen: Instant,
    target_bw_bytes: Option<u64>,
    target_iops: Option<u64>,
    profile: TargetProfile,
    global_memory_limit_bytes: u64,
    is_conservative: bool,
    pending_sample: AggregatedSample,
    last_rt_prop: f64,
}

impl BbrTuner {
    pub fn new(
        cfg: &TargetConfig,
        initial_rtt: Duration,
        global_memory_limit_bytes: u64,
        qgroups_enabled: bool,
    ) -> Self {
        let (min_floor, max_burst, batch_min, batch_max) = match cfg.profile {
            TargetProfile::HDD => constants::PROFILE_HDD_VALUES,
            TargetProfile::Network | TargetProfile::NFS => constants::PROFILE_NETWORK_VALUES,
            TargetProfile::NVMe => constants::PROFILE_NVME_VALUES,
            // FIXED: Reduced Auto/SSD startup values to prevent USB stall
            TargetProfile::SSD | TargetProfile::Auto => (64 * 1024, 4 * 1024 * 1024, 4, 32),
            TargetProfile::SdCard => constants::PROFILE_SDCARD_VALUES,
        };

        let target_bw_bytes = cfg.target_bandwidth_mbps.map(|mbps| mbps * 1024 * 1024);
        
        let mut smoothed_rtt = EwmaFilter::new(constants::TUNER_EWMA_ALPHA);
        let rtt_secs = initial_rtt.as_secs_f64();
        smoothed_rtt.update(rtt_secs);
        
        let mut short_term_latency = EwmaFilter::new(0.3);
        short_term_latency.update(rtt_secs);

        let mut initial_state = TunerState::Startup;
        let mut conservative = false;
        if qgroups_enabled && cfg.enable_versioning {
            warn!("Btrfs Quotas detected. Switching to Conservative Mode (Throttling active).");
            initial_state = TunerState::Conservative;
            conservative = true;
        }

        Self {
            current_batch_size: if conservative { 4 } else { cfg.batch_size },
            current_coalesce_bytes: min_floor,
            current_flush_us: cfg.worker_flush_interval_us,
            hydration_debounce: Duration::from_micros(constants::TUNER_HYDRATION_DEBOUNCE_US),
            state: initial_state,
            storage_class: StorageClass::Unknown,
            path_label: cfg.label.to_string(),
            last_transition: Instant::now(),
            last_probe: Instant::now(),
            btl_bw_filter: WindowedFilter::new(constants::TUNER_WINDOW_SECS, FilterMode::Max),
            max_iops_filter: WindowedFilter::new(constants::TUNER_WINDOW_SECS, FilterMode::Max),
            rt_prop_filter: WindowedFilter::new(constants::TUNER_WINDOW_SECS, FilterMode::Min),
            smoothed_bw: EwmaFilter::new(constants::TUNER_EWMA_ALPHA),
            smoothed_rtt,
            short_term_latency,
            batch_size_min: batch_min,
            batch_size_max: batch_max,
            original_batch_max: batch_max,
            min_coalesce_floor: min_floor,
            max_burst_coalesce_bytes: max_burst,
            last_cycle: Instant::now(),
            last_data_seen: Instant::now(),
            target_bw_bytes,
            target_iops: cfg.target_iops,
            profile: cfg.profile,
            global_memory_limit_bytes,
            is_conservative: conservative,
            pending_sample: AggregatedSample::default(),
            last_rt_prop: rtt_secs.max(0.000001),
        }
    }

    pub fn recommended_ring_depth(&self) -> u32 {
        (self.original_batch_max * 4).clamp(256, 32768) as u32
    }

    fn transition_to(&mut self, new_state: TunerState) -> bool {
        if self.is_conservative {
            if self.state != TunerState::Conservative {
                self.state = TunerState::Conservative;
                return true;
            }
            return false;
        }

        if self.state != new_state {
            let min_duration = if new_state == TunerState::Muted {
                Duration::ZERO
            } else if self.state == TunerState::Muted {
                Duration::from_millis(500)
            } else {
                Duration::from_micros(constants::TUNER_STATE_TRANSITION_DELAY_US)
            };

            if self.last_transition.elapsed() < min_duration {
                return false;
            }

            debug!("Tuner: Transition {:?} -> {:?}", self.state, new_state);
            self.state = new_state;
            self.last_transition = Instant::now();
            return true;
        }
        false
    }

    #[allow(clippy::too_many_arguments)]
    pub fn tune(
        &mut self,
        elapsed_secs: f64,
        bytes_processed: u64,
        ops_processed: u64,
        stress_score: f64,
        pending_len: usize,
        _max_pending: usize,
        chunk_size: u64,
        memory_usage_pct: f64,
        latency_sample: Duration,
        max_latency_sample: Duration,
        current_global_usage: u64,
        avg_event_size: u64,
        quotas_active: bool,
    ) -> TunerOutput {
        let now = Instant::now();

        if self.is_conservative && self.last_probe.elapsed() > Duration::from_secs(300) {
            if !quotas_active {
                debug!("Tuner: Quotas disabled detected. Exiting Conservative mode.");
                self.is_conservative = false;
                self.state = TunerState::Startup;
            }
            self.last_probe = now;
        }

        self.pending_sample.bytes += bytes_processed;
        self.pending_sample.ops += ops_processed;
        self.pending_sample.duration += Duration::from_secs_f64(elapsed_secs);

        let min_sample_duration = Duration::from_millis(10);
        let should_commit = self.pending_sample.duration >= min_sample_duration 
            || (bytes_processed > 0 && self.pending_sample.duration > Duration::from_micros(500));

        if should_commit {
            let sample_duration_secs = self.pending_sample.duration.as_secs_f64();
            
            if self.pending_sample.bytes > 0 && sample_duration_secs > 0.0 {
                let delivery_rate_bytes = self.pending_sample.bytes as f64 / sample_duration_secs;
                self.btl_bw_filter.update(delivery_rate_bytes, now);
                self.last_data_seen = now;
            } else {
                self.btl_bw_filter.prune(now);
            }

            if self.pending_sample.ops > 0 && sample_duration_secs > 0.0 {
                let delivery_rate_iops = self.pending_sample.ops as f64 / sample_duration_secs;
                self.max_iops_filter.update(delivery_rate_iops, now);
                self.last_data_seen = now;
            } else {
                self.max_iops_filter.prune(now);
            }
            self.pending_sample = AggregatedSample::default();
        }

        let gap = now.duration_since(self.last_data_seen);
        if gap > Duration::from_secs(constants::TUNER_IDLE_TIMEOUT_SECS) && self.state != TunerState::Startup && !self.is_conservative {
             if pending_len == 0 {
                 self.transition_to(TunerState::Startup);
                 self.btl_bw_filter.reset();
                 self.max_iops_filter.reset();
                 self.rt_prop_filter.reset();
                 self.storage_class = StorageClass::Unknown;
             }
        }

        let rtt_secs = latency_sample.as_secs_f64();
        if rtt_secs > 0.0 {
            self.rt_prop_filter.update(rtt_secs, now);
            self.short_term_latency.update(max_latency_sample.as_secs_f64());
        } else {
            self.rt_prop_filter.prune(now);
        }

        let btl_bw = self.btl_bw_filter.get_best().unwrap_or(1_000_000.0);
        let max_iops = self.max_iops_filter.get_best().unwrap_or(100.0);
        
        if let Some(best) = self.rt_prop_filter.get_best() {
            self.last_rt_prop = best;
        }
        let rt_prop = self.last_rt_prop;
        let current_latency = self.short_term_latency.value;

        let mut cluster_max_bw = 0.0;
        if !self.is_conservative {
            for r in GLOBAL_TUNER_REGISTRY.iter() {
                if r.key().0 == self.path_label {
                    // FIXED: Read from Arc to prevent torn reads
                    let output = r.value(); 
                    let peer_bw = output.estimated_bw as f64;
                    if peer_bw > cluster_max_bw {
                        cluster_max_bw = peer_bw;
                    }
                }
            }
        }

        let effective_bw = if let Some(target) = self.target_bw_bytes {
            target as f64
        } else {
            let local_estimate = self.smoothed_bw.update(btl_bw);
            if cluster_max_bw > 0.0 && local_estimate < cluster_max_bw * 0.5 {
                cluster_max_bw * 0.5
            } else {
                local_estimate
            }
        };

        let smooth_rtt = self.smoothed_rtt.update(rt_prop);

        if self.state != TunerState::Startup || (now.duration_since(self.last_cycle).as_secs_f64() > 0.5 && rt_prop > 0.0) {
             let rtt_us = rt_prop * 1_000_000.0;
             let is_spiking_hard = current_latency > (rt_prop * 6.0) && current_latency > 0.010;
             
             let new_class = if is_spiking_hard && rtt_us < 200.0 {
                 StorageClass::ThrottledNVMe
             } else if rtt_us < 200.0 {
                 StorageClass::NVMe
             } else if rtt_us > 5000.0 {
                 StorageClass::HDD
             } else {
                 StorageClass::SataSsd
             };

             if self.storage_class != StorageClass::Unknown && new_class != self.storage_class {
                 if new_class == StorageClass::ThrottledNVMe || new_class == StorageClass::HDD {
                     if self.storage_class == StorageClass::NVMe || self.storage_class == StorageClass::SataSsd {
                        debug!("Tuner: Storage Class downgraded ({:?} -> {:?}). Resetting BtlBw.", self.storage_class, new_class);
                        self.btl_bw_filter.reset();
                        self.max_iops_filter.reset();
                        self.smoothed_bw = EwmaFilter::new(constants::TUNER_EWMA_ALPHA);
                     }
                 }
                 
                 if self.profile == TargetProfile::Auto && !self.is_conservative {
                     match new_class {
                         StorageClass::NVMe | StorageClass::ThrottledNVMe => {
                             self.batch_size_max = constants::PROFILE_NVME_VALUES.3;
                             self.max_burst_coalesce_bytes = constants::PROFILE_NVME_VALUES.1;
                             self.original_batch_max = self.batch_size_max;
                         },
                         _ => {
                         }
                     }
                 }
             }
             self.storage_class = new_class;
        }

        let is_latency_spiking = self.storage_class == StorageClass::ThrottledNVMe || 
                                (current_latency > (rt_prop * 4.0) && current_latency > 0.005);

        let is_one_shot = constants::ONE_SHOT_MODE.load(Ordering::Relaxed);
        let stress_limit = if is_one_shot { 2.0 } else { 1.0 };
        let caution_threshold = if is_one_shot { 1.5 } else { 0.8 };
        const RECOVERY_THRESHOLD: f64 = 0.6;

        if stress_score >= stress_limit {
            self.transition_to(TunerState::Muted);
        } else if stress_score > caution_threshold {
            if self.state == TunerState::ProbeBW {
                self.transition_to(TunerState::Drain);
            }
        } else if is_latency_spiking {
            if self.state != TunerState::Muted && self.state != TunerState::Conservative {
                debug!("Tuner: Latency Spike Detected ({:.2}ms vs base {:.2}ms). Forcing Drain.", current_latency * 1000.0, rt_prop * 1000.0);
                self.transition_to(TunerState::Drain);
                self.last_cycle = now;
            }
        } else {
            if self.is_conservative {
                self.transition_to(TunerState::Conservative);
            } else {
                match self.state {
                    TunerState::Startup => {
                        let startup_limit = if is_one_shot { Duration::from_secs(10) } else { Duration::from_secs(constants::TUNER_STARTUP_DURATION_SECS) };
                        let pending_limit = (self.batch_size_max * 2).max(100);
                        let min_samples_collected = self.btl_bw_filter.sample_count() >= 3;
                        let time_up = now.duration_since(self.last_cycle) > startup_limit;
                        let queue_full = pending_len > pending_limit;

                        // Forced transition when Startup takes too long without bandwidth data
                        // Handles ENOENT storms where no copies succeed
                        let forced_timeout = now.duration_since(self.last_cycle) > startup_limit * 3;

                        if (time_up && min_samples_collected) || (queue_full && min_samples_collected) || forced_timeout {
                            if self.transition_to(TunerState::Drain) {
                                self.last_cycle = now;
                                if forced_timeout && !min_samples_collected {
                                    warn!("Tuner: Forced Startup→Drain after {:.1}s with insufficient bandwidth samples",
                                          startup_limit.as_secs_f64() * 3.0);
                                }
                            }
                        }
                    },
                    TunerState::Drain => {
                        if stress_score < RECOVERY_THRESHOLD {
                            let drain_floor = if is_one_shot { 16 } else { 2 };
                            let drain_timeout = Duration::from_secs(1);
                            
                            if pending_len < drain_floor || now.duration_since(self.last_cycle) > drain_timeout {
                                if self.transition_to(TunerState::ProbeBW) {
                                    self.last_cycle = now;
                                }
                            }
                        }
                    },
                    TunerState::ProbeBW | TunerState::Muted => {
                        if now.duration_since(self.last_cycle) > Duration::from_secs(constants::TUNER_DRAIN_TIMEOUT_SECS) {
                            if self.transition_to(TunerState::Drain) {
                                self.last_cycle = now;
                            }
                        } else if stress_score < RECOVERY_THRESHOLD && self.state == TunerState::Muted {
                            self.transition_to(TunerState::ProbeBW);
                        }
                    },
                    _ => {}
                }
            }
        }

        let bdp_bytes = effective_bw * smooth_rtt;
        let bdp_iops = max_iops * smooth_rtt;
        let avg_chunk = if chunk_size > 0 { chunk_size as f64 } else { 4096.0 };
        let bdp_bytes_from_iops = bdp_iops * avg_chunk;
        
        let min_inflight_floor = if is_one_shot {
             32.0 * 1024.0 * 1024.0
        } else if self.storage_class == StorageClass::NVMe || self.storage_class == StorageClass::SataSsd || self.profile == TargetProfile::NVMe {
             chunk_size.max(4096) as f64 * 8.0
        } else {
             chunk_size.max(4096) as f64 * 2.0
        };

        let effective_bdp = if bdp_bytes < min_inflight_floor {
             min_inflight_floor
        } else {
             if bdp_bytes_from_iops > 0.0 && bdp_bytes_from_iops < bdp_bytes {
                 bdp_bytes_from_iops
             } else {
                 bdp_bytes
             }
        };

        let mut pacing_gain = match self.state {
            TunerState::Startup => constants::TUNER_BDP_PACING_GAIN_STARTUP,
            TunerState::Drain => constants::TUNER_BDP_PACING_GAIN_DRAIN,
            TunerState::ProbeBW => constants::TUNER_BDP_PACING_GAIN_PROBE,
            TunerState::Muted => constants::TUNER_BDP_PACING_GAIN_MUTED,
            TunerState::Conservative => 0.5,
            _ => 1.0,
        };

        if self.state == TunerState::Steady && !self.is_conservative && stress_score < caution_threshold {
            pacing_gain = 1.25;
        }

        if stress_score < caution_threshold && !self.is_conservative && !is_latency_spiking {
            if is_one_shot {
                pacing_gain = 4.0;
                if self.state == TunerState::ProbeBW { pacing_gain = 6.0; }
                if self.state == TunerState::Drain { pacing_gain = 2.0; }
            } else {
                if self.state == TunerState::ProbeBW { pacing_gain = 1.5; }
            }
        }

        if stress_score > caution_threshold {
            let range = stress_limit - caution_threshold;
            let excess = stress_score - caution_threshold;
            let ratio = (excess / range).clamp(0.0, 1.0);
            let penalty = ratio * 0.5;
            pacing_gain *= 1.0 - penalty;
        }

        if memory_usage_pct > constants::TUNER_MEMORY_PRESSURE_THRESHOLD {
            pacing_gain *= 0.5;
        }
        
        if is_latency_spiking {
            pacing_gain *= 0.5;
        }

        let target_inflight_bytes = (effective_bdp * pacing_gain) as u64;
        
        let latency_factor = if is_one_shot {
             10.0
        } else if rt_prop < 0.0002 {
             0.001
        } else {
             0.005
        };
        let target_op_size_bw = (effective_bw * latency_factor) as u64;
        
        let calculated_coalesce = if is_one_shot {
             32 * 1024 * 1024
        } else {
             target_op_size_bw
                .max(self.min_coalesce_floor)
                .min(self.max_burst_coalesce_bytes)
        };

        let calculated_batch = (target_inflight_bytes / chunk_size.max(1)) as usize;
        let final_batch = if let Some(iops_limit) = self.target_iops {
             let iops_cap = (iops_limit as f64 * smooth_rtt).ceil() as usize;
             calculated_batch.min(iops_cap)
        } else {
             calculated_batch
        };

        let alpha = if is_latency_spiking { 0.5 } else { 0.2 };
        let new_batch_size = (self.current_batch_size as f64 * (1.0 - alpha) + final_batch as f64 * alpha) as usize;
        
        let mut final_clamped_batch = new_batch_size;
        
        let batch_mem_cost_bytes = final_clamped_batch as u64 * avg_event_size;
        let memory_safety_limit = (self.global_memory_limit_bytes as f64 * constants::TUNER_MEMORY_PRESSURE_THRESHOLD) as u64;
        
        if current_global_usage + batch_mem_cost_bytes > memory_safety_limit {
            let remaining_budget = memory_safety_limit.saturating_sub(current_global_usage);
            let max_allowed_batch = (remaining_budget / avg_event_size.max(1)).min(self.batch_size_max as u64) as usize;
            if final_clamped_batch > max_allowed_batch {
                final_clamped_batch = max_allowed_batch.max(self.batch_size_min);
            }
        }

        self.current_batch_size = final_clamped_batch.max(self.batch_size_min).min(self.batch_size_max);
        self.current_coalesce_bytes = calculated_coalesce;

        let rtt_us_smooth = smooth_rtt * 1_000_000.0;
        let flush_target_us = if self.state == TunerState::Muted {
            (rtt_us_smooth * 8.0).max(1000.0)
        } else if self.state == TunerState::Conservative {
            (rtt_us_smooth * 4.0).max(5000.0)
        } else {
            (rtt_us_smooth * 1.5).max(constants::TUNER_MIN_FLUSH_US as f64)
        };
        
        self.current_flush_us = (flush_target_us as u64)
            .max(constants::TUNER_MIN_FLUSH_US)
            .min(constants::TUNER_MAX_FLUSH_US);

        let (stall_timeout, overall_timeout, postcopy_timeout) =
            self.compute_adaptive_timeouts(smooth_rtt, effective_bw);

        TunerOutput {
            batch_size: self.current_batch_size,
            coalesce_bytes: self.current_coalesce_bytes,
            flush_us: self.current_flush_us,
            state: self.state,
            storage_class: self.storage_class,
            bdp_bytes: target_inflight_bytes,
            estimated_bw: effective_bw as u64,
            segment_stall_timeout_secs: stall_timeout,
            segment_overall_timeout_secs: overall_timeout,
            postcopy_timeout_secs: postcopy_timeout,
        }
    }

    fn compute_adaptive_timeouts(&self, smooth_rtt: f64, effective_bw: f64) -> (u64, u64, u64) {
        // Segment stall timeout: base + rtt_multiplier * smooth_rtt
        let (base_stall, rtt_mult_stall) = match self.storage_class {
            StorageClass::NVMe => (5u64, 10_000u64),
            StorageClass::SataSsd => (15, 5_000),
            StorageClass::HDD => (30, 2_000),
            StorageClass::ThrottledNVMe => (30, 10_000),
            StorageClass::Unknown => (60, 1_000),
        };
        let segment_stall = (base_stall as f64 + rtt_mult_stall as f64 * smooth_rtt)
            .clamp(10.0, 300.0) as u64;

        // Segment overall timeout: BDP-based with storage class max
        let max_overall = match self.storage_class {
            StorageClass::NVMe => 120u64,
            StorageClass::SataSsd => 300,
            StorageClass::HDD => 600,
            StorageClass::ThrottledNVMe => 300,
            StorageClass::Unknown => 600,
        };
        let bdp = effective_bw * smooth_rtt;
        let bw_safe = effective_bw.max(1.0);
        let segment_overall = (bdp / bw_safe * 10.0)
            .clamp(segment_stall as f64 * 3.0, max_overall as f64) as u64;

        // Post-copy metadata timeout: higher multiplier for Merkle computation
        let (base_post, rtt_mult_post) = match self.storage_class {
            StorageClass::NVMe => (10u64, 50_000u64),
            StorageClass::SataSsd => (30, 50_000),
            StorageClass::HDD => (60, 50_000),
            StorageClass::ThrottledNVMe => (60, 50_000),
            StorageClass::Unknown => (300, 10_000),
        };
        let postcopy = (base_post as f64 + rtt_mult_post as f64 * smooth_rtt)
            .clamp(10.0, 600.0) as u64;

        (segment_stall, segment_overall, postcopy)
    }
}
