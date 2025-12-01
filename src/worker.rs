//! # Worker Module
//! 
//! The worker is the core I/O executor. It consumes events from the ordering buffer,
//! applies them to the target filesystem, and manages consistency via locking.
//! It features a BBR-inspired congestion control mechanism to optimize throughput.

use std::{
    sync::{Arc, atomic::{AtomicBool, Ordering}}, 
    collections::{HashMap, VecDeque}, 
    time::{Instant, Duration}, 
    ops::Sub
};
use tokio::{sync::mpsc, task::spawn_blocking};
use io_uring::IoUring;
use std::os::unix::io::AsRawFd;
use std::os::unix::fs::MetadataExt;
use crate::{event::{Event, EventType}, mirror::{SourceInfo, SharedConfig}, config::{TargetConfig, get_flush_multiplier_bounds}, buffer::AlignedBuffer, security, sidecar, identity, ordering, Result, versioning, governor::Governor};
use crate::operations::SmartCopier; 
use tokio::time::interval; 
use tracing::{warn, info, error, debug}; 
use crate::error::FoxingError; 
use std::io;
use crate::metrics;
use parking_lot::Mutex; 
use lru::LruCache; 
use std::path::PathBuf;
use crate::config::{MAX_FAILURE_BACKOFF, ERROR_LIMITER_SECS};
use nix::sys::statvfs::statvfs;
use dashmap::DashMap;
use futures::StreamExt;
use serde::{Serialize, Deserialize};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::time::sleep;

pub type TunerBoard = Arc<DashMap<PathBuf, TunerState>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TunerState { 
    Startup = 0,      
    Drain = 1,        
    ProbeBW = 2,      
    Muted = 3,        
    IdleReset = 4,
    SpacePressure = 6 
}

// ... (WindowedFilter, VdoTuner, ErrorLimiter, CircuitBreaker, FailureState, ShardedLockCache, DirtyEntry, BbrTuner implementations remain unchanged)
// They are included here for compilation completeness.
struct WindowedFilter<T> {
    window_duration: Duration,
    samples: VecDeque<(Instant, T)>,
    mode: FilterMode,
}

enum FilterMode { Min, Max }

impl<T: PartialOrd + Copy + std::fmt::Display> WindowedFilter<T> {
    fn new(window_secs: u64, mode: FilterMode) -> Self {
        Self {
            window_duration: Duration::from_secs(window_secs),
            samples: VecDeque::new(),
            mode,
        }
    }

    fn update(&mut self, val: T, now: Instant) -> T {
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
    
    fn get_best(&self) -> Option<T> {
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
    
    fn reset(&mut self) {
        self.samples.clear();
    }
}

struct VdoTuner {
    enabled: bool,
    active: bool,
    zero_ratio_filter: WindowedFilter<f64>,
    last_probe: Instant,
    probe_interval: Duration,
}

impl VdoTuner {
    fn new(cfg_enabled: bool) -> Self {
        Self {
            enabled: cfg_enabled,
            active: cfg_enabled, 
            zero_ratio_filter: WindowedFilter::new(30, FilterMode::Max), 
            last_probe: Instant::now(),
            probe_interval: Duration::from_secs(30),
        }
    }

    fn should_check_zeros(&mut self, file_size: u64) -> bool {
        if !self.enabled { return false; }
        if !self.active && file_size > 100 * 1024 * 1024 {
             self.active = true;
             debug!("VDO Tuner: Waking up immediately for large file ({} bytes)", file_size);
             return true;
        }
        if self.active { return true; }
        if self.last_probe.elapsed() > self.probe_interval {
            return true; 
        }
        false
    }

    fn update(&mut self, total_bytes: u64, zero_bytes: u64) {
        if !self.enabled { return; }
        if total_bytes == 0 { return; }

        let ratio = zero_bytes as f64 / total_bytes as f64;
        let now = Instant::now();
        let best_ratio = self.zero_ratio_filter.update(ratio, now);

        if self.active {
            if best_ratio < 0.01 {
                debug!("VDO Tuner: Efficiency low ({:.2}%), entering Backoff/Sleep.", best_ratio * 100.0);
                self.active = false;
                self.last_probe = now;
            }
        } else {
            if ratio > 0.01 {
                debug!("VDO Tuner: Probe successful ({:.2}%), Waking Up.", ratio * 100.0);
                self.active = true;
                self.zero_ratio_filter.reset();
            } else {
                self.last_probe = now; 
            }
        }
    }
}

struct ErrorLimiter { last: Mutex<HashMap<&'static str, Instant>> }
impl ErrorLimiter {
    fn new() -> Self { Self { last: Mutex::new(HashMap::new()) } }
    fn check(&self, key: &'static str) -> bool {
        let mut map = self.last.lock();
        let now = Instant::now();
        let entry = map.entry(key).or_insert(now.sub(Duration::from_secs(ERROR_LIMITER_SECS + 1))); 
        if now.duration_since(*entry) < Duration::from_secs(ERROR_LIMITER_SECS) { return false; }
        *entry = now;
        true
    }
}

struct CircuitBreaker { tripped: AtomicBool, last_check: Mutex<Instant>, interval: Duration }
impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self { tripped: AtomicBool::new(self.tripped.load(Ordering::Relaxed)), last_check: Mutex::new(*self.last_check.lock()), interval: self.interval }
    }
}
impl CircuitBreaker {
    fn new(interval_secs: u64) -> Self { Self { tripped: AtomicBool::new(false), last_check: Mutex::new(Instant::now()), interval: Duration::from_secs(interval_secs) } }
    fn can_proceed(&self, path: &std::path::Path, threshold: u64) -> bool {
        let mut last = self.last_check.lock();
        let now = Instant::now();
        if self.tripped.load(Ordering::Relaxed) {
            if now.duration_since(*last) < self.interval { return false; }
            if security::check_capacity(path, threshold) { self.tripped.store(false, Ordering::Relaxed); *last = now; return true; }
            *last = now; return false;
        }
        if !security::check_capacity(path, threshold) { self.tripped.store(true, Ordering::Relaxed); *last = now; return false; }
        true
    }
    fn trip(&self) { self.tripped.store(true, Ordering::Relaxed); }
}

#[derive(Debug)]
struct FailureState { is_failed: AtomicBool, last_failure: Instant, retry_interval: Duration, hibernation_threshold: Duration, max_backoff: Duration }
impl Clone for FailureState {
    fn clone(&self) -> Self { Self { is_failed: AtomicBool::new(self.is_failed.load(Ordering::Relaxed)), last_failure: self.last_failure, retry_interval: self.retry_interval, hibernation_threshold: self.hibernation_threshold, max_backoff: self.max_backoff } }
}
impl FailureState {
    fn new(interval_secs: u64) -> Self {
        let max_backoff_duration = Duration::from_secs(MAX_FAILURE_BACKOFF);
        Self { is_failed: AtomicBool::new(false), last_failure: Instant::now().sub(max_backoff_duration), retry_interval: Duration::from_secs(5), hibernation_threshold: Duration::from_secs(interval_secs), max_backoff: max_backoff_duration }
    }
    fn record_failure(&mut self) {
        self.is_failed.store(true, Ordering::Relaxed);
        let now = Instant::now();
        if now.duration_since(self.last_failure) > self.max_backoff { self.retry_interval = Duration::from_secs(5); } 
        else { self.retry_interval = (self.retry_interval * 2).min(self.max_backoff); }
        self.last_failure = now;
    }
    fn record_success(&mut self) { self.is_failed.store(false, Ordering::Relaxed); self.retry_interval = Duration::from_secs(5); }
    fn can_execute_io(&self) -> bool { if !self.is_failed.load(Ordering::Relaxed) { return true; } self.last_failure.elapsed() >= self.retry_interval }
    fn check_hibernation_needed(&self) -> bool { self.is_failed.load(Ordering::Relaxed) && self.last_failure.elapsed() > self.hibernation_threshold }
}

struct ShardedLockCache { shards: Vec<Mutex<LruCache<u64, Arc<tokio::sync::Mutex<()>>>>> }
impl ShardedLockCache {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(128); // Increased shards
        for _ in 0..128 { shards.push(Mutex::new(LruCache::new(std::num::NonZeroUsize::new(100).unwrap()))); }
        Self { shards }
    }
    
    fn get(&self, key: u64) -> Arc<tokio::sync::Mutex<()>> {
        let idx = (key as usize) % 128;
        let mut s = self.shards[idx].lock();
        s.get_or_insert(key, || Arc::new(tokio::sync::Mutex::new(()))).clone()
    }
    
    fn get_by_path(&self, path: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        self.get(hasher.finish())
    }
}
#[derive(Clone)] struct DirtyEntry { first_dirty: Instant, path: PathBuf, seq: u64, projid: u32 }

struct BbrTuner {
    current_batch_size: usize,
    current_coalesce_bytes: u64,
    flush_multiplier: u32,
    state: TunerState,
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
    fn new(cfg: &TargetConfig) -> Self {
        let (flush_min, _) = get_flush_multiplier_bounds(&cfg.profile);
        let min_floor = match cfg.profile {
            crate::config::TargetProfile::HDD => 512 * 1024, 
            _ => 64 * 1024, 
        };
        Self { 
            current_batch_size: cfg.batch_size, 
            current_coalesce_bytes: min_floor * 2,
            flush_multiplier: flush_min,
            state: TunerState::Startup,
            btl_bw_filter: WindowedFilter::new(10, FilterMode::Max),
            rt_prop_filter: WindowedFilter::new(10, FilterMode::Min),
            batch_size_min: 2,
            batch_size_max: 128, 
            min_coalesce_floor: min_floor,
            max_burst_coalesce_bytes: 8 * 1024 * 1024,
            last_cycle: Instant::now(),
            last_data_seen: Instant::now(),
        }
    }

    fn tune(&mut self, elapsed_secs: f64, bytes_processed: u64, is_stressed: bool, pending_len: usize, max_pending: usize, board: &TunerBoard, path_label: &str) {
        let now = Instant::now();
        let delivery_rate = if elapsed_secs > 0.0001 { bytes_processed as f64 / elapsed_secs } else { 0.0 };
        
        if bytes_processed == 0 {
            if now.duration_since(self.last_data_seen) > Duration::from_secs(30) {
                if self.state != TunerState::Startup {
                    debug!("BBR: Connection idle for 30s. Resetting to Startup/Probe mode.");
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

        self.btl_bw_filter.update(delivery_rate, now);
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
                _ => {}
            }
        }

        let bdp_bytes = btl_bw * rt_prop;
        let pacing_gain = match self.state {
            TunerState::Startup => 2.0, 
            TunerState::Drain => 0.5,   
            TunerState::ProbeBW => 1.25, 
            TunerState::Muted => 0.75,  
            _ => 1.0,
        };

        let target_inflight_bytes = (bdp_bytes * pacing_gain) as u64;
        let target_op_size = (btl_bw * 0.002) as u64;
        
        self.current_coalesce_bytes = target_op_size
            .max(self.min_coalesce_floor)
            .min(self.max_burst_coalesce_bytes);

        let calculated_batch = (target_inflight_bytes / self.current_coalesce_bytes.max(1)) as usize;
        
        self.current_batch_size = calculated_batch
            .max(self.batch_size_min)
            .min(self.batch_size_max);

        board.insert(PathBuf::from(path_label), self.state);
        metrics::TARGET_BATCH_SIZE.with_label_values(&[&path_label]).set(self.current_batch_size as i64);
        metrics::TARGET_COALESCE_BYTES.with_label_values(&[&path_label]).set(self.current_coalesce_bytes as i64);
        metrics::TUNER_STATE.with_label_values(&[&path_label]).set(self.state as i64);
        metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_label]).set(pending_len as f64 / max_pending as f64);
    }
    
    fn should_defer_maintenance(&self) -> bool {
        // Defer maintenance if the BBR state is stressed.
        matches!(self.state, TunerState::Startup | TunerState::Muted | TunerState::Drain | TunerState::SpacePressure)
    }

    fn calculate_version_limits(&self, cfg: &TargetConfig, avail: u64, total: u64) -> (usize, u64) {
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

struct WorkerContext<'a> {
    ring: &'a mut IoUring,
    buf: &'a mut AlignedBuffer,
    dirty_stats: &'a mut HashMap<u64, DirtyEntry>,
    vdo_tuner: &'a mut VdoTuner,
    failure_state: &'a mut FailureState,
    capacity_breaker: &'a CircuitBreaker,
    limiter: &'a ErrorLimiter,
    locks: &'a ShardedLockCache,
    cur_cap_avail: u64,
    cur_cap_total: u64,
}

pub async fn run_worker(
    source: Arc<SourceInfo>, 
    target_cfg: TargetConfig, 
    mut rx_high: mpsc::Receiver<Arc<Event>>, 
    mut rx_low: mpsc::Receiver<Arc<Event>>, 
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>, 
    hydration_tx: mpsc::Sender<PathBuf>, 
    config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard
) -> Result<()> {
    let mut order = ordering::OrderBuf::new();
    let locks = Arc::new(ShardedLockCache::new()); 
    let mut dirty_stats: HashMap<u64, DirtyEntry> = HashMap::new(); 
    let mut flush_interval = interval(Duration::from_millis(100));
    
    let mut ring = match IoUring::new(target_cfg.batch_size as u32) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create io_uring: {}", e); return Err(FoxingError::Io(e)); }
    };
    let mut buf = AlignedBuffer::new(1024*1024);
    let limiter = ErrorLimiter::new();
    let config_reader = config.read().await;
    let capacity_threshold_mb = config_reader.capacity_threshold_mb;
    let capacity_breaker = CircuitBreaker::new(config_reader.breaker_interval_secs);
    let force_flush_base_secs = config_reader.force_flush_interval_secs;
    drop(config_reader);
    let hibernation_threshold_secs = 300; 
    let mut failure_state = FailureState::new(hibernation_threshold_secs);
    let iov = libc::iovec { iov_base: unsafe { buf.capacity_slice_mut() }.as_mut_ptr() as _, iov_len: buf.capacity() };
    if unsafe { ring.submitter().register_buffers(&[iov]) }.is_err() { error!("Failed to register io_uring buffers. Falling back to standard I/O (slower)."); }
    
    let mut tuner = BbrTuner::new(&target_cfg);
    let mut vdo_tuner = VdoTuner::new(target_cfg.vdo_optimization);

    let mut is_hibernating = false;
    let mut last_dropped_check = 0u64;
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;
    
    // LAST HYDRATION REQUEST TIME (Used for Debounce)
    let mut last_hydration_request = Instant::now().sub(Duration::from_secs(5));
    const HYDRATION_DEBOUNCE_SECS: u64 = 5;

    loop {
        let force_flush_age = Duration::from_secs(force_flush_base_secs) * tuner.flush_multiplier;
        
        let event_poll_result = tokio::select! {
            _ = shutdown_rx.recv() => break Ok(()),
            Some(e) = rx_high.recv() => { if is_hibernating { order.push_and_check(e); continue; } Some(e) },
            
            // LOW PRIORITY QUEUE POLLING (Hydration)
            // Throttles rx_low based on BBR state to prioritize high-priority queue
            Some(e) = async {
                let current_state = tuner_board.get(&target_cfg.path).map(|r| *r).unwrap_or(TunerState::Startup);
                
                if matches!(current_state, TunerState::Drain | TunerState::Muted | TunerState::SpacePressure) {
                    sleep(Duration::from_millis(50)).await;
                }

                rx_low.recv().await
            } => { 
                if is_hibernating { 
                    if let Some(evt) = &e {
                        order.push_and_check(evt.clone()); 
                    }
                    continue; 
                } 
                e // Returns Option<Arc<Event>>
            },
            
            _ = flush_interval.tick() => {
                let path_clone = target_cfg.path.clone(); 
                if flush_interval.period().as_millis() == 100 && (Instant::now().elapsed().as_millis() % 1000 < 150) {
                     if let Ok(s) = statvfs(&path_clone) {
                        cur_cap_total = s.blocks() * s.block_size();
                        cur_cap_avail = s.blocks_available() * s.block_size();
                        let label = target_cfg.path.to_string_lossy();
                        metrics::TARGET_CAPACITY_BYTES_TOTAL.with_label_values(&[&label]).set(cur_cap_total as f64);
                        metrics::TARGET_CAPACITY_BYTES_AVAILABLE.with_label_values(&[&label]).set(cur_cap_avail as f64);
                        metrics::TARGET_CAPACITY_INODES_TOTAL.with_label_values(&[&label]).set(s.files() as f64);
                        metrics::TARGET_CAPACITY_INODES_AVAILABLE.with_label_values(&[&label]).set(s.files_available() as f64);
                    }
                }
                
                let time_since_last_req = last_hydration_request.elapsed();
                let should_request_hydration = time_since_last_req >= Duration::from_secs(HYDRATION_DEBOUNCE_SECS);

                if !is_hibernating {
                    let current_dropped = metrics::EVENTS_DROPPED.get();
                    if current_dropped > last_dropped_check {
                        warn!("Detect event drops ({} -> {}). Triggering partial hydration.", last_dropped_check, current_dropped);
                        if should_request_hydration {
                            let _ = hydration_tx.send(source.path.clone()).await; 
                            last_hydration_request = Instant::now();
                        }
                        last_dropped_check = current_dropped;
                    }
                    
                    if order.check_timeouts() {
                        warn!("Gap detected by OrderBuf (Timeout). Triggering partial hydration.");
                        if should_request_hydration {
                            let _ = hydration_tx.send(source.path.clone()).await;
                            last_hydration_request = Instant::now();
                        }
                    }

                    let now = Instant::now();
                    let mut flushing_stats: HashMap<u64, DirtyEntry> = HashMap::new();
                    dirty_stats.retain(|&ino, entry| {
                        if now.duration_since(entry.first_dirty) > force_flush_age { flushing_stats.insert(ino, entry.clone()); false } else { true }
                    });
                    let _committed_inos = spawn_blocking(move || {
                        let mut committed = Vec::new();
                        for (ino, entry) in flushing_stats.into_iter() {
                            if security::commit_epoch(&entry.path, entry.seq, entry.projid).is_ok() { committed.push(ino); }
                        }
                        committed
                    }).await.unwrap_or_default();
                }
                None
            },
            else => break Err(FoxingError::System(nix::Error::last())),
        };

        if event_poll_result.is_none() { continue; }
        let event_ptr = event_poll_result.unwrap();
        
        if metrics::GLOBAL_BUFFER_COUNT.load(Ordering::Relaxed) > metrics::GLOBAL_BUFFER_LIMIT.get() as u64 {
             warn!("Worker detected global buffer overflow. Initiating immediate hibernation for {:?}", target_cfg.path);
             failure_state.record_failure();
        }

        if failure_state.check_hibernation_needed() {
            if !is_hibernating {
                warn!("Target {:?} failed for {}s. Switching to Hibernation mode.", target_cfg.path, failure_state.hibernation_threshold.as_secs());
                let _ = hydration_tx.send(source.path.clone()).await; 
                is_hibernating = true;
            }
        }
        
        if is_hibernating && failure_state.can_execute_io() {
            let probe_path = target_cfg.path.clone();
            let probe_res = spawn_blocking(move || std::fs::read_dir(probe_path)).await;
            if probe_res.is_ok() && probe_res.unwrap().is_ok() {
                info!("Target {:?} recovered. Resuming replication.", target_cfg.path);
                failure_state.record_success();
                is_hibernating = false;
            } else { failure_state.record_failure(); }
        }
        
        if is_hibernating { continue; }
        if event_ptr.event_type == EventType::SequenceGap {
            tracing::warn!("Sequence Gap detected on dev {}. Triggering re-sync.", event_ptr.dev_id);
            if last_hydration_request.elapsed() >= Duration::from_secs(HYDRATION_DEBOUNCE_SECS) {
                let _ = hydration_tx.send(source.path.clone()).await; 
                last_hydration_request = Instant::now();
            }
            order.next_seq = 0; 
            continue;
        }
        if order.next_seq > 0 && event_ptr.seq_num < order.next_seq && event_ptr.seq_num != 0 { crate::metrics::LATE_EVENTS.inc(); continue; }

        let current_coalesce_limit = tuner.current_coalesce_bytes;
        let mut batch_bytes_processed = 0u64;
        let mut current_copy_stats: Option<crate::operations::CopyStats> = None;
        let max_pending = order.max_count;
        let pending_len = order.len();

        let events_to_process = if event_ptr.seq_num == 0 {
            // FIX: Purge pending events for this inode to prevent Time Travel overwrites
            order.purge_inode(event_ptr.inode);
            vec![event_ptr]
        } else {
            if !order.push_and_check(event_ptr.clone()) {
                 warn!("Ordering buffer full. Rejecting event seq {}. Triggering Gap.", event_ptr.seq_num);
                 metrics::EVENTS_DROPPED.inc();
                 order.next_seq = 0;
                 if last_hydration_request.elapsed() >= Duration::from_secs(HYDRATION_DEBOUNCE_SECS) {
                     let _ = hydration_tx.send(source.path.clone()).await;
                     last_hydration_request = Instant::now();
                 }
                 continue;
            }
            
            let mut batch = Vec::new();
            while let Some(e) = order.pop_batch(current_coalesce_limit) {
                batch.push(e);
            }
            batch
        };

        for e in events_to_process {
            let mut ctx = WorkerContext {
                ring: &mut ring,
                buf: &mut buf,
                dirty_stats: &mut dirty_stats,
                vdo_tuner: &mut vdo_tuner,
                failure_state: &mut failure_state,
                capacity_breaker: &capacity_breaker,
                limiter: &limiter,
                locks: &locks,
                cur_cap_avail,
                cur_cap_total,
            };

            let res = process_single_event(&mut ctx, e.clone(), &source, &target_cfg, &tuner, &hydration_tx, capacity_threshold_mb).await;

            match res {
                Ok(Some(stats)) => {
                    batch_bytes_processed += stats.bytes_processed;
                    current_copy_stats = Some(stats);
                },
                Ok(None) => {}, 
                Err(_) => {}
            }
        }
        
        if batch_bytes_processed > 0 {
            if let Some(stats) = current_copy_stats {
                let elapsed_rtt = stats.io_duration.as_secs_f64(); 
                let is_stressed = governor.is_system_stressed();
                tuner.tune(elapsed_rtt, batch_bytes_processed, is_stressed, pending_len, max_pending, &tuner_board, &target_cfg.path.to_string_lossy());
                metrics::REPLICATION_LATENCY.with_label_values(&[&target_cfg.path.to_string_lossy()]).observe(elapsed_rtt);
            }
        }
    }
}

// -----------------------------------------------------------------------------
// EVENT PROCESSOR
// -----------------------------------------------------------------------------

async fn process_single_event(
    ctx: &mut WorkerContext<'_>,
    e: Arc<Event>,
    source: &Arc<SourceInfo>,
    target_cfg: &TargetConfig,
    tuner: &BbrTuner,
    hydration_tx: &mpsc::Sender<PathBuf>,
    capacity_threshold_mb: u64
) -> Result<Option<crate::operations::CopyStats>> {
    let target_cfg_cap = target_cfg.clone();
    let target_cfg_allow = target_cfg.clone();
    let capacity_breaker_clone = ctx.capacity_breaker.clone();
    let check_capacity_result = spawn_blocking(move || { capacity_breaker_clone.can_proceed(&target_cfg_cap.path, capacity_threshold_mb) }).await.unwrap_or(false);

    if !check_capacity_result {
        if ctx.limiter.check("capacity") { error!("Target full: {:?}", target_cfg.path); }
        ctx.capacity_breaker.trip();
        ctx.failure_state.record_failure();
        return Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Target Full")));
    }
    
    let e_clone = e.clone();
    let allow_result = spawn_blocking(move || { target_cfg_allow.allow(std::path::Path::new(&e_clone.name)) }).await.unwrap_or(false);
    if !allow_result { metrics::EVENTS_FILTERED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc(); return Ok(None); }
    
    if e.name.contains(".tmp.") {
        return Ok(None);
    }

    let lock = ctx.locks.get_by_path(&e.name);
    let _g = lock.lock().await;
    
    let (dst, is_synthetic, needs_creation) = identity::resolve_target(&source.inode_map, &e, &target_cfg.path);

    if needs_creation {
        let dst_clone = dst.clone();
        let target_cfg_clone = target_cfg.clone();
        let e_inode = e.inode;
        let e_generation = e.generation;
        let e_name = PathBuf::from(&e.name);
        let res = spawn_blocking(move || {
            let identity_dir = target_cfg_clone.path.join(".mirror").join(".by-identity");
            let _ = std::fs::create_dir_all(&identity_dir);
            let f_result = std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_clone);
            if let Ok(ref f) = f_result {
                let fd = f.as_raw_fd();
                if target_cfg_clone.btrfs_compression || target_cfg_clone.f2fs_compression { let _ = security::enable_compression(fd); }
                if target_cfg_clone.f2fs_pinning { let _ = security::enable_f2fs_pinning(fd); }
                metrics::SIDECAR_FILES_CREATED.inc();
            }
            f_result
        }).await;
        if res.is_ok() {
            identity::update_map(&source.inode_map, e_inode, e_name, e_generation, true);
            metrics::SYNTHETIC_IDENTITY_FILES.inc();
        } else { 
            error!("Failed to create synthetic file: {:?}. This might indicate a missing target directory or IO issue.", res.err()); 
            ctx.failure_state.record_failure();
            return Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Synthetic Creation Failed")));
        }
    }

    if e.event_type == EventType::Write || e.event_type == EventType::WriteRange {
        if !ctx.dirty_stats.contains_key(&e.inode) {
            let dst_clone = dst.clone();
            spawn_blocking(move || sidecar::set_dirty_flag(&dst_clone, true));
            ctx.dirty_stats.insert(e.inode, DirtyEntry { first_dirty: Instant::now(), path: dst.clone(), seq: e.seq_num, projid: e.projid });
        } else { if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) { entry.seq = e.seq_num; } }
    }

    let src = source.mount.join(&e.name);
    
    let res = match e.event_type {
        EventType::Write | EventType::Create | EventType::WriteRange => {
            let src_clone = src.clone();
            let target_cfg_clone = target_cfg.clone();
            let dst_clone = dst.clone(); 
            let e_offset = e.offset; 
            let e_len = e.length;    
            
            let metadata_result = spawn_blocking(move || { 
                debug!("Worker: Reading metadata for source file {:?}", src_clone);
                std::fs::metadata(&src_clone) 
            }).await.unwrap_or(Err(io::Error::new(io::ErrorKind::NotFound, "Metadata task failed")));

            if let Ok(m) = metadata_result {
                if m.is_file() {
                    if !is_synthetic {
                        let dst_parent = dst_clone.parent().map(|p| p.to_path_buf()).unwrap_or(target_cfg_clone.path.clone());
                        let _ = spawn_blocking(move || std::fs::create_dir_all(&dst_parent)).await;
                    }
                     
                     if e.event_type == EventType::Create && (target_cfg.btrfs_compression || target_cfg.f2fs_compression || target_cfg.f2fs_pinning) {
                          let dst_clone_for_opt = dst_clone.clone();
                          let res = spawn_blocking(move || {
                              let f_result = std::fs::OpenOptions::new().write(true).open(&dst_clone_for_opt);
                              if let Ok(f) = f_result {
                                  let fd = f.as_raw_fd();
                                  if target_cfg_clone.btrfs_compression || target_cfg_clone.f2fs_compression { let _ = security::enable_compression(fd); }
                                  if target_cfg_clone.f2fs_pinning { let _ = security::enable_f2fs_pinning(fd); }
                              }
                              Ok::<(), io::Error>(())
                          }).await;
                          if res.is_err() { warn!("Failed compression/pinning setup: {:?}", res.err()); }
                     }
                    
                    let sync_xattrs_src = src.clone();
                    let sync_xattrs_dst = dst_clone.clone(); 
                    
                    let total_size = m.len() as usize;
                    if total_size > 0 {
                        debug!("Worker: Starting xattr sync for {:?} (size {})", dst_clone, total_size);
                        let chunks: Vec<usize> = (0..total_size).step_by(1024*1024).collect();
                        futures::stream::iter(chunks).then(|_sz| {
                            let sx = sync_xattrs_src.clone();
                            let dx = sync_xattrs_dst.clone();
                            async move {
                                let _ = spawn_blocking(move || { security::sync_xattrs(&sx, &dx); }).await;
                            }
                        }).collect::<Vec<_>>().await;
                    }

                    let dynamic_vdo_opt = ctx.vdo_tuner.should_check_zeros(m.len());

                    let copy_res = SmartCopier::copy(
                        &src, 
                        &dst_clone, 
                        ctx.ring, 
                        ctx.buf, 
                        &target_cfg.supports_reflink, 
                        dynamic_vdo_opt, 
                        e_offset, 
                        e_len,
                        target_cfg.direct_io_ok.load(Ordering::Relaxed)
                    ).await;
                    
                    match copy_res {
                        Ok(stats) => {
                            metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(stats.bytes_processed);
                            ctx.vdo_tuner.update(stats.bytes_processed, stats.bytes_zeros);
                            Ok(Ok(Some(stats)))
                        },
                        Err(e) => Err(e)
                    }
                } else { Ok(Ok(None)) } 
            } else if is_synthetic { if ctx.limiter.check("unlinked_read") { warn!("Unlinked write detected for inode {}. Event ignored.", e.inode); } Ok(Ok(None)) } else { Ok(Ok(None)) }
        },
        EventType::Symlink => {
            let dst_clone = dst.clone();
            let src_clone = src.clone();
            Ok(spawn_blocking(move || {
                if let Ok(link_target) = std::fs::read_link(&src_clone) {
                    security::create_symlink(&link_target.to_string_lossy(), &dst_clone)
                } else {
                    Ok(())
                }
            }).await.unwrap_or(Ok(())).map(|_| None).map_err(|e| e.into()))
        },
        EventType::Link => {
            warn!("Hardlink event received. Treating as standard create/copy for now to ensure data persistence.");
            Ok(Ok(None))
        },
        EventType::Mknod => {
            let dst_clone = dst.clone();
            let mode = e.mode;
            let src_clone = src.clone();
            Ok(spawn_blocking(move || {
                if let Ok(m) = std::fs::metadata(&src_clone) {
                    let rdev = m.rdev(); 
                    security::create_mknod(&dst_clone, mode, rdev)
                } else {
                    Ok(())
                }
            }).await.unwrap_or(Ok(())).map(|_| None).map_err(|e| e.into()))
        },
        EventType::Unlink => {
            ctx.dirty_stats.remove(&e.inode);
            let dst_clone = dst.clone();
            let source_clone = source.clone();
            let e_clone = e.clone();
            let res = spawn_blocking(move || {
                if let Some(sp) = sidecar::get_sidecar_path(&dst_clone) { let _ = std::fs::remove_file(sp); }
                if is_synthetic { 
                    debug!("Worker: Unlinking synthetic file {:?}", dst_clone);
                    let _ = std::fs::remove_file(&dst_clone); 
                    source_clone.inode_map.lock().pop(&e_clone.inode); 
                    metrics::SYNTHETIC_IDENTITY_FILES.dec(); 
                }
                let r = std::fs::remove_file(&dst_clone);
                // NEW: Unlink is idempotent. NotFound is not an error.
                if let Err(ref e) = r {
                    if e.kind() == io::ErrorKind::NotFound { return Ok(()); }
                }
                r
            }).await.unwrap_or(Ok(())).map_err(|err| err.into());
            if res.is_ok() { let _ = spawn_blocking(move || { if let Some(parent) = dst.parent() { security::write_dir_integrity_hash(parent, 0); } }).await; }
            Ok(res.map(|_| None))
        },
        EventType::Rename => {
            if let Some(new_name) = &e.new_name {
                metrics::RENAME_EVENTS.inc();
                let new_rel = PathBuf::from(new_name);
                let new_dst = target_cfg.path.join(&new_rel);
                if target_cfg.allow(&new_rel) {
                    let dst_clone = dst.clone(); 
                    let new_dst_clone = new_dst.clone(); 
                    let source_clone = source.clone();
                    let e_inode = e.inode;
                    let e_generation = e.generation;
                    let is_synthetic_state = is_synthetic;
                    
                    let res = spawn_blocking(move || {
                        if let Some(parent) = new_dst_clone.parent() { 
                            if let Err(e) = std::fs::create_dir_all(parent) { 
                                return Err(io::Error::new(io::ErrorKind::Other, format!("Rename target dir creation failed: {}", e))); 
                            } 
                        }
                        
                        debug!("Worker: Attempting rename from {:?} to {:?}", dst_clone, new_dst_clone);
                        let rename_res = std::fs::rename(&dst_clone, &new_dst_clone);
                        
                        if rename_res.is_ok() {
                            let new_rel_clone = new_rel.clone(); 
                            identity::update_map_after_rename(&source_clone.inode_map, e_inode, new_rel_clone, e_generation);
                            
                            if let Some(old_sp) = sidecar::get_sidecar_path(&dst_clone) { 
                                if let Some(new_sp) = sidecar::get_sidecar_path(&new_dst_clone) { 
                                    if old_sp.exists() { 
                                        let _ = std::fs::rename(old_sp, new_sp); 
                                    } 
                                } 
                            }

                            if is_synthetic_state {
                                metrics::SYNTHETIC_IDENTITY_FILES.dec();
                                identity::update_map(&source_clone.inode_map, e_inode, new_rel.clone(), e_generation, false);
                            }
                        }
                        rename_res
                    }).await.unwrap_or(Err(io::Error::new(io::ErrorKind::Other, "Rename task failed"))).map_err(|e| e.into());
                    
                    if res.is_ok() {
                        if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) { entry.path = new_dst.clone(); }
                        let _ = spawn_blocking(move || { 
                            let dst_path_clone = dst.clone(); 
                            let new_dst_path_clone = new_dst.clone(); 
                            if let Some(parent) = dst_path_clone.parent() { security::write_dir_integrity_hash(parent, 0); } 
                            if let Some(parent) = new_dst_path_clone.parent() { security::write_dir_integrity_hash(parent, 0); } 
                        }).await;
                    }
                    Ok(res.map(|_| None))
                } else { Ok(Ok(None)) }
            } else { Ok(Ok(None)) }
        },
        EventType::Mkdir => {
            let dst_clone = dst.clone();
            let res = spawn_blocking(move || { 
                let res = std::fs::create_dir_all(&dst_clone);
                // Directory events must succeed even if parent already exists (race protection)
                if res.is_err() && res.as_ref().unwrap_err().kind() == io::ErrorKind::AlreadyExists {
                    Ok(())
                } else { res }
            }).await.unwrap_or(Ok(())).map_err(|e| e.into());
            
            if res.is_ok() { let _ = spawn_blocking(move || { if let Some(parent) = dst.parent() { security::write_dir_integrity_hash(parent, 0); } }).await; }
            Ok(res.map(|_| None))
        },
        EventType::Rmdir => {
            let dst_clone = dst.clone();
            let res = spawn_blocking(move || { 
                let r = std::fs::remove_dir(&dst_clone);
                // NEW: Rmdir is idempotent-ish. NotFound is not an error.
                if let Err(ref e) = r {
                    if e.kind() == io::ErrorKind::NotFound { return Ok(()); }
                    if e.kind() == io::ErrorKind::NotADirectory { return Ok(()); } // Target might be a file due to race
                }
                r
            }).await.unwrap_or(Ok(())).map_err(|e| e.into());
            if res.is_ok() { let _ = spawn_blocking(move || { if let Some(parent) = dst.parent() { security::write_dir_integrity_hash(parent, 0); } }).await; }
            Ok(res.map(|_| None))
        },
        EventType::Barrier | EventType::Fsync => {
            let dst_clone = dst.clone();
            let seq_num = e.seq_num;
            let projid = e.projid;
            let inode = e.inode;
            let target_root_clone = target_cfg.path.clone();
            let enable_versioning = target_cfg.enable_versioning;
            let is_forced = target_cfg.is_forced_version(&dst_clone);
            let defer_maintenance = tuner.should_defer_maintenance();
            let source_path_for_h = source.path.clone(); 
            let h_tx_clone = hydration_tx.clone();
            
            let (dyn_max_versions, dyn_max_mb) = if is_forced {
                 let forced_count = target_cfg.force_retention_count.unwrap_or(target_cfg.max_versions);
                 if defer_maintenance { warn!("Forcing version retention for inode {} despite high system load.", inode); }
                 metrics::TARGET_FORCED_VERSIONING_ACTIVE.with_label_values(&[&target_cfg.path.to_string_lossy()]).set(1);
                 (forced_count, u64::MAX) 
            } else {
                 metrics::TARGET_FORCED_VERSIONING_ACTIVE.with_label_values(&[&target_cfg.path.to_string_lossy()]).set(0);
                 tuner.calculate_version_limits(&target_cfg, ctx.cur_cap_avail, ctx.cur_cap_total)
            };
            
            let should_cleanup = is_forced || !defer_maintenance;

            if enable_versioning && target_cfg.allow_versioning(&dst_clone) {
                let dst_for_version = dst_clone.clone();
                 let result = spawn_blocking(move || {
                    let _ = security::create_version_snapshot(&dst_for_version, seq_num, &target_root_clone, inode);
                    
                    // FIX 1: Defer aggressive maintenance based on BBR state
                    if should_cleanup {
                        let _ = versioning::cleanup_versions(&dst_for_version, &target_root_clone, dyn_max_versions, dyn_max_mb);
                    }
                    Ok::<(), FoxingError>(())
                }).await;
                 if let Err(e) = result { tracing::error!("Failed MARS Version step for inode {}: {:?}", inode, e); }
            }
            
            let dst_for_commit = dst_clone.clone(); 
            let r = spawn_blocking(move || {
                let r = security::commit_epoch(&dst_for_commit, seq_num, projid); 
                if r.is_ok() { 
                    if let Some(parent) = dst_for_commit.parent() { 
                        if let Ok(hash) = security::calc_dir_integrity_hash_target(parent) { security::write_dir_integrity_hash(parent, hash); } 
                    } 
                }
                r
            }).await.unwrap_or(Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Commit task failed"))));
            
            if let Err(FoxingError::Io(ref io_err)) = r {
                if io_err.kind() == io::ErrorKind::NotFound {
                    warn!("Fsync on {:?} failed (NotFound). Triggering hydration.", dst_clone);
                    let _ = h_tx_clone.send(source_path_for_h).await;
                    Ok(Ok(None))
                } else {
                    Ok(r.map(|_| None))
                }
            } else {
                if r.is_ok() { ctx.dirty_stats.remove(&e.inode); }
                Ok(r.map(|_| None))
            }
        },
        EventType::SetXattr | EventType::RemoveXattr => {
            let src_clone = src.clone();
            let dst_clone = dst.clone();
            let _ = spawn_blocking(move || { security::sync_xattrs(&src_clone, &dst_clone); }).await;
            Ok(Ok(None))
        },
        EventType::Chmod | EventType::Chown | EventType::Utimes => {
            let src_clone = src.clone();
            let dst_clone = dst.clone();
            Ok(spawn_blocking(move || { security::apply_metadata(&src_clone, &dst_clone) }).await.unwrap_or(Ok(())).map(|_| None))
        },
        EventType::Truncate => {
            let dst_clone = dst.clone();
            let length = e.length; 
            Ok(spawn_blocking(move || { security::truncate_file(&dst_clone, length) }).await.unwrap_or(Ok(())).map(|_| None))
        },
        EventType::Fallocate => {
            let dst_clone = dst.clone();
            let offset = e.offset;
            let length = e.length;
            let mode = e.flags as i32;
            debug!("Worker: Handling Fallocate on {:?} with offset={}, len={}, mode={:#x}", dst_clone, offset, length, mode);
            Ok(spawn_blocking(move || { security::do_fallocate(&dst_clone, offset, length, mode) }).await.unwrap_or(Ok(())).map(|_| None))
        },
        _ => {
            debug!("Worker: Unhandled event type {:?} for {:?}", e.event_type, dst);
            Ok(Ok(None))
        }
    };

    match res {
        Ok(inner) => {
            if let Err(err) = inner {
                // If IO error 28 (ENOSPC)
                if let FoxingError::Io(io_err) = &err {
                    if let Some(28) = io_err.raw_os_error() {
                        error!("TARGET FULL (ENOSPC) on {:?}. Tripping circuit breaker immediately.", target_cfg.path);
                        ctx.capacity_breaker.trip();
                        ctx.failure_state.record_failure();
                        let target_cfg_clone = target_cfg.clone();
                        let _ = spawn_blocking(move || { 
                            let target_root = target_cfg_clone.path.parent().unwrap_or(&target_cfg_clone.path);
                            versioning::prune_global_history(target_root, 512 * 1024 * 1024) 
                        }).await;
                    }
                }
                
                // Fixed E0609 by accessing fields directly from `e` which is still in scope
                error!("IO Worker Error during processing {:?} (Inode {}): {:?}", e.event_type, e.inode, err);
                if ctx.limiter.check("io") { error!("IO Error: {:?}", err); }
                ctx.failure_state.record_failure();
                Err(err)
            } else {
                ctx.failure_state.record_success();
                inner
            }
        },
        Err(err) => {
            // Processing error (e.g. breaker tripped)
            ctx.failure_state.record_failure();
            Err(err)
        }
    }
}
