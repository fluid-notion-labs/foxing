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
use crate::{event::{Event, EventType}, mirror::{SourceInfo, SharedConfig, REPAIR_DEBOUNCE_MS}, config::{TargetConfig, get_flush_multiplier_bounds}, buffer::AlignedBuffer, security, sidecar, identity, Result, versioning, governor::Governor};
use crate::operations::SmartCopier;
use tokio::time::interval;
use tracing::{warn, info, error, debug};
use crate::error::{FoxingError}; // Removed unused alias Result as RError
use std::io;
use crate::metrics;
use parking_lot::Mutex;
use lru::LruCache;
use std::path::{Path, PathBuf};
use crate::config::{MAX_FAILURE_BACKOFF, ERROR_LIMITER_SECS};
use nix::sys::statvfs::statvfs;
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::time::sleep;

/// Represents the current phase of the 3-Tier WAL for a specific inode.
/// Tier 1: WriteBulk (Accumulating data in buffer/cache)
/// Tier 2: FsyncCommit (Data flushed, waiting for metadata/barrier)
/// Tier 3: Committed (Rename/Link finalization)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum ExpectedState {
    None,
    WriteBulk,
    FsyncCommit,
}

/// Tracks the in-memory WAL state for a specific inode.
/// WARNING: This is volatile. Crash recovery relies on the 'dirty' xattr sidecar.
#[derive(Clone, Debug)]
struct DirtyEntry {
    first_dirty: Instant,
    path: PathBuf,
    seq: u64,
    projid: u32,
    expected_state: ExpectedState,
}

impl Default for DirtyEntry {
    fn default() -> Self {
        Self {
            first_dirty: Instant::now(),
            path: PathBuf::new(),
            seq: 0,
            projid: 0,
            expected_state: ExpectedState::None,
        }
    }
}

struct PoisonEntry {
    failure_count: u32,
    next_attempt: Instant,
}

struct PoisonCabinet {
    cache: LruCache<u64, PoisonEntry>,
}

impl PoisonCabinet {
    fn new() -> Self {
        Self {
            cache: LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()),
        }
    }
    
    fn check_allowed(&mut self, inode: u64) -> bool {
        if let Some(entry) = self.cache.get(&inode) {
            if Instant::now() < entry.next_attempt {
                return false;
            }
        }
        true
    }

    fn record_failure(&mut self, inode: u64) {
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

    fn record_success(&mut self, inode: u64) {
        if self.cache.contains(&inode) {
            self.cache.pop(&inode);
        }
    }
}

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
             debug!("Vdo Tuner: Waking up immediately for large file ({} bytes)", file_size);
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
        let mut shards = Vec::with_capacity(128);
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

struct BbrTuner {
    current_batch_size: usize,
    current_coalesce_bytes: u64,
    flush_multiplier: u32,
    pub hydration_debounce: Duration,
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
            hydration_debounce: Duration::from_secs(1),
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
                TunerState::CriticalDrain => {
                    if pending_len < 100 {
                         debug!("Critical drain lifted. Buffer cleared to {} events.", pending_len);
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
                    TunerState::Startup => 2.0,
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
                
                (calculated_batch.max(self.batch_size_min).min(self.batch_size_max), calculated_coalesce)
            }
        };

        self.current_batch_size = batch_override;
        self.current_coalesce_bytes = coalesce_override;
        
        self.hydration_debounce = match self.state {
            TunerState::Drain | TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => Duration::from_secs(30),
            _ => Duration::from_secs(1),
        };

        board.insert(PathBuf::from(path_label), self.state);
        metrics::TARGET_BATCH_SIZE.with_label_values(&[&path_label]).set(self.current_batch_size as i64);
        metrics::TARGET_COALESCE_BYTES.with_label_values(&[&path_label]).set(self.current_coalesce_bytes as i64);
        metrics::TUNER_STATE.with_label_values(&[&path_label]).set(self.state as i64);
        metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_label]).set(pending_len as f64 / max_pending as f64);
    }

    fn should_defer_maintenance(&self) -> bool {
        matches!(self.state, TunerState::Startup | TunerState::Muted | TunerState::Drain | TunerState::CriticalDrain)
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
    poison: &'a mut PoisonCabinet,
    cur_cap_avail: u64,
    cur_cap_total: u64,
}

/// Runs the worker loop for a single target.
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
    let mut order = crate::ordering::OrderBuf::new();
    let locks = Arc::new(ShardedLockCache::new());
    
    // Tier 2 WAL State: In-Memory Map of Pending Transactions
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
    let mut poison_cabinet = PoisonCabinet::new();
    
    let iov = libc::iovec { iov_base: unsafe { buf.capacity_slice_mut() }.as_mut_ptr() as _, iov_len: buf.capacity() };
    if unsafe { ring.submitter().register_buffers(&[iov]) }.is_err() { error!("Failed to register io_uring buffers. Falling back to standard I/O (slower)."); }
    
    let mut tuner = BbrTuner::new(&target_cfg);
    let mut vdo_tuner = VdoTuner::new(target_cfg.vdo_optimization);
    
    let mut is_hibernating = false;
    let mut last_dropped_check = 0u64;
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;
    let mut last_hydration_request = Instant::now().sub(Duration::from_secs(30));
    let hydration_tx_clone = hydration_tx.clone();
    
    let ingestion_panic_threshold: usize = (order.max_count as f64 * 0.8) as usize;

    loop {
        if !is_hibernating && !failure_state.can_execute_io() {
             sleep(Duration::from_millis(100)).await;
             continue;
        }

        let event_poll_result = tokio::select! {
            biased;
            _ = shutdown_rx.recv() => break Ok(()),
            Some(e) = rx_high.recv() => {
                metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::Relaxed);
                if is_hibernating { order.push_and_check(e); continue; }
                Some(e)
            },
            Some(e) = async {
                let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);
                let delay = match current_state {
                    TunerState::CriticalDrain => Duration::from_millis(100),
                    TunerState::Drain | TunerState::Muted | TunerState::SpacePressure => Duration::from_millis(10),
                    _ => Duration::ZERO,
                };
                if delay > Duration::ZERO {
                    sleep(delay).await;
                }
                rx_low.recv().await
            } => {
                metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::Relaxed);
                if is_hibernating {
                    order.push_and_check(e.clone());
                    continue;
                }
                Some(e)
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
                let should_request_hydration = time_since_last_req >= tuner.hydration_debounce;

                let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);
                let effective_flush_multiplier = match current_state {
                    TunerState::Drain | TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => 1,
                    _ => tuner.flush_multiplier,
                };
                let dynamic_force_flush_age = Duration::from_secs(force_flush_base_secs) * effective_flush_multiplier;

                if !is_hibernating {
                    let current_dropped = metrics::EVENTS_DROPPED.get();
                    if current_dropped > last_dropped_check {
                        warn!("Detect event drops ({} -> {}). Triggering partial hydration.", last_dropped_check, current_dropped);
                        if should_request_hydration {
                            let _ = hydration_tx_clone.send(source.path.clone()).await;
                            last_hydration_request = Instant::now();
                        }
                        last_dropped_check = current_dropped;
                    }

                    let order_len = order.len();
                    if order_len > ingestion_panic_threshold {
                        warn!("WAL buffer near capacity ({}/{}). FORCING CriticalDrain.", order_len, order.max_count);
                        if current_state != TunerState::CriticalDrain {
                            tuner_board.insert(target_cfg.path.clone(), TunerState::CriticalDrain);
                            tuner.state = TunerState::CriticalDrain;
                        }
                    } else if order.should_throttle_bpf() {
                        warn!("CoDel: Buffer latency exceeded target. Signaling BPF to throttle.");
                        if current_state != TunerState::CriticalDrain {
                            tuner_board.insert(target_cfg.path.clone(), TunerState::CriticalDrain);
                            tuner.state = TunerState::CriticalDrain;
                        }
                    } else if current_state == TunerState::CriticalDrain && order_len < 100 {
                         tuner_board.insert(target_cfg.path.clone(), TunerState::Drain);
                         tuner.state = TunerState::Drain;
                    }

                    if order.check_timeouts() {
                        warn!("Gap detected by OrderBuf (Timeout). Triggering partial hydration.");
                        if should_request_hydration {
                            let _ = hydration_tx_clone.send(source.path.clone()).await;
                            last_hydration_request = Instant::now();
                        }
                    }

                    let now = Instant::now();
                    let mut flushing_stats: HashMap<u64, DirtyEntry> = HashMap::new();
                    
                    dirty_stats.retain(|&ino, entry| {
                        if now.duration_since(entry.first_dirty) > dynamic_force_flush_age { flushing_stats.insert(ino, entry.clone()); false } else { true }
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
        };

        if event_poll_result.is_none() { continue; }
        let event_ptr = event_poll_result.unwrap();

        if failure_state.check_hibernation_needed() {
            if !is_hibernating {
                warn!("Target {:?} failed for {}s. Switching to Hibernation mode.", target_cfg.path, failure_state.hibernation_threshold.as_secs());
                let _ = hydration_tx_clone.send(source.path.clone()).await;
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
        let current_debounce = tuner.hydration_debounce;

        if event_ptr.event_type == EventType::SequenceGap {
            tracing::warn!("Sequence Gap detected on dev {}. Triggering Tier 2 (WAL Sweep) recovery.", event_ptr.dev_id);
            order.next_seq = event_ptr.seq_num;
            if last_hydration_request.elapsed() >= current_debounce {
                for entry in dirty_stats.values() {
                    let _ = hydration_tx_clone.send(entry.path.clone()).await;
                }
                last_hydration_request = Instant::now();
            }
            continue;
        }

        if order.next_seq > 0 && event_ptr.seq_num < order.next_seq && event_ptr.seq_num != 0 { crate::metrics::LATE_EVENTS.inc(); continue; }
        
        let current_coalesce_limit = tuner.current_coalesce_bytes;
        let mut batch_bytes_processed = 0u64;
        let mut current_copy_stats: Option<crate::operations::CopyStats> = None;
        let max_pending = order.max_count;
        let pending_len = order.len();

        let events_to_process_raw = if event_ptr.seq_num == 0 {
            order.purge_inode(event_ptr.inode);
            vec![(event_ptr, false)]
        } else {
            if !order.push_and_check(event_ptr.clone()) {
                 warn!("Ordering buffer full. Rejecting event seq {}. Triggering Gap.", event_ptr.seq_num);
                 metrics::EVENTS_DROPPED.inc();
                 order.next_seq = 0;
                 if last_hydration_request.elapsed() >= current_debounce {
                     let _ = hydration_tx_clone.send(source.path.clone()).await;
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

        for (e, _is_coalesced) in events_to_process_raw {
            if !poison_cabinet.check_allowed(e.inode) {
                continue;
            }
            
            let mut ctx = WorkerContext {
                ring: &mut ring,
                buf: &mut buf,
                dirty_stats: &mut dirty_stats,
                vdo_tuner: &mut vdo_tuner,
                failure_state: &mut failure_state,
                capacity_breaker: &capacity_breaker,
                limiter: &limiter,
                poison: &mut poison_cabinet,
                cur_cap_avail,
                cur_cap_total,
            };

            let (dst, is_synthetic, needs_creation) = identity::resolve_target(&source.inode_map, &e, &target_cfg.path);
            let src = if is_synthetic {
                source.mount.join(e.name.trim_start_matches('/'))
            } else {
                match dst.strip_prefix(&target_cfg.path) {
                    Ok(rel) => source.mount.join(rel),
                    Err(e) => {
                        error!("CRITICAL PATH ERROR: Target path {:?} is not prefixed by target root {:?}. Failing event: {}", dst, target_cfg.path, e);
                        return Err(FoxingError::Security("Cannot determine source path from target path.".into()));
                    }
                }
            };

            let lock = locks.get_by_path(&e.name);
            let _g = lock.lock().await;

            let res = process_single_event_inner(&mut ctx, e.clone(), &source, &target_cfg, &tuner, capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src).await;
            
            match res {
                Ok(Some(stats)) => {
                    batch_bytes_processed += stats.bytes_processed;
                    current_copy_stats = Some(stats);
                    ctx.poison.record_success(e.inode);
                },
                Ok(None) => {},
                Err(err) => {
                    ctx.poison.record_failure(e.inode);
                    order.purge_inode_bulk(e.inode);
                    if let FoxingError::Io(ref io_err) = err {
                        if io_err.kind() == io::ErrorKind::NotFound {
                            if e.seq_num == 0 {
                                debug!("Worker: Hydration write failed (NotFound) for inode {}. Aborting recursion.", e.inode);
                            } else if is_synthetic {
                                warn!("Synthetic file {:?} data operation failed. Triggering targeted repair.", dst);
                                if last_hydration_request.elapsed() >= Duration::from_millis(REPAIR_DEBOUNCE_MS) {
                                    let _ = hydration_tx_clone.send(source.path.clone()).await;
                                    last_hydration_request = Instant::now();
                                }
                            } else {
                                warn!("File {:?} disappeared from source mid-operation. Triggering targeted repair.", src.join(&e.name));
                                if last_hydration_request.elapsed() >= Duration::from_millis(REPAIR_DEBOUNCE_MS) {
                                    let _ = hydration_tx_clone.send(src.join(&e.name)).await;
                                    last_hydration_request = Instant::now();
                                }
                            }
                        } else if io_err.to_string().contains("Target Full") {
                             warn!("Capacity Pressure: Slowing down ingestion for {:?}", target_cfg.path);
                             tuner.state = TunerState::SpacePressure;
                             tuner_board.insert(target_cfg.path.clone(), TunerState::SpacePressure);
                             failure_state.record_failure();
                        }
                    } else if let FoxingError::Versioning(ref v_err) = err {
                         if v_err.contains("WAL Coherence Broken") {
                            error!("WAL T2 Inconsistency detected for Inode {}. Triggering targeted WAL sweep.", e.inode);
                            metrics::WAL_COHERENCE_FAILURES.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc();
                            if last_hydration_request.elapsed() >= Duration::from_millis(REPAIR_DEBOUNCE_MS) {
                                let _ = hydration_tx_clone.send(src.join(&e.name)).await;
                                last_hydration_request = Instant::now();
                            }
                         }
                         failure_state.record_failure();
                    }
                    else if let FoxingError::Io(ref io_err) = err {
                         if io_err.kind() != io::ErrorKind::NotFound {
                              failure_state.record_failure();
                         }
                    } else {
                         failure_state.record_failure();
                    }
                }
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

async fn process_single_event_inner(
    ctx: &mut WorkerContext<'_>,
    e: Arc<Event>,
    source: &Arc<SourceInfo>,
    target_cfg: &TargetConfig,
    tuner: &BbrTuner,
    capacity_threshold_mb: u64,
    dst: &PathBuf,
    is_synthetic: bool,
    needs_creation: bool,
    src: &PathBuf,
) -> Result<Option<crate::operations::CopyStats>> {
    let target_cfg_cap = target_cfg.clone();
    let target_cfg_allow = target_cfg.clone();

    // 1. Capacity Check
    let capacity_breaker_clone = ctx.capacity_breaker.clone();
    let check_capacity_result = spawn_blocking(move || { capacity_breaker_clone.can_proceed(&target_cfg_cap.path, capacity_threshold_mb) }).await.unwrap_or(false);
    if !check_capacity_result {
        if ctx.limiter.check("capacity") { error!("Target full: {:?}", target_cfg.path); }
        ctx.capacity_breaker.trip();
        ctx.failure_state.record_failure();
        return Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Target Full")));
    }

    // 2. Filter Check
    let e_clone = e.clone();
    let allow_result = spawn_blocking(move || { target_cfg_allow.allow(std::path::Path::new(&e_clone.name)) }).await.unwrap_or(false);
    if !allow_result { metrics::EVENTS_FILTERED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc(); return Ok(None); }

    if e.name.contains(".tmp.") { return Ok(None); }

    // 3. Locking (Path lock already acquired in run_worker)

    // 4. Target Directory Setup
    if !is_synthetic {
        let target_dir = dst.parent().map(|p| p.to_path_buf());
        if let Some(target_dir) = target_dir {
            if target_dir != target_cfg.path {
                let check_res: Result<()> = match spawn_blocking({
                    let target_dir_clone = target_dir.clone();
                    move || {
                        if target_dir_clone.exists() && !target_dir_clone.is_dir() {
                            error!("Path collision: Target parent {:?} exists but is not a directory.", target_dir_clone);
                            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Path component is a file, not a directory"));
                        }
                        std::fs::create_dir_all(&target_dir_clone)
                    }
                }).await {
                    Ok(inner_res) => inner_res.map_err(FoxingError::Io),
                    Err(e) => {
                        warn!("Parent directory check task failed with JoinError: {}", e);
                        Err(FoxingError::Join(e))
                    }
                };
                if check_res.is_err() {
                    warn!("Failed to prepare target directory {:?}: {:?}", target_dir, check_res.err());
                }
            }
        }
    }

    // 5. Synthetic File Creation (for metadata/control files)
    if needs_creation {
        let dst_clone = dst.clone();
        let target_cfg_clone = target_cfg.clone();
        let e_inode = e.inode;
        let e_generation = e.generation;
        let e_name = PathBuf::from(&e.name);
        
        let res = spawn_blocking(move || {
            let identity_dir = target_cfg_clone.path.join(".mirror").join(".by-identity");
            let _ = std::fs::create_dir_all(&identity_dir);
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_clone) {
                Ok(f) => return Ok(f),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    debug!("Synthetic file creation raced. Re-opening existing file.");
                    std::fs::OpenOptions::new().write(true).open(&dst_clone)
                },
                Err(e) => return Err(e),
            }
        }).await;

        let final_res = match res {
            Ok(inner_res) => inner_res.map_err(FoxingError::Io),
            Err(e) => Err(FoxingError::Join(e)),
        };

        if let Ok(f) = final_res {
            let fd = f.as_raw_fd();
            if target_cfg.btrfs_compression || target_cfg.f2fs_compression { let _ = security::enable_compression(fd); }
            if target_cfg.f2fs_pinning { let _ = security::enable_f2fs_pinning(fd); }
            metrics::SIDECAR_FILES_CREATED.inc();
            // NOTE: Even if name is empty, we update the map to reflect its synthetic state
            identity::update_map(&source.inode_map, e_inode, e_name, e_generation, true);
            metrics::SYNTHETIC_IDENTITY_FILES.inc();
        } else {
            let mapped_err: Option<FoxingError> = final_res.err();
            error!("Failed to create synthetic file: {:?}. This might indicate a missing target directory or IO issue.", mapped_err);
            ctx.failure_state.record_failure();
            let final_err = mapped_err.unwrap_or_else(|| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Unknown Synthetic Creation Error")));
            if let FoxingError::Io(ref io_err) = final_err {
                if io_err.raw_os_error() == Some(libc::ENOSPC) {
                    ctx.capacity_breaker.trip();
                    error!("Capacity Breaker tripped during synthetic file creation.");
                }
            }
            return Err(final_err);
        }
    }

    // 6. Dirty Flag and WAL State Management
    if matches!(e.event_type,
        EventType::Write | EventType::Create | EventType::WriteRange |
        EventType::Truncate | EventType::Fsync | EventType::Rename |
        EventType::Barrier)
    {
        // Get or create DirtyEntry (Per-File WAL State)
        let entry = ctx.dirty_stats.entry(e.inode).or_insert_with(|| DirtyEntry {
            first_dirty: Instant::now(),
            path: dst.clone(),
            seq: e.seq_num,
            projid: e.projid,
            expected_state: ExpectedState::FsyncCommit, // Default safe expectation
        });

        // --- WAL STATE MACHINE ADVANCEMENT (XFS Consistency Model) ---
        match e.event_type {
            EventType::Create => {
                // Creation of a new file (atomic write starts)
                // If it's a .tmp file, we expect bulk writes followed by Rename/Commit
                if e.name.starts_with(".tmp.") {
                    entry.expected_state = ExpectedState::WriteBulk;
                } else {
                    entry.expected_state = ExpectedState::FsyncCommit;
                }
            },
            EventType::Truncate => {
                entry.expected_state = ExpectedState::WriteBulk; // Truncate must be followed by a Write or Fsync/Commit
            },
            EventType::Write | EventType::WriteRange => {
                // Bulk write occurred, transaction is now open, expect Fsync commit next.
                if entry.expected_state == ExpectedState::WriteBulk || entry.expected_state == ExpectedState::None {
                    entry.expected_state = ExpectedState::FsyncCommit;
                }
            },
            EventType::Rename => {
                // Atomic Commit: This transitions state from FsyncCommit -> None (Done)
                // But we handle the removal later. Here we just update metadata.
                // IMPORTANT: If we see a Rename for a file we thought was WriteBulk, it's an Atomic Commit.
                if entry.expected_state == ExpectedState::WriteBulk {
                    entry.expected_state = ExpectedState::FsyncCommit;
                }
            }
            // Fsync, Barrier handled later when checking for commit.
            _ => {}
        }

        // Update sequence and projid (latest transaction ID)
        entry.seq = e.seq_num;
        entry.projid = e.projid;
        
        let dst_clone = dst.clone();
        spawn_blocking(move || sidecar::set_dirty_flag(&dst_clone, true));
    }

    // 7. Event Processing Dispatch
    let res = match e.event_type {
        EventType::Write | EventType::Create | EventType::WriteRange => {
            let target_cfg_clone = target_cfg.clone();
            let dst_clone = dst.clone();
            let e_offset = e.offset;
            let e_len = e.length;
            let src_clone_for_metadata = src.clone();

            let metadata_result = spawn_blocking(move || {
                for attempt in 0..3 {
                    match std::fs::metadata(&src_clone_for_metadata) {
                        Ok(m) => return Ok(m),
                        Err(e) if e.kind() == io::ErrorKind::NotFound && attempt < 2 => {
                            std::thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(e) => {
                            if attempt == 2 {
                                warn!("Worker: Failed to stat {:?}: {}", src_clone_for_metadata, e);
                            }
                            return Err(e)
                        },
                    }
                }
                Err(io::Error::new(io::ErrorKind::NotFound, "Retries exhausted"))
            }).await.unwrap_or(Err(io::Error::new(io::ErrorKind::NotFound, "Metadata task failed")));

            if let Ok(m) = metadata_result {
                if m.is_file() {
                    let current_src_size = m.len();
                    let initial_is_full_replace = e_offset == 0 && e_len == current_src_size;
                    let file_size_match = initial_is_full_replace || (e_offset + e_len <= current_src_size);
                    
                    if !file_size_match {
                        warn!("Data integrity warning: Incoming event length ({}) exceeds current source size ({}). Clamping/Aborting.", e_len, current_src_size);
                        return Ok(None);
                    }

                    // --- CONSISTENCY CHECK & HEALING (FIX FOR DELTA CHECK FAIL) ---
                    let mut should_force_full_replace = initial_is_full_replace;
                    let mut copy_offset = e_offset;
                    let mut copy_length = e_len;

                    if !should_force_full_replace {
                        // If it's a delta update, check target size consistency.
                        let dst_meta_res = spawn_blocking({
                            let dst_clone_for_meta = dst_clone.clone();
                            move || std::fs::metadata(&dst_clone_for_meta)
                        }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));

                        if let Ok(dst_meta) = dst_meta_res {
                            if dst_meta.len() != current_src_size {
                                warn!("Worker: Target file size mismatch detected (Source: {}, Target: {}). Forcing FULL ATOMIC COPY to heal inconsistency for {:?}",
                                    current_src_size, dst_meta.len(), dst_clone);
                                should_force_full_replace = true;
                            }
                        }
                    }

                    if should_force_full_replace {
                        copy_offset = 0;
                        copy_length = current_src_size;
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

                    let dynamic_vdo_opt = ctx.vdo_tuner.should_check_zeros(m.len());

                    let copy_res = SmartCopier::copy(
                        &src,
                        &dst_clone,
                        ctx.ring,
                        ctx.buf,
                        &target_cfg.supports_reflink,
                        dynamic_vdo_opt,
                        copy_offset,
                        copy_length,
                        target_cfg.direct_io_ok.load(Ordering::Relaxed),
                        current_src_size,
                    ).await;

                    match copy_res {
                        Ok(stats) => {
                            metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(stats.bytes_processed);
                            ctx.vdo_tuner.update(stats.bytes_processed, stats.bytes_zeros);
                            
                            let apply_dst = dst_clone.clone();
                            let apply_src = src.clone();
                            let _ = spawn_blocking(move || {
                                security::sync_xattrs(&apply_src, &apply_dst);
                                security::apply_metadata(&apply_src, &apply_dst)
                            }).await;
                            
                            return Ok(Some(stats));
                        },
                        Err(e) => Err(e)
                    }
                } else { return Ok(None); }
            } else if is_synthetic {
                if ctx.limiter.check("synthetic_data_op") { warn!("Synthetic file data operation attempted without path resolution. Triggering repair path."); }
                return Err(FoxingError::Io(io::Error::new(io::ErrorKind::NotFound, "Synthetic data operation failed; need path resolution.")));
            }
            else if let Err(io_err) = &metadata_result {
                if io_err.kind() == io::ErrorKind::NotFound {
                    warn!("Worker: Source file for inode {} (mapped path {:?}) not found. Assuming stale map entry and forcing fast resolution.", e.inode, src.to_path_buf());
                    source.inode_map.lock().pop(&e.inode);
                    let fast_resolve_res = spawn_blocking({
                        let source_map = source.inode_map.clone();
                        let source_root = source.mount.clone();
                        let inode = e.inode;
                        move || identity::resolve_and_update_path(&source_map, &source_root, inode)
                    }).await;

                    match fast_resolve_res {
                        Ok(Ok(new_path)) => {
                            debug!("Fast refresh successful: Inode {} resolved to {:?}", e.inode, new_path);
                            return Ok(None);
                        },
                        _ => {
                            warn!("Worker: Fast Path resolution failed for inode {}. Triggering targeted repair.", e.inode);
                            return Err(FoxingError::Io(io::Error::new(io::ErrorKind::NotFound, "Fast Path resolution failed, triggering targeted repair.")));
                        }
                    }
                } else {
                    warn!("Worker: Skipping Write for {:?} - Source access failed: {}", dst_clone, io_err);
                }
                return Ok(None);
            } else {
                return Ok(None);
            }
        },
        EventType::Symlink => {
            let dst_clone = dst.clone();
            let src_clone = src.clone();
            let inner_res = spawn_blocking(move || {
                if let Ok(link_target) = std::fs::read_link(&src_clone) {
                    security::create_symlink(&link_target.to_string_lossy(), &dst_clone)
                } else {
                    Ok(())
                }
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return inner_res.map(|_| None);
        },
        EventType::Link => {
            warn!("Hardlink event received. Treating as standard create/copy for now to ensure data persistence.");
            return Ok(None);
        },
        EventType::Mknod => {
            let dst_clone = dst.clone();
            let mode = e.mode;
            let src_clone = src.clone();
            let inner_res = spawn_blocking(move || {
                if let Ok(m) = std::fs::metadata(&src_clone) {
                    let rdev = m.rdev();
                    security::create_mknod(&dst_clone, mode, rdev)
                } else {
                    Ok(())
                }
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return inner_res.map(|_| None);
        },
        EventType::Unlink => {
            ctx.dirty_stats.remove(&e.inode);
            source.inode_map.lock().pop(&e.inode);
            let dst_clone = dst.clone();
            let _e_clone = e.clone();
            let res = spawn_blocking(move || {
                if let Some(sp) = sidecar::get_sidecar_path(&dst_clone) { let _ = std::fs::remove_file(sp); }
                if is_synthetic {
                    debug!("Worker: Unlinking synthetic file {:?}", dst_clone);
                    metrics::SYNTHETIC_IDENTITY_FILES.dec();
                }
                let r = std::fs::remove_file(&dst_clone);
                if let Err(ref e) = r {
                    if e.kind() == io::ErrorKind::NotFound { return Ok(()); }
                }
                r
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));

            if res.is_ok() {
                let dst_clone = dst.clone();
                let _ = spawn_blocking(move || { if let Some(parent) = dst_clone.parent() { security::write_dir_integrity_hash(parent, 0); } }).await;
            }
            return res.map(|_| None);
        },
        EventType::Rename => {
            if let Some(new_name) = &e.new_name {
                let current_dirty_entry = ctx.dirty_stats.get(&e.inode).cloned();
                
                let new_dst_temp = if let Some(parent) = dst.parent() { parent.join(new_name) } else { target_cfg.path.join(new_name) };
                
                // --- ATOMIC COMMIT DETECTION (Tier 3) ---
                // If we see a Rename of a .tmp file, this is the Atomic Commit step of the 3-Tier WAL.
                if e.name.starts_with(".tmp.") && current_dirty_entry.is_some() {
                    let entry = current_dirty_entry.as_ref().unwrap();
                    if entry.expected_state == ExpectedState::FsyncCommit {
                        info!("Worker: Detected Atomic Write Commit via RENAME ({:?}). Forcing epoch commit.", e.name);
                        let res = spawn_blocking({
                            let path = new_dst_temp.clone();
                            let seq = entry.seq;
                            let projid = entry.projid;
                            move || security::commit_epoch(&path, seq, projid)
                        }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));

                        if res.is_ok() {
                            ctx.dirty_stats.remove(&e.inode);
                        } else {
                            warn!("Worker: Failed to commit epoch during Rename detection: {:?}", res.err());
                        }
                    } else {
                         warn!("WAL T2 Failure: RENAME on .tmp file occurred, but state was {:?}. Ignoring rename sequence inconsistency.", entry.expected_state);
                    }
                }

                metrics::RENAME_EVENTS.inc();
                let new_dst = new_dst_temp;
                let new_rel = new_dst.strip_prefix(&target_cfg.path)
                    .unwrap_or_else(|_| Path::new(new_name))
                    .to_path_buf();

                if new_name.contains("..") || new_name.starts_with('/') || !new_dst.starts_with(&target_cfg.path) {
                    return Err(FoxingError::Security(format!("Invalid rename path traversal detected: {}", new_name)));
                }

                if target_cfg.allow(&new_rel) {
                    let dst_clone = dst.clone();
                    let new_dst_clone = new_dst.clone();
                    let source_clone = source.clone();
                    let e_inode = e.inode;
                    let e_generation = e.generation;
                    let is_synthetic_state = is_synthetic;

                    let res = spawn_blocking(move || {
                        info!("Worker: Attempting rename from {:?} to {:?}", dst_clone, new_dst_clone);
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
                    }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));

                    if res.is_ok() {
                        if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) { entry.path = new_dst.clone(); }
                        let dst_clone = dst.clone();
                        let new_dst_clone = new_dst.clone();
                        let _ = spawn_blocking(move || {
                            if let Some(parent) = dst_clone.parent() { security::write_dir_integrity_hash(parent, 0); }
                            if let Some(parent) = new_dst_clone.parent() { security::write_dir_integrity_hash(parent, 0); }
                        }).await;
                    }
                    return res.map(|_| None);
                } else {
                    info!("Rename filtered: {:?}", new_rel);
                    return Ok(None);
                }
            } else { return Ok(None); }
        },
        EventType::Mkdir => {
            let dst_clone = dst.clone();
            let res = spawn_blocking(move || {
                let res = std::fs::create_dir_all(&dst_clone);
                if res.is_err() && res.as_ref().unwrap_err().kind() == io::ErrorKind::AlreadyExists {
                    Ok(())
                } else { res }
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));

            if res.is_ok() {
                let dst_clone = dst.clone();
                let _ = spawn_blocking(move || { if let Some(parent) = dst_clone.parent() { security::write_dir_integrity_hash(parent, 0); } }).await;
            }
            return res.map(|_| None);
        },
        EventType::Rmdir => {
            ctx.dirty_stats.remove(&e.inode);
            source.inode_map.lock().pop(&e.inode);
            let dst_clone = dst.clone();
            let res = spawn_blocking(move || {
                if let Some(sp) = sidecar::get_sidecar_path(&dst_clone) { let _ = std::fs::remove_file(sp); }
                if is_synthetic {
                    debug!("Worker: Unlinking synthetic file {:?}", dst_clone);
                    metrics::SYNTHETIC_IDENTITY_FILES.dec();
                }
                let r = std::fs::remove_dir(&dst_clone);
                if let Err(ref e) = r {
                    if e.kind() == io::ErrorKind::NotFound { return Ok(()); }
                }
                r
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));

            if res.is_ok() {
                let dst_clone = dst.clone();
                let _ = spawn_blocking(move || { if let Some(parent) = dst_clone.parent() { security::write_dir_integrity_hash(parent, 0); } }).await;
            }
            return res.map(|_| None);
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

            if let Some(entry) = ctx.dirty_stats.get(&e.inode) {
                 if entry.expected_state != ExpectedState::FsyncCommit {
                      error!("WAL T2 Failure: Fsync/Barrier received, but expected state was {:?} (Inode {}). Triggering targeted WAL sweep.",
                          entry.expected_state, inode);
                      return Err(FoxingError::Versioning(format!("WAL Coherence Broken: Inode {} needs repair.", inode)));
                 }
            } else {
                // Warning: Fsync received for inode we don't track. Likely restart or cache eviction.
            }

            let (dyn_max_versions, dyn_max_mb) = if is_forced {
                 let forced_count = target_cfg.force_retention_count.unwrap_or(target_cfg.max_versions);
                 if defer_maintenance { warn!("Forcing version retention for inode {} despite high system system load.", inode); }
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
                    if should_cleanup {
                        let _ = versioning::cleanup_versions(&dst_for_version, &target_root_clone, dyn_max_versions, dyn_max_mb);
                    }
                    Ok::<(), FoxingError>(())
                }).await.map_err(FoxingError::Join);
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
            }).await.map_err(FoxingError::Join)?;
            
            if r.is_ok() {
                ctx.dirty_stats.remove(&e.inode);
            }
            return r.map(|_| None);
        },
        EventType::SetXattr | EventType::RemoveXattr => {
            let src_clone = src.clone();
            let dst_clone = dst.clone();
            let _ = spawn_blocking(move || { security::sync_xattrs(&src_clone, &dst_clone); }).await;
            return Ok(None);
        },
        EventType::Chmod | EventType::Chown | EventType::Utimes => {
            let src_clone = src.clone();
            let dst_clone = dst.clone();
            let dst_for_closure = dst_clone.clone();
            let res = spawn_blocking(move || { security::apply_metadata(&src_clone, &dst_for_closure) }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            match res {
                Ok(()) => Ok(None),
                Err(e) => {
                    warn!("Worker: Metadata apply failed for {:?}: {}", dst_clone, e);
                    Err(e)
                }
            }
        },
        EventType::Truncate => {
            let dst_clone = dst.clone();
            let length = e.length;
            let res = spawn_blocking(move || { security::truncate_file(&dst_clone, length) }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return res.map(|_| None);
        },
        EventType::Fallocate => {
            let dst_clone = dst.clone();
            let offset = e.offset;
            let length = e.length;
            let mode = e.flags as i32;
            debug!("Worker: Handling Fallocate on {:?} with offset={}, len={}, mode={:#x}", dst_clone, offset, length, mode);
            let res = spawn_blocking(move || { security::do_fallocate(&dst_clone, offset, length, mode) }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return res.map(|_| None);
        },
        _ => {
            debug!("Worker: Unhandled event type {:?} for {:?}", e.event_type, dst);
            return Ok(None);
        }
    };

    match res {
        Ok(stats_opt) => {
            if stats_opt.is_some() {
            }
            Ok(stats_opt)
        },
        Err(err) => {
            if let FoxingError::Io(io_err) = &err {
                if let Some(28) = io_err.raw_os_error() {
                    error!("TARGET FULL (ENOSPC) on {:?}. Tripping circuit breaker immediately.", target_cfg.path);
                    ctx.capacity_breaker.trip();
                    ctx.failure_state.record_failure();
                    
                    let target_cfg_clone = target_cfg.clone();
                    let target_root_path = target_cfg_clone.path.parent().unwrap_or(&target_cfg_clone.path).to_path_buf();
                    let _ = spawn_blocking(move || {
                        versioning::prune_global_history(&target_root_path, 512 * 1024 * 1024)
                    }).await;
                }
            }
            error!("IO Worker Error during processing {:?} (Inode {}): {:?}. Backoff initiated.", e.event_type, e.inode, err);
            if ctx.limiter.check("io") { error!("IO Error: {:?}", err); }
            Err(err)
        }
    }
}
