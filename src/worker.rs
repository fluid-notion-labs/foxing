// File: foxing/src/worker.rs | Index: 15 of 21 | Function: Worker loop. Fixed match types and error handling.
use std::{
    sync::{Arc, atomic::{AtomicBool, Ordering}}, 
    collections::HashMap, 
    time::{Instant, Duration}, 
    ops::Sub
};
use tokio::{sync::mpsc, task::spawn_blocking};
use io_uring::IoUring;
use std::os::unix::io::AsRawFd;
use crate::{event::{Event, EventType}, mirror::{SourceInfo, SharedConfig}, config::{TargetConfig, get_flush_multiplier_bounds}, buffer::AlignedBuffer, security, sidecar, identity, ordering, Result, versioning, governor::Governor};
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

// Export type for mirror.rs
pub type TunerBoard = Arc<DashMap<PathBuf, TunerState>>;

// ... [Structs: ErrorLimiter, CircuitBreaker, FailureState, ShardedLockCache, DirtyEntry omitted] ...
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
        let mut shards = Vec::with_capacity(64);
        for _ in 0..64 { shards.push(Mutex::new(LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()))); }
        Self { shards }
    }
    fn get(&self, inode: u64) -> Arc<tokio::sync::Mutex<()>> {
        let idx = (inode as usize) % 64;
        let mut s = self.shards[idx].lock();
        s.get_or_insert(inode, || Arc::new(tokio::sync::Mutex::new(()))).clone()
    }
}

#[derive(Clone)] struct DirtyEntry { first_dirty: Instant, path: PathBuf, seq: u64, projid: u32 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunerState { Steady=0, LatencyBackoff=1, RampUp=2, HighLoad=3, EmergencyDrain=4, GovernorThrottled=5, SpacePressure=6 }

struct TargetTuner { latency_target_seconds: f64, aspiration_size: u64, current_coalesce_bytes: u64, max_burst_coalesce_bytes: u64, current_batch_size: usize, batch_size_min: usize, batch_size_max: usize, flush_multiplier: u32, flush_multiplier_min: u32, flush_multiplier_max: u32, state: TunerState }
impl TargetTuner {
    fn new(cfg: &TargetConfig) -> Self {
        let aspiration_size = crate::config::get_default_coalesce_max_bytes(&cfg.profile);
        let (flush_min, flush_max) = get_flush_multiplier_bounds(&cfg.profile);
        let lat_target = cfg.autotune_target_latency_ms as f64 / 1000.0;
        Self { latency_target_seconds: lat_target, aspiration_size, current_coalesce_bytes: aspiration_size, max_burst_coalesce_bytes: 8 * 1024 * 1024, current_batch_size: cfg.batch_size, batch_size_min: 2, batch_size_max: cfg.batch_size * 4, flush_multiplier: flush_min, flush_multiplier_min: flush_min, flush_multiplier_max: flush_max, state: TunerState::Steady }
    }
    fn tune(&mut self, elapsed: f64, cfg: &TargetConfig, is_stressed: bool, pending_events: usize, max_pending: usize, board: &TunerBoard) {
        let buffer_util = pending_events as f64 / max_pending as f64;
        let label = cfg.path.to_string_lossy();
        metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&label]).set(buffer_util);

        if is_stressed {
            self.state = TunerState::GovernorThrottled;
            self.current_batch_size = (self.current_batch_size / 2).max(self.batch_size_min);
            self.current_coalesce_bytes = 4096; 
            self.flush_multiplier = self.flush_multiplier_min;
        } else if buffer_util > 0.8 {
            self.state = TunerState::EmergencyDrain;
            self.current_batch_size = self.batch_size_max;
            self.current_coalesce_bytes = self.max_burst_coalesce_bytes;
            self.flush_multiplier = self.flush_multiplier_max; 
        } else if buffer_util > 0.5 {
            self.state = TunerState::HighLoad;
            self.current_batch_size = (self.current_batch_size + 1).min(self.batch_size_max);
            self.current_coalesce_bytes = (self.current_coalesce_bytes + 512 * 1024).min(self.max_burst_coalesce_bytes);
        } else if elapsed > self.latency_target_seconds {
            self.state = TunerState::LatencyBackoff;
            self.current_batch_size = (self.current_batch_size - 1).max(self.batch_size_min);
            self.current_coalesce_bytes = (self.current_coalesce_bytes / 2).max(4096);
            self.flush_multiplier = self.flush_multiplier.saturating_add(1).min(self.flush_multiplier_max);
        } else if elapsed < (self.latency_target_seconds * 0.25) {
            self.state = TunerState::RampUp;
            if self.current_coalesce_bytes < self.aspiration_size { self.current_coalesce_bytes = (self.current_coalesce_bytes * 2).min(self.aspiration_size); }
            if self.current_batch_size < self.batch_size_max { self.current_batch_size = (self.current_batch_size + 1).min(self.batch_size_max); }
            if self.flush_multiplier > self.flush_multiplier_min { self.flush_multiplier = self.flush_multiplier.saturating_sub(1); }
        } else {
            self.state = TunerState::Steady;
        }
        
        board.insert(cfg.path.clone(), self.state);

        metrics::TARGET_BATCH_SIZE.with_label_values(&[&label]).set(self.current_batch_size as i64);
        metrics::TARGET_COALESCE_BYTES.with_label_values(&[&label]).set(self.current_coalesce_bytes as i64);
        metrics::TARGET_FLUSH_MULTIPLIER.with_label_values(&[&label]).set(self.flush_multiplier as i64);
        metrics::TUNER_STATE.with_label_values(&[&label]).set(self.state as i64);
    }
    
    fn should_defer_maintenance(&self) -> bool {
        matches!(self.state, TunerState::EmergencyDrain | TunerState::GovernorThrottled | TunerState::HighLoad)
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

pub async fn run_worker(
    source: Arc<SourceInfo>, 
    target_cfg: TargetConfig, 
    mut rx: mpsc::Receiver<Arc<Event>>, 
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>, 
    hydration_tx: mpsc::Sender<PathBuf>, 
    config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard
) -> Result<()> {
    let mut order = ordering::OrderBuf::new();
    let locks = Arc::new(ShardedLockCache::new()); 
    let mut dirty_stats: HashMap<u64, DirtyEntry> = HashMap::new(); 
    let mut flush_interval = interval(Duration::from_secs(1));
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
    let mut tuner = TargetTuner::new(&target_cfg);
    let mut is_hibernating = false;
    let mut last_dropped_check = 0u64;
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;

    loop {
        let force_flush_age = Duration::from_secs(force_flush_base_secs) * tuner.flush_multiplier;
        let evt = tokio::select! {
            _ = flush_interval.tick() => {
                if let Ok(s) = statvfs(&target_cfg.path) {
                    cur_cap_total = s.blocks() * s.block_size();
                    cur_cap_avail = s.blocks_available() * s.block_size();
                    let label = target_cfg.path.to_string_lossy();
                    metrics::TARGET_CAPACITY_BYTES_TOTAL.with_label_values(&[&label]).set(cur_cap_total as f64);
                    metrics::TARGET_CAPACITY_BYTES_AVAILABLE.with_label_values(&[&label]).set(cur_cap_avail as f64);
                    metrics::TARGET_CAPACITY_INODES_TOTAL.with_label_values(&[&label]).set(s.files() as f64);
                    metrics::TARGET_CAPACITY_INODES_AVAILABLE.with_label_values(&[&label]).set(s.files_available() as f64);
                }

                if !is_hibernating {
                    let current_dropped = metrics::EVENTS_DROPPED.get();
                    if current_dropped > last_dropped_check {
                        warn!("Detect event drops ({} -> {}). Triggering partial hydration.", last_dropped_check, current_dropped);
                        let _ = hydration_tx.send(source.path.clone()).await; 
                        last_dropped_check = current_dropped;
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
            Some(e) = rx.recv() => { if is_hibernating { order.push_and_check(e); continue; } Some(e) },
            _ = shutdown_rx.recv() => break Ok(()),
            else => break Err(FoxingError::System(nix::Error::last())),
        };

        if evt.is_none() { continue; }
        let evt = evt.unwrap();
        
        if metrics::GLOBAL_BUFFER_COUNT.load(Ordering::Relaxed) > metrics::GLOBAL_BUFFER_LIMIT.get() as u64 {
             warn!("Worker detected global buffer overflow. Initiating immediate hibernation for {:?}", target_cfg.path);
             failure_state.record_failure();
        }

        if failure_state.check_hibernation_needed() {
            if !is_hibernating {
                warn!("Target {:?} failed for {}s. Switching to Hibernation mode.", target_cfg.path, failure_state.hibernation_threshold.as_secs());
                let events_dumped = order.pending.len();
                order.pending.clear();
                if events_dumped > 0 { metrics::GLOBAL_BUFFER_COUNT.fetch_sub(events_dumped as u64, Ordering::Relaxed); }
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
        if evt.event_type == EventType::SequenceGap {
            tracing::warn!("Sequence Gap detected on dev {}. Triggering re-sync.", evt.dev_id);
            let _ = hydration_tx.send(source.path.clone()).await; 
            order.next = 0;
            continue;
        }
        if order.next > 0 && evt.seq_num < order.next { crate::metrics::LATE_EVENTS.inc(); continue; }

        order.push_and_check(evt);
        let current_coalesce_limit = tuner.current_coalesce_bytes;
        let mut io_attempt_successful = true;

        while let Some(e) = order.pop_batch(current_coalesce_limit) {
            let target_cfg_clone = target_cfg.clone();
            let capacity_breaker_clone = capacity_breaker.clone();
            let check_capacity_result = spawn_blocking(move || { capacity_breaker_clone.can_proceed(&target_cfg_clone.path, capacity_threshold_mb) }).await.unwrap_or(false);

            if !check_capacity_result {
                if limiter.check("capacity") { error!("Target full: {:?}", target_cfg.path); }
                capacity_breaker.trip();
                io_attempt_successful = false; 
                order.push_and_check(e);
                break;
            }
            
            let e_clone = e.clone();
            let allow_result = spawn_blocking(move || { target_cfg_clone.allow(std::path::Path::new(&e_clone.name)) }).await.unwrap_or(false);
            if !allow_result { metrics::EVENTS_FILTERED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc(); continue; }

            let lock = locks.get(e.inode);
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
                } else { error!("Failed to create synthetic file: {:?}", res.err()); io_attempt_successful = false; order.push_and_check(e); break; }
            }

            if e.event_type == EventType::Write || e.event_type == EventType::WriteRange {
                if !dirty_stats.contains_key(&e.inode) {
                    let dst_clone = dst.clone();
                    spawn_blocking(move || sidecar::set_dirty_flag(&dst_clone, true));
                    dirty_stats.insert(e.inode, DirtyEntry { first_dirty: Instant::now(), path: dst.clone(), seq: e.seq_num, projid: e.projid });
                } else { if let Some(entry) = dirty_stats.get_mut(&e.inode) { entry.seq = e.seq_num; } }
            }

            let src = source.mount.join(&e.name);
            let start = Instant::now();

            let res = match e.event_type {
                EventType::Write | EventType::Create | EventType::WriteRange => {
                    let src_clone = src.clone();
                    let dst_clone = dst.clone();
                    let target_cfg_clone = target_cfg.clone();
                    let e_offset = e.offset; 
                    let e_len = e.length;    
                    
                    let metadata_result = spawn_blocking(move || { std::fs::metadata(&src_clone) }).await.unwrap_or(Err(io::Error::new(io::ErrorKind::NotFound, "Metadata task failed")));

                    if let Ok(m) = metadata_result {
                        if m.is_file() {
                             if e.event_type == EventType::Create && (target_cfg.btrfs_compression || target_cfg.f2fs_compression || target_cfg.f2fs_pinning) {
                                  let res = spawn_blocking(move || {
                                      let f_result = std::fs::OpenOptions::new().write(true).open(&dst_clone);
                                      if let Ok(f) = f_result {
                                          let fd = f.as_raw_fd();
                                          if target_cfg_clone.btrfs_compression || target_cfg_clone.f2fs_compression { let _ = security::enable_compression(fd); }
                                          if target_cfg_clone.f2fs_pinning { let _ = security::enable_f2fs_pinning(fd); }
                                      }
                                      Ok::<(), io::Error>(())
                                  }).await;
                                  if res.is_err() { warn!("Failed compression/pinning setup: {:?}", res.err()); }
                             }
                            let sync_xattrs_dst = dst.clone();
                            let sync_xattrs_src = src.clone();
                            security::copy_smart(&src, &dst, &mut ring, &mut buf, &target_cfg.direct_io_ok, target_cfg.vdo_optimization, e_offset, e_len).await
                                .map(|sz| {
                                    metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(sz);
                                    // FIX: wrap in Ok() to match Result
                                    Ok(spawn_blocking(move || { security::sync_xattrs(&sync_xattrs_src, &sync_xattrs_dst); }).await.unwrap_or(()))
                                })
                        } else { Ok(Ok(())) } // Nested Result for consistency
                    } else if is_synthetic { if limiter.check("unlinked_read") { warn!("Unlinked write detected for inode {}.", e.inode); } Ok(Ok(())) } else { Ok(Ok(())) }
                },
                EventType::Unlink => {
                    dirty_stats.remove(&e.inode);
                    let dst_clone = dst.clone();
                    let source_clone = source.clone();
                    let e_clone = e.clone();
                    let res = spawn_blocking(move || {
                        if let Some(sp) = sidecar::get_sidecar_path(&dst_clone) { let _ = std::fs::remove_file(sp); }
                        if is_synthetic { let _ = std::fs::remove_file(&dst_clone); source_clone.inode_map.lock().pop(&e_clone.inode); metrics::SYNTHETIC_IDENTITY_FILES.dec(); }
                        std::fs::remove_file(&dst_clone)
                    }).await.unwrap_or(Ok(())).map_err(|err| err.into());
                    if res.is_ok() { let _ = spawn_blocking(move || { if let Some(parent) = dst.parent() { security::write_dir_integrity_hash(parent, 0); } }).await; }
                    Ok(res)
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
                            let res = spawn_blocking(move || {
                                if let Some(parent) = new_dst_clone.parent() { if let Err(e) = std::fs::create_dir_all(parent) { return Err(e); } }
                                let rename_res = std::fs::rename(&dst_clone, &new_dst_clone);
                                if rename_res.is_ok() {
                                    identity::update_map_after_rename(&source_clone.inode_map, e_inode, new_rel, e_generation);
                                    if let Some(old_sp) = sidecar::get_sidecar_path(&dst_clone) { if let Some(new_sp) = sidecar::get_sidecar_path(&new_dst_clone) { if old_sp.exists() { let _ = std::fs::rename(old_sp, new_sp); } } }
                                }
                                rename_res
                            }).await.unwrap_or(Err(io::Error::new(io::ErrorKind::Other, "Rename task failed"))).map_err(|e| e.into());
                            if res.is_ok() {
                                if let Some(entry) = dirty_stats.get_mut(&e.inode) { entry.path = new_dst.clone(); }
                                let _ = spawn_blocking(move || { if let Some(parent) = dst.parent() { security::write_dir_integrity_hash(parent, 0); } if let Some(parent) = new_dst.parent() { security::write_dir_integrity_hash(parent, 0); } }).await;
                            }
                            Ok(res)
                        } else { Ok(Ok(())) }
                    } else { Ok(Ok(())) }
                },
                EventType::Mkdir => {
                    let dst_clone = dst.clone();
                    let res = spawn_blocking(move || { std::fs::create_dir_all(&dst_clone) }).await.unwrap_or(Ok(())).map_err(|e| e.into());
                    if res.is_ok() { let _ = spawn_blocking(move || { if let Some(parent) = dst.parent() { security::write_dir_integrity_hash(parent, 0); } }).await; }
                    Ok(res)
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
                    
                    let (dyn_max_versions, dyn_max_mb) = if is_forced {
                         let forced_count = target_cfg.force_retention_count.unwrap_or(target_cfg.max_versions);
                         if defer_maintenance { warn!("Forcing version retention for inode {} despite high system load.", inode); }
                         metrics::TARGET_FORCED_VERSIONING_ACTIVE.with_label_values(&[&target_cfg.path.to_string_lossy()]).set(1);
                         (forced_count, u64::MAX) 
                    } else {
                         metrics::TARGET_FORCED_VERSIONING_ACTIVE.with_label_values(&[&target_cfg.path.to_string_lossy()]).set(0);
                         tuner.calculate_version_limits(&target_cfg, cur_cap_avail, cur_cap_total)
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
                        }).await;
                         if let Err(e) = result { tracing::error!("Failed MARS Version step for inode {}: {:?}", inode, e); }
                    }
                    let r = spawn_blocking(move || {
                        let r = security::commit_epoch(&dst_clone, seq_num, projid); 
                        if r.is_ok() { if let Some(parent) = dst_clone.parent() { if let Ok(hash) = security::calc_dir_integrity_hash_target(parent) { security::write_dir_integrity_hash(parent, hash); } } }
                        r
                    }).await.unwrap_or(Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Commit task failed"))));
                    if r.is_ok() { dirty_stats.remove(&e.inode); }
                    Ok(r)
                },
                EventType::SetXattr | EventType::RemoveXattr => {
                    let src_clone = src.clone();
                    let dst_clone = dst.clone();
                    // FIX: Wrapped in Ok()
                    Ok(spawn_blocking(move || { security::sync_xattrs(&src_clone, &dst_clone); }).await.unwrap_or(()).map_err(|_| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Xattr sync failed"))).or(Ok(())))
                },
                EventType::Chmod | EventType::Chown | EventType::Utimes => {
                    let src_clone = src.clone();
                    let dst_clone = dst.clone();
                    Ok(spawn_blocking(move || { security::apply_metadata(&src_clone, &dst_clone) }).await.unwrap_or(Ok(())))
                },
                EventType::Truncate => {
                    let dst_clone = dst.clone();
                    let length = e.length; 
                    Ok(spawn_blocking(move || { security::truncate_file(&dst_clone, length) }).await.unwrap_or(Ok(())))
                },
                EventType::Fallocate => {
                    let dst_clone = dst.clone();
                    let offset = e.offset;
                    let length = e.length;
                    let mode = e.mode as i32;
                    Ok(spawn_blocking(move || { security::do_fallocate(&dst_clone, offset, length, mode) }).await.unwrap_or(Ok(())))
                },
                _ => Ok(Ok(()))
            };

            // Flatten the nested Result<Result<...>>
            let final_res = match res {
                Ok(inner) => inner,
                Err(e) => Err(FoxingError::Join(e))
            };

            if let Err(err) = final_res {
                if let FoxingError::Io(e) = &err {
                    if let Some(28) = e.raw_os_error() {
                        error!("TARGET FULL (ENOSPC) on {:?}. Tripping circuit breaker immediately.", target_cfg.path);
                        capacity_breaker.trip();
                        io_attempt_successful = false; 
                        order.push_and_check(e); 
                        break; 
                    }
                }
                
                if limiter.check("io") { error!("IO Error: {:?}", err); }
                io_attempt_successful = false; 
                order.push_and_check(e);
                break;
            } else { failure_state.record_success(); }
            
            let elapsed = start.elapsed().as_secs_f64();
            let is_stressed = governor.is_system_stressed();
            tuner.tune(elapsed, &target_cfg, is_stressed, order.pending.len(), order.max_size, &tuner_board);
            metrics::REPLICATION_LATENCY.with_label_values(&[&target_cfg.path.to_string_lossy()]).observe(elapsed);
        }
        
        if !io_attempt_successful {
            failure_state.record_failure();
            warn!("Target {:?} failed, entering backoff ({:?}).", target_cfg.path, failure_state.retry_interval);
        }
    }
}
