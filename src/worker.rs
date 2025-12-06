use io_uring::IoUring;
use tokio::sync::mpsc;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::Ordering};
use std::io;
use crate::metrics;
use lru::LruCache;
use crate::identity::{self, ResolveResult};
use crate::security;
use crate::wal::{WalState};
use nix::sys::statvfs::statvfs;
use crate::config::{Config, TargetConfig};
use crate::error::{FoxingError, Result};
use crate::buffer::BufferPool;
use crate::tuner::{TunerBoard, BbrTuner, VdoTuner};
use crate::resilience::{PoisonCabinet, CircuitBreaker, FailureState, ErrorLimiter};
use crate::event::{Event, EventType};
use crate::operations::{SmartCopier, CopyStats};
use crate::governor::Governor;
use tokio::sync::RwLock;
use tracing::{info, warn, error, debug};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::time::sleep;
use std::time::{Instant, Duration};
use crate::ordering::Coalescer;
use crate::consistency::{SerializationEngine, OpKind, atomic_rename};
use crate::versioning;
use crate::mirror::SourceInfo;
use std::os::unix::fs::MetadataExt;
use libc;
use std::os::unix::io::AsRawFd;
fn copy_with_reflink_sync(src: &Path, dst: &Path) -> io::Result<u64> {
    if let Some(parent) = dst.parent() {
        if !parent.exists() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Copy Reflink: Failed to create parent {:?}: {}", parent, e);
            }
        }
    }
    let src_file = std::fs::File::open(src).map_err(|e| {
        debug!("Copy Reflink: Source {:?} not accessible: {}", src, e);
        e
    })?;
    let dst_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)
        .map_err(|e| {
            warn!("Copy Reflink: Failed to open destination {:?}: {}", dst, e);
            e
        })?;
    let src_fd = src_file.as_raw_fd();
    let dst_fd = dst_file.as_raw_fd();
    let len = src_file.metadata()?.len();
    let ret = unsafe {
        let req = 0x4020940D;
        #[repr(C)]
        struct FileCloneRange {
            src_fd: i64,
            src_offset: u64,
            src_length: u64,
            dest_offset: u64,
        }
        let args = FileCloneRange {
            src_fd: src_fd as i64,
            src_offset: 0,
            src_length: len,
            dest_offset: 0,
        };
        libc::ioctl(dst_fd, req, &args)
    };
    if ret == 0 {
        return Ok(len);
    }
    std::fs::copy(src, dst)
}
#[derive(Debug)]
pub struct HydrationSender(pub mpsc::Sender<(PathBuf, Option<u64>)>);
impl Clone for HydrationSender {
    fn clone(&self) -> Self {
        HydrationSender(self.0.clone())
    }
}
struct ShardedLockCache {
    shards: Vec<std::sync::Mutex<LruCache<u64, Arc<tokio::sync::Mutex<()>>>>>
}
impl ShardedLockCache {
    fn new(capacity_hint: usize) -> Self {
        let shard_count = 128;
        let per_shard = (capacity_hint / shard_count).max(100);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(std::sync::Mutex::new(LruCache::new(std::num::NonZeroUsize::new(per_shard).unwrap())));
        }
        Self { shards }
    }
    fn get(&self, key: u64) -> Arc<tokio::sync::Mutex<()>> {
        let idx = (key as usize) % 128;
        let mut s = self.shards[idx].lock().unwrap();
        s.get_or_insert(key, || Arc::new(tokio::sync::Mutex::new(()))).clone()
    }
    fn get_by_path(&self, path: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        self.get(hasher.finish())
    }
}
struct WorkerContext<'a> {
    ring: &'a mut IoUring,
    vdo_tuner: &'a mut VdoTuner,
    failure_state: &'a mut FailureState,
    capacity_breaker: &'a CircuitBreaker,
    _limiter: &'a ErrorLimiter,
    _cur_cap_avail: u64,
    _cur_cap_total: u64,
    daemon_id: &'a str,
}
fn initialize_buffer_pool(ring: &mut IoUring, num_buffers: usize, chunk_size_bytes: usize) -> Result<BufferPool> {
    let mut pool = BufferPool::new(num_buffers, chunk_size_bytes);
    let iovs = pool.as_io_vecs();
    if unsafe { ring.submitter().register_buffers(&iovs) }.is_err() {
        error!("Failed to register {} io_uring buffers.", num_buffers);
        return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::Other, "Failed to register buffers")));
    }
    Ok(pool)
}
fn unregister_buffers(ring: &mut IoUring) -> Result<()> {
    if ring.submitter().unregister_buffers().is_err() {
        error!("Failed to unregister io_uring buffers.");
        return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::Other, "Failed to unregister buffers")));
    }
    Ok(())
}
pub async fn run_worker(
    mut rx_main: mpsc::Receiver<Arc<Event>>,
    source: Arc<SourceInfo>,
    target_cfg: TargetConfig,
    config: Arc<RwLock<Config>>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    hydration_trigger: Arc<HydrationSender>,
    _repair_txs: Arc<Vec<mpsc::Sender<Arc<Event>>>>,
    _governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_id: usize,
    _rx_repair: Option<mpsc::Receiver<Arc<Event>>>,
    serialization: Arc<SerializationEngine>,
    daemon_id: String,
) -> Result<()> {
    info!("Worker {} started (Causal Lane)", worker_id);
    // FIX (High Priority): Re-introduce Root Identity Seeding, coordinated by Worker 0.
    if worker_id == 0 {
        if let Ok(meta) = std::fs::metadata(&source.path) {
            let inode = meta.ino();
            let dev = source.dev;
            if source.dir_map.get(inode).is_none() {
                info!("Worker {}: Seeding Root Identity for {:?} -> Inode {} (Dev {})", worker_id, source.path, inode, dev);
                identity::update_map(
                    &source.inode_map,
                    &source.dir_map,
                    dev,
                    inode,
                    PathBuf::from(""),
                    0,
                    false,
                    true,
                    0,
                    0
                );
            }
        } else {
            warn!("Worker {}: Failed to stat source root {:?}. Root identity will be missing!", worker_id, source.path);
        }
    }
    let queue_max_hint = config.read().await.queue_max;
    let locks = Arc::new(ShardedLockCache::new(queue_max_hint));
    let mut coalescer = Coalescer::new(target_cfg.ordering_scan_depth);
    // Removed: let mut dirty_stats: HashMap<u64, DirtyEntry> = HashMap::new();
    let initial_flush_ms = target_cfg.worker_flush_interval_ms;
    let mut flush_timer = Box::pin(tokio::time::sleep(Duration::from_millis(initial_flush_ms)));
    let mut last_capacity_check = Instant::now();
    // Initialize Tuners
    let mut tuner = BbrTuner::new(&target_cfg);
    let mut vdo_tuner = VdoTuner::new(target_cfg.vdo_optimization);
    // Initialize IoUring
    let ring_depth = tuner.recommended_ring_depth().max(32);
    debug!("Worker {}: IoUring depth set to {}", worker_id, ring_depth);
    let mut ring = match IoUring::new(ring_depth) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create io_uring: {}", e); return Err(FoxingError::Io(e)); }
    };
    // Buffer Pool Setup
    let config_reader = config.read().await;
    let buffer_chunk_size_mib = target_cfg.io_buffer_size_mib.max(1);
    let buffer_chunk_size_bytes = (buffer_chunk_size_mib * 1024 * 1024) as usize;
    let capacity_threshold_mb = config_reader.capacity_threshold_mb;
    let _force_flush_base_secs = config_reader.force_flush_interval_secs;
    let total_workers = config_reader.worker_count.max(1);
    let global_limit_mib = config.read().await.global_buffer_limit;
    // Dynamic memory partitioning
    let total_worker_mem_limit_mib = global_limit_mib * 3 / 10; // 30% of global limit for workers
    let worker_mem_limit_mib = total_worker_mem_limit_mib / total_workers as u64;
    let current_max_buffers = (worker_mem_limit_mib / buffer_chunk_size_mib).max(2) as usize;
    let initial_num_buffers = current_max_buffers.min(target_cfg.batch_size).max(4);
    drop(config_reader);
    let mut buffer_pool = match initialize_buffer_pool(&mut ring, initial_num_buffers, buffer_chunk_size_bytes) {
        Ok(pool) => pool,
        Err(e) => return Err(e),
    };
    let src_rwf_uncached_ok = source.rwf_uncached_ok.load(Ordering::Relaxed);
    let dst_rwf_uncached_ok = target_cfg.rwf_uncached_ok.load(Ordering::Relaxed);
    // Resilience
    let limiter = ErrorLimiter::new();
    let capacity_breaker = CircuitBreaker::new(target_cfg.worker_hibernation_secs);
    let mut failure_state = FailureState::new(target_cfg.worker_hibernation_secs);
    let mut poison_cabinet = PoisonCabinet::new();
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;
    let mut is_hibernating = false;
    let mut shutdown_requested = false;
    let mut recent_bytes_processed = 0u64;
    // Alias wal_map outside the loop
    let wal_map = &source.wal_state_map;
    // --- Main Event Loop ---
    let _result: Result<()> = loop {
        // 1. Hibernation Check
        if !is_hibernating && failure_state.check_hibernation_needed() {
             if !is_hibernating {
                 warn!("Target {:?} failed. Hibernating.", target_cfg.path);
                 is_hibernating = true;
             }
        }
        if is_hibernating {
             tokio::select! {
                _ = shutdown_rx.recv() => break Ok(()),
                _ = &mut flush_timer => {
                    flush_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(5));
                    continue;
                }
                Some(_) = rx_main.recv() => { continue; } // Drain events while down
             }
        }
        // 2. Retry Backoff
        if !shutdown_requested {
             if let Some(delay) = failure_state.next_retry_delay() {
                 sleep(delay).await;
                 continue;
             }
        }
        // 3. Event Poll (Single-Lane)
        let event_poll_result = if !shutdown_requested {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Worker {}: Shutdown requested. Draining queue...", worker_id);
                    shutdown_requested = true;
                    // Attempt to grab one event from main queue to continue the draining process
                    match rx_main.try_recv() {
                        Ok(e) => {
                            metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                            Some(e)
                        }
                        Err(_) => None
                    }
                },
                // REMOVED: Priority Repair Lane (rx_repair). Repair logic now routes to HydrationQueue directly.
                Some(e) = rx_main.recv() => {
                    metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                    Some(e)
                },
                _ = &mut flush_timer => {
                    let tuner_tick_start = Instant::now();
                    let is_stressed = _governor.is_system_stressed();
                    let pending_len = coalescer.len();
                    let max_pending = target_cfg.queue_max;
                    let target_label = target_cfg.path.to_string_lossy().to_string();
                    let _recommended_depth = tuner.tune(
                        tuner_tick_start.elapsed().as_secs_f64(),
                        recent_bytes_processed,
                        is_stressed,
                        pending_len,
                        max_pending,
                        &tuner_board,
                        &target_label,
                        buffer_pool.chunk_size() as u64
                    );
                    recent_bytes_processed = 0;
                    let next_interval_ms = tuner.current_flush_ms;
                    flush_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(next_interval_ms));
                    let path_clone = target_cfg.path.clone();
                    let cap_check_interval = Duration::from_millis(target_cfg.worker_capacity_check_interval_ms);
                    if last_capacity_check.elapsed() >= cap_check_interval {
                         last_capacity_check = Instant::now();
                         if let Ok(s) = statvfs(&path_clone) {
                            cur_cap_total = s.blocks() * s.block_size();
                            cur_cap_avail = s.blocks_available() * s.block_size();
                            let label = target_cfg.path.to_string_lossy();
                            metrics::TARGET_CAPACITY_BYTES_TOTAL.with_label_values(&[&label]).set(cur_cap_total as f64);
                            metrics::TARGET_CAPACITY_BYTES_AVAILABLE.with_label_values(&[&label]).set(cur_cap_avail as f64);
                        }
                    }
                    None
                }
            }
        } else {
            // Drain queue aggressively during shutdown
            match rx_main.try_recv() {
                Ok(e) => {
                    metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                    Some(e)
                },
                Err(_) => None
            }
        };
        if let Some(event_ptr) = event_poll_result {
            coalescer.push(event_ptr.clone());
        } else if shutdown_requested {
            if coalescer.is_empty() {
                break Ok(())
            }
        }
        let current_coalesce_limit = tuner.current_coalesce_bytes;
        let effective_batch_size = if shutdown_requested {
            256
        } else {
            tuner.current_batch_size
        };
        // If we have any structural events, we process the single event and then
        // yield, effectively achieving serialization at the worker level.
        let mut events_to_process_raw = Vec::new();
        if coalescer.len() > 0 {
            if let Some(e) = coalescer.pop_batch(current_coalesce_limit) {
                events_to_process_raw.push(e);
            }
            let is_structural = events_to_process_raw.len() == 1 && events_to_process_raw[0].event_type.requires_global_ordering();
            if is_structural {
                // If the event is structural, process only this one event, regardless of batch size.
            } else {
                 // It's a non-structural event, try to fill the batch.
                while events_to_process_raw.len() < effective_batch_size {
                    if let Some(e) = coalescer.pop_batch(current_coalesce_limit) {
                        events_to_process_raw.push(e);
                    } else {
                        break;
                    }
                }
            }
        }
        for e in &events_to_process_raw {
            let (mut dst, is_synthetic, needs_creation) = if matches!(e.event_type, EventType::Rename | EventType::Unlink | EventType::Rmdir) {
                let parent_path_opt = identity::resolve_directory(&source.dir_map, &source.inode_map, e.dev_id, e.parent_inode);
                if let Some(pp) = parent_path_opt {
                    let rel_path = pp.join(&e.name);
                    (target_cfg.path.join(rel_path), false, false)
                } else {
                    let rel_path = PathBuf::from(e.name.trim_start_matches('/'));
                    (target_cfg.path.join(rel_path), true, false)
                }
            } else {
                match identity::resolve_target(&source.inode_map, e, &target_cfg.path) {
                    ResolveResult::Success(p, s, n) => (p, s, n),
                    ResolveResult::NeedsRepair(synthetic_path) => {
                        warn!("Worker {}: Generation mismatch for inode {}. Queueing repair.", worker_id, e.inode);
                        if let Some(parent) = synthetic_path.parent() {
                             let _ = hydration_trigger.0.try_send((parent.to_path_buf(), Some(e.inode)));
                        }
                        continue;
                    }
                }
            };
             if matches!(e.event_type, EventType::Rmdir | EventType::Unlink) {
                 let target_exists = std::fs::symlink_metadata(&dst).is_ok();
                 if !target_exists {
                     debug!("Worker {}: Skipping {} for non-existent target {:?}", worker_id, e.event_type.as_str(), dst);
                     if e.inode != 0 { wal_map.clear_wal_state(e.inode); }
                     continue;
                 }
             }
             if e.event_type == EventType::SequenceGap {
                 warn!("Worker {}: Processing SequenceGap {} -> {}. Triggering repair.", worker_id, e.seq_num, e.name);
                 let path = target_cfg.path.clone();
                 if let Err(_) = hydration_trigger.0.try_send((path.clone(), None)) {
                     warn!("Worker {}: Hydration Trigger FULL. Failed to queue repair for gap at {:?}", worker_id, path);
                 }
                 continue;
             }
             if !poison_cabinet.check_allowed(e.inode) && e.event_type != EventType::Mkdir {
                continue;
             }
             let mut ctx = WorkerContext {
                ring: &mut ring,
                vdo_tuner: &mut vdo_tuner,
                failure_state: &mut failure_state,
                capacity_breaker: &capacity_breaker,
                _limiter: &limiter,
                _cur_cap_avail: cur_cap_avail,
                _cur_cap_total: cur_cap_total,
                daemon_id: &daemon_id,
            };
            let op_kind = match e.event_type {
                EventType::Rename | EventType::Mkdir | EventType::Rmdir |
                EventType::Link | EventType::Symlink | EventType::Unlink => OpKind::Rename,
                _ => OpKind::Write,
            };
            let barrier_inode = if e.inode == 0 && e.event_type == EventType::Unlink {
                let rough_target_path = target_cfg.path.join(e.name.trim_start_matches('/'));
                if let Ok(meta) = std::fs::symlink_metadata(&rough_target_path) {
                    meta.ino()
                } else {
                    0
                }
            } else {
                e.inode
            };
            let _barrier_guard = serialization.acquire_barrier(barrier_inode, op_kind).await;
            let mut src = if is_synthetic {
                source.mount.join(e.name.trim_start_matches('/'))
            } else {
                match dst.strip_prefix(&target_cfg.path) {
                    Ok(rel) => source.mount.join(rel),
                    Err(_) => source.mount.join(e.name.trim_start_matches('/'))
                }
            };
            let lock = locks.get_by_path(&e.name);
            let _g = lock.lock().await;
            let mut attempts = 0;
            let max_retries = 5;
            loop {
                attempts += 1;
                let res = process_single_event_inner(
                    &mut ctx, e.clone(), &source, &target_cfg, &tuner,
                    capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src, &mut buffer_pool,
                    src_rwf_uncached_ok,
                    dst_rwf_uncached_ok,
                    worker_id,
                    hydration_trigger.clone(),
                ).await;
                match res {
                    Ok(Some(stats)) => {
                        recent_bytes_processed += stats.bytes_processed;
                        if e.inode != 0 {
                            if let Some(entry) = wal_map.get_entry_for_inode(e.inode) {
                                if entry.state == WalState::CommitPending {
                                    if let Some(mut guard) = wal_map.rearm_guard(entry) {
                                        wal_map.mark_committed(&mut guard); // Use new function
                                    }
                                }
                            }
                        }
                        break;
                    },
                    Ok(None) => {
                        if e.inode != 0 {
                            wal_map.clear_wal_state(e.inode);
                        }
                        break;
                    },
                    Err(FoxingError::Io(io_err)) if io_err.kind() == io::ErrorKind::NotFound => {
                        if attempts >= max_retries {
                             warn!("Worker {}: Event {}/Inode {} failed after {} retries (NotFound). Dropping & Repairing.", worker_id, e.seq_num, e.inode, max_retries);
                             if let Err(_) = hydration_trigger.0.try_send((src.clone(), Some(e.inode))) {
                                 warn!("Worker {}: Hydration Trigger FULL. Failed to queue repair for {:?}", worker_id, src);
                             }
                             break;
                        }
                        debug!("Worker {}: Event {} failed (NotFound). Retrying with fresh lookup (Attempt {}).", worker_id, e.seq_num, attempts);
                        let mut resolved_via_agressive_lookup = false;
                        if attempts == 1 {
                             let lookup_res = tokio::task::spawn_blocking({
                                 let source_clone = source.clone();
                                 let inode = e.inode;
                                 move || identity::resolve_and_update_path(&source_clone, inode, 0, 0, 0)
                             }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
                             if let Ok(new_rel_path) = lookup_res {
                                 let new_src = source.mount.join(&new_rel_path);
                                 let new_dst = target_cfg.path.join(&new_rel_path);
                                 debug!("Worker {}: Recovery found new path: {:?} -> {:?}", worker_id, dst, new_dst);
                                 src = new_src;
                                 dst = new_dst;
                                 resolved_via_agressive_lookup = true;
                             }
                        }
                        if resolved_via_agressive_lookup {
                            continue;
                        }
                        if !src.exists() {
                            if let Some(new_name) = &e.new_name {
                                let new_parent_ino = if e.new_parent_inode != 0 { e.new_parent_inode } else { e.parent_inode };
                                let parent_path_opt = identity::resolve_directory(&source.dir_map, &source.inode_map, e.dev_id, new_parent_ino);
                                if let Some(pp) = parent_path_opt {
                                    let expected_rel = pp.join(new_name);
                                    let expected_src = source.mount.join(&expected_rel);
                                    if let Ok(meta) = std::fs::metadata(&expected_src) {
                                        if meta.ino() == e.inode {
                                            debug!("Worker {}: Source Race: File moved from {:?} to {:?}. Retrying op.", worker_id, src, expected_src);
                                            src = expected_src;
                                            dst = target_cfg.path.join(expected_rel);
                                            continue;
                                        }
                                    }
                                }
                            }
                            debug!("Worker {}: Source file {:?} disappeared and identity lookup failed. Accepting deletion.", worker_id, src);
                            break;
                        }
                        let sleep_duration = Duration::from_millis(50 * (attempts as u64));
                        sleep(sleep_duration).await;
                        continue;
                    },
                    Err(err) => {
                        error!("Worker {}: Event failed with non-recoverable error: {:?}", worker_id, err);
                        if e.inode != 0 {
                            wal_map.clear_wal_state(e.inode);
                        }
                        break;
                    }
                }
            }
        }
        if events_to_process_raw.len() == 1 && events_to_process_raw[0].event_type.requires_global_ordering() {
             tokio::task::yield_now().await;
        }
    };
    let _ = unregister_buffers(&mut ring);
    Ok(())
}
async fn process_single_event_inner(
    ctx: &mut WorkerContext<'_>,
    e: Arc<Event>,
    source: &Arc<SourceInfo>,
    target_cfg: &TargetConfig,
    _tuner: &BbrTuner,
    _capacity_threshold_mb: u64,
    dst: &PathBuf,
    _is_synthetic: bool,
    needs_creation: bool,
    src: &PathBuf,
    buffer_pool: &mut BufferPool,
    src_rwf_uncached_ok: bool,
    dst_rwf_uncached_ok: bool,
    worker_id: usize,
    hydration_trigger: Arc<HydrationSender>,
) -> Result<Option<CopyStats>> {
    let inode = e.inode;
    let wal_map = &source.wal_state_map;
    let source_clone = source.clone(); // Clone Arc<SourceInfo> here for spawn_blocking blocks
    // FIX (High Priority): Refactor to move all Identity map updates for structural
    // events out of the spawn_blocking blocks to ensure consistency *before* the operation
    // completes (which is needed for subsequent events).
    let identity_update_sync = |rel: PathBuf, is_dir: bool| {
        identity::update_map(
            &source.inode_map,
            &source.dir_map,
            e.dev_id,
            inode,
            rel,
            e.generation,
            false, // is_synthetic
            is_dir,
            e.timestamp_ns,
            e.seq_num
        );
    };
    if needs_creation && !matches!(e.event_type, EventType::Mkdir | EventType::Symlink | EventType::Link | EventType::Mknod) {
        let dst_clone = dst.clone();
        let _target_cfg_path_clone = target_cfg.path.clone();
        let _e_dev = e.dev_id;
        let _e_generation = e.generation;
        let _e_ts = e.timestamp_ns;
        let _e_seq = e.seq_num;
        let _source_map_clone = source.inode_map.clone();
        let _source_dir_map_clone = source.dir_map.clone();
        let res = tokio::task::spawn_blocking(move || {
            let identity_dir = _target_cfg_path_clone.join(".mirror").join(".by-identity");
            let _ = std::fs::create_dir_all(&identity_dir);
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_clone) {
                Ok(_f) => {
                    if let Ok(rel) = dst_clone.strip_prefix(&_target_cfg_path_clone) {
                        // Identity update happens in the parent scope if successful.
                        return Ok(Some(rel.to_path_buf()));
                    }
                    Ok(None)
                },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    Ok(None)
                },
                Err(e) => return Err(e),
            }
        }).await;
        if let Ok(path_opt) = res.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io)) {
            metrics::SIDECAR_FILES_CREATED.inc();
            if let Some(rel_path) = path_opt {
                identity_update_sync(rel_path, false);
            }
        } else {
            debug!("Worker {}: Creation failed (likely race): {:?}", worker_id, e.name);
            return Ok(None);
        }
    }
    let mut wal_guard = None;
    if matches!(e.event_type, EventType::Write | EventType::Create | EventType::WriteRange) {
        // Attempt to begin WAL write
        match wal_map.begin_write(inode, dst.clone(), e.seq_num, ctx.daemon_id.to_string(), e.projid) {
            Ok(guard) => wal_guard = Some(guard),
            Err(e) => {
                if let FoxingError::Io(io_err) = &e {
                    if io_err.kind() == io::ErrorKind::Other && io_err.to_string().starts_with("WAL Conflict") {
                        debug!("WAL Conflict: Skipping write for Inode {} due to active/stale state.", inode);
                        return Ok(None);
                    }
                }
                return Err(e);
            }
        }
    }
    let res = match e.event_type {
        EventType::Write | EventType::Create | EventType::WriteRange => {
            let mut wal_guard_val = wal_guard.ok_or_else(|| {
                FoxingError::Io(io::Error::new(io::ErrorKind::Other, "WAL guard missing for write operation"))
            })?;
            let dst_clone = dst.clone();
            let e_offset = e.offset;
            let e_len = e.length;
            let mut src_clone_for_metadata = src.clone();
            // Advance WAL state to InProgress before starting IO
            wal_map.advance(&mut wal_guard_val, WalState::InProgress)?;
            let mut metadata_result = tokio::task::spawn_blocking({
                let p = src_clone_for_metadata.clone();
                move || std::fs::metadata(&p)
            }).await.unwrap_or(Err(io::ErrorKind::NotFound.into()));
            if metadata_result.is_err() {
                // Aggressive lookup on source path failure
                let resolved_path = tokio::task::spawn_blocking({
                    let source_clone = source_clone.clone();
                    move || identity::resolve_and_update_path(&source_clone, inode, 0, 0, 0)
                }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
                if let Ok(new_rel_path) = resolved_path {
                    let new_src = source.mount.join(new_rel_path);
                    debug!("Source Race: File moved from {:?} to {:?}. Retrying op.", src_clone_for_metadata, new_src);
                    src_clone_for_metadata = new_src.clone();
                    metadata_result = tokio::task::spawn_blocking(move || std::fs::metadata(&new_src)).await.unwrap_or(Err(io::ErrorKind::NotFound.into()));
                }
            }
            if let Ok(m) = metadata_result {
                if m.is_file() {
                    let current_src_size = m.len();
                    let dynamic_vdo_opt = ctx.vdo_tuner.should_check_zeros(m.len());
                    let copy_res = SmartCopier::copy(
                        &src_clone_for_metadata,
                        &dst_clone,
                        ctx.ring,
                        buffer_pool,
                        &target_cfg.supports_reflink,
                        dynamic_vdo_opt,
                        e_offset,
                        e_len,
                        target_cfg.direct_io_ok.load(Ordering::Relaxed),
                        current_src_size,
                        src_rwf_uncached_ok,
                        dst_rwf_uncached_ok,
                        target_cfg.vdo_stall_threshold,
                    ).await;
                    match copy_res {
                        Ok(stats) => {
                            // Advance WAL state to CommitPending after successful copy
                            wal_map.advance(&mut wal_guard_val, WalState::CommitPending)?;
                            metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(stats.bytes_processed as f64);
                            ctx.vdo_tuner.update(stats.bytes_processed, stats.bytes_zeros);
                            let apply_dst = dst_clone.clone();
                            let apply_src = src_clone_for_metadata.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                security::sync_xattrs(&apply_src, &apply_dst);
                                security::apply_metadata(&apply_src, &apply_dst)
                            }).await;
                            // Commit the WAL explicitly before returning Ok
                            wal_map.mark_committed(&mut wal_guard_val); // Use new function
                            Ok(Some(stats))
                        },
                        Err(e) => Err(e)
                    }
                } else {
                    // Source exists but is not a file (e.g., directory), clear WAL and skip.
                    wal_map.clear_wal_state(inode);
                    Ok(None)
                }
            } else {
                warn!("Source Missing: Failed to locate source for inode {} even after retry. ", inode);
                // Source disappeared, clear WAL and indicate failure to outer loop (for possible retry/deletion)
                wal_map.clear_wal_state(inode);
                Ok(None)
            }
        },
        EventType::Rename => {
            let is_dir = (e.mode & libc::S_IFMT) == libc::S_IFDIR;
            let new_rel_path = if let Some(new_name_str) = &e.new_name {
                let parent_ino = if e.new_parent_inode != 0 { e.new_parent_inode } else { e.parent_inode };
                if parent_ino != 0 {
                    match identity::resolve_directory(&source.dir_map, &source.inode_map, e.dev_id, parent_ino) {
                        Some(parent_rel) => parent_rel.join(new_name_str),
                        None => {
                            let source_clone = source_clone.clone();
                            // FIX (Critical): Move Identity operations out of spawn_blocking
                            let resolved = tokio::task::spawn_blocking(move || {
                                identity::resolve_and_update_path(&source_clone, parent_ino, 0, 0, 0)
                            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
                            // FIX (E0308): Correctly handle the Result returned by the async block
                            match resolved {
                                Ok(parent_rel) => parent_rel.join(new_name_str),
                                Err(_) => {
                                    warn!("Worker {}: Rename Parent {} lookup failed. Using naive fallback.", worker_id, parent_ino);
                                    PathBuf::from(new_name_str)
                                }
                            }
                        }
                    }
                } else {
                    PathBuf::from(new_name_str)
                }
            } else {
                PathBuf::new()
            };
            if let Some(_new_name_str) = &e.new_name {
                // FIX (High Priority): Update map *before* rename blocking call begins.
                identity::update_map_after_rename(
                    &source.inode_map,
                    &source.dir_map,
                    e.dev_id,
                    inode,
                    new_rel_path.clone(),
                    e.generation,
                    is_dir,
                    e.timestamp_ns,
                    e.seq_num
                );
                let old_dst_final_log = dst.clone();
                let new_dst_final_log = target_cfg.path.join(&new_rel_path);
                let old_dst_final_move = old_dst_final_log.clone();
                let new_dst_final_move = new_dst_final_log.clone();
                let source_mount = source.mount.clone();
                let source_clone_rename = source_clone.clone();
                let wal_map_clone = wal_map.clone();
                let new_rel_path_clone = new_rel_path.clone();
                let res = tokio::task::spawn_blocking(move || {
                    let start = Instant::now();
                    let max_wait = Duration::from_secs(5);
                    loop {
                        match atomic_rename(&old_dst_final_move, &new_dst_final_move) {
                            Ok(_) => {
                                // WAL clear handled by atomic_rename's internal Journal::end() on the target
                                wal_map_clone.clear_wal_state(inode);
                                return Ok(());
                            },
                            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                                let elapsed = start.elapsed();
                                if let Some(parent) = new_dst_final_move.parent() {
                                    if !parent.exists() {
                                        debug!("Worker: Rename target dir {:?} missing. Creating.", parent);
                                        let _ = std::fs::create_dir_all(parent);
                                        continue;
                                    }
                                }
                                if !old_dst_final_move.exists() {
                                    if new_dst_final_move.exists() {
                                         debug!("Worker: Rename target {:?} already exists. Assuming previous success (Stale event).", new_dst_final_move);
                                         wal_map_clone.clear_wal_state(inode);
                                         return Ok(());
                                    }
                                    let resolved_path = identity::resolve_and_update_path(&source_clone_rename, inode, 0, 0, 0).ok();
                                    if let Some(rel) = resolved_path {
                                        let current_src_abs = source_mount.join(&rel);
                                        if current_src_abs.exists() {
                                            debug!("Worker: Rename Source {:?} missing on target. Attempting SELF-HEAL copy from {:?} -> {:?}",
                                                    old_dst_final_move, current_src_abs, new_dst_final_move);
                                            match copy_with_reflink_sync(&current_src_abs, &new_dst_final_move) {
                                                Ok(_) => {
                                                    let _ = std::fs::remove_file(&old_dst_final_move);
                                                    info!("Worker: SELF-HEAL Success.");
                                                    wal_map_clone.clear_wal_state(inode);
                                                    return Ok(());
                                                }
                                                Err(e) => {
                                                    warn!("Worker: SELF-HEAL Failed: {}", e);
                                                }
                                            }
                                        }
                                    }
                                    return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::NotFound, "Source disappeared")));
                                }
                                if elapsed > max_wait {
                                    return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::NotFound, format!("Source missing after {}s retry: {}", elapsed.as_secs_f64(), e))));
                                }
                                std::thread::sleep(Duration::from_millis(10));
                                continue;
                            },
                            Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) || e.raw_os_error() == Some(libc::ETXTBSY) => {
                                let elapsed = start.elapsed();
                                if elapsed > max_wait {
                                     return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::TimedOut, format!("Timeout on BUSY file: {}", e))));
                                }
                                std::thread::sleep(Duration::from_millis(50));
                                continue;
                            },
                            Err(e) => {
                                wal_map_clone.clear_wal_state(inode);
                                return Err(FoxingError::Io(e));
                            },
                        }
                    }
                }).await.map_err(FoxingError::Join).and_then(|r| r);
                match res {
                    Ok(_) => {
                        metrics::RENAME_EVENTS.inc();
                        info!("Worker {}: Atomic Rename SUCCESS: {:?} -> {:?}", worker_id, old_dst_final_log, new_dst_final_log);
                    },
                    Err(FoxingError::Io(ref io_err)) if io_err.kind() == io::ErrorKind::NotFound => {
                        if new_dst_final_log.exists() {
                            wal_map.clear_wal_state(inode);
                            return Ok(None);
                        }
                        warn!("Worker {}: Source disappeared during rename. Triggering repair for NEW path: {:?}.", worker_id, new_rel_path_clone);
                        let new_src = source.mount.join(&new_rel_path_clone);
                        if let Err(_) = hydration_trigger.0.try_send((new_src, Some(inode))) {
                            warn!("Worker {}: Hydration Trigger FULL. Failed to queue repair for Rename Target", worker_id);
                        }
                        wal_map.clear_wal_state(inode);
                        return Ok(None);
                    },
                    Err(io_err) => {
                        ctx.failure_state.record_failure();
                        wal_map.clear_wal_state(inode);
                        return Err(io_err);
                    }
                }
                return Ok(None);
            } else { return Ok(None); }
        },
        EventType::Mkdir => {
            let dst_clone = dst.clone();
            let _target_cfg_path_clone = target_cfg.path.clone();
            let res = tokio::task::spawn_blocking(move || {
                let r = std::fs::create_dir_all(&dst_clone);
                if r.is_ok() {
                    if let Ok(rel) = dst_clone.strip_prefix(&_target_cfg_path_clone) {
                        return Ok(Some(rel.to_path_buf()));
                    }
                }
                Ok(None)
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
            if let Ok(path_opt) = res {
                if let Some(rel_path) = path_opt {
                    identity_update_sync(rel_path, true);
                }
            }
            Ok(None)
        },
        EventType::Unlink => {
             let src_dev = e.dev_id;
             identity::remove_entry(&source.inode_map, &source.dir_map, src_dev, inode);
             wal_map.clear_wal_state(inode);
             let dst_clone = dst.clone();
             let _res = tokio::task::spawn_blocking(move || {
                 if dst_clone.exists() {
                     std::fs::remove_file(&dst_clone)
                 } else {
                     Ok(())
                 }
             }).await;
             Ok(None)
        },
        EventType::Fsync => {
            if let Some(wal_guard_entry) = wal_map.get_entry_for_inode(inode) {
                if wal_guard_entry.state == WalState::CommitPending {
                    debug!("Fsync: Committing epoch for Inode {} (Seq {})", inode, wal_guard_entry.seq);
                    let dst_clone = wal_guard_entry.path.clone();
                    let seq = wal_guard_entry.seq;
                    let projid = wal_guard_entry.projid;
                    let commit_res = tokio::task::spawn_blocking(move || {
                        security::commit_epoch(&dst_clone, seq, projid)
                    }).await.map_err(FoxingError::Join).and_then(|r| r);
                    match commit_res {
                        Ok(_) => {
                            wal_map.clear_wal_state(inode);
                            info!("Fsync: Inode {} Commit & WAL Clear OK.", inode);
                        },
                        Err(e) => {
                            error!("Fsync: Failed to commit epoch for Inode {}: {:?}. Clearing WAL state for next attempt.", inode, e);
                            wal_map.clear_wal_state(inode);
                        }
                    }
                } else if wal_guard_entry.state == WalState::InProgress {
                    debug!("Fsync: Inode {} ignored, still InProgress.", inode);
                }
            }
            Ok(None)
        }
        EventType::Rmdir => {
            let src_dev = e.dev_id;
            identity::remove_entry(&source.inode_map, &source.dir_map, src_dev, inode);
            wal_map.clear_wal_state(inode);
            let dst_clone = dst.clone();
            let _res = tokio::task::spawn_blocking(move || {
                if dst_clone.exists() {
                    std::fs::remove_dir(&dst_clone)
                } else {
                    Ok(())
                }
            }).await;
            Ok(None)
        }
        EventType::Link => {
             let src_dev = e.dev_id;
             let new_path = src.to_path_buf(); // src is the new link target (relative to mount)
             identity::update_map_after_rename(
                &source.inode_map,
                &source.dir_map,
                src_dev,
                inode,
                new_path.clone(),
                e.generation,
                (e.mode & libc::S_IFMT) == libc::S_IFDIR,
                e.timestamp_ns,
                e.seq_num
            );
            let new_dst = dst.clone();
            let src_clone = source.mount.join(src);
            let res = tokio::task::spawn_blocking(move || {
                security::create_hard_link(&src_clone, &new_dst)
            }).await.map_err(FoxingError::Join).and_then(|r| r);
            if res.is_err() {
                 error!("Link failed for {:?} -> {:?}: {:?}", src, new_dst, res);
                 return Err(res.unwrap_err());
            }
            Ok(None)
        }
        EventType::Symlink => {
            let src_dev = e.dev_id;
            // Name contains the target link content
            let target_content = e.name.clone();
            let link_path = dst.clone();
            let link_path_rel = link_path.strip_prefix(&target_cfg.path).unwrap_or(link_path.as_path()).to_path_buf();
            identity::update_map_after_rename(
                &source.inode_map,
                &source.dir_map,
                src_dev,
                inode,
                link_path_rel,
                e.generation,
                (e.mode & libc::S_IFMT) == libc::S_IFDIR,
                e.timestamp_ns,
                e.seq_num
            );
            let res = tokio::task::spawn_blocking(move || {
                security::create_symlink(&target_content, &link_path)
            }).await.map_err(FoxingError::Join).and_then(|r| r);
            if res.is_err() {
                error!("Symlink failed for {} -> {:?}: {:?}", target_content, link_path, res);
                return Err(res.unwrap_err());
            }
            Ok(None)
        }
        EventType::Mknod => {
             let src_dev = e.dev_id;
             let new_path_rel = dst.strip_prefix(&target_cfg.path).unwrap_or(dst.as_path()).to_path_buf();
             let dev = e.length; // Kernel sends device ID in length for mknod/device files
             identity::update_map_after_rename(
                &source.inode_map,
                &source.dir_map,
                src_dev,
                inode,
                new_path_rel,
                e.generation,
                (e.mode & libc::S_IFMT) == libc::S_IFDIR,
                e.timestamp_ns,
                e.seq_num
            );
            let new_dst = dst.clone();
            let mode = e.mode;
            let res = tokio::task::spawn_blocking(move || {
                security::create_mknod(&new_dst, mode, dev)
            }).await.map_err(FoxingError::Join).and_then(|r| r);
            if res.is_err() {
                error!("Mknod failed for {:?}: {:?}", new_dst, res);
                return Err(res.unwrap_err());
            }
            Ok(None)
        }
        EventType::Truncate | EventType::Fallocate => {
            let dst_clone = dst.clone();
            let new_size = e.length;
            let offset = e.offset;
            let flags = e.flags as i32;
            let res = tokio::task::spawn_blocking(move || {
                if e.event_type == EventType::Truncate {
                    security::truncate_file(&dst_clone, new_size)
                } else {
                    security::do_fallocate(&dst_clone, offset, new_size, flags)
                }
            }).await.map_err(FoxingError::Join).and_then(|r| r);
            if res.is_err() {
                error!("{} failed for {:?}: {:?}", e.event_type.as_str(), dst, res);
                return Err(res.unwrap_err());
            }
            Ok(None)
        }
        EventType::SetXattr | EventType::RemoveXattr => {
            let dst_clone = dst.clone();
            let apply_src = src.clone();
            let _res = tokio::task::spawn_blocking(move || {
                security::sync_xattrs(&apply_src, &dst_clone)
            }).await;
            Ok(None)
        }
        EventType::Chmod | EventType::Chown | EventType::Utimes => {
            let dst_clone = dst.clone();
            let apply_src = src.clone();
            let _res = tokio::task::spawn_blocking(move || {
                security::apply_metadata(&apply_src, &dst_clone)
            }).await;
            Ok(None)
        }
        _ => {
            wal_map.clear_wal_state(inode);
            Ok(None)
        }
    };
    match res {
        Ok(stats_opt) => {
            Ok(stats_opt)
        },
        Err(err) => {
            wal_map.clear_wal_state(inode);
            if let FoxingError::Io(io_err) = &err {
                if let Some(28) = io_err.raw_os_error() {
                     error!("TARGET FULL (ENOSPC).");
                     ctx.capacity_breaker.trip();
                     ctx.failure_state.record_failure();
                     let target_root_path = target_cfg.path.parent().unwrap_or(&target_cfg.path).to_path_buf();
                     let _ = tokio::task::spawn_blocking(move || {
                        versioning::prune_global_history(&target_root_path, 512 * 1024 * 1024)
                    }).await;
                }
            }
            return Err(err);
        }
    }
}
