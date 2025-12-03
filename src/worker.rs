use std::{
    sync::{Arc, atomic::Ordering},
    collections::HashMap,
    time::{Instant, Duration},
};
use tokio::{sync::mpsc, task::spawn_blocking};
use io_uring::IoUring;
use std::os::unix::io::AsRawFd;
// FIX: Removed unused 'config::TargetProfile' import
use crate::{event::{Event, EventType}, mirror::{SourceInfo, SharedConfig}, config::{TargetConfig}, buffer::{BufferPool}, security, sidecar, identity, Result, versioning, governor::Governor};
use crate::operations::{SmartCopier, CopyStats};
use tokio::time::interval;
use tracing::{warn, info, error};
use crate::error::{FoxingError};
use std::io;
use crate::metrics;
use parking_lot::Mutex;
use lru::LruCache;
use std::path::{Path, PathBuf};
use nix::sys::statvfs::statvfs;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::time::sleep;
use crate::wal::{ExpectedState, DirtyEntry};
use crate::tuner::{TunerBoard, TunerState, BbrTuner, VdoTuner};
use crate::resilience::{PoisonCabinet, FailureState, CircuitBreaker, ErrorLimiter};
// Removed unused 'libc' import
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
struct WorkerContext<'a> {
    ring: &'a mut IoUring,
    dirty_stats: &'a mut HashMap<u64, DirtyEntry>,
    vdo_tuner: &'a mut VdoTuner,
    failure_state: &'a mut FailureState,
    capacity_breaker: &'a CircuitBreaker,
    limiter: &'a ErrorLimiter,
    cur_cap_avail: u64,
    cur_cap_total: u64,
}
// Helper to initialize the buffer pool and register with io_uring
fn initialize_buffer_pool(ring: &mut IoUring, num_buffers: usize, chunk_size_bytes: usize) -> Result<BufferPool> {
    let mut pool = BufferPool::new(num_buffers, chunk_size_bytes);
    let iovs = pool.as_io_vecs();
    // E0133 Fix: Added unsafe block around register_buffers
    if unsafe { ring.submitter().register_buffers(&iovs) }.is_err() {
        error!("Failed to register {} io_uring buffers.", num_buffers);
        return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::Other, "Failed to register buffers")));
    }
    Ok(pool)
}
// Helper to unregister buffers (to be called on worker shutdown)
fn unregister_buffers(ring: &mut IoUring) -> Result<()> {
    // Redundant unsafe removed (Warning fix)
    if ring.submitter().unregister_buffers().is_err() {
        error!("Failed to unregister io_uring buffers.");
        return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::Other, "Failed to unregister buffers")));
    }
    Ok(())
}
pub async fn run_worker(
    source: Arc<SourceInfo>,
    target_cfg: TargetConfig,
    mut rx_main: mpsc::Receiver<Arc<Event>>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    _hydration_tx: mpsc::Sender<PathBuf>,
    config: SharedConfig,
    _governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_id: usize,
    mut rx_repair: Option<mpsc::Receiver<Arc<Event>>>,
) -> Result<()> {
    let target_latency_ms = target_cfg.autotune_target_latency_ms;
    let mut order = crate::ordering::OrderBuf::new(target_latency_ms);
    let is_control_plane = worker_id == 0;
    let role_name = if is_control_plane { "ControlPlane" } else { "DataPlane" };
    info!("Worker {} started as {}", worker_id, role_name);
    let locks = Arc::new(ShardedLockCache::new());
    let mut dirty_stats: HashMap<u64, DirtyEntry> = HashMap::new();
    let mut flush_interval = interval(Duration::from_millis(100));
    let config_reader = config.read().await;
    // Initial static sizing based on config limits (fallback/max allocation)
    let buffer_chunk_size_mib = target_cfg.io_buffer_size_mib.max(1); // Use target's tuned value
    let buffer_chunk_size_bytes = (buffer_chunk_size_mib * 1024 * 1024) as usize;
    let capacity_threshold_mb = config_reader.capacity_threshold_mb;
    let force_flush_base_secs = config_reader.force_flush_interval_secs;
    let total_workers = config_reader.worker_count.max(1);
    let global_limit_mib = config_reader.global_buffer_limit;
    // Heuristic: Max initial allocation based on static config for safety
    let total_worker_mem_limit_mib = global_limit_mib * 3 / 10;
    let worker_mem_limit_mib = total_worker_mem_limit_mib / total_workers as u64;
    let initial_max_buffers = (worker_mem_limit_mib / buffer_chunk_size_mib).max(2) as usize;
    let initial_num_buffers = initial_max_buffers.min(target_cfg.batch_size);
    drop(config_reader);
    let mut ring = match IoUring::new(target_cfg.batch_size as u32) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create io_uring: {}", e); return Err(FoxingError::Io(e)); }
    };
    // Initialize initial buffer pool and register.
    let mut buffer_pool = match initialize_buffer_pool(&mut ring, initial_num_buffers, buffer_chunk_size_bytes) {
        Ok(pool) => pool,
        Err(e) => return Err(e),
    };

    // --- NEW: Load RWF_UNCACHED capabilities from Source and Target configs ---
    // Source Read Capability
    let src_rwf_uncached_ok = source.rwf_uncached_ok.load(Ordering::Relaxed);
    // Target Write Capability
    let dst_rwf_uncached_ok = target_cfg.rwf_uncached_ok.load(Ordering::Relaxed);
    // -------------------------------------------------------------------------
    
    info!("Worker {}: BufferPool initialized with {} x {}MB chunks (Total {}MB).",
          worker_id, buffer_pool.capacity(), buffer_chunk_size_mib, buffer_pool.capacity() as u64 * buffer_chunk_size_mib);
    let limiter = ErrorLimiter::new();
    let capacity_breaker = CircuitBreaker::new(config.read().await.breaker_interval_secs);
    let hibernation_threshold_secs = 300;
    let mut failure_state = FailureState::new(hibernation_threshold_secs);
    let mut poison_cabinet = PoisonCabinet::new();
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;
    let mut tuner = BbrTuner::new(&target_cfg);
    let mut vdo_tuner = VdoTuner::new(target_cfg.vdo_optimization);
    let mut is_hibernating = false;
    let mut last_dropped_check = 0u64;
    let mut _gap_mode_until = Instant::now();
    let result = loop {
        if !is_hibernating && failure_state.check_hibernation_needed() {
             if !is_hibernating {
                 warn!("Target {:?} failed. Hibernating.", target_cfg.path);
                 is_hibernating = true;
             }
        }
        if is_hibernating {
             tokio::select! {
                _ = shutdown_rx.recv() => break Ok(()),
                _ = flush_interval.tick() => { continue; }
                Some(_) = rx_main.recv() => { continue; }
             }
        }
        if !failure_state.can_execute_io() {
             sleep(Duration::from_millis(100)).await;
             continue;
        }
        let event_poll_result = tokio::select! {
            _ = shutdown_rx.recv() => break Ok(()),
            Some(e) = async {
                if let Some(rx) = &mut rx_repair {
                    rx.recv().await
                } else {
                    std::future::pending().await
                }
            } => { Some(e) },
            Some(e) = rx_main.recv() => {
                metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::Relaxed);
                Some(e)
            },
            _ = flush_interval.tick() => {
                // --- TUNER TICK & ADAPTIVE BUFFER SIZING (LOGGING ONLY) ---
                let tuner_tick_start = Instant::now();
                let is_stressed = _governor.is_system_stressed();
                let pending_len = order.len();
                let max_pending = target_cfg.queue_max;
                let target_label = target_cfg.path.to_string_lossy().to_string();
                let bytes_processed = 0; // The actual I/O happens in `process_single_event_inner`
                let recommended_depth = tuner.tune(
                    tuner_tick_start.elapsed().as_secs_f64(),
                    bytes_processed,
                    is_stressed,
                    pending_len,
                    max_pending,
                    &tuner_board,
                    &target_label,
                    buffer_pool.chunk_size() as u64
                );
                // ADAPTIVE BUFFER POOL ADJUSTMENT LOGGING
                if recommended_depth != buffer_pool.capacity() {
                    // Note: In 0.7.x, resizing the pool and re-registering buffers is high overhead.
                    // We stick to the statically allocated size and use the BBR-recommended depth
                    // as the *maximum concurrent submissions* limit inside process_single_event_inner.
                    warn!("Worker {}: BBR recommended I/O depth {} (Allocated pool size: {}).", worker_id, recommended_depth, buffer_pool.capacity());
                }
                // ... [Capacity and Flush Checks] ...
                let path_clone = target_cfg.path.clone();
                if flush_interval.period().as_millis() == 100 && (Instant::now().elapsed().as_millis() % 1000 < 150) {
                     if let Ok(s) = statvfs(&path_clone) {
                        cur_cap_total = s.blocks() * s.block_size();
                        cur_cap_avail = s.blocks_available() * s.block_size();
                        let label = target_cfg.path.to_string_lossy();
                        metrics::TARGET_CAPACITY_BYTES_TOTAL.with_label_values(&[&label]).set(cur_cap_total as f64);
                        metrics::TARGET_CAPACITY_BYTES_AVAILABLE.with_label_values(&[&label]).set(cur_cap_avail as f64);
                    }
                }
                if !is_hibernating {
                    let current_dropped = metrics::EVENTS_DROPPED.get();
                    if current_dropped > last_dropped_check {
                        warn!("Detect event drops ({} -> {}).", last_dropped_check, current_dropped);
                        last_dropped_check = current_dropped;
                    }
                    let order_len = order.len();
                    let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);
                     if current_state == TunerState::CriticalDrain && order_len < 100 {
                         tuner_board.insert(target_cfg.path.clone(), TunerState::Drain);
                         tuner.state = TunerState::Drain;
                    }
                    let now = Instant::now();
                    let mut flushing_stats: HashMap<u64, DirtyEntry> = HashMap::new();
                    dirty_stats.retain(|&ino, entry| {
                        if now.duration_since(entry.first_dirty) > Duration::from_secs(force_flush_base_secs) {
                            flushing_stats.insert(ino, entry.clone());
                            false
                        } else { true }
                    });
                    let _committed_inos = spawn_blocking(move || {
                        let mut committed = Vec::new();
                        for (ino, entry) in flushing_stats.into_iter() {
                            if security::commit_epoch(&entry.path, entry.seq, entry.projid).is_ok() {
                                sidecar::clear_wal_state(&entry.path);
                                committed.push(ino);
                            }
                        }
                        committed
                    }).await.unwrap_or_default();
                }
                None
            }
        };
        if event_poll_result.is_none() { continue; }
        let event_ptr = event_poll_result.unwrap();
        if event_ptr.event_type == EventType::SequenceGap {
            tracing::warn!("Sequence Gap detected on dev {}. Activating Gap Recovery Mode.", event_ptr.dev_id);
            let mut new_gap_end = Instant::now() + Duration::from_secs(5);
            let expected_gap_size = event_ptr.seq_num.saturating_sub(order.next_seq);
            if expected_gap_size > 100 {
                let extra_secs = (expected_gap_size / 100).min(30);
                new_gap_end += Duration::from_secs(extra_secs);
            }
            _gap_mode_until = new_gap_end;
            order.next_seq = event_ptr.seq_num;
            continue;
        }
        if !order.push_and_check(event_ptr.clone()) {
             metrics::EVENTS_DROPPED.inc();
             continue;
        }
        let current_coalesce_limit = if is_control_plane { 0 } else { tuner.current_coalesce_bytes };
        let effective_batch_size = if is_control_plane { 1 } else { tuner.current_batch_size };
        let events_to_process_raw = {
            let mut batch = Vec::new();
            while batch.len() < effective_batch_size {
                if let Some(e) = order.pop_batch(current_coalesce_limit) {
                    batch.push(e);
                } else {
                    break;
                }
            }
            batch
        };
        for (e, _is_coalesced) in events_to_process_raw {
             if !poison_cabinet.check_allowed(e.inode) && e.event_type != EventType::Mkdir {
                continue;
             }
             let mut ctx = WorkerContext {
                ring: &mut ring,
                dirty_stats: &mut dirty_stats,
                vdo_tuner: &mut vdo_tuner,
                failure_state: &mut failure_state,
                capacity_breaker: &capacity_breaker,
                limiter: &limiter,
                cur_cap_avail,
                cur_cap_total,
            };
            let (dst, is_synthetic, needs_creation) = identity::resolve_target(&source.inode_map, &e, &target_cfg.path);
            let src = if is_synthetic {
                source.mount.join(e.name.trim_start_matches('/'))
            } else {
                match dst.strip_prefix(&target_cfg.path) {
                    Ok(rel) => source.mount.join(rel),
                    Err(_) => source.mount.join(e.name.trim_start_matches('/'))
                }
            };
            let lock = locks.get_by_path(&e.name);
            let _g = lock.lock().await;
            // Pass the mutable buffer pool reference
            let _ = process_single_event_inner(
                &mut ctx, e, &source, &target_cfg, &tuner,
                capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src, &mut buffer_pool,
                src_rwf_uncached_ok, // Source Read Capability
                dst_rwf_uncached_ok, // Target Write Capability
            ).await;
        }
    };
    // Clean up: Unregister buffers on exit
    let _ = unregister_buffers(&mut ring);
    result
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
    buffer_pool: &mut BufferPool,
    src_rwf_uncached_ok: bool, // Source Read Capability
    dst_rwf_uncached_ok: bool, // Target Write Capability
) -> Result<Option<CopyStats>> {
    let target_cfg_cap = target_cfg.clone();
    let target_cfg_allow = target_cfg.clone();
    let capacity_breaker_clone = ctx.capacity_breaker.clone();
    let check_capacity_result = spawn_blocking(move || {
        capacity_breaker_clone.can_proceed(&target_cfg_cap.path, capacity_threshold_mb)
    }).await.unwrap_or(false);
    if !check_capacity_result {
        if ctx.limiter.check("capacity") { error!("TARGET FULL (ENOSPC)."); }
        ctx.capacity_breaker.trip();
        ctx.failure_state.record_failure();
        return Err(FoxingError::Io(io::ErrorKind::Other.into())); // Return FoxingError::Io wrapped in io::ErrorKind::Other
    }
    let e_clone = e.clone();
    let allow_result = spawn_blocking(move || {
        target_cfg_allow.allow(std::path::Path::new(&e_clone.name))
    }).await.unwrap_or(false);
    if !allow_result {
        metrics::EVENTS_FILTERED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc();
        return Ok(None);
    }
    if e.name.contains(".tmp.") { return Ok(None); }
    if !is_synthetic {
        let target_dir = dst.parent().map(|p| p.to_path_buf());
        if let Some(target_dir) = target_dir {
            if target_dir != target_cfg.path {
                let check_res: Result<()> = match spawn_blocking({
                    let target_dir_clone = target_dir.clone();
                    move || {
                        if target_dir_clone.exists() && !target_dir_clone.is_dir() {
                            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Path component is a file, not a directory"));
                        }
                        std::fs::create_dir_all(&target_dir_clone)
                    }
                }).await {
                    Ok(inner_res) => inner_res.map_err(FoxingError::Io),
                    Err(e) => Err(FoxingError::Join(e))
                };
                if check_res.is_err() {
                    warn!("Failed to prepare target directory {:?}: {:?}", target_dir, check_res.err());
                }
            }
        }
    }
    if needs_creation {
        let dst_clone = dst.clone();
        let target_cfg_clone = target_cfg.clone();
        let e_inode = e.inode;
        let e_generation = e.generation;
        let source_map = source.inode_map.clone();
        let res = spawn_blocking(move || {
            let identity_dir = target_cfg_clone.path.join(".mirror").join(".by-identity");
            let _ = std::fs::create_dir_all(&identity_dir);
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_clone) {
                Ok(f) => {
                    if let Ok(rel) = dst_clone.strip_prefix(&target_cfg_clone.path) {
                        identity::update_map(&source_map, e_inode, rel.to_path_buf(), e_generation, false);
                    }
                    Ok(f)
                },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
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
            metrics::SYNTHETIC_IDENTITY_FILES.inc();
        } else {
            let mapped_err: Option<FoxingError> = final_res.err();
            error!("Failed to create synthetic file: {:?}. This might indicate a missing target directory or IO issue.", mapped_err);
            ctx.failure_state.record_failure();
            return Err(mapped_err.unwrap());
        }
    }
    if matches!(e.event_type,
        EventType::Write | EventType::Create | EventType::WriteRange |
        EventType::Truncate | EventType::Fsync | EventType::Rename |
        EventType::Barrier)
    {
        let entry = ctx.dirty_stats.entry(e.inode).or_insert_with(|| DirtyEntry {
            first_dirty: Instant::now(),
            path: dst.clone(),
            seq: e.seq_num,
            projid: e.projid,
            expected_state: ExpectedState::None,
            persisted_state: ExpectedState::None,
        });
        entry.seq = e.seq_num;
        entry.projid = e.projid;
        match e.event_type {
            EventType::Create => {
                entry.expected_state = if e.name.starts_with(".tmp.") { ExpectedState::WriteBulk } else { ExpectedState::FsyncCommit };
                entry.path = dst.clone();
            },
            EventType::Truncate => { entry.expected_state = ExpectedState::WriteBulk; },
            EventType::Write | EventType::WriteRange => {
                if entry.expected_state == ExpectedState::WriteBulk || entry.expected_state == ExpectedState::None {
                    entry.expected_state = ExpectedState::FsyncCommit;
                }
            },
            EventType::Rename => {
                if entry.expected_state == ExpectedState::WriteBulk { entry.expected_state = ExpectedState::FsyncCommit; }
            }
            _ => {}
        }
        if entry.expected_state != entry.persisted_state {
            let dst_clone = dst.clone();
            let seq = entry.seq;
            let state_str = entry.expected_state.as_str();
            spawn_blocking(move || {
                sidecar::persist_wal_state(&dst_clone, seq, state_str);
            });
            entry.persisted_state = entry.expected_state.clone();
        }
    }
    let res = match e.event_type {
        EventType::Write | EventType::Create | EventType::WriteRange => {
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
                        Err(e) => return Err(e),
                    }
                }
                Err(io::Error::new(io::ErrorKind::NotFound, "Retries exhausted"))
            }).await.unwrap_or(Err(io::Error::new(io::ErrorKind::NotFound, "Metadata task failed")));
            if let Ok(m) = metadata_result {
                if m.is_file() {
                    let current_src_size = m.len();
                    let dynamic_vdo_opt = ctx.vdo_tuner.should_check_zeros(m.len());
                    let copy_res = SmartCopier::copy(
                        &src,
                        &dst_clone,
                        ctx.ring,
                        buffer_pool, // Pass the pool
                        &target_cfg.supports_reflink,
                        dynamic_vdo_opt,
                        e_offset,
                        e_len,
                        target_cfg.direct_io_ok.load(Ordering::Relaxed),
                        current_src_size,
                        src_rwf_uncached_ok, // Source Read Capability
                        dst_rwf_uncached_ok, // Target Write Capability
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
                return Err(FoxingError::Io(io::ErrorKind::NotFound.into()));
            } else {
                if let Err(io_err) = &metadata_result {
                     if io_err.kind() == io::ErrorKind::NotFound {
                        warn!("Worker: Source file for inode {} not found. Invalidating map.", e.inode);
                        source.inode_map.lock().pop(&e.inode);
                     }
                }
                return Ok(None);
            }
        },
        EventType::Rename => {
            if let Some(new_name) = &e.new_name {
                let current_dirty_entry = ctx.dirty_stats.get(&e.inode).cloned();
                let new_dst_temp = if e.new_parent_inode != 0 {
                    if let Some(parent_path) = identity::resolve_directory(&source.inode_map, e.new_parent_inode) {
                        target_cfg.path.join(parent_path).join(new_name)
                    } else {
                        match identity::resolve_and_update_path(&source.inode_map, &source.mount, e.new_parent_inode) {
                             Ok(rel_path) => target_cfg.path.join(rel_path).join(new_name),
                             Err(_) => {
                                 warn!("Worker: Parent inode {} missing from cache and aggressive lookup failed. Falling back to old path parent.", e.new_parent_inode);
                                 if let Some(parent) = dst.parent() {
                                      parent.join(new_name)
                                 } else {
                                      target_cfg.path.join(new_name)
                                 }
                             }
                        }
                    }
                } else {
                    if let Some(parent) = dst.parent() { parent.join(new_name) } else { target_cfg.path.join(new_name) }
                };
                if e.name.starts_with(".tmp.") && current_dirty_entry.is_some() {
                    let entry = current_dirty_entry.as_ref().unwrap();
                    if entry.expected_state == ExpectedState::FsyncCommit {
                        let res = spawn_blocking({
                            let path = new_dst_temp.clone();
                            let seq = entry.seq;
                            let projid = entry.projid;
                            move || security::commit_epoch(&path, seq, projid)
                        }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
                        if res.is_ok() {
                            sidecar::clear_wal_state(&new_dst_temp);
                            ctx.dirty_stats.remove(&e.inode);
                        }
                    }
                }
                metrics::RENAME_EVENTS.inc();
                let new_dst = new_dst_temp;
                let new_rel = new_dst.strip_prefix(&target_cfg.path)
                    .unwrap_or_else(|_| Path::new(new_name))
                    .to_path_buf();
                if target_cfg.allow(&new_rel) {
                    let dst_clone = dst.clone();
                    let new_dst_clone = new_dst.clone();
                    let source_clone = source.clone();
                    let e_inode = e.inode;
                    let e_generation = e.generation;
                    let res = spawn_blocking(move || {
                        info!("Worker: Rename {:?} -> {:?}", dst_clone, new_dst_clone);
                        if let Some(p) = new_dst_clone.parent() {
                            let _ = std::fs::create_dir_all(p);
                        }
                        let rename_res = std::fs::rename(&dst_clone, &new_dst_clone);
                        if rename_res.is_ok() {
                            identity::update_map_after_rename(&source_clone.inode_map, e_inode, new_rel.clone(), e_generation);
                            if let Some(old_sp) = sidecar::get_sidecar_path(&dst_clone) {
                                if let Some(new_sp) = sidecar::get_sidecar_path(&new_dst_clone) {
                                    if old_sp.exists() { let _ = std::fs::rename(old_sp, new_sp); }
                                }
                            }
                        }
                        rename_res
                    }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
                    if res.is_ok() {
                        if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) { entry.path = new_dst.clone(); }
                    }
                    return res.map(|_| None);
                } else {
                    return Ok(None);
                }
            } else { return Ok(None); }
        },
        EventType::Mkdir => {
            let dst_clone = dst.clone();
            let res = spawn_blocking(move || {
                let r = std::fs::create_dir_all(&dst_clone);
                if r.is_err() && r.as_ref().unwrap_err().kind() == io::ErrorKind::AlreadyExists { Ok(()) } else { r }
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
            if res.is_ok() {
                if let Ok(rel) = dst.strip_prefix(&target_cfg.path) {
                    identity::update_map(&source.inode_map, e.inode, rel.to_path_buf(), e.generation, false);
                }
            }
            return res.map(|_| None);
        },
        EventType::Rmdir | EventType::Unlink => {
            ctx.dirty_stats.remove(&e.inode);
            source.inode_map.lock().pop(&e.inode);
            let dst_clone = dst.clone();
            let is_rmdir = matches!(e.event_type, EventType::Rmdir);
            let res = spawn_blocking(move || {
                if let Some(sp) = sidecar::get_sidecar_path(&dst_clone) { let _ = std::fs::remove_file(sp); }
                let r = if is_rmdir { std::fs::remove_dir(&dst_clone) } else { std::fs::remove_file(&dst_clone) };
                if let Err(ref e) = r {
                    if e.kind() == io::ErrorKind::NotFound { return Ok(()); }
                }
                r
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
            return res.map(|_| None);
        },
        EventType::Barrier | EventType::Fsync => {
            if e.inode == 0 { return Ok(None); }
            let dst_clone = dst.clone();
            let seq_num = e.seq_num;
            let projid = e.projid;
            let inode = e.inode;
            let dst_clone_for_metadata = dst_clone.clone();
            let file_size_res = spawn_blocking(move || std::fs::metadata(&dst_clone_for_metadata).map(|m| m.len())).await.unwrap_or(Err(io::ErrorKind::NotFound.into()));
            match file_size_res {
                Ok(0) => {
                    if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) {
                        entry.expected_state = ExpectedState::None;
                        let dst_clone = dst.clone();
                        spawn_blocking(move || sidecar::clear_wal_state(&dst_clone));
                    }
                    return Ok(None);
                },
                Err(_) => return Ok(None),
                _ => {}
            };
            let target_root_clone = target_cfg.path.clone();
            let enable_versioning = target_cfg.enable_versioning;
            let is_forced = target_cfg.is_forced_version(&dst_clone);
            let defer_maintenance = tuner.should_defer_maintenance();
            let (dyn_max_versions, dyn_max_mb) = if is_forced {
                 let forced_count = target_cfg.force_retention_count.unwrap_or(target_cfg.max_versions);
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
                    sidecar::clear_wal_state(&dst_for_commit);
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
            return res.map(|_| None);
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
            let res = spawn_blocking(move || { security::do_fallocate(&dst_clone, offset, length, mode) }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return res.map(|_| None);
        },
        EventType::Symlink => {
            let dst_clone = dst.clone();
            let src_clone = src.clone();
            let inner_res = spawn_blocking(move || {
                if let Ok(link_target) = std::fs::read_link(&src_clone) {
                    security::create_symlink(&link_target.to_string_lossy(), &dst_clone)
                } else { Ok(()) }
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return inner_res.map(|_| None);
        },
        _ => { return Ok(None); }
    };
    match res {
        Ok(stats_opt) => Ok(stats_opt),
        Err(err) => {
            if let FoxingError::Io(io_err) = &err {
                if let Some(28) = io_err.raw_os_error() {
                     // FIX: Removed trailing backslash from string literal
                     error!("TARGET FULL (ENOSPC).");
                     ctx.capacity_breaker.trip();
                     ctx.failure_state.record_failure();
                     let target_root_path = target_cfg.path.parent().unwrap_or(&target_cfg.path).to_path_buf();
                     let _ = spawn_blocking(move || {
                        versioning::prune_global_history(&target_root_path, 512 * 1024 * 1024)
                    }).await;
                }
            }
            Err(err)
        }
    }
}
