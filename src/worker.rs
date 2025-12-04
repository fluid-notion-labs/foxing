use io_uring::IoUring;
use tokio::sync::mpsc;
use tokio::time::interval;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, atomic::Ordering};
use std::io;
use crate::metrics;
use parking_lot::Mutex;
use lru::LruCache;
use crate::identity;
use crate::security;
use crate::wal::{DirtyEntry, ExpectedState};
use crate::sidecar::{self};
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
use crate::mirror::SourceInfo; // Canonical SourceInfo is imported

struct ShardedLockCache { shards: Vec<Mutex<LruCache<u64, Arc<tokio::sync::Mutex<()>>>>> }
impl ShardedLockCache {
    fn new(capacity_hint: usize) -> Self {
        let shard_count = 128;
        let per_shard = (capacity_hint / shard_count).max(100);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(LruCache::new(std::num::NonZeroUsize::new(per_shard).unwrap())));
        }
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
// Now properly exported with pub
pub async fn run_worker(
    mut rx_main: mpsc::Receiver<Arc<Event>>,
    source: Arc<SourceInfo>,
    target_cfg: TargetConfig,
    config: Arc<RwLock<Config>>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    _hydration_tx: mpsc::Sender<PathBuf>,
    _repair_txs: Arc<Vec<mpsc::Sender<Arc<Event>>>>,
    _governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_id: usize,
    mut rx_repair: Option<mpsc::Receiver<Arc<Event>>>,
    serialization: Arc<SerializationEngine>,
) -> Result<()> {
    let is_control_plane = worker_id == 0;
    let role_name = if is_control_plane { "ControlPlane" } else { "DataPlane" };
    info!("Worker {} started as {}", worker_id, role_name);
    let queue_max_hint = config.read().await.queue_max;
    let locks = Arc::new(ShardedLockCache::new(queue_max_hint));
    let mut coalescer = Coalescer::new(target_cfg.ordering_scan_depth);
    let mut dirty_stats: HashMap<u64, DirtyEntry> = HashMap::new();
    let mut flush_interval = interval(Duration::from_millis(target_cfg.worker_flush_interval_ms));
    let mut last_capacity_check = Instant::now();
    let mut tuner = BbrTuner::new(&target_cfg);
    let ring_depth = if is_control_plane { 128 } else { tuner.recommended_ring_depth() };
    debug!("Worker {}: IoUring depth set to {}", worker_id, ring_depth);
    let mut ring = match IoUring::new(ring_depth) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create io_uring: {}", e); return Err(FoxingError::Io(e)); }
    };
    let config_reader = config.read().await;
    let buffer_chunk_size_mib = target_cfg.io_buffer_size_mib.max(1);
    let buffer_chunk_size_bytes = (buffer_chunk_size_mib * 1024 * 1024) as usize;
    let capacity_threshold_mb = config_reader.capacity_threshold_mb;
    let force_flush_base_secs = config_reader.force_flush_interval_secs;
    let total_workers = config_reader.worker_count.max(1);
    let global_limit_mib = config.read().await.global_buffer_limit;
    let total_worker_mem_limit_mib = global_limit_mib * 3 / 10;
    let worker_mem_limit_mib = total_worker_mem_limit_mib / total_workers as u64;
    let current_max_buffers = (worker_mem_limit_mib / buffer_chunk_size_mib).max(2) as usize;
    let initial_num_buffers = current_max_buffers.min(target_cfg.batch_size);
    drop(config_reader);
    let mut buffer_pool = match initialize_buffer_pool(&mut ring, initial_num_buffers, buffer_chunk_size_bytes) {
        Ok(pool) => pool,
        Err(e) => return Err(e),
    };
    let src_rwf_uncached_ok = source.rwf_uncached_ok.load(Ordering::Relaxed);
    let dst_rwf_uncached_ok = target_cfg.rwf_uncached_ok.load(Ordering::Relaxed);
    info!("Worker {}: BufferPool initialized with {} x {}MB chunks.", worker_id, buffer_pool.capacity(), buffer_chunk_size_mib);
    let limiter = ErrorLimiter::new();
    let capacity_breaker = CircuitBreaker::new(target_cfg.worker_hibernation_secs);
    let mut failure_state = FailureState::new(target_cfg.worker_hibernation_secs);
    let mut poison_cabinet = PoisonCabinet::new();
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;
    let mut vdo_tuner = VdoTuner::new(target_cfg.vdo_optimization);
    let mut is_hibernating = false;
    let mut last_dropped_check = 0.0;
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
        if !is_control_plane {
             if let Some(delay) = failure_state.next_retry_delay() {
                 sleep(delay).await;
                 continue;
             }
        }
        let event_poll_result = tokio::select! {
            _ = shutdown_rx.recv() => break Ok(()),
            Some(e) = async {
                if let Some(rx) = &mut rx_repair { rx.recv().await } else { std::future::pending().await }
            } => { Some(e) },
            Some(e) = rx_main.recv() => {
                metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                Some(e)
            },
            _ = flush_interval.tick() => {
                let tuner_tick_start = Instant::now();
                let is_stressed = if is_control_plane { false } else { _governor.is_system_stressed() };
                let pending_len = coalescer.len();
                let max_pending = target_cfg.queue_max;
                let target_label = target_cfg.path.to_string_lossy().to_string();
                let bytes_processed = 0;
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
                if !is_control_plane && recommended_depth != buffer_pool.capacity() {
                    let current_depth = ring.submission().capacity();
                    let diff = (recommended_depth as i32 - current_depth as i32).abs();
                    if diff > (current_depth as i32 / 4) || (recommended_depth < 64 && diff > 10) {
                        info!("Worker {}: Resizing IoUring ({} -> {}).", worker_id, current_depth, recommended_depth);
                        while ring.completion().len() > 0 {
                            let _ = ring.submit_and_wait(1);
                        }
                        if let Err(e) = unregister_buffers(&mut ring) {
                            error!("Worker {}: Failed to unregister buffers during resize: {}", worker_id, e);
                        }
                        match IoUring::new(recommended_depth as u32) {
                            Ok(new_ring) => {
                                ring = new_ring;
                                let desired_bufs = (recommended_depth as usize).min(current_max_buffers);
                                if desired_bufs != buffer_pool.capacity() {
                                    match initialize_buffer_pool(&mut ring, desired_bufs, buffer_chunk_size_bytes) {
                                        Ok(new_pool) => buffer_pool = new_pool,
                                        Err(e) => error!("Worker {}: Buffer pool resize failed: {}", worker_id, e),
                                    }
                                } else {
                                    let iovs = buffer_pool.as_io_vecs();
                                    unsafe { let _ = ring.submitter().register_buffers(&iovs); }
                                }
                            },
                            Err(e) => error!("Worker {}: Failed to resize IoUring: {}", worker_id, e),
                        }
                    }
                }
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
                if !is_hibernating {
                    let current_dropped = metrics::EVENTS_DROPPED.get();
                    if current_dropped > last_dropped_check {
                        warn!("Detect event drops ({} -> {}).", last_dropped_check, current_dropped);
                        last_dropped_check = current_dropped;
                    }
                    let now = Instant::now();
                    let mut flushing_stats: HashMap<u64, DirtyEntry> = HashMap::new();
                    dirty_stats.retain(|&ino, entry| {
                        if now.duration_since(entry.first_dirty) > Duration::from_secs(force_flush_base_secs) {
                            flushing_stats.insert(ino, entry.clone());
                            false
                        } else { true }
                    });
                    let _committed_inos = tokio::task::spawn_blocking(move || {
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

        // START CRITICAL RENAME IDENTITY FIX
        // The worker must update the identity map *before* resolving the target path,
        // as the cached path for the inode might be stale (pointing to the old name).
        if matches!(event_ptr.event_type, EventType::Rename) {
            let src_clone = source.clone();
            let e_inode = event_ptr.inode;
            let e_generation = event_ptr.generation;
            let new_parent_inode = event_ptr.new_parent_inode;
            let new_name = event_ptr.new_name.clone();

            let lookup_task = tokio::task::spawn_blocking(move || {
                // Case 1: BPF data is complete (best case)
                if new_parent_inode != 0 && new_name.is_some() {
                    let new_name_str = new_name.as_ref().unwrap();
                    let mut cache = src_clone.inode_map.lock();
                    let new_rel_path = if let Some((parent_path, _)) = cache.get(&new_parent_inode) {
                        parent_path.join(new_name_str)
                    } else {
                        PathBuf::from(new_name_str)
                    };
                    identity::update_map_after_rename(&src_clone.inode_map, e_inode, new_rel_path.clone(), e_generation);
                    return Ok(());
                }

                // Case 2: BPF data incomplete (needs aggressive lookup)
                match identity::resolve_and_update_path(&src_clone.inode_map, &src_clone.mount, e_inode) {
                    Ok(new_source_path_rel) => {
                        // Successfully found the file's new location on the source
                        identity::update_map_after_rename(&src_clone.inode_map, e_inode, new_source_path_rel, e_generation);
                        Ok(())
                    },
                    Err(_) => {
                        // File not found (likely deleted or moved outside the source root)
                        Err(io::Error::new(io::ErrorKind::NotFound, "Inode not found after aggressive search."))
                    }
                }
            });

            match lookup_task.await {
                Ok(Ok(_)) => {
                    // Map successfully updated with the NEW path, proceed to process the rename op (which will correctly use the new path)
                }
                Ok(Err(e)) => {
                    warn!("Worker {}: RENAME identity lookup failed for Inode {} ({:?}). Assuming eventual cleanup/deletion.", worker_id, e_inode, e);
                    continue; // Skip processing this RENAME operation if we can't determine the new identity.
                }
                Err(e) => {
                    error!("Worker {}: Join error during RENAME identity lookup: {:?}", worker_id, e);
                    continue;
                }
            }
        }
        // END CRITICAL RENAME IDENTITY FIX

        // Push to local coalescing buffer
        coalescer.push(event_ptr.clone());
        let current_coalesce_limit = if is_control_plane { 0 } else { tuner.current_coalesce_bytes };
        let effective_batch_size = if is_control_plane {
            // Control plane is latency-optimized (Batch=1) unless severely backed up.
            if coalescer.len() > 100 { 16 } else { 1 }
        } else {
            tuner.current_batch_size
        };
        let events_to_process_raw = {
            let mut batch = Vec::new();
            while batch.len() < effective_batch_size {
                if let Some(e) = coalescer.pop_batch(current_coalesce_limit) {
                    // Critical structural metadata events (Rename, Fsync, Barrier) must be processed
                    // immediately if they follow bulk data, even if the batch size isn't met.
                    if is_control_plane && matches!(e.event_type, EventType::Rename | EventType::Fsync | EventType::Barrier) && !batch.is_empty() {
                        batch.push(e);
                        break;
                    }
                    batch.push(e);
                } else {
                    break;
                }
            }
            batch
        };
        for e in events_to_process_raw {
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
            // resolve_target now uses the updated path from the map (if it was a RENAME event)
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
            // GLOBAL BARRIER: Strict Consistency
            let op_kind = match e.event_type {
                EventType::Rename | EventType::Mkdir | EventType::Rmdir |
                EventType::Link | EventType::Symlink | EventType::Unlink => OpKind::Rename, // Exclusive
                _ => OpKind::Write, // Shared
            };
            let _barrier_guard = serialization.acquire_barrier(e.inode, op_kind).await;
            let _ = process_single_event_inner(
                &mut ctx, e, &source, &target_cfg, &tuner,
                capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src, &mut buffer_pool,
                src_rwf_uncached_ok,
                dst_rwf_uncached_ok,
                is_control_plane,
                worker_id,
            ).await;
        }
    };
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
    src_rwf_uncached_ok: bool,
    dst_rwf_uncached_ok: bool,
    is_control_plane: bool,
    worker_id: usize,
) -> Result<Option<CopyStats>> {
    let target_cfg_cap = target_cfg.clone();
    let target_cfg_allow = target_cfg.clone();
    let capacity_breaker_clone = ctx.capacity_breaker.clone();
    let check_capacity_result = if is_control_plane { true } else {
        tokio::task::spawn_blocking(move || {
            capacity_breaker_clone.can_proceed(&target_cfg_cap.path, capacity_threshold_mb)
        }).await.unwrap_or(false)
    };
    if !check_capacity_result {
        if ctx.limiter.check("capacity") { error!("TARGET FULL (ENOSPC)."); }
        ctx.capacity_breaker.trip();
        ctx.failure_state.record_failure();
        return Err(FoxingError::Io(io::ErrorKind::Other.into()));
    }
    let e_clone = e.clone();
    let allow_result = tokio::task::spawn_blocking(move || {
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
                let check_res: Result<()> = match tokio::task::spawn_blocking({
                    let target_dir_clone = target_dir.clone();
                    move || {
                        if target_dir_clone.exists() && !target_dir_clone.is_dir() {
                            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Path component is a file, not a directory"));
                        }
                        std::fs::create_dir_all(&target_dir_clone)
                    }
                }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io)) {
                    Ok(_inner_res) => Ok(()),
                    Err(e) => Err(e)
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
        let res = tokio::task::spawn_blocking(move || {
            let identity_dir = target_cfg_clone.path.join(".mirror").join(".by-identity");
            let _ = std::fs::create_dir_all(&identity_dir);
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_clone) {
                Ok(_f) => {
                    if let Ok(rel) = dst_clone.strip_prefix(&target_cfg_clone.path) {
                        identity::update_map(&source_map, e_inode, rel.to_path_buf(), e_generation, false);
                    }
                    Ok(())
                },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    let _ = std::fs::OpenOptions::new().write(true).open(&dst_clone);
                    Ok(())
                },
                Err(e) => return Err(e),
            }
        }).await;
        if let Ok(_f) = res.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io)) {
            metrics::SIDECAR_FILES_CREATED.inc();
        } else {
            ctx.failure_state.record_failure();
            return Err(FoxingError::Io(io::ErrorKind::Other.into()));
        }
    }
    if matches!(e.event_type, EventType::Write | EventType::Create | EventType::Rename) {
        let entry = ctx.dirty_stats.entry(e.inode).or_insert_with(|| DirtyEntry {
            first_dirty: Instant::now(),
            path: dst.clone(),
            seq: e.seq_num,
            projid: e.projid,
            expected_state: ExpectedState::None,
            persisted_state: ExpectedState::None,
        });
        entry.seq = e.seq_num;
    }
    let res = match e.event_type {
        EventType::Write | EventType::Create | EventType::WriteRange => {
            let dst_clone = dst.clone();
            let e_offset = e.offset;
            let e_len = e.length;
            let src_clone_for_metadata = src.clone();
            let metadata_result = tokio::task::spawn_blocking(move || {
                std::fs::metadata(&src_clone_for_metadata)
            }).await.unwrap_or(Err(io::ErrorKind::NotFound.into()));
            if let Ok(m) = metadata_result {
                if m.is_file() {
                    let current_src_size = m.len();
                    let dynamic_vdo_opt = ctx.vdo_tuner.should_check_zeros(m.len());
                    let copy_res = SmartCopier::copy(
                        &src,
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
                            metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(stats.bytes_processed as f64);
                            ctx.vdo_tuner.update(stats.bytes_processed, stats.bytes_zeros);
                            let apply_dst = dst_clone.clone();
                            let apply_src = src.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                security::sync_xattrs(&apply_src, &apply_dst);
                                security::apply_metadata(&apply_src, &apply_dst)
                            }).await;
                            return Ok(Some(stats));
                        },
                        Err(e) => Err(e)
                    }
                } else { return Ok(None); }
            } else { return Ok(None); }
        },
        EventType::Rename => {
            if let Some(_new_name) = &e.new_name {
                // The correct *destination* path is now `dst` because the map was pre-updated.
                // Determine the original (old) target path from the event's name field
                let old_dst = target_cfg.path.join(e.name.trim_start_matches('/'));
                let new_dst = dst.to_path_buf(); // This is the final new path from resolve_target

                if old_dst == new_dst {
                    warn!("Worker {}: Rename event for Inode {} resulted in identical paths: {:?}. Skipping atomic rename.", worker_id, e.inode, new_dst);
                    return Ok(None);
                }

                if let Some(p) = new_dst.parent() {
                    if !p.exists() {
                        let _ = tokio::task::spawn_blocking({ let p = p.to_path_buf(); move || std::fs::create_dir_all(p) }).await;
                    }
                }

                // Clone needed because new_dst is used outside the closure (line 567)
                let new_dst_for_rename = new_dst.clone();

                // If the old path exists and is not the new path, perform the atomic rename.
                if old_dst.exists() {
                    let res = tokio::task::spawn_blocking(move || {
                        // old_dst is captured by move here, which is fine since it's not used later.
                        atomic_rename(&old_dst, &new_dst_for_rename).map_err(FoxingError::Io)
                    }).await.map_err(FoxingError::Join).and_then(|r| r);

                    metrics::RENAME_EVENTS.inc();

                    if res.is_err() {
                        return res.map(|_| None);
                    }
                } else {
                    debug!("Worker {}: Old target path for RENAME event {:?} does not exist. Skipping atomic rename.", worker_id, old_dst);
                }
                
                // Update dirty stats with the new path
                // new_dst is available here because new_dst_for_rename (a clone) was moved.
                if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) { entry.path = new_dst.clone(); }

                return Ok(None);
            } else { return Ok(None); }
        },
        EventType::Mkdir => {
            let dst_clone = dst.clone();
            let res = tokio::task::spawn_blocking(move || std::fs::create_dir_all(&dst_clone)).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
            return res.map(|_| None);
        },
        EventType::Rmdir | EventType::Unlink => {
            ctx.dirty_stats.remove(&e.inode);
            source.inode_map.lock().pop(&e.inode);
            let dst_clone = dst.clone();
            let is_rmdir = matches!(e.event_type, EventType::Rmdir);
            let res = tokio::task::spawn_blocking(move || {
                if let Some(sp) = sidecar::get_sidecar_path(&dst_clone) { let _ = std::fs::remove_file(sp); }
                let r = if is_rmdir { std::fs::remove_dir(&dst_clone) } else { std::fs::remove_file(&dst_clone) };
                if let Err(ref e) = r { if e.kind() == io::ErrorKind::NotFound { return Ok(()); } }
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
            let file_size_res = tokio::task::spawn_blocking(move || std::fs::metadata(&dst_clone_for_metadata).map(|m| m.len())).await.unwrap_or(Err(io::ErrorKind::NotFound.into()));
            match file_size_res {
                Ok(0) => {
                    // Wal fix for Empty File Corruption Race: skip commit and clear dirty state
                    if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) {
                        entry.expected_state = ExpectedState::None;
                        let dst_clone = dst.clone();
                        tokio::task::spawn_blocking(move || sidecar::clear_wal_state(&dst_clone));
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
                 metrics::TARGET_FORCED_VERSIONING_ACTIVE.with_label_values(&[&target_cfg.path.to_string_lossy()]).set(1.0);
                 (forced_count, u64::MAX)
            } else {
                 metrics::TARGET_FORCED_VERSIONING_ACTIVE.with_label_values(&[&target_cfg.path.to_string_lossy()]).set(0.0);
                 tuner.calculate_version_limits(&target_cfg, ctx.cur_cap_avail, ctx.cur_cap_total)
            };
            let should_cleanup = is_forced || !defer_maintenance;
            if enable_versioning && target_cfg.allow_versioning(&dst_clone) {
                let dst_for_version = dst_clone.clone();
                 let result = tokio::task::spawn_blocking(move || {
                    let _ = security::create_version_snapshot(&dst_for_version, seq_num, &target_root_clone, inode);
                    if should_cleanup {
                        let _ = versioning::cleanup_versions(&dst_for_version, &target_root_clone, dyn_max_versions, dyn_max_mb);
                    }
                    Ok::<(), FoxingError>(())
                }).await.map_err(FoxingError::Join);
                 if let Err(e) = result { tracing::error!("Failed MARS Version step for inode {}: {:?}", inode, e); }
            }
            let dst_for_commit = dst_clone.clone();
            let r = tokio::task::spawn_blocking(move || {
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
            let _ = tokio::task::spawn_blocking(move || { security::sync_xattrs(&src_clone, &dst_clone); }).await;
            return Ok(None);
        },
        EventType::Chmod | EventType::Chown | EventType::Utimes => {
            let src_clone = src.clone();
            let dst_clone = dst.clone();
            let dst_for_closure = dst_clone.clone();
            let res = tokio::task::spawn_blocking(move || { security::apply_metadata(&src_clone, &dst_for_closure) }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return res.map(|_| None);
        },
        EventType::Truncate => {
            let dst_clone = dst.clone();
            let length = e.length;
            let res = tokio::task::spawn_blocking(move || { security::truncate_file(&dst_clone, length) }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return res.map(|_| None);
        },
        EventType::Fallocate => {
            let dst_clone = dst.clone();
            let offset = e.offset;
            let length = e.length;
            let mode = e.flags as i32;
            let res = tokio::task::spawn_blocking(move || { security::do_fallocate(&dst_clone, offset, length, mode) }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(|e| e.into()));
            return res.map(|_| None);
        },
        EventType::Symlink => {
            let dst_clone = dst.clone();
            let src_clone = src.clone();
            let inner_res = tokio::task::spawn_blocking(move || {
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
                     error!("TARGET FULL (ENOSPC).");
                     ctx.capacity_breaker.trip();
                     ctx.failure_state.record_failure();
                     let target_root_path = target_cfg.path.parent().unwrap_or(&target_cfg.path).to_path_buf();
                     let _ = tokio::task::spawn_blocking(move || {
                        versioning::prune_global_history(&target_root_path, 512 * 1024 * 1024)
                    }).await;
                }
            }
            Err(err)
        }
    }
}
