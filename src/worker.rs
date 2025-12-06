use io_uring::IoUring;
use tokio::sync::mpsc;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, atomic::Ordering};
use std::io;
use crate::metrics;
use parking_lot::Mutex;
use lru::LruCache;
use crate::identity::{self, ResolveResult};
use crate::security;
use crate::wal::{DirtyEntry, ExpectedState};
use crate::sidecar::{self, WalState};
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
#[derive(Debug)]
pub struct HydrationSender(pub mpsc::Sender<PathBuf>);
impl Clone for HydrationSender {
    fn clone(&self) -> Self {
        HydrationSender(self.0.clone())
    }
}
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
    mut rx_repair: Option<mpsc::Receiver<Arc<Event>>>,
    serialization: Arc<SerializationEngine>,
    daemon_id: String,
) -> Result<()> {
    let is_control_plane = worker_id < 2;
    let role_name = if is_control_plane { "ControlPlane" } else { "DataPlane" };
    info!("Worker {} started as {}", worker_id, role_name);
    if is_control_plane {
        if let Ok(meta) = std::fs::metadata(&source.path) {
            let inode = meta.ino();
            let dev = source.dev;
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
        } else {
            warn!("Worker {}: Failed to stat source root {:?}. Root identity will be missing!", worker_id, source.path);
        }
    }
    let queue_max_hint = config.read().await.queue_max;
    let locks = Arc::new(ShardedLockCache::new(queue_max_hint));
    let mut coalescer = Coalescer::new(target_cfg.ordering_scan_depth);
    let mut dirty_stats: HashMap<u64, DirtyEntry> = HashMap::new();
    let initial_flush_ms = target_cfg.worker_flush_interval_ms;
    let mut flush_timer = Box::pin(tokio::time::sleep(Duration::from_millis(initial_flush_ms)));
    let mut last_capacity_check = Instant::now();
    let mut tuner = BbrTuner::new(&target_cfg);
    let mut vdo_tuner = VdoTuner::new(target_cfg.vdo_optimization);
    // Control plane needs less ring depth as it does mostly metadata ops
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
    // Calculate memory limits
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
    let limiter = ErrorLimiter::new();
    let capacity_breaker = CircuitBreaker::new(target_cfg.worker_hibernation_secs);
    let mut failure_state = FailureState::new(target_cfg.worker_hibernation_secs);
    let mut poison_cabinet = PoisonCabinet::new();
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;
    let mut is_hibernating = false;
    let mut shutdown_requested = false;
    let _result: Result<()> = loop {
        // 1. Circuit Breaker / Hibernation Check
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
                Some(_) = rx_main.recv() => { continue; } // Drain events while sleeping
             }
        }
        // 2. Failure Backoff (Data Plane Only)
        if !is_control_plane && !shutdown_requested {
             if let Some(delay) = failure_state.next_retry_delay() {
                 sleep(delay).await;
                 continue;
             }
        }
        // 3. Event Polling
        let event_poll_result = if !shutdown_requested {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Worker {}: Shutdown requested. Draining queue...", worker_id);
                    shutdown_requested = true;
                    // Try to grab one last event if available
                    match rx_main.try_recv() {
                        Ok(e) => {
                            metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                            Some(e)
                        }
                        Err(_) => None
                    }
                },
                Some(e) = async {
                    if let Some(rx) = &mut rx_repair { rx.recv().await } else { std::future::pending().await }
                } => {
                    // Priority Lane: Check for inversion
                    if e.event_type == EventType::Unlink || e.event_type == EventType::SequenceGap {
                        let mut lookahead_count = 0;
                        while let Ok(main_event) = rx_main.try_recv() {
                            metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                            coalescer.push(main_event);
                            lookahead_count += 1;
                            if lookahead_count > 50 { break; } // Bounded lookahead
                        }
                        if lookahead_count > 0 {
                            debug!("Worker {}: Priority Inversion Fix - Pulled {} events ahead of Repair Unlink", worker_id, lookahead_count);
                        }
                    }
                    Some(e)
                },
                Some(e) = rx_main.recv() => {
                    metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                    Some(e)
                },
                _ = &mut flush_timer => {
                    let tuner_tick_start = Instant::now();
                    let is_stressed = if is_control_plane { false } else { _governor.is_system_stressed() };
                    let pending_len = coalescer.len();
                    let max_pending = target_cfg.queue_max;
                    let target_label = target_cfg.path.to_string_lossy().to_string();
                    let bytes_processed = 0;
                    let _recommended_depth = tuner.tune(
                        tuner_tick_start.elapsed().as_secs_f64(),
                        bytes_processed,
                        is_stressed,
                        pending_len,
                        max_pending,
                        &tuner_board,
                        &target_label,
                        buffer_pool.chunk_size() as u64
                    );
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
                    if !is_hibernating {
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
            }
        } else {
            let repair_event = if let Some(rx) = &mut rx_repair {
                rx.try_recv().ok()
            } else { None };
            if let Some(e) = repair_event {
                Some(e)
            } else {
                match rx_main.try_recv() {
                    Ok(e) => {
                        metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                        Some(e)
                    },
                    Err(_) => None
                }
            }
        };
        if let Some(event_ptr) = event_poll_result {
            coalescer.push(event_ptr.clone());
        } else if shutdown_requested {
            if coalescer.is_empty() {
                break Ok(());
            }
        }
        let current_coalesce_limit = if is_control_plane { 0 } else { tuner.current_coalesce_bytes };
        let effective_batch_size = if shutdown_requested {
            256
        } else if is_control_plane {
            if coalescer.len() > 10 { 16 } else { 2 }
        } else {
            tuner.current_batch_size
        };
        let events_to_process_raw = {
            let mut batch = Vec::new();
            while batch.len() < effective_batch_size {
                let limit = if shutdown_requested { 0 } else { current_coalesce_limit };
                if let Some(e) = coalescer.pop_batch(limit) {
                    if is_control_plane && matches!(e.event_type, EventType::Rename | EventType::Fsync | EventType::Barrier) && !batch.is_empty() {
                        if !shutdown_requested {
                            batch.push(e);
                            break;
                        }
                    }
                    batch.push(e);
                } else {
                    break;
                }
            }
            batch
        };
        for e in events_to_process_raw {
             if e.event_type == EventType::SequenceGap {
                 warn!("Worker {}: Processing SequenceGap {} -> {}. Triggering repair.", worker_id, e.seq_num, e.name);
                 let path = target_cfg.path.clone();
                 let _ = hydration_trigger.0.try_send(path);
                 continue;
             }
             if !poison_cabinet.check_allowed(e.inode) && e.event_type != EventType::Mkdir {
                continue;
             }
             let mut ctx = WorkerContext {
                ring: &mut ring,
                dirty_stats: &mut dirty_stats,
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
            
            // Resolve Target: Do this AFTER barrier to ensure we see updates from Rename operations
            let (mut dst, is_synthetic, needs_creation) = match identity::resolve_target(&source.inode_map, &e, &target_cfg.path) {
                ResolveResult::Success(p, s, n) => (p, s, n),
                ResolveResult::NeedsRepair(synthetic_path) => {
                    warn!("Worker {}: Generation mismatch for inode {}. Queueing repair.", worker_id, e.inode);
                    if let Some(parent) = synthetic_path.parent() {
                         let _ = hydration_trigger.0.try_send(parent.to_path_buf());
                    }
                    continue;
                }
            };
            
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
            
            // RETRY LOOP for handling Race Conditions (e.g. WAL Race where file moved during queueing)
            let mut attempts = 0;
            // Increased max retries for high-concurrency/torture tests where lookup might lag
            let max_retries = 10;
            loop {
                attempts += 1;
                let res = process_single_event_inner(
                    &mut ctx, e.clone(), &source, &target_cfg, &tuner,
                    capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src, &mut buffer_pool,
                    src_rwf_uncached_ok,
                    dst_rwf_uncached_ok,
                    is_control_plane,
                    worker_id,
                    hydration_trigger.clone(),
                ).await;

                match res {
                    Ok(_) => break,
                    Err(FoxingError::Io(io_err)) if io_err.kind() == io::ErrorKind::NotFound => {
                        if attempts >= max_retries {
                             warn!("Worker {}: Event {}/Inode {} failed after {} retries (NotFound). Dropping.", worker_id, e.seq_num, e.inode, max_retries);
                             break;
                        }
                        // AGGRESSIVE RECOVERY:
                        // The file isn't where we thought it was. A rename likely raced us.
                        // Force a fresh lookup from the source to find the new path.
                        // Add linear backoff to let file system settle.
                        let sleep_duration = Duration::from_millis(50 * (attempts as u64));
                        sleep(sleep_duration).await;
                        
                        debug!("Worker {}: Event {} failed (NotFound). Retrying with fresh lookup (Attempt {}).", worker_id, e.seq_num, attempts);
                        let lookup_res = tokio::task::spawn_blocking({
                            let source_clone = source.clone();
                            let inode = e.inode;
                            move || identity::resolve_and_update_path(&source_clone, inode, 0, 0, 0)
                        }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));

                        if let Ok(new_src_rel) = lookup_res {
                             let new_src = source.mount.join(&new_src_rel);
                             let new_dst = target_cfg.path.join(&new_src_rel);
                             debug!("Worker {}: Recovery found new path: {:?} -> {:?}", worker_id, dst, new_dst);
                             src = new_src;
                             dst = new_dst;
                        } else {
                             // If we can't find it on source, it's truly gone. Stop retrying.
                             break;
                        }
                    },
                    Err(e) => {
                        // Other errors (e.g. ENOSPC) are fatal for this event
                        error!("Worker {}: Event failed with non-recoverable error: {:?}", worker_id, e);
                        break;
                    }
                }
            }
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
    tuner: &BbrTuner,
    _capacity_threshold_mb: u64,
    dst: &PathBuf,
    _is_synthetic: bool,
    needs_creation: bool,
    src: &PathBuf,
    buffer_pool: &mut BufferPool,
    src_rwf_uncached_ok: bool,
    dst_rwf_uncached_ok: bool,
    _is_control_plane: bool,
    worker_id: usize,
    hydration_trigger: Arc<HydrationSender>,
) -> Result<Option<CopyStats>> {
    if needs_creation && !matches!(e.event_type, EventType::Mkdir | EventType::Symlink | EventType::Link | EventType::Mknod) {
        let dst_clone = dst.clone();
        let target_cfg_clone = target_cfg.clone();
        let e_inode = e.inode;
        let e_dev = e.dev_id;
        let e_generation = e.generation;
        let e_ts = e.timestamp_ns;
        let e_seq = e.seq_num;
        let source_map = source.inode_map.clone();
        let source_dir_map = source.dir_map.clone();
        let res = tokio::task::spawn_blocking(move || {
            let identity_dir = target_cfg_clone.path.join(".mirror").join(".by-identity");
            let _ = std::fs::create_dir_all(&identity_dir);
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_clone) {
                Ok(_f) => {
                    if let Ok(rel) = dst_clone.strip_prefix(&target_cfg_clone.path) {
                        identity::update_map(&source_map, &source_dir_map, e_dev, e_inode, rel.to_path_buf(), e_generation, false, false, e_ts, e_seq);
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
            return Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Creation failed")));
        }
    }
    if matches!(e.event_type, EventType::Write | EventType::Create | EventType::WriteRange) {
        let already_tracked = ctx.dirty_stats.contains_key(&e.inode);
        if !already_tracked {
            let dst_for_wal = dst.clone();
            let daemon_id = ctx.daemon_id.to_string();
            let seq = e.seq_num;
            let wal_path = dst_for_wal.clone();
            let transition_success = tokio::task::spawn_blocking(move || {
                sidecar::atomic_wal_transition(&wal_path, WalState::None, WalState::IntentPending, &daemon_id, seq)
            }).await.unwrap_or(Ok(false));
            if let Ok(false) = transition_success {
                if dst_for_wal.exists() {
                     debug!("WAL State Conflict for {:?}. Assuming existing intent is valid.", dst_for_wal);
                }
            }
        }
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
            let mut src_clone_for_metadata = src.clone();
            let daemon_id = ctx.daemon_id.to_string();
            let e_seq = e.seq_num;
            let dst_for_phase2 = dst_clone.clone();
            let phase2_success = tokio::task::spawn_blocking(move || {
                sidecar::atomic_wal_transition(&dst_for_phase2, WalState::IntentPending, WalState::InProgress, &daemon_id, e_seq)
            }).await.unwrap_or(Ok(false));
            if let Ok(false) = phase2_success {
                 if !dst_clone.exists() {
                     debug!("WAL Race: File {:?} disappeared during Intent -> InProgress transition. Triggering retry.", dst_clone);
                     // CHANGED: Return explicit error to trigger retry loop in caller
                     return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::NotFound, "WAL Target Lost")));
                 }
                 error!("WAL Critical: Failed transition Intent -> InProgress for {:?}. Aborting write.", dst_clone);
                 return Ok(None);
            }
            let mut metadata_result = tokio::task::spawn_blocking({
                let p = src_clone_for_metadata.clone();
                move || std::fs::metadata(&p)
            }).await.unwrap_or(Err(io::ErrorKind::NotFound.into()));
            if metadata_result.is_err() {
                let resolved_path = tokio::task::spawn_blocking({
                    let source_clone = source.clone();
                    let e_inode = e.inode;
                    move || identity::resolve_and_update_path(&source_clone, e_inode, 0, 0, 0)
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
                            let dst_for_phase3 = dst_clone.clone();
                            let daemon_id_p3 = ctx.daemon_id.to_string();
                            let _ = tokio::task::spawn_blocking(move || {
                                sidecar::atomic_wal_transition(&dst_for_phase3, WalState::InProgress, WalState::CommitPending, &daemon_id_p3, e_seq)
                            }).await;
                            metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(stats.bytes_processed as f64);
                            ctx.vdo_tuner.update(stats.bytes_processed, stats.bytes_zeros);
                            let apply_dst = dst_clone.clone();
                            let apply_src = src_clone_for_metadata.clone();
                            let _ = tokio::task::spawn_blocking(move || {
                                security::sync_xattrs(&apply_src, &apply_dst);
                                security::apply_metadata(&apply_src, &apply_dst)
                            }).await;
                            return Ok(Some(stats));
                        },
                        Err(e) => Err(e)
                    }
                } else {
                    return Ok(None);
                }
            } else {
                warn!("Source Missing: Failed to locate source for inode {} even after retry.", e.inode);
                return Ok(None);
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
                            let source_clone = source.clone();
                            let resolved = tokio::task::spawn_blocking(move || {
                                identity::resolve_and_update_path(&source_clone, parent_ino, 0, 0, 0)
                            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
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
                identity::update_map_after_rename(
                    &source.inode_map,
                    &source.dir_map,
                    e.dev_id,
                    e.inode,
                    new_rel_path.clone(),
                    e.generation,
                    is_dir,
                    e.timestamp_ns,
                    e.seq_num
                );
                if is_dir {
                    source.dir_map.clear();
                }
                let old_rel_path = if let Some(parent_rel) = identity::resolve_directory(&source.dir_map, &source.inode_map, e.dev_id, e.parent_inode) {
                     parent_rel.join(&e.name)
                } else {
                     PathBuf::from(&e.name)
                };
                let old_dst_final = target_cfg.path.join(&old_rel_path);
                let new_dst_final = target_cfg.path.join(&new_rel_path);
                let old_dst_final_clone = old_dst_final.clone();
                let new_dst_final_clone = new_dst_final.clone();
                let io_latency_ms = tuner.current_flush_ms.max(1);
                let is_stressed = matches!(tuner.state, crate::tuner::TunerState::HighLoad | crate::tuner::TunerState::Muted | crate::tuner::TunerState::CriticalDrain);
                let hydration_trigger_clone = hydration_trigger.clone();
                let target_root_path = target_cfg.path.clone();
                let res = tokio::task::spawn_blocking(move || {
                    let start = Instant::now();
                    let multiplier = if is_stressed { 50 } else { 10 };
                    let max_wait = Duration::from_millis(io_latency_ms * multiplier).clamp(Duration::from_secs(30), Duration::from_secs(120));
                    let mut triggered_hydration = false;
                    loop {
                        match atomic_rename(&old_dst_final_clone, &new_dst_final_clone) {
                            Ok(_) => return Ok(()),
                            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                                let elapsed = start.elapsed();
                                if !triggered_hydration && elapsed > Duration::from_millis(200) {
                                    if let Ok(rel_path) = old_dst_final_clone.strip_prefix(&target_root_path) {
                                        let _ = hydration_trigger_clone.0.try_send(rel_path.to_path_buf());
                                    }
                                    if let Some(parent) = new_dst_final_clone.parent() {
                                        if !parent.exists() {
                                            if let Ok(rel_parent) = parent.strip_prefix(&target_root_path) {
                                                let _ = hydration_trigger_clone.0.try_send(rel_parent.to_path_buf());
                                            }
                                        }
                                    }
                                    triggered_hydration = true;
                                }
                                if elapsed > max_wait {
                                    return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::NotFound, format!("Source missing after {}s retry: {}", elapsed.as_secs_f64(), e))));
                                }
                                if elapsed < Duration::from_micros(500) {
                                    std::thread::yield_now();
                                } else if elapsed < Duration::from_millis(10) {
                                    std::thread::sleep(Duration::from_micros(50));
                                } else {
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                                continue;
                            },
                            Err(e) => return Err(FoxingError::Io(e)),
                        }
                    }
                }).await.map_err(FoxingError::Join).and_then(|r| r);
                match res {
                    Ok(_) => {
                        metrics::RENAME_EVENTS.inc();
                        info!("Worker {}: Atomic Rename SUCCESS: {:?} -> {:?}", worker_id, old_dst_final, new_dst_final);
                        let cleanup_path = new_dst_final.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            sidecar::clear_wal_state(&cleanup_path);
                        });
                        ctx.dirty_stats.remove(&e.inode);
                    },
                    Err(FoxingError::Io(ref io_err)) if io_err.kind() == io::ErrorKind::NotFound => {
                        if new_dst_final.exists() {
                            ctx.dirty_stats.remove(&e.inode);
                            return Ok(None);
                        }
                        error!("Rename failed (Source Missing) and not at Dest: {:?} -> {:?}. Triggering delayed repair.", old_dst_final, new_dst_final);
                        source.active_repairs.insert(new_dst_final.clone());
                        let trigger_clone = hydration_trigger.clone();
                        let parent_clone = new_dst_final.parent().unwrap_or(&new_dst_final).to_path_buf();
                        let active_repairs_clone = source.active_repairs.clone();
                        let dst_clone_cleanup = new_dst_final.clone();
                        tokio::spawn(async move {
                            sleep(Duration::from_secs(5)).await;
                            active_repairs_clone.remove(&dst_clone_cleanup);
                            let _ = trigger_clone.0.try_send(parent_clone);
                        });
                        ctx.dirty_stats.remove(&e.inode);
                        return Ok(None);
                    },
                    Err(io_err) => {
                        ctx.failure_state.record_failure();
                        ctx.dirty_stats.remove(&e.inode);
                        return Err(io_err);
                    }
                }
                return Ok(None);
            } else { return Ok(None); }
        },
        EventType::Mkdir => {
            let dst_clone = dst.clone();
            let source_map_clone = source.inode_map.clone();
            let source_dir_map_clone = source.dir_map.clone();
            let e_inode = e.inode;
            let e_dev = e.dev_id;
            let e_generation = e.generation;
            let e_seq = e.seq_num;
            let e_ts = e.timestamp_ns;
            let target_cfg_path_clone = target_cfg.path.clone();
            let res = tokio::task::spawn_blocking(move || {
                let r = std::fs::create_dir_all(&dst_clone);
                if r.is_ok() {
                    if let Ok(rel) = dst_clone.strip_prefix(&target_cfg_path_clone) {
                        identity::update_map(&source_map_clone, &source_dir_map_clone, e_dev, e_inode, rel.to_path_buf(), e_generation, false, true, e_ts, e_seq);
                    }
                }
                r
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
            return res.map(|_| None);
        },
        EventType::Unlink => {
             let src_inode = e.inode;
             let src_dev = e.dev_id;
             identity::remove_entry(&source.inode_map, &source.dir_map, src_dev, src_inode);
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
            return Err(err);
        }
    }
}
