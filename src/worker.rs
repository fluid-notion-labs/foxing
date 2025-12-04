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
    let is_control_plane = worker_id == 0;
    let role_name = if is_control_plane { "ControlPlane" } else { "DataPlane" };
    info!("Worker {} started as {}", worker_id, role_name);
    if is_control_plane {
        if let Ok(meta) = std::fs::metadata(&source.path) {
            let inode = meta.ino();
            let dev = source.dev;
            info!("Worker 0: Seeding Root Identity for {:?} -> Inode {} (Dev {})", source.path, inode, dev);
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
            warn!("Worker 0: Failed to stat source root {:?}. Root identity will be missing!", source.path);
        }
    }
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
    let mut shutdown_requested = false;
    let _last_dropped_check = 0.0;
    let _result: Result<()> = loop {
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
        if !is_control_plane && !shutdown_requested {
             if let Some(delay) = failure_state.next_retry_delay() {
                 sleep(delay).await;
                 continue;
             }
        }
        let event_poll_result = if !shutdown_requested {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Worker {}: Shutdown requested. Draining queue...", worker_id);
                    shutdown_requested = true;
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
                        // Resizing logic omitted
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
            if matches!(event_ptr.event_type, EventType::Rename) {
                let src_clone = source.clone();
                let e_inode = event_ptr.inode;
                let e_dev = event_ptr.dev_id;
                let e_generation = event_ptr.generation;
                let e_seq = event_ptr.seq_num;
                let e_ts = event_ptr.timestamp_ns;
                let e_mode = event_ptr.mode;
                let is_dir = (e_mode & libc::S_IFMT) == libc::S_IFDIR;
                let new_parent_inode = event_ptr.new_parent_inode;
                let new_name_opt = event_ptr.new_name.clone();
                let mut parent_is_known = false;
                if new_parent_inode != 0 {
                    let dir_map = src_clone.dir_map.lock();
                    if dir_map.contains(&(e_dev, new_parent_inode)) {
                        parent_is_known = true;
                    } else {
                        let inode_map = src_clone.inode_map.lock();
                        if inode_map.contains(&(e_dev, new_parent_inode)) {
                            parent_is_known = true;
                        }
                    }
                }
                let has_complete_bpf_data = new_parent_inode != 0 &&
                                          parent_is_known &&
                                          new_name_opt.is_some() &&
                                          new_name_opt.as_ref().map_or(false, |n| !n.is_empty());
                debug!("RENAME Handler: Inode {}, Old Name: {:?}, New Parent Inode: {}, New Name: {:?}, BPF Data Valid: {}",
                    e_inode, event_ptr.name, new_parent_inode, new_name_opt, has_complete_bpf_data);
                const AGGRESSIVE_LOOKUP_TIMEOUT: Duration = Duration::from_millis(500);
                let mut fast_path_success = false;
                if has_complete_bpf_data {
                    let new_name_string = new_name_opt.clone().unwrap();
                    let dir_map_clone = src_clone.dir_map.clone();
                    let inode_map_clone = src_clone.inode_map.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        let parent_path_opt = identity::resolve_directory(&dir_map_clone, &inode_map_clone, e_dev, new_parent_inode);
                        if let Some(parent_path) = parent_path_opt {
                            let new_rel_path = parent_path.join(&new_name_string);
                            identity::update_map_after_rename(&inode_map_clone, &dir_map_clone, e_dev, e_inode, new_rel_path, e_generation, is_dir, e_ts, e_seq);
                            Ok(parent_path)
                        } else {
                            Err(io::Error::new(io::ErrorKind::NotFound, "Parent directory not in DirMap or InodeMap"))
                        }
                    }).await.map_err(FoxingError::Join);
                    if result.is_err() || matches!(result, Ok(Err(_))) {
                        warn!("RENAME Handler: Fast-path (BPF trust) failed or parent not in map. Falling back to slow path.");
                    } else {
                        debug!("RENAME Handler: Fast-path identity map update successful.");
                        fast_path_success = true;
                    }
                }
                if !fast_path_success {
                    if !has_complete_bpf_data {
                        warn!("RENAME Handler: BPF data incomplete or parent unknown. Falling back to aggressive FS scan with timeout.");
                    } else {
                         warn!("RENAME Handler: Parent not in DirMap. Performing aggressive FS scan for identity resolution.");
                    }
                    let dir_map_clone = src_clone.dir_map.clone();
                    let inode_map_clone = src_clone.inode_map.clone();
                    let lookup_task = tokio::task::spawn_blocking(move || {
                        match identity::resolve_and_update_path(
                            &inode_map_clone,
                            &dir_map_clone,
                            &src_clone.mount,
                            e_inode,
                            e_generation,
                            e_ts,
                            e_seq
                        ) {
                            Ok(current_path) => Ok(current_path),
                            Err(e) => {
                                warn!("RENAME Handler: Aggressive lookup failed for Inode {}: {:?}", e_inode, e);
                                Err(e)
                            }
                        }
                    });
                    match tokio::time::timeout(AGGRESSIVE_LOOKUP_TIMEOUT, lookup_task).await {
                        Ok(Ok(Ok(_new_path))) => {
                        }
                        Ok(Ok(Err(e))) => {
                            warn!("Worker {}: RENAME identity fix failed (IO Error) for Inode {} ({:?}). Skipping event.",
                                  worker_id, e_inode, e);
                            coalescer.push(event_ptr.clone());
                            continue;
                        }
                        Ok(Err(e)) => {
                            warn!("Worker {}: RENAME identity fix failed (Join Error) for Inode {} ({:?}). Skipping event.",
                                  worker_id, e_inode, e);
                            coalescer.push(event_ptr.clone());
                            continue;
                        }
                        Err(_) => {
                            warn!("Worker {}: RENAME identity lookup TIMED OUT ({}ms) for Inode {}. Skipping event to avoid stall.",
                                  worker_id, AGGRESSIVE_LOOKUP_TIMEOUT.as_millis(), e_inode);
                            coalescer.push(event_ptr.clone());
                            continue;
                        }
                    }
                }
            }
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
            if coalescer.len() > 100 { 16 } else { 1 }
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
            let op_kind = match e.event_type {
                EventType::Rename | EventType::Mkdir | EventType::Rmdir |
                EventType::Link | EventType::Symlink | EventType::Unlink => OpKind::Rename,
                _ => OpKind::Write,
            };
            let _barrier_guard = serialization.acquire_barrier(e.inode, op_kind).await;
            let _ = process_single_event_inner(
                &mut ctx, e, &source, &target_cfg, &tuner,
                capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src, &mut buffer_pool,
                src_rwf_uncached_ok,
                dst_rwf_uncached_ok,
                is_control_plane,
                worker_id,
                hydration_trigger.clone(),
            ).await;
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
    is_synthetic: bool,
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
                        // CORRECTED: Use actual is_synthetic value from resolve_target to avoid cache poisoning
                        identity::update_map(&source_map, &source_dir_map, e_dev, e_inode, rel.to_path_buf(), e_generation, is_synthetic, false, e_ts, e_seq);
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
    if matches!(e.event_type, EventType::Write | EventType::Create | EventType::Rename | EventType::WriteRange) {
        let already_tracked = ctx.dirty_stats.contains_key(&e.inode);
        if !already_tracked {
            let dst_for_wal = dst.clone();
            let daemon_id = ctx.daemon_id.to_string();
            let seq = e.seq_num;
            let wal_res = tokio::task::spawn_blocking(move || {
                sidecar::update_wal(&dst_for_wal, WalState::IntentPending, seq, &daemon_id);
            }).await;
            if let Err(e) = wal_res {
                warn!("Failed to persist WAL Intent for {:?}: {:?}", dst, e);
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
            let src_clone_for_metadata = src.clone();
            let daemon_id = ctx.daemon_id.to_string();
            let e_seq = e.seq_num;
            let dst_for_phase2 = dst_clone.clone();
            let _ = tokio::task::spawn_blocking(move || {
                sidecar::update_wal(&dst_for_phase2, WalState::InProgress, e_seq, &daemon_id);
            }).await;
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
                            let dst_for_phase3 = dst_clone.clone();
                            let daemon_id_p3 = ctx.daemon_id.to_string();
                            let _ = tokio::task::spawn_blocking(move || {
                                sidecar::update_wal(&dst_for_phase3, WalState::CommitPending, e_seq, &daemon_id_p3);
                            }).await;
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
            if let Some(new_name_str) = &e.new_name {
                let mut old_dst_final = if !is_synthetic { dst.clone() } else { target_cfg.path.join(e.name.trim_start_matches('/')) };
                if e.parent_inode != 0 {
                    let mut old_parent_opt = identity::resolve_directory(
                        &source.dir_map,
                        &source.inode_map,
                        e.dev_id,
                        e.parent_inode
                    );
                    if old_parent_opt.is_none() {
                         let src_clone_lookup = source.clone();
                         let parent_ino = e.parent_inode;
                         let dev_id = e.dev_id;
                         let _ = tokio::task::spawn_blocking(move || {
                             let _ = identity::resolve_and_update_path(
                                 &src_clone_lookup.inode_map,
                                 &src_clone_lookup.dir_map,
                                 &src_clone_lookup.mount,
                                 parent_ino,
                                 0, 0, 0
                             );
                         }).await;
                         old_parent_opt = identity::resolve_directory(
                             &source.dir_map,
                             &source.inode_map,
                             e.dev_id,
                             e.parent_inode
                         );
                    }
                    if let Some(parent) = old_parent_opt {
                        old_dst_final = target_cfg.path.join(parent).join(&e.name);
                    }
                }
                let mut new_dst_final = if !is_synthetic { dst.clone() } else { target_cfg.path.join(new_name_str) };
                if e.new_parent_inode != 0 {
                    let mut new_parent_path_opt = identity::resolve_directory(
                        &source.dir_map,
                        &source.inode_map,
                        e.dev_id,
                        e.new_parent_inode
                    );
                    if new_parent_path_opt.is_none() {
                        let src_clone_lookup = source.clone();
                        let parent_ino = e.new_parent_inode;
                        let dev_id = e.dev_id;
                        let _ = tokio::task::spawn_blocking(move || {
                             let _ = identity::resolve_and_update_path(
                                 &src_clone_lookup.inode_map,
                                 &src_clone_lookup.dir_map,
                                 &src_clone_lookup.mount,
                                 parent_ino,
                                 0, 0, 0
                             );
                        }).await;
                        new_parent_path_opt = identity::resolve_directory(
                            &source.dir_map,
                            &source.inode_map,
                            e.dev_id,
                            e.new_parent_inode
                        );
                    }
                    if let Some(parent_rel) = new_parent_path_opt {
                        new_dst_final = target_cfg.path.join(parent_rel).join(new_name_str);
                    } else {
                        if let Some(old_parent) = old_dst_final.parent() {
                            new_dst_final = old_parent.join(new_name_str);
                        }
                    }
                } else {
                    if let Some(old_parent) = old_dst_final.parent() {
                        new_dst_final = old_parent.join(new_name_str);
                    }
                }
                if old_dst_final == new_dst_final {
                    warn!("Worker {}: Rename event for Inode {} resulted in identical paths: {:?}. Skipping atomic rename.", worker_id, e.inode, new_dst_final);
                    return Ok(None);
                }
                let old_dst_clone = old_dst_final.clone();
                let target_cfg_path_clone = target_cfg.path.clone();
                let new_dst_for_rename_clone = new_dst_final.clone();
                let src_map_clone = source.inode_map.clone();
                let src_dir_map_clone = source.dir_map.clone();
                let is_dir = (e.mode & libc::S_IFMT) == libc::S_IFDIR;
                let e_inode = e.inode;
                let e_dev = e.dev_id;
                let e_generation = e.generation;
                let e_seq = e.seq_num;
                let e_ts = e.timestamp_ns;
                let parent_dir = new_dst_final.parent().map(|p| p.to_path_buf());
                let parent_dir_clone = parent_dir.clone();
                let parent_check_res = tokio::task::spawn_blocking(move || {
                    if let Some(parent) = parent_dir_clone {
                        if parent != target_cfg_path_clone && !parent.exists() {
                            std::fs::create_dir_all(&parent)
                        } else {
                            Ok(())
                        }
                    } else {
                        Ok(())
                    }
                }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
                if let Err(e) = parent_check_res {
                    error!("Worker {}: RENAME failed to create target directory {:?}: {:?}", worker_id, parent_dir, e);
                    ctx.failure_state.record_failure();
                    return Err(e);
                }
                let res = tokio::task::spawn_blocking(move || {
                    atomic_rename(&old_dst_clone, &new_dst_for_rename_clone).map_err(FoxingError::Io)
                }).await.map_err(FoxingError::Join).and_then(|r| r);
                match res {
                    Ok(_) => {
                        metrics::RENAME_EVENTS.inc();
                        info!("Worker {}: Atomic Rename SUCCESS: {:?} -> {:?}", worker_id, old_dst_final, new_dst_final);
                        let hydration_tx_for_move = hydration_trigger.clone();
                        let new_full_path_for_validation = new_dst_final.clone();
                        tokio::task::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            if !new_full_path_for_validation.exists() {
                                warn!("Identity drift detected for inode {}: Rename succeeded but file missing at {:?}. Triggering targeted hydration repair.",
                                      e_inode, new_full_path_for_validation);
                                let _ = hydration_tx_for_move.0.send(new_full_path_for_validation.clone()).await;
                            }
                        });
                        let new_rel_path_to_store = match new_dst_final.strip_prefix(&target_cfg.path) {
                            Ok(rel) => rel.to_path_buf(),
                            Err(_) => new_dst_final.to_path_buf(),
                        };
                        identity::update_map_after_rename(&src_map_clone, &src_dir_map_clone, e_dev, e_inode, new_rel_path_to_store, e_generation, is_dir, e_ts, e_seq);
                    },
                    Err(FoxingError::Io(ref e)) if e.kind() == io::ErrorKind::NotFound => {
                        warn!("Rename source missing: {:?}. Attempting fallback checks.", old_dst_final);
                        let dest_exists = tokio::task::spawn_blocking({
                            let path = new_dst_final.clone();
                            move || {
                                if path.exists() {
                                    if let Ok(_meta) = std::fs::metadata(&path) {
                                        return true;
                                    }
                                }
                                false
                            }
                        }).await.unwrap_or(false);
                        if dest_exists {
                            debug!("Idempotency check passed: File already at destination {:?}.", new_dst_final);
                            return Ok(None);
                        }
                        error!("Rename failed and file lost. Triggering resync of parent.");
                        if let Some(parent) = new_dst_final.parent() {
                            let _ = hydration_trigger.0.send(parent.to_path_buf()).await;
                        }
                        return Ok(None);
                    },
                    Err(e) => {
                        error!("Worker {}: Atomic RENAME FAILED (old: {:?}, new: {:?}) due to: {:?}", worker_id, old_dst_final, new_dst_final, e);
                        ctx.failure_state.record_failure();
                        return Err(e);
                    }
                }
                if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) { entry.path = new_dst_final.clone(); }
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
