use io_uring::IoUring;
use tokio::sync::mpsc;
use std::path::{Path, PathBuf};
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
        let req = 0x4020940D; // FICLONERANGE
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
pub struct HydrationSender(pub mpsc::Sender<PathBuf>);
impl Clone for HydrationSender {
    fn clone(&self) -> Self {
        HydrationSender(self.0.clone())
    }
}

struct ShardedLockCache {
    shards: Vec<Mutex<LruCache<u64, Arc<tokio::sync::Mutex<()>>>>>
}

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
    info!("Worker {} started (Causal Lane)", worker_id);
    
    if worker_id == 0 {
        if let Ok(meta) = std::fs::metadata(&source.path) {
            let inode = meta.ino();
            let dev = source.dev;
            info!("Worker {}: Seeding Root Identity for {:?} -> Inode {} (Dev {})", worker_id, source.path, inode, dev);
            identity::update_map(
                &source.inode_map,
                &source.dir_map,
                dev,
                inode,
                PathBuf::from(""), // Relative path of root is empty string
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

    let ring_depth = tuner.recommended_ring_depth().max(32);
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
    let initial_num_buffers = current_max_buffers.min(target_cfg.batch_size).max(4);
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
    let mut recent_bytes_processed = 0u64;

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
                _ = &mut flush_timer => { 
                    flush_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(5));
                    continue; 
                }
                Some(_) = rx_main.recv() => { continue; } 
             }
        }

        if !shutdown_requested {
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
                } => {
                    if e.event_type == EventType::Unlink || e.event_type == EventType::SequenceGap {
                        let mut lookahead_count = 0;
                        while let Ok(main_event) = rx_main.try_recv() {
                            metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                            coalescer.push(main_event);
                            lookahead_count += 1;
                            if lookahead_count > 50 { break; }
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

        let current_coalesce_limit = tuner.current_coalesce_bytes;
        let effective_batch_size = if shutdown_requested { 
            256
        } else { 
            tuner.current_batch_size 
        };

        let events_to_process_raw = {
            let mut batch = Vec::new();
            while batch.len() < effective_batch_size {
                let limit = if shutdown_requested { 0 } else { current_coalesce_limit };
                if let Some(e) = coalescer.pop_batch(limit) {
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
                 if let Err(_) = hydration_trigger.0.try_send(path.clone()) {
                     warn!("Worker {}: Hydration Trigger FULL. Failed to queue repair for gap at {:?}", worker_id, path);
                 }
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
                match identity::resolve_target(&source.inode_map, &e, &target_cfg.path) {
                    ResolveResult::Success(p, s, n) => (p, s, n),
                    ResolveResult::NeedsRepair(synthetic_path) => {
                        warn!("Worker {}: Generation mismatch for inode {}. Queueing repair.", worker_id, e.inode);
                        if let Some(parent) = synthetic_path.parent() {
                             let _ = hydration_trigger.0.try_send(parent.to_path_buf());
                        }
                        continue;
                    }
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
                        break;
                    },
                    Ok(None) => break,
                    Err(FoxingError::Io(io_err)) if io_err.kind() == io::ErrorKind::NotFound => {
                        if attempts >= max_retries {
                             warn!("Worker {}: Event {}/Inode {} failed after {} retries (NotFound). Dropping & Repairing.", worker_id, e.seq_num, e.inode, max_retries);
                             if let Err(_) = hydration_trigger.0.try_send(src.clone()) {
                                 warn!("Worker {}: Hydration Trigger FULL. Failed to queue repair for {:?}", worker_id, src);
                             }
                             break;
                        }
                        
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
                                            debug!("Worker {}: Source Race - File moved to {:?}. Updating operation.", worker_id, expected_src);
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
                    },
                    Err(e) => {
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
                    Ok(())
                },
                Err(e) => return Err(e),
            }
        }).await;

        if let Ok(_f) = res.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io)) {
            metrics::SIDECAR_FILES_CREATED.inc();
        } else {
            debug!("Worker {}: Creation failed (likely race): {:?}", worker_id, e.name);
            return Ok(None);
        }
    }

    if matches!(e.event_type, EventType::Write | EventType::Create | EventType::WriteRange) {
        let already_tracked = ctx.dirty_stats.contains_key(&e.inode);
        if !already_tracked {
            let dst_for_wal = dst.clone();
            let daemon_id = ctx.daemon_id.to_string();
            let seq = e.seq_num;
            let wal_path = dst_for_wal.clone();
            
            if dst_for_wal.exists() {
                let transition_success = tokio::task::spawn_blocking(move || {
                    sidecar::atomic_wal_transition(&wal_path, WalState::None, WalState::IntentPending, &daemon_id, seq)
                }).await.unwrap_or(Ok(false));
                
                if let Ok(false) = transition_success {
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
            if dst_for_phase2.exists() {
                let phase2_success = tokio::task::spawn_blocking(move || {
                    sidecar::atomic_wal_transition(&dst_for_phase2, WalState::IntentPending, WalState::InProgress, &daemon_id, e_seq)
                }).await.unwrap_or(Ok(false));
                
                if let Ok(false) = phase2_success {
                     if !dst_clone.exists() {
                         debug!("WAL Race: File {:?} disappeared during Intent -> InProgress transition.", dst_clone);
                         return Ok(None);
                     } else {
                         return Err(FoxingError::Io(std::io::Error::new(io::ErrorKind::Other, "WAL Transition Failed")));
                     }
                }
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
                            if dst_for_phase3.exists() {
                                let _ = tokio::task::spawn_blocking(move || {
                                    sidecar::atomic_wal_transition(&dst_for_phase3, WalState::InProgress, WalState::CommitPending, &daemon_id_p3, e_seq)
                                }).await;
                            }

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

                let old_dst_final = dst.clone();
                let new_dst_final = target_cfg.path.join(&new_rel_path);
                
                let old_dst_final_clone = old_dst_final.clone();
                let new_dst_final_clone = new_dst_final.clone();
                let source_mount = source.mount.clone();
                let e_inode = e.inode;
                let source_clone = source.clone();
                let target_cfg_clone = target_cfg.clone();
                let new_rel_path_clone = new_rel_path.clone(); 

                let res = tokio::task::spawn_blocking(move || {
                    let start = Instant::now();
                    let max_wait = Duration::from_secs(5);

                    loop {
                        match atomic_rename(&old_dst_final_clone, &new_dst_final_clone) {
                            Ok(_) => return Ok(()),
                            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
                                let elapsed = start.elapsed();
                                
                                if let Some(parent) = new_dst_final_clone.parent() {
                                    if !parent.exists() {
                                        debug!("Worker: Rename target dir {:?} missing. Creating.", parent);
                                        let _ = std::fs::create_dir_all(parent);
                                        continue;
                                    }
                                }

                                if !old_dst_final_clone.exists() {
                                    if new_dst_final_clone.exists() {
                                         debug!("Worker: Rename target {:?} already exists. Assuming previous success.", new_dst_final_clone);
                                         return Ok(());
                                    }

                                    let mut resolved_path = None;
                                    let mut tried_heuristic = false;
                                    
                                    if let Ok(new_rel) = new_dst_final_clone.strip_prefix(&target_cfg_clone.path) {
                                        let expected_src = source_mount.join(new_rel);
                                        if let Ok(meta) = std::fs::metadata(&expected_src) {
                                            if meta.ino() == e_inode {
                                                 resolved_path = Some(new_rel.to_path_buf());
                                                 tried_heuristic = true;
                                            }
                                        }
                                    }

                                    if !tried_heuristic {
                                        resolved_path = source_clone.inode_map.get_path(e_inode);
                                        if resolved_path.is_none() {
                                            resolved_path = identity::resolve_and_update_path(&source_clone, e_inode, 0, 0, 0).ok();
                                        }
                                    }

                                    if let Some(rel) = resolved_path {
                                        let current_src_abs = source_mount.join(&rel);
                                        if current_src_abs.exists() {
                                            debug!("Worker: Rename Source {:?} missing on target. Attempting SELF-HEAL copy from {:?} -> {:?}", 
                                                    old_dst_final_clone, current_src_abs, new_dst_final_clone);
                                            match copy_with_reflink_sync(&current_src_abs, &new_dst_final_clone) {
                                                Ok(_) => {
                                                    let _ = std::fs::remove_file(&old_dst_final_clone);
                                                    info!("Worker: SELF-HEAL Success.");
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
                        
                        warn!("Worker {}: Source disappeared during rename. Triggering repair for NEW path: {:?}.", worker_id, new_rel_path_clone);
                        
                        let new_src = source.mount.join(&new_rel_path_clone);
                        if let Err(_) = hydration_trigger.0.try_send(new_src) {
                            warn!("Worker {}: Hydration Trigger FULL. Failed to queue repair for Rename Target", worker_id);
                        }
                        
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
                if let Some(28) = io_err.raw_os_error() { // ENOSPC
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
