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
use crate::mirror::SourceInfo;

/// Wrapper around the mpsc::Sender to enforce strict typing and prevent E0282 inference errors.
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

pub async fn run_worker(
    mut rx_main: mpsc::Receiver<Arc<Event>>,
    source: Arc<SourceInfo>,
    target_cfg: TargetConfig,
    config: Arc<RwLock<Config>>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    hydration_trigger: Arc<HydrationSender>, // Strongly typed wrapper
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
    // Control Plane (Worker 0) processes metadata which is small but frequent. 
    // It doesn't need a massive ring, but needs low latency.
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
    
    // Calculate memory limits
    let total_workers = config_reader.worker_count.max(1);
    let global_limit_mib = config.read().await.global_buffer_limit;
    // reserve 30% of global limit for worker IO buffers
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
    
    // Main Event Loop
    // FIX E0282: Explicitly annotate the result type so break Ok(()) can infer the Error type
    let result: Result<()> = loop {
        // Hibernation Logic
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
                Some(_) = rx_main.recv() => { continue; } // Drain queue while sleeping
             }
        }

        // Backoff if failing (only for Data Plane)
        if !is_control_plane {
             if let Some(delay) = failure_state.next_retry_delay() {
                 sleep(delay).await;
                 continue;
             }
        }

        // 1. Fetch Event
        let event_poll_result = tokio::select! {
            _ = shutdown_rx.recv() => break Ok(()),
            Some(e) = async {
                if let Some(rx) = &mut rx_repair { rx.recv().await } else { std::future::pending().await }
            } => { Some(e) }, // Priority repair events
            Some(e) = rx_main.recv() => { 
                metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::SeqCst);
                Some(e) 
            },
            _ = flush_interval.tick() => {
                // Tuner logic ...
                let tuner_tick_start = Instant::now();
                let is_stressed = if is_control_plane { false } else { _governor.is_system_stressed() };
                let pending_len = coalescer.len();
                let max_pending = target_cfg.queue_max;
                let target_label = target_cfg.path.to_string_lossy().to_string();
                
                // We pass 0 for bytes_processed here as this is just the tick update
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
                
                // Dynamic Ring Resizing
                if !is_control_plane && recommended_depth != buffer_pool.capacity() {
                    let current_depth = ring.submission().capacity();
                    let diff = (recommended_depth as i32 - current_depth as i32).abs();
                    
                    // Hysteresis to prevent flapping
                    if diff > (current_depth as i32 / 4) || (recommended_depth < 64 && diff > 10) {
                        info!("Worker {}: Resizing IoUring ({} -> {}).", worker_id, current_depth, recommended_depth);
                        
                        // Drain completion queue first
                        while ring.completion().len() > 0 {
                            let _ = ring.submit_and_wait(1);
                        }

                        if let Err(e) = unregister_buffers(&mut ring) {
                            error!("Worker {}: Failed to unregister buffers during resize: {}", worker_id, e);
                        }
                        
                        match IoUring::new(recommended_depth as u32) {
                            Ok(new_ring) => {
                                ring = new_ring;
                                // Re-alloc buffer pool
                                let desired_bufs = (recommended_depth as usize).min(current_max_buffers);
                                if desired_bufs != buffer_pool.capacity() {
                                    match initialize_buffer_pool(&mut ring, desired_bufs, buffer_chunk_size_bytes) {
                                        Ok(new_pool) => buffer_pool = new_pool,
                                        Err(e) => error!("Worker {}: Buffer pool resize failed: {}", worker_id, e),
                                    }
                                } else {
                                    // Re-register existing pool
                                    let iovs = buffer_pool.as_io_vecs();
                                    unsafe { let _ = ring.submitter().register_buffers(&iovs); }
                                }
                            },
                            Err(e) => error!("Worker {}: Failed to resize IoUring: {}", worker_id, e),
                        }
                    }
                }

                // Check Disk Capacity
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

                // Periodic WAL Flush
                if !is_hibernating {
                    let current_dropped = metrics::EVENTS_DROPPED.get();
                    if current_dropped > last_dropped_check {
                        warn!("Detect event drops ({} -> {}).", last_dropped_check, current_dropped);
                        last_dropped_check = current_dropped;
                    }

                    let now = Instant::now();
                    let mut flushing_stats: HashMap<u64, DirtyEntry> = HashMap::new();
                    
                    // Move entries that need flushing to a temp map
                    dirty_stats.retain(|&ino, entry| {
                        if now.duration_since(entry.first_dirty) > Duration::from_secs(force_flush_base_secs) {
                            flushing_stats.insert(ino, entry.clone());
                            false
                        } else { true }
                    });

                    // Flush metadata in background
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

                None // Continue loop
            }
        };

        if event_poll_result.is_none() { continue; }
        let event_ptr = event_poll_result.unwrap();

        // START CRITICAL RENAME IDENTITY FIX
        if matches!(event_ptr.event_type, EventType::Rename) {
            let src_clone = source.clone();
            let e_inode = event_ptr.inode;
            let e_generation = event_ptr.generation;
            let new_parent_inode = event_ptr.new_parent_inode;
            let new_name_opt = event_ptr.new_name.clone(); // Clone Option<String> outside closure
            
            // Validate BPF data completeness
            let has_complete_bpf_data = new_parent_inode != 0 && new_name_opt.is_some() && new_name_opt.as_ref().map_or(false, |n| !n.is_empty());
            
            debug!("RENAME Handler: Inode {}, Old Name: {:?}, New Parent Inode: {}, New Name: {:?}, BPF Data Complete: {}", 
                e_inode, event_ptr.name, new_parent_inode, new_name_opt, has_complete_bpf_data);

            const AGGRESSIVE_LOOKUP_TIMEOUT: Duration = Duration::from_millis(500);

            if has_complete_bpf_data {
                // *** FAST PATH: Trust BPF and perform pre-rename map update (Change 1 & 2) ***
                let new_name_string = new_name_opt.clone().unwrap();
                let dir_map_clone = src_clone.dir_map.clone();
                let inode_map_clone = src_clone.inode_map.clone();
                
                let result = tokio::task::spawn_blocking(move || {
                    // Gap 10 Check: If resolve_directory returns None, we need to fall back to the slow path
                    let parent_path_opt = identity::resolve_directory(&dir_map_clone, new_parent_inode);
                    
                    if let Some(parent_path) = parent_path_opt {
                        let new_rel_path = parent_path.join(&new_name_string);
                        // Update the map immediately using BPF's authoritative state
                        identity::update_map_after_rename(&inode_map_clone, &dir_map_clone, e_inode, new_rel_path, e_generation, false);
                        Ok(parent_path)
                    } else {
                        Err(io::Error::new(io::ErrorKind::NotFound, "Parent directory not in DirMap"))
                    }
                }).await.map_err(FoxingError::Join);

                if result.is_err() || matches!(result, Ok(Err(_))) {
                    warn!("RENAME Handler: Fast-path (BPF trust) failed or parent not in map. Falling back to slow path.");
                } else {
                    debug!("RENAME Handler: Fast-path identity map update successful.");
                    coalescer.push(event_ptr.clone());
                    continue; // Skip the slow path
                }
            }
            
            // SLOW PATH: Aggressive FS scan
            if !has_complete_bpf_data {
                warn!("RENAME Handler: BPF data incomplete. Falling back to aggressive FS scan with timeout.");
            } else {
                 warn!("RENAME Handler: Parent not in DirMap. Performing aggressive FS scan for identity resolution.");
            }

            let dir_map_clone = src_clone.dir_map.clone();
            let inode_map_clone = src_clone.inode_map.clone();
            
            let lookup_task = tokio::task::spawn_blocking(move || {
                match identity::resolve_and_update_path(&inode_map_clone, &dir_map_clone, &src_clone.mount, e_inode) {
                    Ok(current_path) => Ok(current_path),
                    Err(e) => {
                        warn!("RENAME Handler: Aggressive lookup failed for Inode {}: {:?}", e_inode, e);
                        Err(e)
                    }
                }
            });

            match tokio::time::timeout(AGGRESSIVE_LOOKUP_TIMEOUT, lookup_task).await {
                Ok(Ok(Ok(_new_path))) => {
                    // Identity map updated by thread, proceed
                }
                Ok(Ok(Err(e))) => {
                    warn!("Worker {}: RENAME identity fix failed (IO Error) for Inode {} ({:?}). Skipping event.", 
                          worker_id, e_inode, e);
                    // Push anyway to attempt standard processing, though it might fail
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
        // END CRITICAL RENAME IDENTITY FIX

        coalescer.push(event_ptr.clone());
        
        // Batch Processing
        let current_coalesce_limit = if is_control_plane { 0 } else { tuner.current_coalesce_bytes };
        let effective_batch_size = if is_control_plane { 
            if coalescer.len() > 100 { 16 } else { 1 } // Metadata is fast, batch small
        } else { 
            tuner.current_batch_size 
        };

        let events_to_process_raw = {
            let mut batch = Vec::new();
            while batch.len() < effective_batch_size {
                if let Some(e) = coalescer.pop_batch(current_coalesce_limit) {
                    // Control Plane barrier for structural events
                    if is_control_plane && matches!(e.event_type, EventType::Rename | EventType::Fsync | EventType::Barrier) && !batch.is_empty() {
                        batch.push(e);
                        break; // Force flush before structural change
                    }
                    batch.push(e);
                } else {
                    break;
                }
            }
            batch
        };

        for e in events_to_process_raw {
             // Poison Cabinet check
             if !poison_cabinet.check_allowed(e.inode) && e.event_type != EventType::Mkdir {
                // Skip poisoned inode (unless it's a Mkdir which might fix it)
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

            // Resolve Targets
            let (dst, is_synthetic, needs_creation) = identity::resolve_target(&source.inode_map, &e, &target_cfg.path);
            let src = if is_synthetic {
                source.mount.join(e.name.trim_start_matches('/'))
            } else {
                match dst.strip_prefix(&target_cfg.path) {
                    Ok(rel) => source.mount.join(rel),
                    Err(_) => source.mount.join(e.name.trim_start_matches('/')) // Fallback
                }
            };

            // Path Locking
            let lock = locks.get_by_path(&e.name);
            let _g = lock.lock().await;

            // Serialization Barrier
            let op_kind = match e.event_type {
                EventType::Rename | EventType::Mkdir | EventType::Rmdir | 
                EventType::Link | EventType::Symlink | EventType::Unlink => OpKind::Rename,
                _ => OpKind::Write,
            };
            let _barrier_guard = serialization.acquire_barrier(e.inode, op_kind).await;

            // Process
            let _ = process_single_event_inner(
                &mut ctx, e, &source, &target_cfg, &tuner, 
                capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src, &mut buffer_pool,
                src_rwf_uncached_ok,
                dst_rwf_uncached_ok,
                is_control_plane,
                worker_id,
                hydration_trigger.clone(), // Clone the trigger for use inside the inner function/closures
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
    hydration_trigger: Arc<HydrationSender>, // Accepting argument to resolve E0425
) -> Result<Option<CopyStats>> {

    let target_cfg_cap = target_cfg.clone();
    let target_cfg_allow = target_cfg.clone();
    
    // Capacity Check
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

    // Filters
    let e_clone = e.clone();
    let allow_result = tokio::task::spawn_blocking(move || {
        target_cfg_allow.allow(std::path::Path::new(&e_clone.name))
    }).await.unwrap_or(false);

    if !allow_result {
        metrics::EVENTS_FILTERED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc();
        return Ok(None);
    }

    if e.name.contains(".tmp.") { return Ok(None); }

    // Directory creation safety
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
                    // Don't fail the whole worker, just this event might fail
                }
            }
        }
    }

    // Identity File Creation (Sidecars for Synthetic)
    if needs_creation {
        let dst_clone = dst.clone();
        let target_cfg_clone = target_cfg.clone();
        let e_inode = e.inode;
        let e_generation = e.generation;
        let source_map = source.inode_map.clone();
        let source_dir_map = source.dir_map.clone();

        let res = tokio::task::spawn_blocking(move || {
            let identity_dir = target_cfg_clone.path.join(".mirror").join(".by-identity");
            let _ = std::fs::create_dir_all(&identity_dir);
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_clone) {
                Ok(_f) => {
                    if let Ok(rel) = dst_clone.strip_prefix(&target_cfg_clone.path) {
                        // Gap 9 Fix: Pass `is_dir=false` explicitly
                        identity::update_map(&source_map, &source_dir_map, e_inode, rel.to_path_buf(), e_generation, false, false);
                    }
                    Ok(())
                },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    // Update mtime/touch
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

    // Mark Dirty Stats
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

    // Dispatch by Type
    let res = match e.event_type {
        EventType::Write | EventType::Create | EventType::WriteRange => {
            let dst_clone = dst.clone();
            let e_offset = e.offset;
            let e_len = e.length;
            
            // Check source size first
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
                            
                            // Post-copy metadata sync
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
                let old_dst = target_cfg.path.join(e.name.trim_start_matches('/'));
                let new_dst = dst.to_path_buf();
                
                let old_dst_clone = old_dst.clone();
                let target_cfg_path_clone = target_cfg.path.clone();
                let new_dst_for_rename_clone = new_dst.clone();
                let src_map_clone = source.inode_map.clone();
                let src_dir_map_clone = source.dir_map.clone();
                let is_dir = (e.mode & libc::S_IFMT) == libc::S_IFDIR;
                let e_inode = e.inode;
                let e_generation = e.generation;

                // Ensure parent exists
                let parent_dir = new_dst.parent().map(|p| p.to_path_buf());
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

                // Check for no-op
                if old_dst == new_dst {
                    warn!("Worker {}: Rename event for Inode {} resulted in identical paths: {:?}. Skipping atomic rename.", worker_id, e.inode, new_dst);
                    return Ok(None);
                }

                if old_dst.exists() {
                    let res = tokio::task::spawn_blocking(move || {
                        let res = atomic_rename(&old_dst_clone, &new_dst_for_rename_clone).map_err(FoxingError::Io);
                        res
                    }).await.map_err(FoxingError::Join).and_then(|r| r);
                    
                    metrics::RENAME_EVENTS.inc();

                    if let Ok(_) = res {
                        // === HYDRATION TRIGGER LOGIC START ===
                        // Resolves E0425: hydration_trigger is now available in scope
                        let hydration_tx_for_move = hydration_trigger.clone();
                        let new_full_path_for_validation = new_dst.clone();
                        
                        // UNUSED VARIABLE FIX: Removed unused target_cfg_clone
                        
                        tokio::task::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            if !new_full_path_for_validation.exists() {
                                warn!("Identity drift detected for inode {}: Rename succeeded but file missing at {:?}. Triggering targeted hydration repair.", 
                                      e_inode, new_full_path_for_validation);
                                // Resolves E0282: HydrationSender wrapper ensures type is known
                                let _ = hydration_tx_for_move.0.send(new_full_path_for_validation.clone()).await;
                            }
                        });
                        // === HYDRATION TRIGGER LOGIC END ===

                        let new_rel_path_to_store = match new_dst.strip_prefix(&target_cfg.path) {
                            Ok(rel) => rel.to_path_buf(),
                            Err(_) => new_dst.to_path_buf(),
                        };
                        // Always update map on rename success
                        if is_dir {
                            identity::update_map_after_rename(&src_map_clone, &src_dir_map_clone, e_inode, new_rel_path_to_store, e_generation, true);
                        }
                    } else if let Err(e) = res {
                        error!("Worker {}: Atomic RENAME FAILED (old: {:?}, new: {:?}) due to: {:?}", worker_id, old_dst, new_dst, e);
                        ctx.failure_state.record_failure();
                        return Err(e);
                    }
                } else {
                    debug!("Worker {}: Old target path for RENAME event {:?} does not exist. Skipping atomic rename.", worker_id, old_dst);
                    // Just update the map if it's a directory, so we track where it *should* be
                    let new_rel_path_to_store = match new_dst.strip_prefix(&target_cfg.path) {
                        Ok(rel) => rel.to_path_buf(),
                        Err(_) => new_dst.to_path_buf(),
                    };
                    if is_dir {
                        let res = tokio::task::spawn_blocking(move || {
                             identity::update_map_after_rename(&src_map_clone, &src_dir_map_clone, e_inode, new_rel_path_to_store, e_generation, true);
                             Ok::<(), FoxingError>(())
                        }).await;
                        if res.is_err() {
                            warn!("Worker {}: Directory identity update failed for non-existent source file: {:?}", worker_id, res.err());
                        }
                    }
                }

                if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) { entry.path = new_dst.clone(); }
                return Ok(None);
            } else { return Ok(None); }
        },
        EventType::Mkdir => {
            let dst_clone = dst.clone();
            let source_map_clone = source.inode_map.clone();
            let source_dir_map_clone = source.dir_map.clone();
            let e_inode = e.inode;
            let e_generation = e.generation;
            let target_cfg_path_clone = target_cfg.path.clone();

            let res = tokio::task::spawn_blocking(move || {
                let r = std::fs::create_dir_all(&dst_clone);
                if r.is_ok() {
                    if let Ok(rel) = dst_clone.strip_prefix(&target_cfg_path_clone) {
                        identity::update_map(&source_map_clone, &source_dir_map_clone, e_inode, rel.to_path_buf(), e_generation, false, true);
                    }
                }
                r
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
            
            return res.map(|_| None);
        },
        EventType::Rmdir | EventType::Unlink => {
            // Remove from dirty stats and cache
            ctx.dirty_stats.remove(&e.inode);
            source.inode_map.lock().pop(&e.inode);
            source.dir_map.lock().pop(&e.inode);

            let dst_clone = dst.clone();
            let is_rmdir = matches!(e.event_type, EventType::Rmdir);

            let res = tokio::task::spawn_blocking(move || {
                // Clear sidecar first
                if let Some(sp) = sidecar::get_sidecar_path(&dst_clone) { let _ = std::fs::remove_file(sp); }
                let r = if is_rmdir { std::fs::remove_dir(&dst_clone) } else { std::fs::remove_file(&dst_clone) };
                r.map(|_| ()).map_err(FoxingError::Io)
            }).await.map_err(FoxingError::Join).and_then(|r| r.map(|_| None));
            return res;
        },
        EventType::Barrier | EventType::Fsync => {
            if e.inode == 0 { return Ok(None); }
            
            let dst_clone = dst.clone();
            let seq_num = e.seq_num;
            let projid = e.projid;
            let inode = e.inode;

            // Check if file still exists on target before fsync
            let dst_clone_for_metadata = dst_clone.clone();
            let file_size_res = tokio::task::spawn_blocking(move || std::fs::metadata(&dst_clone_for_metadata).map(|m| m.len())).await.unwrap_or(Err(io::ErrorKind::NotFound.into()));
            
            match file_size_res {
                Ok(0) => {
                    // 0-byte file during fsync often means race condition or temp file.
                    // Clear state to avoid locking up WAL
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

            // Versioning (MARS)
            let target_root_clone = target_cfg.path.clone();
            let enable_versioning = target_cfg.enable_versioning;
            let is_forced = target_cfg.is_forced_version(&dst_clone);
            let defer_maintenance = tuner.should_defer_maintenance();
            
            let (dyn_max_versions, dyn_max_mb) = if is_forced {
                 let forced_count = target_cfg.force_retention_count.unwrap_or(target_cfg.max_versions);
                 metrics::TARGET_FORCED_VERSIONING_ACTIVE.with_label_values(&[&target_cfg.path.to_string_lossy()]).set(1.0);
                 (forced_count, u64::MAX) // Unlimited size for forced files
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

            // Commit WAL
            let dst_for_commit = dst_clone.clone();
            let r = tokio::task::spawn_blocking(move || {
                let r = security::commit_epoch(&dst_for_commit, seq_num, projid);
                if r.is_ok() {
                    sidecar::clear_wal_state(&dst_for_commit);
                    // Update directory hash for consistency
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

    // Error Handling
    match res {
        Ok(stats_opt) => Ok(stats_opt),
        Err(err) => {
            if let FoxingError::Io(io_err) = &err {
                if let Some(28) = io_err.raw_os_error() {
                     error!("TARGET FULL (ENOSPC).");
                     ctx.capacity_breaker.trip();
                     ctx.failure_state.record_failure();
                     
                     // Emergency prune
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
