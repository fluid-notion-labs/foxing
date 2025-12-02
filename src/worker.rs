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
use crate::{event::{Event, EventType}, mirror::{SourceInfo, SharedConfig}, config::{TargetConfig}, buffer::AlignedBuffer, security, sidecar, identity, Result, versioning, governor::Governor};
use crate::operations::{SmartCopier, CopyStats};
use tokio::time::interval;
use tracing::{warn, info, error, debug};
use crate::error::{FoxingError};
use std::io;
use crate::metrics;
use parking_lot::Mutex;
use lru::LruCache;
use std::path::{Path, PathBuf};
use nix::sys::statvfs::statvfs;
use dashmap::DashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::time::sleep;
use crate::mirror::HydrationTx;
use crate::wal::{ExpectedState, DirtyEntry};
use crate::tuner::{TunerBoard, TunerState, BbrTuner, VdoTuner};
use crate::resilience::{PoisonCabinet, FailureState, CircuitBreaker, ErrorLimiter};

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
    buf: &'a mut AlignedBuffer,
    dirty_stats: &'a mut HashMap<u64, DirtyEntry>,
    vdo_tuner: &'a mut VdoTuner,
    failure_state: &'a mut FailureState,
    capacity_breaker: &'a CircuitBreaker,
    limiter: &'a ErrorLimiter,
    poison: &'a mut PoisonCabinet,
    cur_cap_avail: u64,
    cur_cap_total: u64,
    hydration_tx: &'a HydrationTx,
    dir_cache: &'a mut LruCache<PathBuf, ()>,
}

pub async fn run_worker(
    source: Arc<SourceInfo>,
    target_cfg: TargetConfig,
    mut rx_main: mpsc::Receiver<Arc<Event>>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    hydration_tx: mpsc::Sender<PathBuf>,
    config: SharedConfig,
    governor: Arc<Governor>,
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
    let mut dir_cache: LruCache<PathBuf, ()> = LruCache::new(std::num::NonZeroUsize::new(5000).unwrap());
    
    let iov = libc::iovec { iov_base: unsafe { buf.capacity_slice_mut() }.as_mut_ptr() as _, iov_len: buf.capacity() };
    if unsafe { ring.submitter().register_buffers(&[iov]) }.is_err() { error!("Failed to register io_uring buffers."); }
    
    let mut tuner = BbrTuner::new(&target_cfg);
    let mut vdo_tuner = VdoTuner::new(target_cfg.vdo_optimization);
    let mut is_hibernating = false;
    let mut last_dropped_check = 0u64;
    let mut cur_cap_avail = 0u64;
    let mut cur_cap_total = 0u64;
    let mut last_hydration_request = Instant::now().sub(Duration::from_secs(30));
    let hydration_tx_clone = hydration_tx; 
    let mut gap_mode_until = Instant::now();
    let ingestion_panic_threshold: usize = (order.max_count as f64 * 0.8) as usize;

    loop {
        if !is_hibernating && !failure_state.can_execute_io() {
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
                if is_hibernating { order.push_and_check(e); continue; }
                let is_meta = e.event_type == EventType::Mkdir || e.event_type == EventType::Rmdir || e.event_type == EventType::Rename;
                if !is_control_plane && is_meta {
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
        
        if failure_state.check_hibernation_needed() {
             if !is_hibernating {
                 warn!("Target {:?} failed. Hibernating.", target_cfg.path);
                 let _ = hydration_tx_clone.send(source.path.clone()).await;
                 is_hibernating = true;
             }
        }
        if is_hibernating { continue; }

        if event_ptr.event_type == EventType::SequenceGap {
            tracing::warn!("Sequence Gap detected on dev {}. Activating Gap Recovery Mode.", event_ptr.dev_id);
            let mut new_gap_end = Instant::now() + Duration::from_secs(5);
            let expected_gap_size = event_ptr.seq_num.saturating_sub(order.next_seq);
            if expected_gap_size > 100 {
                let extra_secs = (expected_gap_size / 100).min(30);
                new_gap_end += Duration::from_secs(extra_secs);
            }
            gap_mode_until = new_gap_end;
            order.next_seq = event_ptr.seq_num;
            continue;
        }
        let is_in_gap_mode = Instant::now() < gap_mode_until;
        if order.next_seq > 0 && event_ptr.seq_num < order.next_seq && event_ptr.seq_num != 0 {
            if is_in_gap_mode && matches!(event_ptr.event_type,
                EventType::Fsync | EventType::Rename | EventType::Unlink | EventType::Create | EventType::Barrier)
            {
                let (dst, _, _) = identity::resolve_target(&source.inode_map, &event_ptr, &target_cfg.path);
                let src_path = if dst.starts_with(&target_cfg.path) {
                     match dst.strip_prefix(&target_cfg.path) {
                         Ok(rel) => source.mount.join(rel),
                         Err(_) => source.mount.join(event_ptr.name.trim_start_matches('/')),
                     }
                } else {
                     source.mount.join(event_ptr.name.trim_start_matches('/'))
                };
                if !source.active_repairs.contains(&src_path) {
                    let _ = hydration_tx_clone.send(src_path).await;
                }
            }
            crate::metrics::LATE_EVENTS.inc();
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
                buf: &mut buf,
                dirty_stats: &mut dirty_stats,
                vdo_tuner: &mut vdo_tuner,
                failure_state: &mut failure_state,
                capacity_breaker: &capacity_breaker,
                limiter: &limiter,
                poison: &mut poison_cabinet,
                cur_cap_avail: 0, 
                cur_cap_total: 0,
                hydration_tx: &hydration_tx_clone,
                dir_cache: &mut dir_cache,
            };
            
            let (dst, is_synthetic, needs_creation) = identity::resolve_target(&source.inode_map, &e, &target_cfg.path);
            let src = if is_synthetic {
                source.mount.join(e.name.trim_start_matches('/'))
            } else {
                match dst.strip_prefix(&target_cfg.path) {
                    Ok(rel) => source.mount.join(rel),
                    Err(e) => {
                         error!("Path Error: {}", e);
                         continue;
                    }
                }
            };
            
            let lock = locks.get_by_path(&e.name);
            let _g = lock.lock().await;
            
            let _ = process_single_event_inner(
                &mut ctx, e, &source, &target_cfg, &tuner, 
                capacity_threshold_mb, &dst, is_synthetic, needs_creation, &src
            ).await;
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
) -> Result<Option<CopyStats>> {
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
    if e.name.contains(".tmp.") { return Ok(None); }
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
                if e.name.starts_with(".tmp.") {
                    entry.expected_state = ExpectedState::WriteBulk;
                } else {
                    entry.expected_state = ExpectedState::FsyncCommit;
                }
                entry.path = dst.clone();
            },
            EventType::Truncate => {
                entry.expected_state = ExpectedState::WriteBulk;
            },
            EventType::Write | EventType::WriteRange => {
                if entry.expected_state == ExpectedState::WriteBulk || entry.expected_state == ExpectedState::None {
                    entry.expected_state = ExpectedState::FsyncCommit;
                }
            },
            EventType::Rename => {
                if entry.expected_state == ExpectedState::WriteBulk {
                    entry.expected_state = ExpectedState::FsyncCommit;
                }
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
                    let mut should_force_full_replace = initial_is_full_replace;
                    let mut copy_offset = e_offset;
                    let mut copy_length = e_len;
                    if !should_force_full_replace {
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
                            sidecar::clear_wal_state(&new_dst_temp);
                            ctx.dirty_stats.remove(&e.inode);
                        } else {
                            warn!("WAL T2 Failure: RENAME on .tmp file occurred, but state was {:?}. Ignoring rename sequence inconsistency.", entry.expected_state);
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
            let dst_for_blocking = dst_clone.clone();
            if ctx.dir_cache.contains(&dst_clone) {
                return Ok(None);
            }
            let res = spawn_blocking(move || {
                let res = std::fs::create_dir_all(&dst_for_blocking);
                if res.is_err() && res.as_ref().unwrap_err().kind() == io::ErrorKind::AlreadyExists {
                    Ok(())
                } else { res }
            }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io));
            match res {
                Ok(()) => {
                    ctx.dir_cache.put(dst_clone.clone(), ());
                    let _ = spawn_blocking(move || { if let Some(parent) = dst_clone.parent() { security::write_dir_integrity_hash(parent, 0); } }).await;
                },
                Err(_) => {}
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
            if e.inode == 0 { 
                debug!("WAL: Fsync/Barrier received for ephemeral/junk Inode 0. Dropping event.");
                return Ok(None);
            }
            let dst_clone = dst.clone();
            let src_clone = src.clone();
            let seq_num = e.seq_num;
            let projid = e.projid;
            let inode = e.inode;
            let hydration_tx_ref = ctx.hydration_tx;
            let dst_clone_for_metadata = dst_clone.clone();
            let file_size_res = spawn_blocking(move || std::fs::metadata(&dst_clone_for_metadata).map(|m| m.len())).await.unwrap_or(Err(io::Error::new(io::ErrorKind::NotFound, "Metadata failed")));
            let file_size = match file_size_res {
                Ok(len) => len,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    warn!("WAL: Fsync/Barrier received for non-existent file {:?}. Skipping commit. Triggering repair for source: {:?}", dst_clone, src_clone);
                    if ctx.limiter.check("fsync_file_not_found") {
                        let _ = hydration_tx_ref.send(src_clone).await;
                    }
                    return Ok(None);
                },
                Err(e) => return Err(FoxingError::Io(e)),
            };
            if file_size == 0 {
                warn!("WAL T2 Failure: Fsync/Barrier for Inode {} received, but target file is 0 bytes. Skipping commit (Empty file race).", inode);
                if let Some(entry) = ctx.dirty_stats.get_mut(&e.inode) {
                    entry.expected_state = ExpectedState::None;
                    let dst_clone = dst.clone();
                    spawn_blocking(move || {
                        sidecar::clear_wal_state(&dst_clone);
                    });
                }
                return Ok(None);
            }
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
