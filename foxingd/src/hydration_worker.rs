use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use std::fs;
use std::os::unix::fs::MetadataExt;
use walkdir::WalkDir;
use tracing::{warn, debug, info, error};
use notify::{Watcher, RecursiveMode, RecommendedWatcher, EventKind};
use crate::config::{TargetConfig, Config};
use crate::mirror::{SourceInfo, SharedConfig};
use fxcp_core::governor::Governor;
use crate::tuner::{TunerBoard, TunerState, GLOBAL_TUNER_REGISTRY};
use fxcp_core::security;
use crate::Result;
use crate::error::FoxingError;
use crate::identity::{self};
use std::os::unix::io::AsRawFd;
use tokio::sync::{mpsc};
use std::collections::{HashMap};
use fxcp_core::consistency::SerializationEngine;
use fxcp_core::sidecar;
use std::fs::OpenOptions;
use crate::event::{Event};
use fxcp_core::operations::{CopyStats, probe_capabilities};
use fxcp_core::buffer::BufferPool;
use fxcp_core::constants;
use dashmap::DashMap;
use rayon::prelude::*;
use tokio::io::unix::AsyncFd;
use crate::metrics;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use io_uring::IoUring;
use libc;
use std::io::ErrorKind;
use fxcp_core::sidecar::{SyncSignature, get_sync_signature, set_sync_signature};
use fxcp_core::hashing;
use rand::seq::IndexedRandom;

#[derive(Debug)]
pub struct HydrationState {
    pub active: AtomicBool,
    pub scanned: AtomicU64,
    pub synced: AtomicU64,
    pub shutdown_requested: AtomicBool, // Added shutdown flag
}

impl Default for HydrationState {
    fn default() -> Self {
        Self {
            active: AtomicBool::new(false),
            scanned: AtomicU64::new(0),
            synced: AtomicU64::new(0),
            shutdown_requested: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationMode {
    Streaming,
    PrioritizeStructure
}

#[derive(Debug, PartialEq, Clone)]
struct HydrationAllocation {
    chunk_size_bytes: usize,
    buffers_per_ring: usize,
    label: String,
}

fn calculate_hydration_allocation(
    cfg: &Config,
    _target_cfg: &TargetConfig,
    worker_count: usize,
    global_pending_events: u64,
    current_label: Option<&str>,
) -> HydrationAllocation {
    let global_limit_bytes = cfg.global_buffer_limit * 1024 * 1024;
    let storm_threshold = (cfg.global_buffer_limit * 20).clamp(2000, 100_000);
    
    let is_storming = match current_label {
        Some("hydration-primary") => global_pending_events > (storm_threshold as f64 * 1.1) as u64,
        Some("hydration-yield") => global_pending_events > (storm_threshold as f64 * 0.9) as u64,
        _ => global_pending_events > storm_threshold,
    };

    let (label, memory_share_pct) = if is_storming {
        ("hydration-yield", 0.20)
    } else {
        ("hydration-primary", 0.70)
    };

    let total_budget = (global_limit_bytes as f64 * memory_share_pct) as u64;
    let per_worker_budget = total_budget / worker_count.max(1) as u64;
    
    let chunk_size_bytes = (cfg.io_buffer_size_mib * 1024 * 1024).max(4096) as usize;
    let min_buffers = 2;
    let max_buffers = per_worker_budget / chunk_size_bytes as u64;
    let buffers_per_ring = max_buffers.max(min_buffers as u64) as usize;

    HydrationAllocation {
        chunk_size_bytes,
        buffers_per_ring,
        label: label.to_string(),
    }
}

pub struct Hydrator {
    pub source: Arc<SourceInfo>,
    pub targets: Vec<TargetConfig>,
    governor: Arc<Governor>,
    _tuner_board: TunerBoard,
    _repair_txs: Vec<mpsc::Sender<Arc<Event>>>,
    _serialization_engines: HashMap<PathBuf, Arc<SerializationEngine>>,
    _daemon_id: String,
    pub mode: HydrationMode,
    pub global_buffer_limit: u64,
}

impl Hydrator {
    pub fn new(
        source: Arc<SourceInfo>,
        targets: Vec<TargetConfig>,
        governor: Arc<Governor>,
        tuner_board: TunerBoard,
        repair_txs: Vec<mpsc::Sender<Arc<Event>>>,
        serialization_engines: HashMap<PathBuf, Arc<SerializationEngine>>,
        daemon_id: String,
        mode: HydrationMode,
        global_buffer_limit: u64,
    ) -> Self {
        Self {
            source,
            targets,
            governor,
            _tuner_board: tuner_board,
            _repair_txs: repair_txs,
            _serialization_engines: serialization_engines,
            _daemon_id: daemon_id,
            mode,
            global_buffer_limit,
        }
    }

    pub fn repair_path(&self, path: PathBuf) {
        debug!("Hydration: Targeted repair requested for {:?}", path);
        if let Err(e) = self.process_path(&path, true, None, &mut None) {
            warn!("Hydration: Failed to repair specific path {:?}: {:?}", path, e);
        }
    }

    pub fn full_scan(&self, enable_watching: bool) {
        self.source.hydration.active.store(true, Ordering::SeqCst);
        info!("Hydration: Starting full scan for {:?} (Mode: {:?}, Enable Watch: {})", 
              self.source.path, self.mode, enable_watching);
        
        let root_dev = self.source.dev;
        let cross = self.source.cross_subvolumes;
        
        let mut job_buffer: Option<Vec<(PathBuf, TargetConfig, u64)>> = if self.mode == HydrationMode::PrioritizeStructure {
            Some(Vec::with_capacity(10_000))
        } else {
            None
        };

        let mut dirs = Vec::new();
        let mut files = Vec::new();
        
        let walker = WalkDir::new(&self.source.path).sort_by_file_name();
        let high_watermark = (self.global_buffer_limit * 20).clamp(2000, 100_000);

        for entry_res in walker.into_iter().filter_entry(move |e| {
            if cross { return true; }
            if let Ok(meta) = e.metadata() {
                if meta.dev() as u32 != root_dev {
                    return false;
                }
            }
            true
        }) {
            if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                info!("Hydration: Scan aborted by shutdown signal.");
                self.source.hydration.active.store(false, Ordering::SeqCst);
                return;
            }

            let pending = metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).get() as u64;
            if pending > high_watermark {
                // FIXED: Check shutdown flag during stall/sleep
                for _ in 0..5 {
                    if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                        info!("Hydration: Scan aborted by shutdown signal during backpressure pause.");
                        self.source.hydration.active.store(false, Ordering::SeqCst);
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }

            match entry_res {
                Ok(entry) => {
                    let path = entry.path().to_path_buf();
                    if entry.file_type().is_dir() {
                        dirs.push(path);
                    } else if entry.file_type().is_file() {
                        files.push(path);
                    }
                },
                Err(e) => warn!("Hydration: WalkDir error: {}", e),
            }
        }

        dirs.sort_by_key(|p| p.components().count());
        
        let targets_ref = &self.targets;
        let source_path = &self.source.path;
        let source_mount = &self.source.mount;

        info!("Hydration: Creating {} directory structures in parallel...", dirs.len());
        
        let dir_mappings: Vec<(PathBuf, PathBuf)> = dirs.iter().filter_map(|p| {
            let rel = if let Ok(r) = p.strip_prefix(source_path) {
                Some(r.to_path_buf())
            } else {
                p.strip_prefix(source_mount).ok().map(|r| r.to_path_buf())
            };
            rel.map(|r| (p.clone(), r))
        }).collect();

        dir_mappings.par_iter().for_each(|(src, rel)| {
            if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { return; }
            for target in targets_ref {
                let target_dir = target.path.join(rel);
                if !target_dir.exists() {
                    let _ = fs::create_dir_all(&target_dir);
                }
                if !security::is_dir_metadata_synced(src, &target_dir) {
                    if let Err(e) = security::apply_metadata(src, &target_dir) {
                        debug!("Hydration: Failed to apply directory metadata for {:?}: {}", target_dir, e);
                    }
                }
            }
        });

        if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            info!("Hydration: Scan aborted after directory creation.");
            self.source.hydration.active.store(false, Ordering::SeqCst);
            return;
        }

        info!("Hydration: Pre-computing signatures for {} files...", files.len());
        
        let verification_results: Vec<(PathBuf, TargetConfig, u64)> = files
            .par_iter()
            .flat_map(|path| {
                if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { return Vec::new(); }
                let rel = match path.strip_prefix(source_path) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => match path.strip_prefix(source_mount) {
                        Ok(r) => r.to_path_buf(),
                        Err(_) => return Vec::new(),
                    }
                };
                
                let meta = match fs::metadata(path) {
                    Ok(m) => m,
                    Err(_) => return Vec::new(),
                };
                let ino = meta.ino();
                
                let mut jobs = Vec::new();
                for target in targets_ref {
                    metrics::HASH_VERIFICATIONS_TOTAL.inc();
                    
                    match self.sync_file_needed(path, &rel, &meta, ino, target) {
                        Ok(true) => {
                            jobs.push((rel.clone(), target.clone(), ino));
                        },
                        Ok(false) => {
                            crate::metrics::HYDRATION_HASH_SKIPPED.inc();
                        },
                        Err(e) => {
                            warn!("Hydration: Verification failed for {:?}: {}. Queuing for retry/copy.", path, e);
                            jobs.push((rel.clone(), target.clone(), ino));
                        }
                    }
                }
                jobs
            })
            .collect();

        info!("Hydration: {} files require synchronization", verification_results.len());
        
        let low_watermark = high_watermark / 2;

        for (rel_path, target_cfg, ino) in verification_results {
            if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                break;
            }

            self.governor.pace_hydration();
            
            let pending = metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).get() as u64;
            if pending > low_watermark {
                // FIXED: Check shutdown flag during stall/sleep
                for _ in 0..5 {
                    if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }

            if let Some(buf) = &mut job_buffer {
                buf.push((rel_path, target_cfg, ino));
                if buf.len() >= 1_000 {
                    self.flush_buffer_partial(buf);
                }
            } else {
                let bulk_job_queue = self.source.bulk_job_queue.lock();
                if let Some(queue_sender) = bulk_job_queue.as_ref() {
                    queue_sender.submit_job(rel_path, target_cfg, Some(ino));
                    self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        self.flush_buffer(job_buffer);
        
        self.source.hydration.active.store(false, Ordering::SeqCst);
        info!("Hydration: Full scan complete for {:?}", self.source.path);
        
        if !self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            self.start_watcher_if_needed(enable_watching);
        }
    }

    fn start_watcher_if_needed(&self, enable: bool) {
        if enable {
            info!("Hydration: Initializing inotify watcher post-scan...");
            match self.setup_watcher_internal() {
                Ok(watcher) => {
                    let mut w_guard = self.source.watchers.lock();
                    w_guard.push(watcher);
                    info!("Hydration: Watcher active.");
                },
                Err(e) => {
                    warn!("Hydration: Failed to start watcher after scan: {:?}", e);
                }
            }
        }
    }

    fn flush_buffer(&self, buffer: Option<Vec<(PathBuf, TargetConfig, u64)>>) {
        if let Some(jobs) = buffer {
            if !jobs.is_empty() && !self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                info!("Hydration: Structure scan done. Flushing {} deferred data jobs...", jobs.len());
                let bulk_job_queue = self.source.bulk_job_queue.lock();
                if let Some(queue_sender) = bulk_job_queue.as_ref() {
                    for (rel_path, target_cfg, ino) in jobs {
                        if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { break; }
                        queue_sender.submit_job(rel_path, target_cfg, Some(ino));
                        self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn flush_buffer_partial(&self, buffer: &mut Vec<(PathBuf, TargetConfig, u64)>) {
        if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { return; }
        let bulk_job_queue = self.source.bulk_job_queue.lock();
        if let Some(queue_sender) = bulk_job_queue.as_ref() {
            for (rel_path, target_cfg, ino) in buffer.drain(..) {
                queue_sender.submit_job(rel_path, target_cfg, Some(ino));
                self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn setup_watcher_internal(&self) -> Result<RecommendedWatcher> {
        let (tx, rx) = std::sync::mpsc::channel::<std::result::Result<notify::Event, notify::Error>>();
        let source_ref = self.source.clone();
        let targets_ref = self.targets.clone();
        
        let process_logic = move |path: PathBuf, kind: EventKind| {
            let _ = Self::process_path_static(&source_ref, &targets_ref, &path, true, Some(kind));
        };

        std::thread::spawn(move || {
            while let Ok(res) = rx.recv() {
                match res {
                    Ok(event) => {
                        match event.kind {
                            EventKind::Modify(_) | EventKind::Access(_) => continue,
                            _ => {}
                        }
                        for path in event.paths {
                            process_logic(path, event.kind);
                        }
                    },
                    Err(e) => warn!("Hydration: Watcher error: {:?}", e),
                }
            }
        });

        let mut watcher = notify::recommended_watcher(tx)
            .map_err(|e| crate::error::FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        
        watcher.watch(&self.source.path, RecursiveMode::Recursive)
            .map_err(|e| crate::error::FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
            
        Ok(watcher)
    }

    fn process_path_static(
        source: &Arc<SourceInfo>,
        targets: &[TargetConfig],
        path: &Path,
        _urgent: bool,
        _kind: Option<EventKind>
    ) -> Result<()> {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with(".foxing") || name.contains(".tmp.") || name.ends_with(".swap_tmp") {
                return Ok(());
            }
        }

        let rel = match path.strip_prefix(&source.path) {
            Ok(r) => r.to_path_buf(),
            Err(e) => {
                match path.strip_prefix(&source.mount) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => {
                        debug!("Hydration: Failed to strip prefix for {:?} (Source: {:?}, Mount: {:?}): {}", path, source.path, source.mount, e);
                        return Ok(());
                    }
                }
            }
        };

        let m = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(_) => return Ok(()),
        };
        let ino = m.ino();
        let dev = m.dev() as u32;
        let is_dir = m.is_dir();

        if m.nlink() > 1 || is_dir || _urgent {
            identity::update_map(
                &source.inode_map, &source.dir_map, dev, ino, 
                rel.clone(), std::u32::MAX, false, is_dir, u64::MAX, u64::MAX
            );
        }

        let bulk_job_queue = source.bulk_job_queue.lock();
        if let Some(queue_sender) = bulk_job_queue.as_ref() {
            for target_cfg in targets {
                if m.is_file() {
                    queue_sender.submit_job(rel.clone(), target_cfg.clone(), Some(ino));
                    source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                } else if is_dir {
                    let dst_path = target_cfg.path.join(&rel);
                    if !dst_path.exists() {
                        let _ = fs::create_dir_all(&dst_path);
                    }
                    if !security::is_dir_metadata_synced(path, &dst_path) {
                        if let Err(e) = security::apply_metadata(path, &dst_path) {
                            debug!("Hydration: Metadata apply failed for {:?}: {}", dst_path, e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn process_path(
        &self,
        path: &Path,
        urgent: bool,
        _kind: Option<EventKind>,
        job_buffer: &mut Option<Vec<(PathBuf, TargetConfig, u64)>>
    ) -> Result<()> {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with(".foxing") || name.contains(".tmp.") || name.ends_with(".swap_tmp") {
                return Ok(());
            }
        }

        let rel = match path.strip_prefix(&self.source.path) {
            Ok(r) => r.to_path_buf(),
            Err(e) => {
                match path.strip_prefix(&self.source.mount) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => {
                        debug!("Hydration: Failed to strip prefix for {:?} (Source: {:?}, Mount: {:?}): {}", path, self.source.path, self.source.mount, e);
                        return Ok(());
                    }
                }
            }
        };

        let m = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(_) => return Ok(()),
        };
        let ino = m.ino();
        let dev = m.dev() as u32;
        let is_dir = m.is_dir();

        if m.nlink() > 1 || is_dir || urgent {
            identity::update_map(
                &self.source.inode_map, &self.source.dir_map, dev, ino, 
                rel.clone(), std::u32::MAX, false, is_dir, u64::MAX, u64::MAX
            );
        }

        let bulk_job_queue = self.source.bulk_job_queue.lock();
        if let Some(queue_sender) = bulk_job_queue.as_ref() {
            for target_cfg in &self.targets {
                if m.is_file() {
                    if urgent || self.sync_file_needed(path, rel.as_path(), &m, ino, target_cfg)? {
                        if let Some(buf) = job_buffer {
                            buf.push((rel.clone(), target_cfg.clone(), ino));
                        } else {
                            queue_sender.submit_job(rel.clone(), target_cfg.clone(), Some(ino));
                            self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        crate::metrics::HYDRATION_HASH_SKIPPED.inc();
                    }
                } else if is_dir {
                    let dst_path = target_cfg.path.join(&rel);
                    if !dst_path.exists() {
                        let _ = fs::create_dir_all(&dst_path);
                    }
                    if !security::is_dir_metadata_synced(path, &dst_path) {
                        if let Err(e) = security::apply_metadata(path, &dst_path) {
                            debug!("Hydration: Metadata apply failed for {:?}: {}", dst_path, e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn sync_file_needed(&self, src_path: &Path, rel: &Path, src_meta: &fs::Metadata, _ino: u64, target_cfg: &TargetConfig) -> Result<bool> {
        let dst_path = target_cfg.path.join(rel);
        
        if sidecar::is_dirty(&dst_path) {
            if self.mode == HydrationMode::Streaming {
                debug!("Hydration: Skipping DIRTY file {:?} (BPF active).", dst_path);
                return Ok(false);
            }
            info!("Hydration: Found DIRTY flag for {:?}. Forcing repair (Recovery Mode).", dst_path);
            return Ok(true);
        }

        let src_sig = match SyncSignature::compute(src_path) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to compute source signature for {:?}: {}", src_path, e);
                return Ok(true);
            }
        };

        if let Some(dst_sig) = get_sync_signature(&dst_path) {
            if src_sig.matches(&dst_sig) {
                metrics::HASH_CACHE_HITS.inc();
                return Ok(false);
            }
        }

        if let Ok(df) = OpenOptions::new().read(true).open(&dst_path) {
            if !hashing::is_hashing_enabled() {
                if security::acquire_read_lock(df.as_raw_fd()).is_err() {
                    return Ok(true);
                }
                
                let dm = df.metadata()?;
                if dm.len() != src_meta.len() { return Ok(true); }
                if dm.mtime() != src_meta.mtime() { return Ok(true); }
                return Ok(false);
            }

            match hashing::verify_incremental(src_path, &dst_path, src_meta.len()) {
                Ok(true) => {
                    let _ = set_sync_signature(&dst_path, &src_sig);
                    Ok(false)
                },
                Ok(false) => Ok(true),
                Err(e) => {
                    warn!("Hash verification failed for {:?}: {}", dst_path, e);
                    Ok(true)
                }
            }
        } else {
            Ok(true)
        }
    }
}

pub async fn run_hydration_worker_loop(
    rx: Arc<tokio::sync::Mutex<Receiver<crate::hydration::HydrationJob>>>,
    source: Arc<SourceInfo>,
    config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_count: usize,
    worker_id: usize,
    tracker: Arc<DashMap<u64, (u32, std::time::Instant)>>,
    pending_count: Arc<std::sync::atomic::AtomicUsize>,
    stats_senders: Arc<HashMap<PathBuf, Vec<UnboundedSender<CopyStats>>>>,
) -> Result<()> {
    
    use fxcp_core::operations::FsyncLatencyTracker;

    let mut current_alloc = {
        let cfg = config.read().await;
        calculate_hydration_allocation(&cfg, &TargetConfig {
            path: PathBuf::new(),
            profile: crate::config::TargetProfile::Auto,
            ..Default::default()
        }, worker_count, 0, None)
    };

    info!("Hydration Worker {}: Init [{}] - Buffers: {}", worker_id, current_alloc.label, current_alloc.buffers_per_ring);

    let mut ring = IoUring::new((current_alloc.buffers_per_ring + 8) as u32).map_err(|e| FoxingError::Io(e))?;
    
    // FIXED: Buffer allocation robustness and fallback
    let mut buffer_pool = match BufferPool::new(current_alloc.buffers_per_ring, current_alloc.chunk_size_bytes, constants::MINIMUM_ALIGNMENT_BYTES) {
        Ok(pool) => pool,
        Err(e) => {
            warn!("Hydration Worker {}: Allocation failed for optimal buffer count ({}), attempting minimal fallback. Error: {:?}", worker_id, current_alloc.buffers_per_ring, e);
            
            // Try minimal safe allocation (64MB)
            let minimal_buffers = 16;
            let minimal_chunk = 4 * 1024 * 1024;
            match BufferPool::new(minimal_buffers, minimal_chunk, constants::MINIMUM_ALIGNMENT_BYTES) {
                Ok(pool) => {
                    info!("Hydration Worker {}: Fallback allocation succeeded ({} x {}MB).", worker_id, minimal_buffers, minimal_chunk / 1024 / 1024);
                    pool
                },
                Err(critical) => {
                    error!("Hydration Worker {}: CRITICAL MEMORY FAILURE. Exiting worker thread. Error: {:?}", worker_id, critical);
                    return Err(critical.into());
                }
            }
        }
    };

    {
        let iovs = buffer_pool.as_io_vecs();
        unsafe { ring.submitter().register_buffers(&iovs) }.map_err(|e| FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
    }

    let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    if eventfd < 0 { return Err(FoxingError::Io(std::io::Error::last_os_error())); }
    
    ring.submitter().register_eventfd(eventfd).map_err(|e| FoxingError::Io(e))?;
    let async_fd = Arc::new(AsyncFd::new(eventfd).map_err(|e| FoxingError::Io(e))?);

    let source_caps = probe_capabilities(&source.path);
    let mut last_check = std::time::Instant::now();
    let mut fsync_tracker = FsyncLatencyTracker::default();
    let mut current_limit: Option<usize> = None;
    
    let skip_fsync = constants::ONE_SHOT_MODE.load(Ordering::Relaxed);
    if skip_fsync {
        info!("Hydration Worker {}: Running in One-Shot Mode (Fast Sync). Per-file fsync disabled.", worker_id);
    }

    loop {
        // FIXED: Check for shutdown signal in worker loop
        if source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            info!("Hydration Worker {}: Shutdown requested.", worker_id);
            break;
        }

        if last_check.elapsed() > std::time::Duration::from_secs(5) {
            let pending_events = metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).get() as u64;
            let desired_alloc = {
                let cfg = config.read().await;
                calculate_hydration_allocation(
                    &cfg,
                    &TargetConfig {
                        path: PathBuf::new(),
                        profile: crate::config::TargetProfile::Auto,
                        ..Default::default()
                    },
                    worker_count,
                    pending_events,
                    Some(&current_alloc.label)
                )
            };

            let mut dynamic_limit_from_tuner = None;
            for r in GLOBAL_TUNER_REGISTRY.iter() {
                 let output = r.value();
                 let bdp = output.bdp_bytes;
                 if bdp > 0 {
                     let buffers_needed = (bdp / current_alloc.chunk_size_bytes as u64) as usize;
                     dynamic_limit_from_tuner = Some(buffers_needed);
                     break;
                 }
            }

            if desired_alloc.label == "hydration-yield" {
                current_limit = Some(buffer_pool.capacity() / 5);
                if current_alloc.label != "hydration-yield" {
                    info!("Hydration Worker {}: Entering Yield Mode (Limit: {} buffers)", worker_id, current_limit.unwrap());
                }
            } else if let Some(tuner_limit) = dynamic_limit_from_tuner {
                let safe_limit = tuner_limit.min(buffer_pool.capacity());
                current_limit = Some(safe_limit);
            } else {
                current_limit = None;
            }

            current_alloc = desired_alloc;
            last_check = std::time::Instant::now();
        }

        if buffer_pool.get_ptr(0).is_none() {
            error!("Hydration Worker Buffer Pool invalid or exhausted. Exiting loop.");
            break;
        }

        let job = {
            let mut lock = rx.lock().await;
            // Use try_recv loop to allow checking shutdown flag
            loop {
                if source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                    break None;
                }
                match lock.try_recv() {
                    Ok(j) => break Some(j),
                    Err(mpsc::error::TryRecvError::Empty) => {
                        drop(lock);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        lock = rx.lock().await;
                    },
                    Err(mpsc::error::TryRecvError::Disconnected) => break None,
                }
            }
        };

        match job {
            Some(job) => {
                let target_path_root = job.target_cfg.path.clone();
                let target_caps = probe_capabilities(&target_path_root);
                
                let stats_opt = process_hydration_job(
                    job.clone(), &source, &governor, &tuner_board, &mut ring, &mut buffer_pool, 
                    async_fd.clone(), &source_caps, &target_caps, &tracker, &mut fsync_tracker,
                    current_limit,
                    skip_fsync
                ).await?;

                if let Some(stats) = stats_opt {
                     if let Some(senders) = stats_senders.get(&job.target_cfg.path) {
                         if let Some(sender) = senders.choose(&mut rand::rng()) {
                             let _ = sender.send(stats);
                         }
                     }
                }
                pending_count.fetch_sub(1, Ordering::SeqCst);
            },
            None => break,
        }
    }

    let _ = ring.submitter().unregister_buffers();
    Ok(())
}

async fn wait_for_reconnect(path: &Path) {
    let mut backoff = 1;
    while !path.exists() {
        warn!("Waiting for reconnect: {:?} not found. Retrying in {}s...", path, backoff);
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
    info!("Reconnect detected: {:?} is back.", path);
}

fn is_disconnect_error(err: &FoxingError) -> bool {
    if let FoxingError::Io(e) = err {
        if let Some(code) = e.raw_os_error() {
            return code == libc::ENODEV || code == libc::EIO || code == libc::EROFS || code == libc::ENOENT;
        }
        if e.kind() == ErrorKind::NotFound {
            return true;
        }
    }
    false
}

pub async fn process_hydration_job(
    job: crate::hydration::HydrationJob,
    source: &Arc<SourceInfo>,
    governor: &Arc<Governor>,
    tuner_board: &TunerBoard,
    ring: &mut io_uring::IoUring,
    buffer_pool: &mut BufferPool,
    async_fd: Arc<AsyncFd<std::os::unix::io::RawFd>>,
    source_caps: &Arc<fxcp_core::operations::Capabilities>,
    target_caps_ref: &Arc<fxcp_core::operations::Capabilities>,
    tracker: &Arc<DashMap<u64, (u32, std::time::Instant)>>,
    fsync_tracker: &mut fxcp_core::operations::FsyncLatencyTracker,
    buffer_limit: Option<usize>,
    skip_fsync: bool,
) -> Result<Option<CopyStats>> {
    use tokio::task::spawn_blocking;
    use fxcp_core::operations::{SmartCopier};
    use std::time::Duration;
    use fxcp_core::constants;
    use std::io::ErrorKind;
    use crate::identity;
    

    let crate::hydration::HydrationJob { mut rel_path, target_cfg, inode: job_inode } = job;
    
    let target_caps = target_caps_ref.clone();
    if !target_cfg.target_uncached { target_caps.atomic_writes.store(false, Ordering::Relaxed); }
    if !target_cfg.atomic_writes { target_caps.atomic_writes.store(false, Ordering::Relaxed); }

    let inode = if let Some(ino) = job_inode {
        ino
    } else {
        std::fs::metadata(&source.mount.join(&rel_path)).map(|m| m.ino()).unwrap_or(0)
    };

    let mut current_source_path = source.mount.join(&rel_path);
    let mut current_target_path = target_cfg.path.join(&rel_path);

    if inode != 0 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let source_clone = source.clone();
        
        spawn_blocking(move || {
            let res = identity::resolve_and_update_path(&source_clone, inode, 0, 0, 0);
            let _ = tx.send(res);
        });

        if let Ok(Ok(new_full_path)) = rx.await.map_err(|e| FoxingError::Io(std::io::Error::new(ErrorKind::BrokenPipe, format!("Oneshot RecvError during path resolution: {}", e)))) {
            let new_abs_path = source.mount.join(&new_full_path);
            if new_abs_path != current_source_path {
                if let Ok(new_rel) = new_abs_path.strip_prefix(&source.mount) {
                    info!("Hydration Recovery: Path updated for inode {} from {:?} to {:?}.", inode, current_source_path, new_abs_path);
                    rel_path = new_rel.to_path_buf();
                    current_source_path = new_abs_path;
                    current_target_path = target_cfg.path.join(&rel_path);
                }
            }
        }
    }

    if let Ok(_metadata) = std::fs::metadata(&current_source_path) {
        tracker.remove(&inode);
    }

    let direct_io_ok = target_cfg.direct_io_ok.load(Ordering::Relaxed);
    let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);

    if governor.is_system_stressed() || matches!(current_state, TunerState::Muted) {
        tokio::task::yield_now().await;
    }

    let mut attempts = 0;
    let max_attempts = constants::HYDRATION_COPY_MAX_ATTEMPTS;
    let mut success = false;
    let mut final_stats = None;

    while attempts < max_attempts && !success {
        if source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            return Ok(None);
        }

        attempts += 1;
        
        let file_size_res = tokio::fs::metadata(&current_source_path).await.map_err(FoxingError::Io);
        
        let copy_result: Result<Option<CopyStats>> = match file_size_res {
            Ok(metadata) => {
                if metadata.is_dir() {
                    if !current_target_path.exists() {
                        let _ = fs::create_dir_all(&current_target_path);
                    }
                    return Ok(None);
                }
                if !metadata.is_file() {
                     return Ok(None);
                }
                
                let file_size = metadata.len();
                
                SmartCopier::copy_with_limit(
                    &current_source_path,
                    &current_target_path,
                    ring,
                    buffer_pool,
                    None,
                    async_fd.clone(),
                    target_cfg.vdo_optimization,
                    0,
                    file_size,
                    direct_io_ok,
                    file_size,
                    source_caps,
                    &target_caps,
                    target_cfg.vdo_stall_threshold,
                    target_cfg.source_uncached,
                    target_cfg.target_uncached,
                    &None,
                    Some(governor.clone()),
                    target_cfg.path.to_string_lossy().to_string(),
                    fsync_tracker,
                    buffer_limit,
                    skip_fsync
                ).await.map(Some).map_err(FoxingError::from)
            },
            Err(e) => Err(e),
        };

        match copy_result {
            Ok(Some(stats)) => {
                crate::metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(stats.bytes_processed as f64);
                
                let src_path_clone = current_source_path.clone();
                let dst_path_clone = current_target_path.clone();
                
                let metadata_result: std::result::Result<(), FoxingError> = spawn_blocking(move || {
                    security::sync_xattrs(&src_path_clone, &dst_path_clone);
                    security::apply_metadata(&src_path_clone, &dst_path_clone)
                }).await
                .map_err(FoxingError::Join)
                .and_then(|inner| inner.map_err(Into::into));

                if metadata_result.is_err() {
                    warn!("Hydration: Failed to apply metadata/clear state for {:?}. Retrying.", current_target_path);
                } else {
                    success = true;
                    final_stats = Some(stats);
                    source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                }
            },
            Ok(None) => {
                success = true;
            }
            Err(e) => {
                if let FoxingError::Io(ref io_err) = e {
                    if io_err.kind() == ErrorKind::NotFound {
                        info!("Hydration: File vanished before replication (skipping): {:?}", current_source_path);
                        return Ok(None);
                    }
                }
                
                if is_disconnect_error(&e) {
                    warn!("Hydration: Disconnect detected for {:?}. Pausing worker to wait for reconnect...", current_source_path);
                    wait_for_reconnect(&source.mount).await;
                    if current_source_path.exists() {
                        info!("Hydration: Device reconnected. Resuming job for {:?}.", current_source_path);
                        attempts = 0;
                        continue;
                    }
                }

                let exponent = attempts.min(10) as u32;
                let backoff_ms = (constants::HYDRATION_COPY_BACKOFF_BASE_MS * 2u64.pow(exponent)).min(constants::HYDRATION_COPY_BACKOFF_MAX_MS);
                
                use rand::Rng;
                let jitter_ms = rand::rng().random_range(0..constants::HYDRATION_COPY_JITTER_MAX_MS);
                let delay = Duration::from_millis(backoff_ms + jitter_ms);
                
                tokio::time::sleep(delay).await;
                
                if attempts < max_attempts {
                    warn!("Hydration copy FAILED for {:?} (Attempt {}/{}) due to {:?}. Retrying in {:?}...", 
                          rel_path, attempts, max_attempts, e, delay);
                } else {
                    error!("Hydration copy POISONED after {} attempts for {:?}: {:?}", max_attempts, rel_path, e);
                    return Err(e);
                }
            }
        }
    }

    if !success {
        return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "Hydration job failed repeated attempts")));
    }

    Ok(final_stats)
}
