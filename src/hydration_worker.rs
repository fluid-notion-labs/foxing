use std::sync::{Arc, atomic::Ordering};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{error, warn, debug};
use crate::error::{Result, FoxingError};
use crate::mirror::{SourceInfo, SharedConfig};
use crate::config::TargetConfig;
use crate::operations::{SmartCopier, CopyStats};
use crate::tuner::{TunerBoard, TunerState};
use crate::security;
use crate::governor::Governor;
use tokio::task::spawn_blocking;
use crate::metrics;
use std::time::{Duration, Instant};
use std::io::ErrorKind;
use crate::identity;
use crate::buffer::BufferPool;
use std::os::unix::fs::MetadataExt;
use crate::sidecar;
use dashmap::DashMap;

#[derive(Debug)]
pub struct HydrationJob {
    pub rel_path: PathBuf,
    pub target_cfg: TargetConfig,
}

#[derive(Debug)]
pub struct HydrationQueue {
    sender: mpsc::Sender<HydrationJob>,
    #[allow(dead_code)]
    rename_failure_tracker: Arc<DashMap<u64, (u32, Instant)>>,
}

fn initialize_hydration_buffer_pool(cfg: &SharedConfig, worker_count: usize) -> Result<BufferPool> {
    let config_reader = futures::executor::block_on(cfg.read());
    let global_limit_mib = config_reader.global_buffer_limit;
    let total_hydration_workers = worker_count.max(1);
    
    // Dedicated buffer memory for hydration (20% of global limit)
    let buffer_chunk_size_mib = config_reader.io_buffer_size_mib.max(1);
    let total_hydration_mem_limit_mib = global_limit_mib * 2 / 10;
    let worker_mem_limit_mib = total_hydration_mem_limit_mib / total_hydration_workers as u64;
    
    // Calculate buffer count
    let num_io_buffers = (worker_mem_limit_mib / buffer_chunk_size_mib).max(2) as usize;
    let buffer_chunk_size_bytes = (buffer_chunk_size_mib * 1024 * 1024) as usize;

    let pool = BufferPool::new(num_io_buffers, buffer_chunk_size_bytes);
    debug!("Hydration Worker: Initialized {} x {}MB buffers (Total {}MB).", 
           pool.capacity(), buffer_chunk_size_mib, pool.capacity() as u64 * buffer_chunk_size_mib);
    Ok(pool)
}

impl HydrationQueue {
    pub fn new(
        source: Arc<SourceInfo>,
        config: SharedConfig,
        governor: Arc<Governor>,
        tuner_board: TunerBoard,
        worker_count: usize
    ) -> (Self, Vec<tokio::task::JoinHandle<Result<()>>>) {
        // FOXING DEBUG: Increased queue size from 1,000 to 100,000 to handle Metadata Storms.
        // During torture tests, ~5000 events can generate thousands of repair requests if dropped.
        let (tx, rx) = mpsc::channel(100_000);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let tracker = Arc::new(DashMap::new());

        let mut handles = Vec::new();

        for id in 0..worker_count {
            let rx_clone = rx.clone();
            let source_clone = source.clone();
            let config_clone = config.clone();
            let governor_clone = governor.clone();
            let tuner_clone = tuner_board.clone();
            let tracker_clone = tracker.clone();

            handles.push(tokio::spawn(async move {
                run_hydration_worker_loop(rx_clone, source_clone, config_clone, governor_clone, tuner_clone, worker_count, id, tracker_clone).await
            }));
        }

        (Self { sender: tx, rename_failure_tracker: tracker }, handles)
    }

    pub fn submit_job(&self, rel_path: PathBuf, target_cfg: TargetConfig) {
        let job = HydrationJob { rel_path, target_cfg };
        if let Err(_) = self.sender.try_send(job) {
            debug!("Hydration Queue Full. Dropping job.");
        }
    }
}

async fn run_hydration_worker_loop(
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<HydrationJob>>>,
    source: Arc<SourceInfo>,
    config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_count: usize,
    _worker_id: usize,
    tracker: Arc<DashMap<u64, (u32, Instant)>>,
) -> Result<()> {
    // Each worker gets its own io_uring instance
    let mut ring = match io_uring::IoUring::new(4) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create hydration io_uring: {}", e); return Err(e.into()); }
    };

    let mut buffer_pool = initialize_hydration_buffer_pool(&config, worker_count)?;
    
    // Register buffers
    {
        let iovs = buffer_pool.as_io_vecs();
        if unsafe { ring.submitter().register_buffers(&iovs) }.is_err() {
            error!("Failed to register buffers in hydration worker.");
            return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Failed to register io_uring buffers")));
        }
    }

    let src_rwf_uncached_ok = source.rwf_uncached_ok.load(Ordering::Relaxed);

    loop {
        let job = {
            let mut lock = rx.lock().await;
            lock.recv().await
        };

        match job {
            Some(job) => {
                process_hydration_job(job, &source, &governor, &tuner_board, &mut ring, &mut buffer_pool, src_rwf_uncached_ok, &tracker).await?;
            },
            None => break, // Channel closed
        }
    }

    let _ = ring.submitter().unregister_buffers();
    Ok(())
}

async fn process_hydration_job(
    job: HydrationJob,
    source: &Arc<SourceInfo>,
    governor: &Arc<Governor>,
    tuner_board: &TunerBoard,
    ring: &mut io_uring::IoUring,
    buffer_pool: &mut BufferPool,
    src_rwf_uncached_ok: bool,
    tracker: &Arc<DashMap<u64, (u32, Instant)>>,
) -> Result<()> {
    let HydrationJob { mut rel_path, target_cfg } = job;
    let target_rwf_uncached_ok = target_cfg.rwf_uncached_ok.load(Ordering::Relaxed);
    
    let source_path_start = source.mount.join(&rel_path);
    let mut target_path = target_cfg.path.join(&rel_path);

    // 1. Resolve Identity / Handle Rename Correction
    if let Ok(metadata) = std::fs::metadata(&source_path_start) {
        let inode = metadata.ino();

        // Check rename loop tracker
        let should_backoff = if let Some(mut entry) = tracker.get_mut(&inode) {
            let (count, last_attempt) = *entry.value();
            if count > 10 {
                warn!("Hydration: Inode {} permanently failed {} attempts. Skipping.", inode, count);
                return Ok(());
            }
            if count > 3 {
                let backoff = Duration::from_millis(100 * 2u64.pow(count));
                if last_attempt.elapsed() < backoff {
                    true
                } else {
                    entry.value_mut().1 = Instant::now();
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if should_backoff {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        match spawn_blocking({
            let source_clone = source.clone();
            move || identity::resolve_and_update_path(&source_clone, inode, 0, 0, 0)
        }).await.map_err(FoxingError::Join).and_then(|r| r.map_err(FoxingError::Io)) {
            Ok(new_full_path) => {
                if let Ok(new_rel) = new_full_path.strip_prefix(&source.mount) {
                    if new_rel != rel_path.as_path() {
                        debug!("Hydration Worker: Correcting renamed path {} -> {}", rel_path.to_string_lossy(), new_rel.to_string_lossy());
                        target_path = target_cfg.path.join(new_rel);
                        rel_path = new_rel.to_path_buf();
                    }
                }
                tracker.remove(&inode);
            },
            Err(FoxingError::Io(e)) if e.kind() == ErrorKind::NotFound => {
                warn!("Hydration Worker: Inode lookup failed for former path {:?}. Assuming deletion.", rel_path);
                let mut entry = tracker.entry(inode).or_insert((0, Instant::now()));
                entry.value_mut().0 += 1;
            },
            Err(e) => return Err(e),
        }
    }

    let source_path = source.mount.join(&rel_path);
    if !source_path.exists() {
        // If source disappeared, cleanup target
        if target_path.exists() {
            warn!("Hydration Worker: Source path {:?} disappeared. Deleting target: {:?}", source_path, target_path);
            let target_path_clone = target_path.clone();
            let _ = spawn_blocking(move || {
                if target_path_clone.is_dir() {
                    std::fs::remove_dir(&target_path_clone)
                } else {
                    std::fs::remove_file(&target_path_clone)
                }
            }).await;
        }
        return Ok(());
    }
    let target_path_lossy = target_path.to_string_lossy().to_string();

    // 2. Tuner Pacing
    let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);
    if governor.is_system_stressed() || 
       matches!(current_state, TunerState::Muted | TunerState::CriticalDrain) 
    {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 3. Ensure Parent Exists
    if let Some(parent) = target_path.parent() {
        if !parent.exists() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    // 4. Copy Loop
    let mut attempts = 0;
    let max_attempts = 5;
    let mut success = false;

    while attempts < max_attempts && !success {
        attempts += 1;
        let file_size_res = std::fs::metadata(&source_path);
        
        let copy_result: Result<Option<CopyStats>> = match file_size_res {
            Ok(metadata) => {
                if !metadata.is_file() {
                    if metadata.is_dir() && !target_path.exists() {
                        let _ = std::fs::create_dir_all(&target_path);
                    }
                    return Ok(()); // Directories handled, done.
                }

                let file_size = metadata.len();
                let direct_io_ok = target_cfg.direct_io_ok.load(Ordering::Relaxed);
                
                SmartCopier::copy(
                    &source_path,
                    &target_path,
                    ring,
                    buffer_pool,
                    &target_cfg.supports_reflink,
                    target_cfg.vdo_optimization,
                    0, // Offset 0 for full file
                    file_size,
                    direct_io_ok,
                    file_size,
                    src_rwf_uncached_ok,
                    target_rwf_uncached_ok,
                    target_cfg.vdo_stall_threshold,
                ).await.map(Some).map_err(FoxingError::from)
            },
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(()); // Disappeared during copy prep
                }
                Err(FoxingError::Io(e))
            }
        };

        match copy_result {
            Ok(Some(stats)) => {
                metrics::BYTES_REPLICATED.with_label_values(&[&target_path_lossy]).inc_by(stats.bytes_processed as f64);
                
                // Metadata Sync in blocking thread
                let src_path_clone = source_path.clone();
                let dst_path_clone = target_path.clone();
                let apply_res = spawn_blocking(move || {
                    security::sync_xattrs(&src_path_clone, &dst_path_clone);
                    let _ = security::apply_metadata(&src_path_clone, &dst_path_clone);
                    sidecar::clear_wal_state(&dst_path_clone);
                }).await;

                if apply_res.is_err() {
                    warn!("Hydration: Failed to apply metadata/clear state for {:?}. Retrying.", target_path);
                } else {
                    success = true;
                    source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                }
            },
            Ok(None) => {
                success = true;
            }
            Err(e) => {
                let delay = Duration::from_millis(100 * (attempts as u64).min(5));
                
                // Check for identity change (rename race during copy)
                if let FoxingError::Io(io_err) = &e {
                    if io_err.kind() == ErrorKind::Other || io_err.raw_os_error() == Some(5) { // EIO or similar
                        let _ = spawn_blocking({
                            let source_clone = source.clone();
                            move || identity::resolve_and_update_path(&source_clone, 0, 0, 0, 0)
                        }).await;
                    }
                }

                if attempts < max_attempts {
                    warn!("Hydration copy FAILED for {:?} (Attempt {}/{}) due to {:?}. Delaying {:?}.", 
                          rel_path, attempts, max_attempts, e, delay);
                    tokio::time::sleep(delay).await;
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

    Ok(())
}
