use std::sync::{Arc, atomic::Ordering};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{error, warn, debug}; 
use crate::error::{Result, FoxingError};
use crate::mirror::{SourceInfo, SharedConfig};
use crate::config::TargetConfig; 
use crate::operations::SmartCopier;
use crate::tuner::{TunerBoard, TunerState};
use crate::security;
use crate::governor::Governor;
use tokio::task::spawn_blocking;
use crate::metrics;
use std::time::Duration;

#[derive(Debug)]
pub struct HydrationJob {
    pub rel_path: PathBuf,
    pub target_cfg: TargetConfig,
}

#[derive(Debug)] 
pub struct HydrationQueue {
    sender: mpsc::Sender<HydrationJob>,
}

impl HydrationQueue {
    pub fn new(
        source: Arc<SourceInfo>,
        config: SharedConfig,
        governor: Arc<Governor>,
        tuner_board: TunerBoard,
        worker_count: usize
    ) -> (Self, Vec<tokio::task::JoinHandle<Result<()>>>) {
        let (tx, rx) = mpsc::channel(1000);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let mut handles = Vec::new();

        for _ in 0..worker_count {
            let rx_clone = rx.clone();
            let source_clone = source.clone();
            let config_clone = config.clone();
            let governor_clone = governor.clone();
            let tuner_clone = tuner_board.clone();

            handles.push(tokio::spawn(async move {
                run_hydration_worker_loop(rx_clone, source_clone, config_clone, governor_clone, tuner_clone).await
            }));
        }

        (Self { sender: tx }, handles)
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
    _config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
) -> Result<()> {
    let mut ring = match io_uring::IoUring::new(4) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create hydration io_uring: {}", e); return Err(e.into()); }
    };
    let mut buf = crate::buffer::AlignedBuffer::new(1024 * 1024);

    loop {
        let job = {
            let mut lock = rx.lock().await;
            lock.recv().await
        };

        match job {
            Some(job) => {
                process_hydration_job(job, &source, &governor, &tuner_board, &mut ring, &mut buf).await?;
            },
            None => break,
        }
    }
    Ok(())
}

async fn process_hydration_job(
    job: HydrationJob,
    source: &Arc<SourceInfo>,
    governor: &Arc<Governor>,
    tuner_board: &TunerBoard,
    ring: &mut io_uring::IoUring,
    buf: &mut crate::buffer::AlignedBuffer,
) -> Result<()> {
    let HydrationJob { rel_path, target_cfg } = job;
    let source_path = source.mount.join(&rel_path);
    let target_path = target_cfg.path.join(&rel_path);
    
    let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);
    if governor.is_system_stressed() ||
       matches!(current_state, TunerState::Muted | TunerState::CriticalDrain)
    {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let target_path_lossy = target_cfg.path.to_string_lossy().to_string();
    
    if let Some(parent) = target_path.parent() {
        if !parent.exists() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    let mut attempts = 0;
    let max_attempts = 3;
    let mut success = false;

    while attempts < max_attempts && !success {
        attempts += 1;
        let file_size_res = std::fs::metadata(&source_path);
        
        let res = match file_size_res {
            Ok(metadata) => {
                let file_size = metadata.len();
                let direct_io_ok = target_cfg.direct_io_ok.load(Ordering::Relaxed);
                
                let copy_res = SmartCopier::copy(
                    &source_path,
                    &target_path,
                    ring,
                    buf,
                    &target_cfg.supports_reflink,
                    target_cfg.vdo_optimization,
                    0,
                    file_size,
                    direct_io_ok,
                    file_size,
                ).await;

                if let Ok(stats) = &copy_res {
                    metrics::BYTES_REPLICATED.with_label_values(&[&target_path_lossy]).inc_by(stats.bytes_processed);
                }
                copy_res.map_err(FoxingError::from)
            },
            Err(e) => Err(FoxingError::Io(e))
        };

        match res {
            Ok(_) => {
                success = true;
                let src_path_clone = source_path.clone();
                let dst_path_clone = target_path.clone();
                
                let _ = spawn_blocking(move || {
                    security::sync_xattrs(&src_path_clone, &dst_path_clone);
                    security::apply_metadata(&src_path_clone, &dst_path_clone)
                }).await;

                source.hydration.synced.fetch_add(1, Ordering::Relaxed);
            },
            Err(e) => {
                if attempts < max_attempts {
                    let delay = Duration::from_millis(50 * (attempts as u64));
                    tokio::time::sleep(delay).await;
                } else {
                    warn!("Hydration copy FAILED for {:?}: {:?}", rel_path, e);
                }
            }
        }
    }

    Ok(())
}
