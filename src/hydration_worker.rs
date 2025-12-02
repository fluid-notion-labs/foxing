use std::sync::{Arc, atomic::Ordering};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{info, error, warn, debug};
use crate::error::{Result, FoxingError};
use crate::mirror::{SourceInfo, SharedConfig};
use crate::config::TargetConfig;
use crate::operations::SmartCopier;
use crate::worker::{TunerBoard, TunerState};
use crate::security;
use crate::governor::Governor;
use tokio::task::spawn_blocking;
use crate::metrics;

#[derive(Debug)]
pub struct HydrationJob {
    pub rel_path: PathBuf,
    pub target_cfg: TargetConfig,
}

pub async fn run_hydration_worker(
    source: Arc<SourceInfo>,
    mut job_rx: mpsc::Receiver<HydrationJob>,
    _config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
) -> Result<()> {
    info!("Hydration Worker started.");
    
    let mut ring = match io_uring::IoUring::new(4) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create hydration io_uring: {}", e); return Err(e.into()); }
    };
    
    let mut buf = crate::buffer::AlignedBuffer::new(64 * 1024);

    while let Some(job) = job_rx.recv().await {
        let HydrationJob { rel_path, target_cfg } = job;
        
        let source_path = source.mount.join(&rel_path);
        let target_path = target_cfg.path.join(&rel_path);
        
        let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);
        
        if governor.is_system_stressed() ||
           matches!(current_state, TunerState::Muted | TunerState::CriticalDrain)
        {
             debug!("Hydration worker pacing aggressively due to system/target stress: {:?}", current_state);
             tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        let target_path_lossy = target_cfg.path.to_string_lossy().to_string();
        
        let file_size_res = std::fs::metadata(&source_path);
        
        let res = match file_size_res {
            Ok(metadata) => {
                let file_size = metadata.len();
                let direct_io_ok = target_cfg.direct_io_ok.load(Ordering::Relaxed);
                
                let copy_res = SmartCopier::copy(
                    &source_path,
                    &target_path,
                    &mut ring,
                    &mut buf,
                    &target_cfg.supports_reflink,
                    target_cfg.vdo_optimization,
                    0,
                    file_size,
                    direct_io_ok,
                    file_size,
                ).await;

                if let Ok(stats) = &copy_res {
                    metrics::BYTES_REPLICATED.with_label_values(&[&target_path_lossy]).inc_by(stats.bytes_processed);
                    metrics::REPLICATION_LATENCY.with_label_values(&[&target_path_lossy]).observe(stats.io_duration.as_secs_f64());
                }
                copy_res.map_err(FoxingError::from)
            },
            Err(e) => Err(FoxingError::Io(e))
        };

        match res {
            Ok(_) => {
                info!("Hydration sync complete for {:?} bytes", rel_path);
                let src_path_clone = source_path.clone();
                let dst_path_clone = target_path.clone();
                
                let meta_res = spawn_blocking(move || {
                    security::sync_xattrs(&src_path_clone, &dst_path_clone);
                    security::apply_metadata(&src_path_clone, &dst_path_clone)
                }).await;

                match meta_res {
                    Ok(Ok(_)) => {
                        source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                    },
                    Ok(Err(e)) => {
                        error!("Hydration post-copy metadata/sync failed for {:?}: {:?}", rel_path, e);
                    },
                    Err(e) => {
                        error!("Hydration task failed to join: {:?}", e);
                    }
                }
            },
            Err(e) => {
                warn!("Hydration copy failed for {:?}: {:?}", rel_path, e);
            }
        }
    }
    
    info!("Hydration Worker shut down.");
    Ok(())
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
        _worker_count: usize,
    ) -> (Self, Vec<tokio::task::JoinHandle<Result<()>>>) {
        let (sender, receiver) = mpsc::channel(source.lru_size * 2);
        let mut handles = Vec::new();
        
        let source_clone = source.clone();
        let config_clone = config.clone();
        let governor_clone = governor.clone();
        let tuner_board_clone = tuner_board.clone();
        
        // Currently spawning 1 worker regardless of worker_count to avoid contention on single io_uring/buffer
        // In production, we'd spawn `worker_count` tasks, each with its own ring/buffer.
        let handle = tokio::spawn(run_hydration_worker(
            source_clone,
            receiver,
            config_clone,
            governor_clone,
            tuner_board_clone,
        ));
        
        handles.push(handle);
        (Self { sender }, handles)
    }

    pub fn submit_job(&self, rel_path: PathBuf, target_cfg: TargetConfig) {
        let job = HydrationJob { rel_path, target_cfg };
        if self.sender.try_send(job).is_err() {
            warn!("Hydration job queue full. Dropping job.");
        }
    }
}
