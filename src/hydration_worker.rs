use std::sync::{Arc, atomic::Ordering};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{error, warn, debug, info};
use crate::error::{Result, FoxingError};
use crate::mirror::{SourceInfo, SharedConfig};
use crate::config::TargetConfig;
use crate::operations::{SmartCopier, CopyStats};
use crate::tuner::{TunerBoard, TunerState};
use crate::security;
use crate::governor::Governor;
use tokio::task::spawn_blocking;
use crate::metrics;
use std::time::Duration;
use std::io::ErrorKind;
use crate::identity;
use crate::buffer::BufferPool;
use std::os::unix::fs::MetadataExt;
use crate::versioning;
use crate::sidecar;

#[derive(Debug)]
pub struct HydrationJob {
    pub rel_path: PathBuf,
    pub target_cfg: TargetConfig,
}

#[derive(Debug)]
pub struct HydrationQueue {
    sender: mpsc::Sender<HydrationJob>,
}

fn initialize_hydration_buffer_pool(cfg: &SharedConfig, worker_count: usize) -> Result<BufferPool> {
    let config_reader = futures::executor::block_on(cfg.read());
    let global_limit_mib = config_reader.global_buffer_limit;
    let total_hydration_workers = worker_count.max(1);
    let buffer_chunk_size_mib = config_reader.io_buffer_size_mib.max(1);
    let total_hydration_mem_limit_mib = global_limit_mib * 2 / 10;
    let worker_mem_limit_mib = total_hydration_mem_limit_mib / total_hydration_workers as u64;
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
        let (tx, rx) = mpsc::channel(1000);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let mut handles = Vec::new();
        
        for id in 0..worker_count {
            let rx_clone = rx.clone();
            let source_clone = source.clone();
            let config_clone = config.clone();
            let governor_clone = governor.clone();
            let tuner_clone = tuner_board.clone();
            
            handles.push(tokio::spawn(async move {
                run_hydration_worker_loop(rx_clone, source_clone, config_clone, governor_clone, tuner_clone, worker_count, id).await
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
    config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_count: usize,
    _worker_id: usize,
) -> Result<()> {
    let mut ring = match io_uring::IoUring::new(4) {
        Ok(r) => r,
        Err(e) => { error!("Failed to create hydration io_uring: {}", e); return Err(e.into()); }
    };
    
    let mut buffer_pool = initialize_hydration_buffer_pool(&config, worker_count)?;
    {
        let iovs = buffer_pool.as_io_vecs();
        if unsafe { ring.submitter().register_buffers(&iovs) }.is_err() {
            error!("Failed to register buffers in hydration worker.");
            return Err(FoxingError::Io(std::io
