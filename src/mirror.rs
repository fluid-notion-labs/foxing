use std::{sync::{Arc}, collections::HashMap, path::PathBuf, fs};
use crate::{config::{Config, TargetConfig}, event::{EventQueue}, worker::{self, TunerBoard, TunerState}, identity, Result, governor::Governor};
use crate::hydration::Hydrator;
use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::{RwLock, mpsc};
use std::os::unix::fs::{MetadataExt};
use tracing::{info, error, debug}; 
use dashmap::DashMap;

pub type SharedConfig = Arc<RwLock<Config>>;
pub type HydrationTx = mpsc::Sender<PathBuf>;
pub type HydrationRx = mpsc::Receiver<PathBuf>;

pub struct SourceInfo { 
    pub path: PathBuf, 
    pub mount: PathBuf, 
    pub dev: u32,
    pub hydration: Arc<crate::hydration::HydrationState>,
    pub inode_map: identity::InodeMap,
    pub lru_size: usize, 
}

pub struct Manager {
    config: SharedConfig, 
    sources: HashMap<u32, Arc<SourceInfo>>,
    watchers: Vec<notify::RecommendedWatcher>,
    hydration_handles: Arc<Mutex<Vec<std::thread::JoinHandle<Result<()>>>>>,
    pub queues: HashMap<u32, Vec<Arc<EventQueue>>>,
    pub governor: Arc<Governor>,
    pub tuner_board: TunerBoard,
}

fn find_mount_point(path: &PathBuf) -> Result<(PathBuf, u32, bool)> {
    let canonical = path.canonicalize().map_err(|e| crate::error::FoxingError::Io(e))?;
    debug!("Finding mount point for canonical path: {:?}", canonical);
    let mounts_content = fs::read_to_string("/proc/mounts").map_err(|e| crate::error::FoxingError::Io(e))?;
    
    #[derive(Debug)]
    struct MountEntry {
        _device: String,
        mount_point: PathBuf,
        _fstype: String,
        is_loopback: bool,
    }
    
    let mut mounts: Vec<MountEntry> = Vec::new();
    for line in mounts_content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 { continue; }
        let device = parts[0];
        let mount_point = PathBuf::from(parts[1]);
        let fstype = parts[2];
        let is_loopback = device.starts_with("/dev/loop");
        mounts.push(MountEntry {
            _device: device.to_string(),
            mount_point,
            _fstype: fstype.to_string(),
            is_loopback,
        });
    }
    
    mounts.sort_by(|a, b| b.mount_point.components().count().cmp(&a.mount_point.components().count()));
    
    for mount in &mounts {
        if canonical.starts_with(&mount.mount_point) {
            let mount_meta = fs::metadata(&mount.mount_point).map_err(|e| crate::error::FoxingError::Io(e))?;
            let raw_dev = mount_meta.dev();
            let major = ((raw_dev >> 8) & 0xfff) as u32;
            let minor = ((raw_dev & 0xff) | ((raw_dev >> 12) & 0xfff00)) as u32;
            let kernel_dev_id = (major << 20) | minor;
            return Ok((mount.mount_point.clone(), kernel_dev_id, mount.is_loopback));
        }
    }
    
    let meta = fs::metadata(&canonical).map_err(|e| crate::error::FoxingError::Io(e))?;
    let raw_dev = meta.dev();
    let major = ((raw_dev >> 8) & 0xfff) as u32;
    let minor = ((raw_dev & 0xff) | ((raw_dev >> 12) & 0xfff00)) as u32;
    let kernel_dev_id = (major << 20) | minor;
    Ok((canonical, kernel_dev_id, false))
}

impl Manager {
    pub async fn new(cfg: SharedConfig) -> Self { 
        let _cloned_cfg = cfg.clone(); 
        let config_reader = cfg.read().await;
        let governor = Arc::new(Governor::new(
            config_reader.max_system_load_avg,
            config_reader.hydration_delay_ms
        ));

        let mut sources = HashMap::new();
        let cache_size_raw = (config_reader.global_buffer_limit / 5) as usize; 
        let cache_size = NonZeroUsize::new(cache_size_raw.max(10000)).unwrap_or_else(|| NonZeroUsize::new(10000).unwrap());
        
        for sc in &config_reader.sources { 
            match find_mount_point(&sc.path) {
                Ok((mount_path, dev_id, _is_loopback)) => {
                    info!("Source: {:?} (DevID: 0x{:08x})", sc.path, dev_id);
                    sources.insert(dev_id, Arc::new(SourceInfo { 
                        path: sc.path.clone(), 
                        mount: mount_path, 
                        dev: dev_id,
                        hydration: Arc::new(crate::hydration::HydrationState::default()),
                        inode_map: Arc::new(Mutex::new(LruCache::new(cache_size))),
                        lru_size: cache_size.get(),
                    }));
                },
                Err(e) => error!("Failed to determine mount point for {:?}: {}", sc.path, e),
            }
        }
        
        drop(config_reader);
        Self { 
            config: cfg, 
            sources, 
            watchers: Vec::new(),
            hydration_handles: Arc::new(Mutex::new(Vec::new())), 
            queues: HashMap::new(),
            governor,
            tuner_board: Arc::new(DashMap::new()),
        }
    }

    pub async fn start(&mut self) -> (HashMap<u32, Vec<Arc<EventQueue>>>, Vec<tokio::task::JoinHandle<Result<()>>>, Vec<tokio::sync::mpsc::Sender<()>>, HydrationRx) {
        let mut queues: HashMap<u32, Vec<Arc<EventQueue>>> = HashMap::new();
        let mut handles = Vec::new();
        let mut shutdowns = Vec::new();
        let (hydration_tx, hydration_rx) = mpsc::channel(32); 

        let config_reader = self.config.read().await; 
        let global_queue_max = config_reader.queue_max; 

        for (dev, src) in &self.sources {
            if let Some(source_cfg) = config_reader.sources.iter().find(|s| s.path == src.path) {
                let mut hydration_targets: Vec<(TargetConfig, Arc<EventQueue>, Arc<EventQueue>)> = Vec::new();

                for tgt_cfg in &source_cfg.targets {
                    let target_workers = tgt_cfg.worker_count; 
                    let (high_tx_raw, high_rx_raw) = crate::event::create_fanout(global_queue_max, target_workers);
                    let high_queue_arc = Arc::new(high_tx_raw);
                    let (low_tx_raw, low_rx_raw) = crate::event::create_fanout(global_queue_max, target_workers);
                    let low_queue_arc = Arc::new(low_tx_raw);

                    queues.entry(*dev).or_default().push(high_queue_arc.clone());
                    
                    self.tuner_board.insert(tgt_cfg.path.clone(), TunerState::Startup);

                    let mut high_rxs = high_rx_raw.into_iter();
                    let mut low_rxs = low_rx_raw.into_iter();

                    while let (Some(rx_h), Some(rx_l)) = (high_rxs.next(), low_rxs.next()) {
                        let s = src.clone();
                        let t = tgt_cfg.clone();
                        let c = self.config.clone(); 
                        let h_tx = hydration_tx.clone();
                        let (sd_tx, sd_rx) = tokio::sync::mpsc::channel(1);
                        shutdowns.push(sd_tx);
                        
                        handles.push(tokio::spawn(worker::run_worker(
                            s, t, rx_h, rx_l, sd_rx, h_tx, c, 
                            self.governor.clone(), self.tuner_board.clone()
                        ))); 
                    }
                    
                    if tgt_cfg.initial_sync {
                        hydration_targets.push((tgt_cfg.clone(), high_queue_arc.clone(), low_queue_arc.clone()));
                    }
                }

                if !hydration_targets.is_empty() {
                    let hydrator = Arc::new(Hydrator::new(
                        src.clone(), 
                        hydration_targets, 
                        self.governor.clone(), 
                        self.tuner_board.clone()
                    ));

                    if let Ok(watcher) = hydrator.clone().start_watcher() {
                        self.watchers.push(watcher);
                    }

                    let h_clone = hydrator.clone();
                    let thread_handle = std::thread::spawn(move || {
                        h_clone.full_scan();
                        Ok(())
                    });
                    self.hydration_handles.lock().push(thread_handle);
                }
            }
        }
        
        self.queues = queues.clone();
        (queues, handles, shutdowns, hydration_rx)
    }

    pub fn wait_hydration(&self) {
        let mut handles = self.hydration_handles.lock();
        for h in handles.drain(..) {
            let _ = h.join();
        }
    }
}
