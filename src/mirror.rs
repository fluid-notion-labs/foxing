use std::{sync::{Arc}, collections::HashMap, path::PathBuf, fs};
use crate::{config::{Config, TargetConfig}, event::{EventQueue}, worker::{self, TunerBoard, TunerState}, identity, Result, governor::Governor};
use crate::hydration::Hydrator;
use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::{RwLock, mpsc};
use std::os::unix::fs::{MetadataExt};
use tracing::{info, error, debug, warn}; 
use dashmap::DashMap;

pub type SharedConfig = Arc<RwLock<Config>>;
pub type HydrationTx = mpsc::Sender<PathBuf>;
pub type HydrationRx = mpsc::Receiver<PathBuf>;

pub struct SourceInfo { 
    pub path: PathBuf, 
    pub mount: PathBuf, 
    pub dev: u32, 
    pub dev_ids: Vec<u32>, 
    pub hydration: Arc<crate::hydration::HydrationState>,
    pub inode_map: identity::InodeMap,
    pub lru_size: usize, 
}

pub struct Manager {
    config: SharedConfig, 
    sources: HashMap<u32, Arc<SourceInfo>>, 
    watchers: Vec<notify::RecommendedWatcher>,
    hydration_handles: Arc<Mutex<Vec<std::thread::JoinHandle<Result<()>>>>>,
    // NEW: Store hydrators to trigger them later
    hydrators: Vec<Arc<Hydrator>>,
    pub queues: HashMap<u32, Vec<Arc<EventQueue>>>,
    pub governor: Arc<Governor>,
    pub tuner_board: TunerBoard,
}

/// Defense-in-Depth Device ID Resolution.
fn resolve_all_device_ids(path: &PathBuf) -> Result<(PathBuf, Vec<u32>)> {
    let canonical = path.canonicalize().map_err(|e| crate::error::FoxingError::Io(e))?;
    let mut ids = Vec::new();
    let mut mount_point = canonical.clone();

    debug!("Resolving device IDs for path: {:?}", canonical);

    if let Ok(meta) = fs::metadata(&canonical) {
        let rdev = meta.dev();
        let maj = ((rdev >> 8) & 0xfff) as u32;
        let min = ((rdev & 0xff) | ((rdev >> 12) & 0xfff00)) as u32;
        let stat_id = (maj << 20) | min;
        ids.push(stat_id);
    }

    if let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") {
        let mut best_len = 0;
        for line in mountinfo.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 5 { continue; }
            let mount_root = parts[4]; 
            if canonical.to_string_lossy().starts_with(mount_root) {
                 if mount_root.len() >= best_len {
                     best_len = mount_root.len();
                     mount_point = PathBuf::from(mount_root);
                     let maj_min = parts[2];
                     let dev_parts: Vec<&str> = maj_min.split(':').collect();
                     if dev_parts.len() == 2 {
                         let major: u32 = dev_parts[0].parse().unwrap_or(0);
                         let minor: u32 = dev_parts[1].parse().unwrap_or(0);
                         let kernel_id = (major << 20) | minor;
                         if !ids.contains(&kernel_id) { ids.push(kernel_id); }
                     }
                 }
            }
        }
    }
    Ok((mount_point, ids))
}

impl Manager {
    pub async fn new(cfg: SharedConfig) -> Self { 
        let _cloned_cfg = cfg.clone(); 
        let config_reader = cfg.read().await;
        let governor = Arc::new(Governor::new(
            config_reader.max_system_load_avg,
            config_reader.hydration_delay_ms
        ));

        let mut target_ids_to_exclude = Vec::new();
        for sc in &config_reader.sources {
            for tc in &sc.targets {
                match resolve_all_device_ids(&tc.path) {
                    Ok((_, ids)) => target_ids_to_exclude.extend(ids),
                    Err(e) => warn!("Failed to resolve target device ID for exclusion {:?}: {}", tc.path, e),
                }
            }
        }

        let mut sources = HashMap::new();
        let cache_size_raw = (config_reader.global_buffer_limit / 5) as usize; 
        let cache_size = NonZeroUsize::new(cache_size_raw.max(10000)).unwrap_or_else(|| NonZeroUsize::new(10000).unwrap());
        
        for sc in &config_reader.sources { 
            match resolve_all_device_ids(&sc.path) {
                Ok((mount_path, mut dev_ids)) => {
                    if dev_ids.is_empty() { continue; }
                    dev_ids.retain(|id| !target_ids_to_exclude.contains(id));
                    if dev_ids.is_empty() {
                        error!("Source {:?} has NO device IDs left after excluding Targets!", sc.path);
                        continue;
                    }
                    let primary_dev = dev_ids[0]; 
                    info!("Source: {:?} (Mount: {:?})", sc.path, mount_path);
                    sources.insert(primary_dev, Arc::new(SourceInfo { 
                        path: sc.path.clone(), 
                        mount: mount_path, 
                        dev: primary_dev, 
                        dev_ids: dev_ids.clone(), 
                        hydration: Arc::new(crate::hydration::HydrationState::default()),
                        inode_map: Arc::new(Mutex::new(LruCache::new(cache_size))),
                        lru_size: cache_size.get(),
                    }));
                },
                Err(e) => error!("Failed to resolve device IDs for {:?}: {}", sc.path, e),
            }
        }
        
        drop(config_reader);
        Self { 
            config: cfg, 
            sources, 
            watchers: Vec::new(),
            hydration_handles: Arc::new(Mutex::new(Vec::new())), 
            queues: HashMap::new(),
            hydrators: Vec::new(),
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

        for (_primary_dev, src) in &self.sources {
            if let Some(source_cfg) = config_reader.sources.iter().find(|s| s.path == src.path) {
                let mut hydration_targets: Vec<(TargetConfig, Arc<EventQueue>, Arc<EventQueue>)> = Vec::new();

                for tgt_cfg in &source_cfg.targets {
                    let target_workers = tgt_cfg.worker_count; 
                    let (high_tx_raw, high_rx_raw) = crate::event::create_fanout(global_queue_max, target_workers);
                    let high_queue_arc = Arc::new(high_tx_raw);
                    let (low_tx_raw, low_rx_raw) = crate::event::create_fanout(global_queue_max, target_workers);
                    let low_queue_arc = Arc::new(low_tx_raw);

                    for alt_dev_id in &src.dev_ids {
                        queues.entry(*alt_dev_id).or_default().push(high_queue_arc.clone());
                    }
                    
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

                    // Save hydrator for manual triggering
                    self.hydrators.push(hydrator.clone());

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

    // UPDATED: Accepts a path to distinguish Full Scan vs Targeted Repair
    pub fn trigger_hydration(&self, target_path: PathBuf) {
        for h in &self.hydrators {
            // Check if the requested path belongs to this hydrator's source root
            if target_path.starts_with(&h.source.path) {
                let h_clone = h.clone();
                
                // If the request IS the root, do a full scan.
                if target_path == h.source.path {
                    let thread_handle = std::thread::spawn(move || {
                        h_clone.full_scan();
                        Ok(())
                    });
                    self.hydration_handles.lock().push(thread_handle);
                } else {
                    // Otherwise, do a targeted repair
                    // FIX: Clone the path so we don't move the shared reference variable
                    let repair_path = target_path.clone();
                    let thread_handle = std::thread::spawn(move || {
                        h_clone.repair_path(repair_path);
                        Ok(())
                    });
                    self.hydration_handles.lock().push(thread_handle);
                }
            }
        }
    }

    pub fn wait_hydration(&self) {
        let mut handles = self.hydration_handles.lock();
        for h in handles.drain(..) {
            let _ = h.join();
        }
    }
}
