use std::{sync::{Arc}, collections::HashMap, path::PathBuf, fs};
use crate::{config::{Config, TargetConfig}, event::{EventQueue}, worker::{self, TunerBoard, TunerState}, identity, Result, governor::Governor};
use crate::hydration::{Hydrator};
use crate::hydration_worker::{HydrationQueue};
use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::{RwLock, mpsc};
use std::os::unix::fs::{MetadataExt};
use tracing::{info, error, debug, warn};
use dashmap::DashMap;
use std::time::{Instant, Duration, SystemTime, UNIX_EPOCH};
use std::ops::Sub;
use crate::error::FoxingError;

pub type SharedConfig = Arc<RwLock<Config>>;
pub type HydrationTx = mpsc::Sender<PathBuf>;
pub type HydrationRx = mpsc::Receiver<PathBuf>;

pub const REPAIR_DEBOUNCE_MS: u64 = 5000;
const MIN_FULL_SCAN_DEBOUNCE_SECS: u64 = 1;

fn calculate_full_scan_debounce(tuner_board: &TunerBoard) -> Duration {
    let mut max_delay = Duration::from_secs(MIN_FULL_SCAN_DEBOUNCE_SECS);
    for r in tuner_board.iter() {
        let current_state = *r.value();
        let delay = match current_state {
            TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => Duration::from_secs(45),
            TunerState::Drain => Duration::from_secs(15),
            _ => Duration::from_secs(MIN_FULL_SCAN_DEBOUNCE_SECS),
        };
        if delay > max_delay {
            max_delay = delay;
        }
    }
    max_delay
}

#[derive(Debug)]
pub struct SourceInfo {
    pub path: PathBuf,
    pub mount: PathBuf,
    pub dev: u32,
    pub dev_ids: Vec<u32>,
    pub hydration: Arc<crate::hydration::HydrationState>,
    pub inode_map: identity::InodeMap,
    pub lru_size: usize,
    pub bulk_job_queue: Mutex<Option<HydrationQueue>>,
    // Updated to RwLock for interior mutability during initialization
    pub queues: RwLock<HashMap<u32, Vec<Arc<EventQueue>>>>,
}

pub struct Manager {
    config: SharedConfig,
    sources: HashMap<u32, Arc<SourceInfo>>,
    watchers: Vec<notify::RecommendedWatcher>,
    hydration_handles: Arc<Mutex<Vec<std::thread::JoinHandle<Result<()>>>>>,
    hydrators: Vec<Arc<Hydrator>>,
    pub governor: Arc<Governor>,
    pub tuner_board: TunerBoard,
    repair_tracker: Arc<DashMap<PathBuf, Instant>>,
    bulk_hydration_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
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

        let mut sources: HashMap<u32, Arc<SourceInfo>> = HashMap::new();
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
                        bulk_job_queue: Mutex::new(None),
                        queues: RwLock::new(HashMap::new()),
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
            hydrators: Vec::new(),
            governor,
            tuner_board: Arc::new(DashMap::new()),
            repair_tracker: Arc::new(DashMap::new()),
            bulk_hydration_handles: Vec::new(),
        }
    }

    pub async fn start(&mut self) -> (HashMap<u32, Vec<Arc<EventQueue>>>, Vec<tokio::task::JoinHandle<Result<()>>>, Vec<tokio::sync::mpsc::Sender<()>>, HydrationRx) {
        let mut all_queues_map: HashMap<u32, Vec<Arc<EventQueue>>> = HashMap::new();
        let mut handles = Vec::new();
        let mut shutdowns = Vec::new();
        let (hydration_tx, hydration_rx_moved) = mpsc::channel(32);
        let (_, hydration_rx_dummy) = mpsc::channel(1);
        
        let config_reader = self.config.read().await;
        let global_queue_max = config_reader.queue_max;

        for (_primary_dev, src) in self.sources.iter_mut() {
            let mut queues_for_source = HashMap::new();
            if let Some(source_cfg) = config_reader.sources.iter().find(|s| s.path == src.path) {
                let mut hydration_targets: Vec<TargetConfig> = Vec::new();
                
                for tgt_cfg in &source_cfg.targets {
                    let target_workers = tgt_cfg.worker_count;
                    let (high_tx_raw, high_rx_raw) = crate::event::create_fanout(global_queue_max, target_workers);
                    let high_queue_arc = Arc::new(high_tx_raw);
                    
                    let (low_tx_raw, low_rx_raw) = crate::event::create_fanout(global_queue_max, target_workers);
                    let low_queue_arc = Arc::new(low_tx_raw);

                    for alt_dev_id in &src.dev_ids {
                        queues_for_source.entry(*alt_dev_id).or_insert_with(Vec::new).push(high_queue_arc.clone());
                        queues_for_source.entry(*alt_dev_id).or_insert_with(Vec::new).push(low_queue_arc.clone());
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
                        hydration_targets.push(tgt_cfg.clone());
                    }
                }
                
                // Write the queues into the SourceInfo Arc via RwLock
                {
                    let mut q_write = src.queues.write().await;
                    *q_write = queues_for_source;
                }

                let bulk_worker_count = config_reader.worker_count.min(4);
                let (queue, bulk_handles) = HydrationQueue::new(
                    src.clone(), 
                    self.config.clone(),
                    self.governor.clone(),
                    self.tuner_board.clone(),
                    bulk_worker_count,
                );
                
                *src.bulk_job_queue.lock() = Some(queue);
                self.bulk_hydration_handles.extend(bulk_handles);

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

        // Collect all queues for BPF initialization (Read Lock needed temporarily)
        for (dev_id, src) in self.sources.iter() {
            let q_read = src.queues.read().await;
            all_queues_map.insert(*dev_id, q_read.get(dev_id).cloned().unwrap_or_default());
        }

        let hydrators_arc = Arc::new(self.hydrators.clone());
        let tuner_board_clone = self.tuner_board.clone();
        let repair_tracker_clone = self.repair_tracker.clone();

        let debounce_handle = tokio::spawn(async move {
            let mut last_full_scan = Instant::now().sub(Duration::from_secs(MIN_FULL_SCAN_DEBOUNCE_SECS));
            let mut hydration_rx = hydration_rx_moved;
            
            while let Some(path) = hydration_rx.recv().await {
                let required_debounce = calculate_full_scan_debounce(&tuner_board_clone);
                
                if path.parent().is_none() || path.ends_with(path.file_name().unwrap_or_default()) {
                    let now = Instant::now();
                    if now.duration_since(last_full_scan) > required_debounce {
                        warn!("Hydration MANAGER: Triggering FULL scan. Required debounce: {:?}. Elapsed: {:?}.", 
                              required_debounce, now.duration_since(last_full_scan));
                        last_full_scan = now;
                        
                        if let Some(hydrator) = hydrators_arc.iter().find(|h| path.starts_with(&h.source.path)) {
                            let h_clone = hydrator.clone();
                            let thread_handle = std::thread::spawn(move || {
                                h_clone.full_scan();
                                Ok(())
                            });
                            match thread_handle.join() {
                                Ok(Ok(())) => {},
                                Ok(Err(e)) => {
                                    error!("Hydration scan thread returned error: {:?}", e);
                                    return Err(e);
                                }
                                Err(e) => {
                                    error!("Hydration scan thread panic: {:?}", e);
                                    return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Hydration thread panic")));
                                }
                            }
                        }
                    } else {
                        debug!("Hydration MANAGER: Full scan debounced. Next attempt in {}ms.", 
                                (required_debounce - now.duration_since(last_full_scan)).as_millis());
                    }
                } else {
                    let now = Instant::now();
                    let repaired_path = path.clone();
                    
                    repair_tracker_clone.retain(|_, time| now.duration_since(*time) < Duration::from_millis(REPAIR_DEBOUNCE_MS));
                    
                    if repair_tracker_clone.get(&repaired_path).is_some() {
                        debug!("Hydration MANAGER: Skipping redundant repair request for {:?}", repaired_path);
                        continue;
                    }
                    
                    repair_tracker_clone.insert(repaired_path.clone(), now);
                    
                    if let Some(hydrator) = hydrators_arc.iter().find(|h| repaired_path.starts_with(&h.source.path)) {
                        let h_clone = hydrator.clone();
                        let thread_handle = std::thread::spawn(move || {
                            h_clone.repair_path(repaired_path);
                            Ok(())
                        });
                        match thread_handle.join() {
                            Ok(Ok(())) => {},
                            Ok(Err(e)) => {
                                error!("Hydration repair thread returned error: {:?}", e);
                                return Err(e);
                            }
                            Err(e) => {
                                error!("Hydration repair thread panic: {:?}", e);
                                return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Hydration thread panic")));
                            }
                        }
                    }
                }
            }
            Ok(())
        });
        
        handles.push(debounce_handle);
        (all_queues_map, handles, shutdowns, hydration_rx_dummy)
    }

    pub fn trigger_hydration(&self, target_path: PathBuf) {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos();
        if nanos % 25 == 0 {
             let now = Instant::now();
             self.repair_tracker.retain(|_, time| now.duration_since(*time) < Duration::from_millis(REPAIR_DEBOUNCE_MS));
        }
        
        for h in &self.hydrators {
            if target_path.starts_with(&h.source.path) {
                let h_clone = h.clone();
                if target_path == h.source.path {
                    let thread_handle = std::thread::spawn(move || {
                        h_clone.full_scan();
                        Ok(())
                    });
                    self.hydration_handles.lock().push(thread_handle);
                } else {
                    let now = Instant::now();
                    if let Some(last_repair) = self.repair_tracker.get(&target_path) {
                         if now.duration_since(*last_repair) < Duration::from_millis(REPAIR_DEBOUNCE_MS) {
                             debug!("Skipping redundant repair request for {:?}", target_path);
                             return;
                         }
                    }
                    self.repair_tracker.insert(target_path.clone(), now);
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

    pub fn wait_hydration(&mut self) {
        let mut handles = self.hydration_handles.lock();
        for h in handles.drain(..) {
            let _ = h.join();
        }
        
        // Use drain(..) to consume handles since JoinHandle is not Clone
        for h in self.bulk_hydration_handles.drain(..) {
            // Abort background tasks immediately on shutdown
            h.abort(); 
        }
    }
}

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
