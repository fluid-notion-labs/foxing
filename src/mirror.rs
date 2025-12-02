use std::{sync::{Arc}, collections::HashMap, path::PathBuf, fs};
use crate::{config::{Config, TargetConfig}, event::{EventQueue, Event}, worker::{self}, identity, Result, governor::Governor};
use crate::hydration::{Hydrator};
use crate::hydration_worker::{HydrationQueue};
use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::{RwLock, mpsc};
use std::os::unix::fs::{MetadataExt};
use tracing::{info, error, warn};
use dashmap::{DashMap, DashSet};
use std::time::{Instant, Duration, SystemTime, UNIX_EPOCH};
use std::ops::Sub;
use crate::error::FoxingError;
use crate::tuner::{TunerBoard, TunerState};


pub type SharedConfig = Arc<RwLock<Config>>;
pub type HydrationTx = mpsc::Sender<PathBuf>;
pub type HydrationRx = mpsc::Receiver<PathBuf>;

struct RepairGuard {
    path: PathBuf,
    set: Arc<DashSet<PathBuf>>,
}

impl Drop for RepairGuard {
    fn drop(&mut self) {
        self.set.remove(&self.path);
    }
}

fn calculate_adaptive_debounces(tuner_board: &TunerBoard) -> (Duration, Duration) {
    let mut max_stress_level = 0;
    for r in tuner_board.iter() {
        let level = match *r.value() {
            TunerState::Startup | TunerState::IdleReset => 0,
            TunerState::ProbeBW => 1,
            TunerState::Drain => 2,
            TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => 3,
        };
        if level > max_stress_level { max_stress_level = level; }
    }
    match max_stress_level {
        0 => (Duration::from_millis(50), Duration::from_millis(500)),
        1 => (Duration::from_millis(200), Duration::from_secs(2)),
        2 => (Duration::from_millis(1000), Duration::from_secs(5)),
        _ => (Duration::from_secs(2), Duration::from_secs(10)),
    }
}

fn calculate_adaptive_registry_limit(tuner_board: &TunerBoard) -> usize {
    let mut max_stress_level = 0;
    for r in tuner_board.iter() {
        let level = match *r.value() {
            TunerState::Startup | TunerState::IdleReset => 0,
            TunerState::ProbeBW => 1,
            TunerState::Drain => 2,
            TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => 3,
        };
        if level > max_stress_level { max_stress_level = level; }
    }
    match max_stress_level {
        0 => 256,
        1 => 512,
        2 => 1024,
        _ => 128,
    }
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
    pub queues: RwLock<HashMap<u32, Vec<Arc<EventQueue>>>>,
    pub active_repairs: Arc<DashSet<PathBuf>>,
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
        
        let cache_size_raw = (config_reader.global_buffer_limit / 5) as usize;
        let cache_size = NonZeroUsize::new(cache_size_raw.max(10000)).unwrap_or_else(|| NonZeroUsize::new(10000).unwrap());

        let mut sources: HashMap<u32, Arc<SourceInfo>> = HashMap::new();
        let mut target_ids_to_exclude = Vec::new(); 
        
        for sc in &config_reader.sources {
            for tc in &sc.targets {
                if let Ok(meta) = fs::metadata(&tc.path) {
                    let rdev = meta.dev();
                    let maj = ((rdev >> 8) & 0xfff) as u32;
                    let min = ((rdev & 0xff) | ((rdev >> 12) & 0xfff00)) as u32;
                    let stat_id = (maj << 20) | min;
                    target_ids_to_exclude.push(stat_id);
                }
            }
        }

        for sc in &config_reader.sources {
            match resolve_all_device_ids(&sc.path) {
                Ok((mount_path, mut dev_ids)) => {
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
                        active_repairs: Arc::new(DashSet::new()),
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
                let mut hydration_repair_txs: Vec<mpsc::Sender<Arc<Event>>> = Vec::new();

                for tgt_cfg in &source_cfg.targets {
                    let target_workers = tgt_cfg.worker_count.max(2);
                    
                    let (fanout_tx, fanout_rxs_vec) = crate::event::create_fanout(global_queue_max, target_workers);
                    let fanout_queue_arc = Arc::new(fanout_tx);
                    
                    let (repair_tx_raw, repair_rx_raw) = crate::event::create_fanout(1000, 1); 
                    if let Some(tx) = repair_tx_raw.senders.first() {
                        hydration_repair_txs.push(tx.clone());
                    }
                    
                    let primary_repair_rx = repair_rx_raw.into_iter().next();
                    let mut repair_rx_option = Some(primary_repair_rx.unwrap());

                    for alt_dev_id in &src.dev_ids {
                        queues_for_source.entry(*alt_dev_id).or_insert_with(Vec::new).push(fanout_queue_arc.clone());
                    }
                    
                    self.tuner_board.insert(tgt_cfg.path.clone(), TunerState::Startup);

                    let num_workers = fanout_rxs_vec.len();
                    let mut fanout_rxs_iter = fanout_rxs_vec.into_iter(); 
                    for worker_id in 0..num_workers {
                        let rx = fanout_rxs_iter.next().unwrap();
                        let s = src.clone();
                        let t = tgt_cfg.clone();
                        let c = self.config.clone();
                        let h_tx = hydration_tx.clone();
                        let (sd_tx, sd_rx) = tokio::sync::mpsc::channel(1);
                        shutdowns.push(sd_tx);
                        
                        let repair_channel = if worker_id == 0 {
                            repair_rx_option.take()
                        } else {
                            None
                        };

                        handles.push(tokio::spawn(worker::run_worker(
                            s, t, rx, 
                            sd_rx, h_tx, c, 
                            self.governor.clone(), self.tuner_board.clone(),
                            worker_id, 
                            repair_channel
                        )));
                    }
                    
                    if tgt_cfg.initial_sync {
                        hydration_targets.push(tgt_cfg.clone());
                    }
                }
                
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
                        self.tuner_board.clone(),
                        hydration_repair_txs, 
                    ));
                    
                    if let Ok(watcher) = hydrator.clone().start_watcher() {
                        self.watchers.push(watcher);
                    }
                    
                    self.hydrators.push(hydrator.clone());
                    
                    let h_clone = hydrator.clone();
                    let thread_handle = std::thread::spawn(move || {
                        h_clone.full_scan();
                        Ok::<(), FoxingError>(())
                    });
                    self.hydration_handles.lock().push(thread_handle);
                }
            }
        }

        let hydrators_arc = Arc::new(self.hydrators.clone());
        let tuner_board_clone = self.tuner_board.clone();
        let repair_tracker_clone = self.repair_tracker.clone();

        let debounce_handle = tokio::spawn(async move {
            let mut last_full_scan = Instant::now().sub(Duration::from_secs(60));
            let mut hydration_rx = hydration_rx_moved;
            
            while let Some(path) = hydration_rx.recv().await {
                let (repair_debounce, full_scan_debounce) = calculate_adaptive_debounces(&tuner_board_clone);
                
                if path.parent().is_none() || path.ends_with(path.file_name().unwrap_or_default()) {
                    let now = Instant::now();
                    if now.duration_since(last_full_scan) > full_scan_debounce {
                        warn!("Hydration MANAGER: Triggering FULL scan. Required debounce: {:?}. Elapsed: {:?}.", full_scan_debounce, now.duration_since(last_full_scan));
                        last_full_scan = now;
                        
                        if let Some(hydrator) = hydrators_arc.iter().find(|h| path.starts_with(&h.source.path)) {
                            let h_clone = hydrator.clone();
                            let thread_handle = std::thread::spawn(move || {
                                h_clone.full_scan();
                                Ok::<(), FoxingError>(())
                            });
                            match thread_handle.join() {
                                Ok(Ok(())) => {},
                                Ok(Err(_e)) => error!("Hydration scan thread returned error"),
                                Err(_e) => error!("Hydration scan thread panic"),
                            }
                        }
                    }
                } else {
                    if let Some(hydrator) = hydrators_arc.iter().find(|h| path.starts_with(&h.source.path)) {
                        if hydrator.source.active_repairs.contains(&path) { continue; }
                        let registry_limit = calculate_adaptive_registry_limit(&tuner_board_clone);
                        if hydrator.source.active_repairs.len() >= registry_limit { continue; }

                        let now = Instant::now();
                        let repaired_path = path.clone();
                        
                        repair_tracker_clone.retain(|_, time| now.duration_since(*time) < repair_debounce);
                        
                        if repair_tracker_clone.get(&repaired_path).is_some() { continue; }
                        repair_tracker_clone.insert(repaired_path.clone(), now);
                        
                        hydrator.source.active_repairs.insert(repaired_path.clone());
                        
                        let set_clone = hydrator.source.active_repairs.clone();
                        let path_for_guard = repaired_path.clone();
                        let path_for_repair = repaired_path.clone();

                        let h_clone = hydrator.clone();
                        let thread_handle = std::thread::spawn(move || {
                            let _guard = RepairGuard { path: path_for_guard, set: set_clone };
                            h_clone.repair_path(path_for_repair);
                            Ok::<(), FoxingError>(())
                        });
                        
                        match thread_handle.join() {
                            Ok(Ok(())) => {},
                            Ok(Err(e)) => error!("Hydration repair error: {:?}", e),
                            Err(_e) => error!("Hydration repair panic"),
                        }
                    }
                }
            }
            Ok(())
        });
        
        handles.push(debounce_handle);
        (all_queues_map, handles, shutdowns, hydration_rx_dummy)
    }

    pub fn wait_hydration(&mut self) {
        let mut handles = self.hydration_handles.lock();
        for h in handles.drain(..) {
            let _ = h.join();
        }
        for h in self.bulk_hydration_handles.drain(..) {
            h.abort(); 
        }
    }
}

fn resolve_all_device_ids(path: &PathBuf) -> Result<(PathBuf, Vec<u32>)> {
    let canonical = path.canonicalize().map_err(|e| crate::error::FoxingError::Io(e))?;
    let mut ids = Vec::new();
    if let Ok(meta) = fs::metadata(&canonical) {
        let rdev = meta.dev();
        let maj = ((rdev >> 8) & 0xfff) as u32;
        let min = ((rdev & 0xff) | ((rdev >> 12) & 0xfff00)) as u32;
        let stat_id = (maj << 20) | min;
        ids.push(stat_id);
    }
    
    let mount_point = canonical.clone();
    
    if ids.is_empty() {
        return Err(crate::error::FoxingError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "Could not determine device ID for path.")));
    }

    Ok((mount_point, ids))
}
