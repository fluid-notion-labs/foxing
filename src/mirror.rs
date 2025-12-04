use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};
use std::sync::Arc;
use crate::event::EventQueue;
use crate::config::Config;
use crate::worker;
use crate::hydration::Hydrator;
use crate::governor::Governor;
use crate::error::Result;
use crate::tuner::{TunerState, TunerBoard};
use std::time::{Duration, Instant};
use tracing::{info, warn, error};
use std::ops::Sub;
use crate::error::FoxingError;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use crate::consistency::SerializationEngine;
use dashmap::{DashMap, DashSet};
use std::fs;
use crate::identity;
use crate::hydration_worker::HydrationQueue;
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
    let max_stress_level = tuner_board.iter().map(|r| {
        match *r.value() {
            TunerState::Startup | TunerState::IdleReset => 0,
            TunerState::ProbeBW => 1,
            TunerState::Drain => 2,
            TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => 3,
            _ => 0,
        }
    }).max().unwrap_or(0);
    match max_stress_level {
        0 => (Duration::from_millis(50), Duration::from_millis(500)),
        1 => (Duration::from_millis(200), Duration::from_secs(2)),
        2 => (Duration::from_millis(1000), Duration::from_secs(5)),
        _ => (Duration::from_secs(2), Duration::from_secs(10)),
    }
}
fn _calculate_adaptive_registry_limit(tuner_board: &TunerBoard) -> usize {
    for r in tuner_board.iter() {
        if matches!(*r.value(), TunerState::CriticalDrain) { return 10; }
    }
    1000
}
#[derive(Debug)]
pub struct SourceInfo {
    pub path: PathBuf,
    pub mount: PathBuf,
    pub dev: u32,
    pub dev_ids: Vec<u32>,
    pub hydration: Arc<crate::hydration::HydrationState>,
    pub inode_map: identity::InodeMap,
    pub dir_map: identity::DirMap, // NEW: Directory-only inode map
    pub lru_size: usize,
    pub bulk_job_queue: Mutex<Option<HydrationQueue>>,
    pub queues: RwLock<HashMap<u32, Vec<Arc<EventQueue>>>>,
    pub active_repairs: Arc<DashSet<PathBuf>>,
    pub rwf_uncached_ok: Arc<AtomicBool>,
}
pub struct Manager {
    pub config: SharedConfig,
    pub sources: HashMap<u32, Arc<SourceInfo>>,
    watchers: Vec<notify::RecommendedWatcher>,
    hydration_handles: Arc<Mutex<Vec<std::thread::JoinHandle<Result<()>>>>>,
    hydrators: Vec<Arc<Hydrator>>,
    pub governor: Arc<Governor>,
    pub tuner_board: TunerBoard,
    repair_tracker: Arc<DashMap<PathBuf, Instant>>,
    bulk_hydration_handles: Vec<tokio::task::JoinHandle<Result<()>>>,
}
fn resolve_all_device_ids(path: &PathBuf) -> Result<(PathBuf, Vec<u32>)> {
    let canonical = path.canonicalize().map_err(|e| crate::error::FoxingError::Io(e))?;
    let mut ids = Vec::new();
    if let Ok(meta) = fs::metadata(&canonical) {
        use std::os::unix::fs::MetadataExt;
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
impl Manager {
    pub async fn new(cfg: SharedConfig) -> Self {
        let config_reader = cfg.read().await;
        let governor = Arc::new(Governor::new(
            config_reader.max_system_load_avg,
            config_reader.hydration_delay_ms,
            config_reader.governor_psi_io_threshold,
            config_reader.governor_psi_cpu_threshold,
        ));
        let cache_size_raw = (config_reader.global_buffer_limit / 5) as usize;
        let cache_size = NonZeroUsize::new(cache_size_raw.max(10000)).unwrap_or_else(|| NonZeroUsize::new(10000).unwrap());
        let dir_cache_size = NonZeroUsize::new(cache_size.get().min(10000)).unwrap_or_else(|| NonZeroUsize::new(1000).unwrap());
        let mut sources: HashMap<u32, Arc<SourceInfo>> = HashMap::new();
        let mut target_ids_to_exclude = Vec::new();
        for sc in &config_reader.sources {
            for tc in &sc.targets {
                if let Ok(meta) = fs::metadata(&tc.path) {
                    use std::os::unix::fs::MetadataExt;
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
                        dir_map: Arc::new(Mutex::new(LruCache::new(dir_cache_size))), // Initialize DirMap
                        lru_size: cache_size.get(),
                        bulk_job_queue: Mutex::new(None),
                        queues: RwLock::new(HashMap::new()),
                        active_repairs: Arc::new(DashSet::new()),
                        rwf_uncached_ok: sc.rwf_uncached_ok.clone(),
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
        for (_primary_dev, src) in self.sources.iter_mut() {
            if let Some(source_cfg) = config_reader.sources.iter().find(|s| s.path == src.path) {
                let mut hydration_targets = Vec::new();
                let mut hydration_repair_txs = Vec::new();
                let mut queues_for_source: HashMap<u32, Vec<Arc<EventQueue>>> = HashMap::new();
                for tgt_cfg in &source_cfg.targets {
                    let serialization_engine = SerializationEngine::new();
                    let target_workers = tgt_cfg.worker_count.max(2);
                    let (fanout_tx, fanout_rxs_vec) = crate::event::create_fanout(config_reader.queue_max, target_workers);
                    let fanout_queue_arc = Arc::new(fanout_tx);
                    let (repair_tx_raw, repair_rx_raw) = crate::event::create_fanout(10_000, 1);
                    if let Some(tx) = repair_tx_raw.senders.first() {
                        hydration_repair_txs.push(tx.clone());
                    }
                    let mut repair_rx_option = Some(repair_rx_raw.into_iter().next().unwrap());
                    for alt_dev_id in &src.dev_ids {
                        queues_for_source.entry(*alt_dev_id).or_insert_with(Vec::new).push(fanout_queue_arc.clone());
                    }
                    self.tuner_board.insert(tgt_cfg.path.clone(), TunerState::Startup);
                    for (i, rx) in fanout_rxs_vec.into_iter().enumerate() {
                        let (sd_tx, sd_rx) = mpsc::channel(1);
                        shutdowns.push(sd_tx);
                        let repair_channel = if i == 0 { repair_rx_option.take() } else { None };
                        handles.push(tokio::spawn(worker::run_worker(
                            rx,
                            src.clone(),
                            tgt_cfg.clone(),
                            self.config.clone(),
                            sd_rx,
                            hydration_tx.clone(),
                            Arc::new(hydration_repair_txs.clone()),
                            self.governor.clone(),
                            self.tuner_board.clone(),
                            i,
                            repair_channel,
                            serialization_engine.clone(),
                        )));
                    }
                    if tgt_cfg.initial_sync {
                        hydration_targets.push(tgt_cfg.clone());
                    }
                }
                let mut q_write = src.queues.write().await;
                *q_write = queues_for_source.clone();
                drop(q_write);
                for (k, v) in queues_for_source {
                    all_queues_map.entry(k).or_insert_with(Vec::new).extend(v);
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
        let _repair_tracker_clone = self.repair_tracker.clone();
        let source_root_canonical = fs::canonicalize(
            self.sources.values().next().map(|s| s.path.as_path()).unwrap_or(Path::new("/"))
        ).unwrap_or_else(|_| PathBuf::from("/"));
        let debounce_handle = tokio::spawn(async move {
            let mut last_full_scan = Instant::now().sub(Duration::from_secs(60));
            let mut hydration_rx = hydration_rx_moved;
            while let Some(path) = hydration_rx.recv().await {
                let is_root_request = path == source_root_canonical;
                let is_targeted_repair = path.exists() && !is_root_request;
                if is_targeted_repair {
                    if let Some(hydrator) = hydrators_arc.iter().find(|h| path.starts_with(&h.source.path)) {
                        if let Some(tgt_cfg) = hydrator.targets.iter().next() {
                            if let Some(queue) = hydrator.source.bulk_job_queue.lock().as_ref() {
                                if let Ok(rel_path) = path.strip_prefix(&hydrator.source.mount) {
                                    info!("Hydration MANAGER: IMMEDIATE repair dispatch for file {:?}", path);
                                    queue.submit_job(rel_path.to_path_buf(), tgt_cfg.clone());
                                }
                            }
                        }
                    }
                    continue;
                }
                if is_root_request {
                    let now = Instant::now();
                    let (_repair_debounce, full_scan_debounce) = calculate_adaptive_debounces(&tuner_board_clone);
                    if now.duration_since(last_full_scan) > full_scan_debounce {
                        warn!("Hydration MANAGER: Triggering FULL scan. Required debounce: {:?}.", full_scan_debounce);
                        last_full_scan = now;
                        for h in hydrators_arc.iter() {
                            let h_clone = h.clone();
                            std::thread::spawn(move || {
                                h_clone.full_scan();
                                Ok::<(), FoxingError>(())
                            });
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
