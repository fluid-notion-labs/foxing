use parking_lot::Mutex;
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock, broadcast};
use std::sync::Arc;
use crate::event::EventQueue;
use crate::config::Config;
use crate::error::{FoxingError, Result};
use std::path::{Path, PathBuf};
use std::time::{Instant, Duration};
use tracing::{info, error, debug};
use crate::tuner::{TunerState, TunerBoard};
use crate::hydration::HydrationQueue;
use crate::hydration_worker::{HydrationState, Hydrator, HydrationMode};
use crate::identity::{self, ShardedInodeMap, ShardedDirMap};
use crate::worker::{HydrationSender, run_worker, BarrierCoordinator};
use dashmap::{DashMap, DashSet};
use std::ops::Sub;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use fxcp_core::governor::Governor;
use fxcp_core::security;
use uuid::Uuid;
use fxcp_core::versioning::VersionIndex;
use crate::identity_watch::ProactiveIndex;
use crate::projector::IdentityProjector;
use std::os::unix::fs::MetadataExt;
use tokio::task::JoinSet;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use rand::Rng;
use notify::RecommendedWatcher;
use std::os::unix::io::AsRawFd;
use fxcp_core::operations::CopyStats;

pub type SharedConfig = Arc<RwLock<Config>>;
pub type HydrationTx = mpsc::UnboundedSender<(PathBuf, Option<u64>)>;
pub type HydrationRx = mpsc::UnboundedReceiver<(PathBuf, Option<u64>)>;

#[derive(Debug)]
pub struct SourceInfo {
    pub path: PathBuf,
    pub mount: PathBuf,
    pub dev: u32,
    pub dev_ids: Vec<u32>,
    pub hydration: Arc<HydrationState>,
    pub inode_map: ShardedInodeMap,
    pub dir_map: ShardedDirMap,
    pub lru_size: usize,
    pub bulk_job_queue: Mutex<Option<HydrationQueue>>,
    pub queues: RwLock<HashMap<u32, Vec<Arc<EventQueue>>>>,
    pub active_repairs: Arc<DashSet<PathBuf>>,
    pub hydrated_inodes: Arc<DashSet<u64>>,
    pub rwf_uncached_ok: Arc<AtomicBool>,
    pub version_index: Arc<VersionIndex>,
    pub identity_index: Option<Arc<ProactiveIndex>>,
    pub projector: Option<Arc<IdentityProjector>>,
    pub lock_map: Arc<DashMap<u64, fs::File>>,
    pub max_inode_map_entries: usize,
    pub cross_subvolumes: bool,
    pub watchers: Mutex<Vec<RecommendedWatcher>>,
    pub fs_root_relative_path: Option<PathBuf>,
}

pub struct Manager {
    pub config: SharedConfig,
    pub sources: HashMap<u32, Arc<SourceInfo>>,
    pub hydrators: Vec<Arc<Hydrator>>,
    pub governor: Arc<Governor>,
    pub tuner_board: TunerBoard,
    pub daemon_id: String,
    pub default_hydration_mode: HydrationMode,
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

fn probe_storage_latency(path: &Path) -> Duration {
    let probe_file = path.join(".foxing_latency_probe");
    let start = Instant::now();
    let mut success_count = 0;
    for _ in 0..3 {
        if let Ok(mut f) = std::fs::File::create(&probe_file) {
            use std::io::Write;
            if f.write_all(b"probe").is_ok() && f.sync_all().is_ok() {
                success_count += 1;
            }
        }
        let _ = std::fs::remove_file(&probe_file);
    }
    if success_count > 0 {
        start.elapsed() / success_count
    } else {
        Duration::from_millis(1)
    }
}

impl Manager {
    pub async fn new(cfg: SharedConfig) -> Self {
        let daemon_id = Uuid::new_v4().to_string();
        info!("Daemon Session ID: {}", daemon_id);
        
        let config_reader = cfg.read().await;
        let governor = Arc::new(Governor::new(
            config_reader.max_system_load_avg,
            config_reader.hydration_delay_ms,
            config_reader.governor_psi_io_threshold,
            config_reader.governor_psi_cpu_threshold,
        ));

        let limit_mb = config_reader.global_buffer_limit;
        let limit_bytes = limit_mb * 1024 * 1024;
        let calculated_entries = limit_bytes / 4096;
        let max_entries = (calculated_entries as usize).clamp(10_000, 1_000_000);
        let lru_size_val = max_entries / 10;
        
        info!("Memory Config: Global Buffer {}MB -> Max Identity Entries: {}", limit_mb, max_entries);

        let mut sources: HashMap<u32, Arc<SourceInfo>> = HashMap::new();
        for sc in &config_reader.sources {
            match resolve_all_device_ids(&sc.path) {
                Ok((mount_path, dev_ids)) => {
                    let primary_dev = dev_ids[0];
                    info!("Source: {:?} (Mount: {:?}, DevID: 0x{:x})", sc.path, mount_path, primary_dev);
                    
                    let fs_root_relative_path = if let Ok(f) = fs::File::open(&mount_path) {
                        if let Ok(meta) = f.metadata() {
                            fxcp_core::operations::btrfs_resolve_inode(f.as_raw_fd(), meta.ino()).ok()
                        } else { None }
                    } else { None };

                    if let Some(ref p) = fs_root_relative_path {
                        info!("Btrfs: Source root is at subvolume offset: {:?}", p);
                    }

                    let version_root = if let Some(first_target) = sc.targets.first() {
                        first_target.path.clone()
                    } else {
                        sc.path.clone()
                    };

                    let version_index = Arc::new(fxcp_core::versioning::VersionIndex::new(version_root));
                    let identity_index = Some(ProactiveIndex::new(sc.path.clone()));
                    
                    let inode_map = Arc::new(DashMap::new());
                    let dir_map = Arc::new(DashMap::new());
                    let projector = Arc::new(IdentityProjector::new(
                        inode_map.clone(), 
                        dir_map.clone(),
                        primary_dev
                    ));

                    sources.insert(primary_dev, Arc::new(SourceInfo {
                        path: sc.path.clone(),
                        mount: mount_path,
                        dev: primary_dev,
                        dev_ids: dev_ids.clone(),
                        hydration: Arc::new(HydrationState::default()),
                        inode_map,
                        dir_map,
                        lru_size: lru_size_val,
                        bulk_job_queue: Mutex::new(None),
                        queues: RwLock::new(HashMap::new()),
                        active_repairs: Arc::new(DashSet::new()),
                        hydrated_inodes: Arc::new(DashSet::new()),
                        rwf_uncached_ok: sc.rwf_uncached_ok.clone(),
                        version_index,
                        projector: Some(projector),
                        identity_index: identity_index,
                        lock_map: Arc::new(DashMap::new()),
                        max_inode_map_entries: max_entries,
                        cross_subvolumes: sc.cross_subvolumes,
                        watchers: Mutex::new(Vec::new()),
                        fs_root_relative_path,
                    }));
                },
                Err(e) => error!("Failed to resolve device IDs for {:?}: {}", sc.path, e),
            }
        }

        for sc in &config_reader.sources {
            for t in &sc.targets {
                let xattr_ok = security::probe_xattr_support(&t.path);
                t.xattr_supported.store(xattr_ok, std::sync::atomic::Ordering::Relaxed);
                
                if t.path.exists() {
                    let _ = fxcp_core::sidecar::remove_metadata(&t.path, "user.foxing_dir_hash_pending");
                }
            }
        }
        drop(config_reader);

        Self {
            config: cfg,
            sources,
            hydrators: Vec::new(),
            governor,
            tuner_board: Arc::new(DashMap::new()),
            daemon_id,
            default_hydration_mode: HydrationMode::Streaming,
        }
    }

    pub fn set_hydration_mode(&mut self, mode: HydrationMode) {
        self.default_hydration_mode = mode;
    }

    pub async fn start(&mut self) -> Result<(HashMap<u32, Vec<Arc<EventQueue>>>, JoinSet<Result<()>>, Vec<tokio::sync::mpsc::Sender<()>>, UnboundedReceiver<(PathBuf, Option<u64>)>)> {
        let mut all_queues_map: HashMap<u32, Vec<Arc<EventQueue>>> = HashMap::new();
        let mut tasks = JoinSet::new();
        let mut shutdowns = Vec::new();
        let (raw_hydration_tx, hydration_rx) = unbounded_channel();
        let hydration_tx = Arc::new(HydrationSender(raw_hydration_tx));
        let config_reader = self.config.read().await;
        let global_limit = config_reader.global_buffer_limit;

        for (_primary_dev, src) in self.sources.iter_mut() {
            let v_index = src.version_index.clone();
            std::thread::spawn(move || {
                v_index.index_directory();
            });

            if let Ok(meta) = fs::metadata(&src.path) {
                let inode = meta.ino();
                let dev = src.dev;
                identity::update_map(
                    &src.inode_map, &src.dir_map, dev, inode, PathBuf::from(""), 
                    0, false, true, 0, 0
                );
            }

            // Unified Epoch Pruning Task
            let inode_map_gc = src.inode_map.clone();
            let limit = src.max_inode_map_entries;
            let dev_id = src.dev;
            
            tasks.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                let jitter_ms = rand::rng().random_range(0..5000) as u64;
                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                
                loop {
                    interval.tick().await;
                    let map_ref = inode_map_gc.clone();
                    // Fetch current Global Epoch (Sequence Number)
                    let current_seq = match crate::bpf::get_device_stats().get(&dev_id) {
                        Some((seq, _)) => *seq,
                        None => 0
                    };
                    
                    let _ = tokio::task::spawn_blocking(move || {
                        identity::prune_expired_entries(&map_ref, limit, current_seq);
                    }).await;
                }
            });

            if let Some(source_cfg) = config_reader.sources.iter().find(|s| s.path == src.path) {
                let mut hydration_targets = Vec::new();
                let hydration_repair_txs = Vec::new();
                let mut queues_for_source: HashMap<u32, Vec<Arc<EventQueue>>> = HashMap::new();
                
                // Changed from Duration to CopyStats
                let mut target_stats_senders: HashMap<PathBuf, Vec<mpsc::UnboundedSender<CopyStats>>> = HashMap::new();

                for tgt_cfg in &source_cfg.targets {
                    let initial_latency = probe_storage_latency(&tgt_cfg.path);
                    let target_workers = tgt_cfg.worker_count.max(2);
                    let (fanout_tx, fanout_rxs_vec) = crate::event::create_fanout(config_reader.queue_max, target_workers);
                    let fanout_queue_arc = Arc::new(fanout_tx);
                    
                    for alt_dev_id in &src.dev_ids {
                        queues_for_source.entry(*alt_dev_id).or_insert_with(Vec::new).push(fanout_queue_arc.clone());
                    }

                    self.tuner_board.insert(tgt_cfg.path.clone(), TunerState::Startup);
                    let (cmd_tx, _) = broadcast::channel(16);
                    let (ack_tx, ack_rx) = mpsc::channel(target_workers);
                    let ack_rx_arc = Arc::new(tokio::sync::Mutex::new(ack_rx));
                    
                    let mut worker_stats_txs = Vec::new();

                    for (i, rx) in fanout_rxs_vec.into_iter().enumerate() {
                        let (sd_tx, sd_rx) = mpsc::channel(1);
                        shutdowns.push(sd_tx);
                        
                        let barrier = BarrierCoordinator {
                            worker_id: i,
                            total_workers: target_workers,
                            cmd_tx: cmd_tx.clone(),
                            cmd_rx: cmd_tx.subscribe(),
                            ack_tx: ack_tx.clone(),
                            ack_rx: if i == 0 { Some(ack_rx_arc.clone()) } else { None },
                        };

                        // Channel for CopyStats
                        let (stats_tx, stats_rx) = mpsc::unbounded_channel();
                        worker_stats_txs.push(stats_tx);

                        tasks.spawn(run_worker(
                            rx, src.clone(), tgt_cfg.clone(), self.config.clone(), sd_rx, 
                            hydration_tx.clone(), Arc::new(hydration_repair_txs.clone()), 
                            self.governor.clone(), self.tuner_board.clone(), i, None,
                            barrier, self.daemon_id.clone(), initial_latency,
                            stats_rx
                        ));
                    }
                    
                    target_stats_senders.insert(tgt_cfg.path.clone(), worker_stats_txs);

                    if tgt_cfg.initial_sync {
                        hydration_targets.push(tgt_cfg.clone());
                    }
                }

                let mut q_write = src.queues.write().await;
                *q_write = queues_for_source.clone();
                drop(q_write);

                for (k, v) in &queues_for_source {
                    all_queues_map.entry(*k).or_insert_with(Vec::new).extend(v.iter().cloned());
                }

                let bulk_worker_count = config_reader.worker_count.min(4);
                let stats_senders_arc = Arc::new(target_stats_senders);
                
                let queue = HydrationQueue::new_in_scope(
                    src.clone(), self.config.clone(), self.governor.clone(), self.tuner_board.clone(), bulk_worker_count, &mut tasks,
                    stats_senders_arc
                );
                *src.bulk_job_queue.lock() = Some(queue);

                if !hydration_targets.is_empty() {
                    let hydrator = Arc::new(Hydrator::new(
                        src.clone(), hydration_targets, self.governor.clone(), 
                        self.tuner_board.clone(), hydration_repair_txs, 
                        HashMap::new(),
                        self.daemon_id.clone(),
                        self.default_hydration_mode,
                        global_limit, // Passed here
                    ));
                    self.hydrators.push(hydrator.clone());
                    
                    src.hydration.active.store(true, Ordering::SeqCst);
                    let h_clone = hydrator.clone();
                    let should_enable_watch = self.default_hydration_mode == HydrationMode::Streaming;
                    
                    std::thread::spawn(move || {
                        let _ = h_clone.full_scan(should_enable_watch);
                        Ok::<(), FoxingError>(())
                    });
                }
            }
        }

        let hydrators_arc = Arc::new(self.hydrators.clone());
        let source_root_canonical = fs::canonicalize(
            self.sources.values().next().map(|s| s.path.as_path()).unwrap_or(Path::new("/"))
        ).unwrap_or_else(|_| PathBuf::from("/"));
        
        let hydration_rx_consumer = hydration_rx;
        
        tasks.spawn(async move {
            let hydrators_arc = hydrators_arc;
            let mut hydration_rx_task = hydration_rx_consumer;
            let mut last_full_scan = Instant::now().sub(Duration::from_secs(60));
            let mut batch_buffer: Vec<(PathBuf, Option<u64>)> = Vec::with_capacity(100);

            loop {
                // Check if any source requested a rescan (e.g. after target recovery)
                // or if the rescan flag was set by worker error streak detection.
                for h in hydrators_arc.iter() {
                    if h.source.hydration.request_rescan.swap(false, Ordering::SeqCst) {
                        let now = Instant::now();
                        if now.duration_since(last_full_scan) > Duration::from_secs(5) {
                            last_full_scan = now;
                            info!("Hydration: Rescan triggered for {:?}", h.source.path);
                            let h_clone = h.clone();
                            h.source.hydration.active.store(true, Ordering::SeqCst);
                            std::thread::spawn(move || {
                                let _ = h_clone.full_scan(false);
                                Ok::<(), FoxingError>(())
                            });
                        }
                    }
                }

                let first_item = hydration_rx_task.recv().await;
                if first_item.is_none() { break; }

                batch_buffer.push(first_item.unwrap());
                while batch_buffer.len() < 100 {
                     match hydration_rx_task.try_recv() {
                         Ok(item) => batch_buffer.push(item),
                         Err(mpsc::error::TryRecvError::Empty) => break,
                         Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
                     }
                }

                for (path, inode_opt) in batch_buffer.drain(..) {
                    let is_root_request = path == source_root_canonical;
                    
                    if !is_root_request {
                        // Find matching hydrator — support both absolute and relative paths
                        let matching_hydrator = hydrators_arc.iter().find(|h| {
                            path.starts_with(&h.source.path) || path.starts_with(&h.source.mount)
                        });

                        if let Some(hydrator) = matching_hydrator {
                            // Clone queue to avoid holding lock across await
                            let queue_opt = hydrator.source.bulk_job_queue.lock().as_ref().cloned();
                            if let Some(queue) = queue_opt {
                                // Compute relative path from whichever prefix matches
                                let rel_path = path.strip_prefix(&hydrator.source.path)
                                    .or_else(|_| path.strip_prefix(&hydrator.source.mount))
                                    .unwrap_or(&path)
                                    .to_path_buf();

                                // Dedup via active_repairs to prevent repair storms
                                if hydrator.source.active_repairs.insert(rel_path.clone()) {
                                    // Route to ALL targets (not just first)
                                    for tgt_cfg in &hydrator.targets {
                                        queue.submit_job(rel_path.clone(), tgt_cfg.clone(), inode_opt).await;
                                    }
                                    debug!("Repair: Submitted job for {:?} to {} targets", rel_path, hydrator.targets.len());
                                } else {
                                    debug!("Repair: Skipping duplicate for {:?}", rel_path);
                                }
                            }
                        } else {
                            // Fallback: try treating path as relative, check against each source
                            for h in hydrators_arc.iter() {
                                let candidate = h.source.path.join(&path);
                                if candidate.exists() || h.source.mount.join(&path).exists() {
                                    // Clone queue to avoid holding lock across await
                                    let queue_opt = h.source.bulk_job_queue.lock().as_ref().cloned();
                                    if let Some(queue) = queue_opt {
                                        if h.source.active_repairs.insert(path.clone()) {
                                            for tgt_cfg in &h.targets {
                                                queue.submit_job(path.clone(), tgt_cfg.clone(), inode_opt).await;
                                            }
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        continue;
                    }

                    if is_root_request {
                        let now = Instant::now();
                        if now.duration_since(last_full_scan) > Duration::from_secs(5) {
                            last_full_scan = now;
                            for h in hydrators_arc.iter() {
                                let h_clone = h.clone();
                                h.source.hydration.active.store(true, Ordering::SeqCst);
                                std::thread::spawn(move || {
                                    let _ = h_clone.full_scan(false);
                                    Ok::<(), FoxingError>(())
                                });
                            }
                        }
                    }
                }
            }
            Ok(())
        });

        let (_, dummy_rx) = mpsc::unbounded_channel();
        Ok((all_queues_map, tasks, shutdowns, dummy_rx))
    }
}
