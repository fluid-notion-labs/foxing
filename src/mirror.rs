use std::{sync::{Arc, atomic::Ordering}, collections::HashMap, path::PathBuf, fs};
use crate::{config::{Config, TargetConfig}, event::{EventQueue, Event, EventType}, worker::{self, TunerBoard, TunerState}, metrics, identity, sidecar, security, Result, governor::Governor};
use walkdir::WalkDir;
use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::{RwLock, mpsc};
use std::os::unix::fs::{MetadataExt};
use std::os::unix::io::AsRawFd; 
use tracing::{info, error, warn};
use std::time::{Duration, Instant};
use dashmap::DashMap;

pub type SharedConfig = Arc<RwLock<Config>>;
pub type HydrationTx = mpsc::Sender<PathBuf>;
pub type HydrationRx = mpsc::Receiver<PathBuf>;

/// Runtime information about a mirrored source filesystem.
pub struct SourceInfo { 
    pub path: PathBuf, 
    pub mount: PathBuf, 
    pub dev: u32,
    pub hydration: Arc<HydrationState>,
    pub inode_map: identity::InodeMap,
    pub lru_size: usize, 
}

pub struct HydrationState { 
    pub active: std::sync::atomic::AtomicBool, 
    pub scanned: std::sync::atomic::AtomicU64,
    pub synced: std::sync::atomic::AtomicU64 
}

pub struct Manager {
    config: SharedConfig, 
    sources: HashMap<u32, Arc<SourceInfo>>,
    hydration_handles: Arc<Mutex<Vec<std::thread::JoinHandle<Result<()>>>>>,
    pub queues: HashMap<u32, Vec<Arc<EventQueue>>>,
    pub governor: Arc<Governor>,
    // SHARED STATE: Real-time tuner status for all targets
    pub tuner_board: TunerBoard,
}

impl Manager {
    pub fn new(cfg: SharedConfig) -> Self { 
        let _cloned_cfg = cfg.clone(); 
        
        let config_reader = cfg.blocking_read();
        let governor = Arc::new(Governor::new(
            config_reader.max_system_load_avg,
            config_reader.hydration_delay_ms
        ));

        let mut sources = HashMap::new();
        let cache_size_raw = (config_reader.global_buffer_limit / 5) as usize; 
        let cache_size = NonZeroUsize::new(cache_size_raw.max(10000)).unwrap_or_else(|| NonZeroUsize::new(10000).unwrap());
        
        for sc in &config_reader.sources { 
            if let Ok(m) = fs::metadata(&sc.path) {
                let dev_id = m.dev() as u32;
                let mut mount_path = sc.path.clone();
                
                if let Ok(txt) = fs::read_to_string("/proc/mounts") {
                    let canonical_path = sc.path.canonicalize().unwrap_or(sc.path.clone());
                    mount_path = txt.lines()
                        .filter_map(|line| {
                            let parts: Vec<&str> = line.split_whitespace().collect();
                            if parts.len() >= 2 {
                                let m = PathBuf::from(parts[1]);
                                if canonical_path.starts_with(&m) {
                                    return Some(m);
                                }
                            }
                            None
                        })
                        .max_by_key(|p| p.as_os_str().len())
                            .unwrap_or(mount_path);
                }

                sources.insert(dev_id, Arc::new(SourceInfo { 
                    path: sc.path.clone(), 
                    mount: mount_path, 
                    dev: dev_id,
                    hydration: Arc::new(HydrationState { 
                        active: std::sync::atomic::AtomicBool::new(false),
                        scanned: std::sync::atomic::AtomicU64::new(0),
                        synced: std::sync::atomic::AtomicU64::new(0)
                    }),
                    inode_map: Arc::new(Mutex::new(LruCache::new(cache_size))),
                    lru_size: cache_size.get(),
                }));
            } else {
                 error!("Failed to get metadata for source path: {:?}", sc.path);
            }
        }
        
        drop(config_reader);
        Self { 
            config: cfg, 
            sources, 
            hydration_handles: Arc::new(Mutex::new(Vec::new())), 
            queues: HashMap::new(),
            governor,
            tuner_board: Arc::new(DashMap::new()),
        }
    }

    pub fn start(&mut self) -> (HashMap<u32, Vec<Arc<EventQueue>>>, Vec<tokio::task::JoinHandle<Result<()>>>, Vec<tokio::sync::mpsc::Sender<()>>, HydrationRx) {
        let mut queues: HashMap<u32, Vec<Arc<EventQueue>>> = HashMap::new();
        let mut handles = Vec::new();
        let mut shutdowns = Vec::new();
        let (hydration_tx, hydration_rx) = mpsc::channel(32); 

        let config_reader = self.config.blocking_read(); 
        let global_queue_max = config_reader.queue_max; 

        for (dev, src) in &self.sources {
            if let Some(source_cfg) = config_reader.sources.iter().find(|s| s.path == src.path) {
                
                let mut hydration_targets: Vec<(TargetConfig, Arc<EventQueue>)> = Vec::new();

                for tgt_cfg in &source_cfg.targets {
                    
                    let target_workers = tgt_cfg.worker_count; 
                    
                    let (queue_sender, worker_receivers) = crate::event::create_fanout(global_queue_max, target_workers); 
                    let queue_arc = Arc::new(queue_sender);
                    
                    queues.entry(*dev).or_default().push(queue_arc.clone());
                    
                    // Register initial state in board
                    self.tuner_board.insert(tgt_cfg.path.clone(), TunerState::Steady);

                    for rx in worker_receivers {
                        let s = src.clone();
                        let t = tgt_cfg.clone();
                        let c = self.config.clone(); 
                        let h_tx = hydration_tx.clone();
                        let (sd_tx, sd_rx) = tokio::sync::mpsc::channel(1);
                        shutdowns.push(sd_tx);
                        // Pass TunerBoard to worker
                        handles.push(tokio::spawn(worker::run_worker(s, t, rx, sd_rx, h_tx, c, self.governor.clone(), self.tuner_board.clone()))); 
                    }
                    
                    if tgt_cfg.initial_sync {
                        hydration_targets.push((tgt_cfg.clone(), queue_arc.clone()));
                    }
                }

                if !hydration_targets.is_empty() {
                    self.spawn_hydration_thread(src.clone(), hydration_targets, self.hydration_handles.clone(), self.governor.clone(), self.tuner_board.clone());
                }
            }
        }
        self.queues = queues.clone();
        (queues, handles, shutdowns, hydration_rx)
    }

    /// Spawns a dedicated thread pool to perform an epoch-based hydration scan with Governor throttling AND Opportunistic Tuner checks.
    pub fn spawn_hydration_thread(
        &self, 
        s: Arc<SourceInfo>, 
        targets: Vec<(TargetConfig, Arc<EventQueue>)>, 
        handles_arc: Arc<Mutex<Vec<std::thread::JoinHandle<Result<()>>>>>,
        governor: Arc<Governor>,
        tuner_board: TunerBoard
    ) {
         let dev_str = s.dev.to_string();
         let source_path = s.path.clone();
         
         let h = std::thread::spawn(move || {
            info!("Starting epoch-based hydration scan on source: {:?} for {} targets", source_path, targets.len());
            s.hydration.active.store(true, Ordering::Relaxed);
            metrics::HYDRATION_ACTIVE.with_label_values(&[&dev_str]).set(1);
            
            let walk = WalkDir::new(&source_path).into_iter();
            
            for entry_result in walk {
                // 1. SYSTEM HEALTH CHECK (Governor)
                governor.pace_hydration();

                if let Ok(e) = entry_result {
                    let path = e.path();
                    
                    if let Ok(rel) = path.strip_prefix(&s.mount) {
                        
                        let path_clone = path.to_path_buf();
                        let rel_clone = rel.to_path_buf();
                        let dev_id = s.dev;
                        let inode_map_clone = s.inode_map.clone();
                        let hydration_clone = s.hydration.clone();
                        
                        let targets_clone = targets.clone();
                        let dev_str_clone = dev_str.clone();
                        let board_clone = tuner_board.clone();

                        let _ = (move || -> Result<()> {
                            // READ SOURCE METADATA ONCE
                            let m = fs::metadata(&path_clone)?;
                            let ino = m.ino();
                            
                            identity::update_map(&inode_map_clone, ino, rel_clone.clone(), std::u32::MAX, false);
                            
                            let count = hydration_clone.scanned.fetch_add(1, Ordering::Relaxed);
                            if count % 100 == 0 { metrics::HYDRATION_SCANNED.with_label_values(&[&dev_str_clone]).set(count as i64); }

                            for (target_cfg, q) in &targets_clone {
                                
                                // 2. OPPORTUNISTIC CHECK (Per Target)
                                // If this specific target is under load, back off hydration for it.
                                if let Some(state) = board_clone.get(&target_cfg.path) {
                                    match *state {
                                        TunerState::HighLoad | TunerState::EmergencyDrain | TunerState::GovernorThrottled => {
                                            // Target is struggling. Slow down hydration push to this target.
                                            // Since we are in a single loop for all targets, a simple sleep here 
                                            // slows down everything, which is acceptable (Backpressure propagation).
                                            std::thread::sleep(Duration::from_millis(50)); 
                                        },
                                        _ => {}
                                    }
                                }

                                if m.is_file() {
                                    let dst_path = target_cfg.path.join(&rel_clone);
                                    
                                    let needs_sync = sidecar::is_dirty(&dst_path) || {
                                        let needs_sync = match fs::File::open(&dst_path) {
                                            Ok(df) => {
                                                if let Err(e) = security::acquire_mandatory_lock(df.as_raw_fd()) {
                                                    warn!("Failed to lock {:?} during hydration check: {}. Assuming sync needed.", dst_path, e);
                                                    true
                                                } else {
                                                    let dm = df.metadata()?;
                                                    let target_epoch = security::get_target_epoch(&dst_path);
                                                    let target_mtime = dm.mtime();
                                                    let src_mtime = m.mtime();

                                                    let integrity_hash = security::get_dir_integrity_hash(path_clone.parent().unwrap_or(&path_clone));
                                                    let target_hash = security::get_dir_integrity_hash(dst_path.parent().unwrap_or(&dst_path));
                                                    
                                                    if dm.len() != m.len() {
                                                        true
                                                    } else if integrity_hash != 0 && integrity_hash == target_hash {
                                                        metrics::HYDRATION_HASH_SKIPPED.inc();
                                                        false
                                                    } 
                                                    else if target_epoch == 0 || target_mtime < src_mtime {
                                                        true
                                                    } else {
                                                        false
                                                    }
                                                }
                                            },
                                            Err(_) => true 
                                        };
                                        needs_sync
                                    };
                                    
                                    if needs_sync {
                                        let evt = Event {
                                            event_type: EventType::Write, dev_id, inode: ino, parent_inode: 0,
                                            seq_num: 0, offset: 0, length: m.len(), name: rel_clone.to_string_lossy().to_string(),
                                            new_name: None, generation: 0, projid: 0, 
                                            mode: 0, 
                                            created_at: Instant::now()
                                        };
                                        q.push(Arc::new(evt));
                                        metrics::HYDRATION_SYNCED.with_label_values(&[&dev_str_clone]).inc();
                                        
                                        if let Some(parent) = dst_path.parent() {
                                            if let Ok(hash) = security::calc_dir_integrity_hash_target(parent) {
                                                 security::write_dir_integrity_hash(parent, hash);
                                            }
                                        }
                                    }
                                } else if m.is_dir() {
                                    let dst_dir_path = target_cfg.path.join(&rel_clone);
                                    if dst_dir_path.exists() {
                                        if let Ok(hash) = security::calc_dir_integrity_hash_target(&dst_dir_path) {
                                             security::write_dir_integrity_hash(&dst_dir_path, hash);
                                        }
                                    }
                                }
                            }
                            Ok(())
                        })().unwrap_or_else(|e| {
                            error!("Hydration task failed for {:?}: {:?}", path, e);
                        });
                    }
                }
            }
            info!("Epoch-based hydration scan completed for source: {:?}", source_path);
            s.hydration.active.store(false, Ordering::Relaxed);
            metrics::HYDRATION_ACTIVE.with_label_values(&[&dev_str]).set(0);
            Ok(())
        });
        handles_arc.lock().push(h);
    }

    pub fn wait_hydration(&self) {
        let mut handles = self.hydration_handles.lock();
        for h in handles.drain(..) {
            let _ = h.join();
        }
    }
}
