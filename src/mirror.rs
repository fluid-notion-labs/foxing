use std::{sync::{Arc, atomic::Ordering}, collections::HashMap, path::PathBuf, fs};
// FIX: Import TunerBoard and TunerState from worker::* and rename worker::run_worker below
use crate::{config::{Config, TargetConfig}, event::{EventQueue, Event, EventType}, worker::{self, TunerBoard, TunerState}, metrics, identity, sidecar, security, Result, governor::Governor};
use walkdir::WalkDir;
use parking_lot::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::{RwLock, mpsc};
use std::os::unix::fs::{MetadataExt};
use std::os::unix::io::AsRawFd; 
use tracing::{info, error, warn, debug};
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
    pub tuner_board: TunerBoard,
}

/// Enhanced mount detection that handles loopback devices
fn find_mount_point(path: &PathBuf) -> Result<(PathBuf, u32, bool)> {
    // Get the canonical path first
    let canonical = path.canonicalize()
        .map_err(|e| crate::error::FoxingError::Io(e))?;
    
    debug!("Finding mount point for canonical path: {:?}", canonical);
    
    // Read /proc/mounts
    let mounts_content = fs::read_to_string("/proc/mounts")
        .map_err(|e| crate::error::FoxingError::Io(e))?;
    
    #[derive(Debug)]
    struct MountEntry {
        device: String,
        mount_point: PathBuf,
        fstype: String,
        is_loopback: bool,
    }
    
    let mut mounts: Vec<MountEntry> = Vec::new();
    
    for line in mounts_content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }
        
        let device = parts[0];
        let mount_point = PathBuf::from(parts[1]);
        let fstype = parts[2];
        
        // Check if this is a loopback device
        let is_loopback = device.starts_with("/dev/loop");
        
        mounts.push(MountEntry {
            device: device.to_string(),
            mount_point,
            fstype: fstype.to_string(),
            is_loopback,
        });
    }
    
    // Sort by path depth (longest first) to find most specific mount
    mounts.sort_by(|a, b| {
        b.mount_point.components().count()
            .cmp(&a.mount_point.components().count())
    });
    
    // Find the mount point that contains our path
    for mount in &mounts {
        if canonical.starts_with(&mount.mount_point) {
            debug!("Found mount: device={}, mount_point={:?}, fstype={}, is_loopback={}", 
                   mount.device, mount.mount_point, mount.fstype, mount.is_loopback);
            
            // Get the device ID from the mount point
            let mount_meta = fs::metadata(&mount.mount_point)
                .map_err(|e| crate::error::FoxingError::Io(e))?;
            
            // CRITICAL: The kernel's dev_t uses new format encoding
            // stat() returns: (major << 20) | (minor & 0xff) | ((minor & 0xfff00) << 12)
            // We need to convert this to match what BPF sees
            let raw_dev = mount_meta.dev();
            
            // Extract major and minor from stat's dev_t
            let major = ((raw_dev >> 8) & 0xfff) as u32;
            let minor = ((raw_dev & 0xff) | ((raw_dev >> 12) & 0xfff00)) as u32;
            
            // Reconstruct in kernel format: (major << 20) | minor
            let kernel_dev_id = (major << 20) | minor;
            
            debug!("Device ID conversion: raw=0x{:016x}, major={}, minor={}, kernel_format=0x{:08x} ({})", 
                   raw_dev, major, minor, kernel_dev_id, kernel_dev_id);
            
            // For loopback devices, we need to verify this is the correct device
            if mount.is_loopback {
                let backing_file = get_loop_backing_file(&mount.device);
                debug!("Loopback device {} backing file: {:?}", mount.device, backing_file);
            }
            
            return Ok((mount.mount_point.clone(), kernel_dev_id, mount.is_loopback));
        }
    }
    
    // Fallback: use the path itself as mount point
    let meta = fs::metadata(&canonical)
        .map_err(|e| crate::error::FoxingError::Io(e))?;
    
    let raw_dev = meta.dev();
    let major = ((raw_dev >> 8) & 0xfff) as u32;
    let minor = ((raw_dev & 0xff) | ((raw_dev >> 12) & 0xfff00)) as u32;
    let kernel_dev_id = (major << 20) | minor;
    
    warn!("Could not find explicit mount point for {:?}, using path itself with dev_id 0x{:08x}", 
          canonical, kernel_dev_id);
    
    Ok((canonical, kernel_dev_id, false))
}

/// Get the backing file for a loopback device
fn get_loop_backing_file(loop_device: &str) -> Option<PathBuf> {
    // Extract loop number (e.g., "/dev/loop0" -> "0")
    let loop_num = loop_device.strip_prefix("/dev/loop")?;
    
    // Try to read the backing_file from sysfs
    let backing_file_path = format!("/sys/block/loop{}/loop/backing_file", loop_num);
    fs::read_to_string(backing_file_path)
        .ok()
        .map(|s| PathBuf::from(s.trim()))
}

/// Debug helper to show all device IDs in the system
fn debug_system_devices() {
    debug!("=== System Device Debug ===");
    
    if let Ok(mounts) = fs::read_to_string("/proc/mounts") {
        for line in mounts.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                let mount_point = parts[1];
                if let Ok(meta) = fs::metadata(mount_point) {
                    let dev_id = meta.dev() as u32;
                    debug!("Mount: {} -> dev_id=0x{:08x} ({})", mount_point, dev_id, dev_id);
                }
            }
        }
    }
    
    debug!("=== End Device Debug ===");
}

impl Manager {
    pub async fn new(cfg: SharedConfig) -> Self { 
        let _cloned_cfg = cfg.clone(); 
        
        let config_reader = cfg.read().await;
        let governor = Arc::new(Governor::new(
            config_reader.max_system_load_avg,
            config_reader.hydration_delay_ms
        ));

        // Debug: Show all system devices
        debug_system_devices();

        let mut sources = HashMap::new();
        let cache_size_raw = (config_reader.global_buffer_limit / 5) as usize; 
        let cache_size = NonZeroUsize::new(cache_size_raw.max(10000)).unwrap_or_else(|| NonZeroUsize::new(10000).unwrap());
        
        for sc in &config_reader.sources { 
            info!("Processing source path: {:?}", sc.path);
            
            match find_mount_point(&sc.path) {
                Ok((mount_path, dev_id, is_loopback)) => {
                    info!("Source: {:?}", sc.path);
                    info!("  Mount point: {:?}", mount_path);
                    info!("  Device ID: 0x{:08x} ({})", dev_id, dev_id);
                    info!("  Is loopback: {}", is_loopback);
                    
                    // Double-check: verify the path actually has this device ID
                    if let Ok(meta) = fs::metadata(&sc.path) {
                        let path_raw = meta.dev();
                        
                        // FIX: Convert the path's raw device ID to kernel format before comparing
                        let p_maj = ((path_raw >> 8) & 0xfff) as u32;
                        let p_min = ((path_raw & 0xff) | ((path_raw >> 12) & 0xfff00)) as u32;
                        let path_kernel_dev = (p_maj << 20) | p_min;

                        if path_kernel_dev != dev_id {
                            error!("DEVICE MISMATCH: Path {:?} has dev_id 0x{:08x} (raw: 0x{:x}) but mount point has 0x{:08x}", 
                                   sc.path, path_kernel_dev, path_raw, dev_id);
                            error!("This will cause BPF filtering issues!");
                        }
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
                },
                Err(e) => {
                    error!("Failed to determine mount point for {:?}: {}", sc.path, e);
                }
            }
        }
        
        if sources.is_empty() {
            error!("No valid sources configured! Check your config.toml paths.");
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

    pub async fn start(&mut self) -> (HashMap<u32, Vec<Arc<EventQueue>>>, Vec<tokio::task::JoinHandle<Result<()>>>, Vec<tokio::sync::mpsc::Sender<()>>, HydrationRx) {
        let mut queues: HashMap<u32, Vec<Arc<EventQueue>>> = HashMap::new();
        let mut handles = Vec::new();
        let mut shutdowns = Vec::new();
        let (hydration_tx, hydration_rx) = mpsc::channel(32); 

        let config_reader = self.config.read().await; 
        let global_queue_max = config_reader.queue_max; 

        info!("Starting workers for {} sources", self.sources.len());

        for (dev, src) in &self.sources {
            info!("Starting workers for device 0x{:08x} ({})", dev, dev);
            
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
                        // FIX: Use worker::run_worker which is publically available in src/worker.rs
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
        
        info!("Worker startup complete. Monitoring {} devices", self.queues.len());
        for (dev, qs) in &self.queues {
            info!("  Device 0x{:08x}: {} queue(s)", dev, qs.len());
        }
        
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
                                if let Some(state) = board_clone.get(&target_cfg.path) {
                                    match *state {
                                        TunerState::HighLoad | TunerState::EmergencyDrain | TunerState::GovernorThrottled => {
                                            std::thread::sleep(Duration::from_millis(50)); 
                                        },
                                        _ => {}
                                    }
                                }

                                if m.is_file() {
                                    let dst_path = target_cfg.path.join(&rel_clone);
                                    
                                    let needs_sync = sidecar::is_dirty(&dst_path) || {
                                        // CRITICAL FIX: Open with write access to allow mandatory locking check
                                        // Previously this caused EBADF warnings during hydration
                                        let needs_sync = match std::fs::OpenOptions::new().read(true).write(true).open(&dst_path) {
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
                                            flags: 0, // FIX: Initialize flags to 0 for hydration events
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
