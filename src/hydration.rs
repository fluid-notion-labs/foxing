use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use std::fs;
use std::time::Instant;
use std::os::unix::fs::{MetadataExt, FileTypeExt};
use walkdir::WalkDir;
use tracing::{info, warn, debug};
use notify::{Watcher, RecursiveMode, RecommendedWatcher, EventKind};
use crate::config::{TargetConfig, TargetProfile};
use crate::event::{Event, EventType};
use crate::mirror::SourceInfo;
use crate::governor::Governor;
use crate::tuner::{TunerBoard, TunerState};
use crate::{security, identity, sidecar, metrics, Result};
use std::os::unix::io::AsRawFd;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct HydrationState {
    pub active: AtomicBool,
    pub scanned: AtomicU64,
    pub synced: AtomicU64,
}

impl Default for HydrationState {
    fn default() -> Self {
        Self {
            active: AtomicBool::new(false),
            scanned: AtomicU64::new(0),
            synced: AtomicU64::new(0),
        }
    }
}

pub struct Hydrator {
    pub source: Arc<SourceInfo>,
    targets: Vec<TargetConfig>,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
    repair_txs: Vec<mpsc::Sender<Arc<Event>>>,
}

impl Hydrator {
    pub fn new(
        source: Arc<SourceInfo>,
        targets: Vec<TargetConfig>,
        governor: Arc<Governor>,
        tuner_board: TunerBoard,
        repair_txs: Vec<mpsc::Sender<Arc<Event>>>,
    ) -> Self {
        Self { source, targets, governor, tuner_board, repair_txs }
    }

    pub fn repair_path(&self, path: PathBuf) {
        debug!("Hydration: Targeted repair requested for {:?}", path);
        if !path.exists() {
             if let Ok(rel) = path.strip_prefix(&self.source.path) {
                 if !rel.as_os_str().is_empty() {
                     self.queue_deletion(rel);
                 }
             }
             return;
        }
        if let Err(e) = self.process_path(&path, true, None) {
            warn!("Hydration: Failed to repair specific path {:?}: {:?}", path, e);
        }
    }

    fn queue_deletion(&self, rel: &Path) {
        if self.repair_txs.is_empty() { return; }
        let path_str = rel.to_string_lossy();
        let idx = (path_str.len()) % self.repair_txs.len();
        let evt = Event {
            event_type: EventType::Unlink,
            dev_id: self.source.dev,
            inode: 0, parent_inode: 0, seq_num: 0, offset: 0, length: 0,
            name: path_str.to_string(), new_name: None, generation: 0, projid: 0, mode: 0, flags: 0,
            process_name: "hydration_repair".into(), interactive: false,
            created_at: Instant::now(),
        };
        if let Err(_) = self.repair_txs[idx].try_send(Arc::new(evt)) {
            warn!("Hydration: PRIORITY LANE FULL for deletion of {:?}. This implies extreme overload.", rel);
        }
    }

    pub fn full_scan(&self) {
        if self.source.hydration.active.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            warn!("Hydration: Scan requested but ALREADY ACTIVE for {:?}. Skipping.", self.source.path);
            return;
        }
        let dev_str = self.source.dev.to_string();
        metrics::HYDRATION_ACTIVE.with_label_values(&[&dev_str]).set(1);
        warn!("Hydration: STARTED full background scan of {:?}", self.source.path);
        let walk = WalkDir::new(&self.source.path).into_iter();
        for entry_result in walk {
            self.governor.pace_hydration();
            let yield_now = self.targets.iter().any(|cfg| {
                self.tuner_board.get(&cfg.path).map(|state| {
                    matches!(*state.value(), TunerState::Muted | TunerState::Drain | TunerState::SpacePressure | TunerState::CriticalDrain)
                }).unwrap_or(false)
            });
            if yield_now {
                std::thread::yield_now();
            } else if self.source.hydration.scanned.load(Ordering::Relaxed) % 100 == 0 {
                 std::thread::yield_now();
            }
            match entry_result {
                Ok(entry) => {
                    let path = entry.path();
                    if let Err(e) = self.process_path(path, false, None) {
                        warn!("Hydration error processing {:?}: {:?}", path, e);
                    }
                }
                Err(e) => warn!("Hydration walk error: {}", e),
            }
        }
        if !self.governor.is_system_stressed() {
            warn!("Hydration: Starting DELETION SWEEP.");
            for target_cfg in &self.targets {
                let target_walk = WalkDir::new(&target_cfg.path).into_iter();
                for entry_result in target_walk {
                    if let Ok(entry) = entry_result {
                        let target_path = entry.path();
                        let path_str = target_path.to_string_lossy();
                        if path_str.contains(".mirror") ||
                           path_str.contains(".foxing_meta") ||
                           path_str.contains(".tmp.") {
                            continue;
                        }
                        if let Ok(rel) = target_path.strip_prefix(&target_cfg.path) {
                            if rel.as_os_str().is_empty() { continue; }
                            let source_path = self.source.path.join(rel);
                            if !source_path.exists() {
                                if let Some(parent_rel) = rel.parent() {
                                    let parent_path = target_cfg.path.join(parent_rel);
                                    if !parent_path.exists() {
                                        let _ = fs::create_dir_all(&parent_path).map_err(|e| {
                                            warn!("Hydration: Failed to create missing parent directory {:?}: {}", parent_path, e);
                                            e
                                        });
                                    }
                                }
                                warn!("Hydration: Found ZOMBIE file {:?}. Queueing UNLINK.", rel);
                                self.queue_deletion(rel);
                            }
                        }
                    }
                }
            }
        } else {
            warn!("Hydration: Skipping deletion sweep due to System Stress.");
        }
        warn!("Hydration: FINISHED full scan for {:?}", self.source.path);
        self.source.hydration.active.store(false, Ordering::SeqCst);
        metrics::HYDRATION_ACTIVE.with_label_values(&[&dev_str]).set(0);
    }

    pub fn start_watcher(self: Arc<Self>) -> Result<RecommendedWatcher> {
        let s = self.clone();
        let path = self.source.path.clone();
        
        let (tx, rx) = std::sync::mpsc::channel::<(PathBuf, EventKind)>();
        
        std::thread::Builder::new()
            .name("foxing-hydration-proc".into())
            .spawn(move || {
                debug!("Hydration: Processor thread started.");
                while let Ok((p, kind)) = rx.recv() {
                    if let Err(e) = s.process_path(&p, true, Some(kind)) {
                        warn!("Inotify hydration processing error for {:?}: {:?}", p, e);
                    }
                }
                debug!("Hydration: Processor thread stopped.");
            })
            .map_err(|e| crate::error::FoxingError::Io(e))?;

        let mut watcher = notify::recommended_watcher(move |res: std::result::Result<notify::Event, notify::Error>| {
            match res {
                Ok(event) => {
                    match event.kind {
                        EventKind::Access(_) => return,
                        _ => {}
                    }
                    for path in event.paths {
                        let _ = tx.send((path, event.kind.clone()));
                    }
                },
                Err(e) => warn!("Inotify watch error: {:?}", e),
            }
        }).map_err(|e| crate::error::FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

        watcher.watch(&path, RecursiveMode::Recursive)
            .map_err(|e| crate::error::FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        
        info!("Hydration: Inotify watcher started for {:?}", path);
        Ok(watcher)
    }

    pub fn process_path(&self, path: &Path, urgent: bool, kind: Option<EventKind>) -> Result<()> {
        let rel = match path.strip_prefix(&self.source.mount) {
            Ok(r) => r.to_path_buf(),
            Err(_) => return Ok(()),
        };

        let m = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(_) => return Ok(()),
        };
        
        let ino = m.ino();

        identity::update_map(&self.source.inode_map, ino, rel.clone(), std::u32::MAX, false);

        if !urgent {
            self.source.hydration.scanned.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics::LIVE_ADDITIONS.inc();
        }

        if let Some(EventKind::Modify(_)) = kind {
            return Ok(());
        }

        let bulk_job_queue = self.source.bulk_job_queue.lock();
        let queue_sender = bulk_job_queue.as_ref().expect("Bulk hydration queue must be initialized.");

        for target_cfg in &self.targets {
            let current_state = self.tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);
            let low_limit = match target_cfg.profile {
                 TargetProfile::HDD => 1 * 1024 * 1024,
                 _                  => 5 * 1024 * 1024,
            };
            let large_file_threshold = match current_state {
                TunerState::Startup | TunerState::ProbeBW | TunerState::IdleReset => 64 * 1024 * 1024,
                TunerState::Drain | TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => low_limit,
            };
            let is_dir_meta = m.is_dir() || m.is_symlink();
            let _is_large_file = m.is_file() && m.len() > large_file_threshold;

            if !urgent {
                self.check_tuner_pause(target_cfg);
            }

            if m.is_file() && (m.len() > 0 || urgent) {
                if self.sync_file_needed(path, rel.as_path(), &m)? {
                    warn!("Hydration: Submitting BULK JOB for {:?}", rel);
                    queue_sender.submit_job(rel.clone(), target_cfg.clone());
                    self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                }
            } else if is_dir_meta {
                if m.is_symlink() {
                    self.sync_symlink(path, rel.as_path(), &m, ino, target_cfg)?;
                } else if m.is_dir() {
                    self.sync_dir_hash(rel.as_path(), target_cfg)?;
                } else {
                    let ft = m.file_type();
                    if ft.is_block_device() || ft.is_char_device() || ft.is_fifo() {
                         self.sync_special(path, rel.as_path(), &m, ino, target_cfg)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn sync_symlink(&self, src_path: &Path, rel: &Path, _m: &fs::Metadata, ino: u64, target_cfg: &TargetConfig) -> Result<()> {
        let dst_path = target_cfg.path.join(rel);
        let needs_sync = if let Ok(target) = fs::read_link(src_path) {
            if let Ok(existing) = fs::read_link(&dst_path) {
                target != existing
            } else { true }
        } else { false };
        if needs_sync {
            if !self.repair_txs.is_empty() {
                let path_str = rel.to_string_lossy();
                let idx = (path_str.len()) % self.repair_txs.len();
                let evt = Event {
                    event_type: EventType::Symlink,
                    dev_id: self.source.dev,
                    inode: ino, parent_inode: 0, seq_num: 0, offset: 0, length: 0,
                    name: path_str.to_string(), new_name: None, generation: 0, projid: 0, mode: 0, flags: 0,
                    process_name: "hydration".into(), interactive: false,
                    created_at: Instant::now(),
                };
                let _ = self.repair_txs[idx].try_send(Arc::new(evt));
            }
        }
        Ok(())
    }

    fn sync_file_needed(&self, src_path: &Path, rel: &Path, m: &fs::Metadata) -> Result<bool> {
        let target_cfg = self.targets.iter().find(|cfg| rel.starts_with(cfg.path.strip_prefix(&self.source.mount).unwrap_or(Path::new("")))).unwrap();
        let dst_path = target_cfg.path.join(rel);
        
        if sidecar::is_dirty(&dst_path) {
            debug!("Hydration: Skipping {:?} - active write in progress", dst_path);
            return Ok(false); 
        }

        let needs_sync = {
            match std::fs::OpenOptions::new().read(true).write(true).open(&dst_path) {
                Ok(df) => {
                    if security::acquire_mandatory_lock(df.as_raw_fd()).is_err() { true }
                    else {
                        let dm = df.metadata()?;
                        let target_epoch = security::get_target_epoch(&dst_path);
                        let src_parent = src_path.parent().unwrap_or(src_path);
                        let dst_parent = dst_path.parent().unwrap_or(&dst_path);
                        let integrity_hash = security::get_valid_dir_hash(src_parent);
                        let target_hash = security::get_dir_integrity_hash(dst_parent);
                        
                        if dm.len() != m.len() { true }
                        else if integrity_hash != 0 && integrity_hash == target_hash {
                            metrics::HYDRATION_HASH_SKIPPED.inc();
                            false
                        }
                        else if target_epoch == 0 || dm.mtime() != m.mtime() { true }
                        else { false }
                    }
                },
                Err(_) => true
            }
        };
        Ok(needs_sync)
    }

    fn sync_special(&self, _src_path: &Path, rel: &Path, m: &fs::Metadata, ino: u64, target_cfg: &TargetConfig) -> Result<()> {
        let dst_path = target_cfg.path.join(rel);
        if !dst_path.exists() {
            if !self.repair_txs.is_empty() {
                let path_str = rel.to_string_lossy();
                let idx = (path_str.len()) % self.repair_txs.len();
                let evt = Event {
                    event_type: EventType::Mknod,
                    dev_id: self.source.dev,
                    inode: ino, parent_inode: 0, seq_num: 0, offset: 0, length: 0,
                    name: path_str.to_string(), new_name: None, generation: 0, projid: 0, mode: m.mode(), flags: 0,
                    process_name: "hydration".into(), interactive: false,
                    created_at: Instant::now(),
                };
                let _ = self.repair_txs[idx].try_send(Arc::new(evt));
            }
        }
        Ok(())
    }

    fn sync_dir_hash(&self, rel: &Path, target_cfg: &TargetConfig) -> Result<()> {
        let dst_dir_path = target_cfg.path.join(rel);
        
        if !dst_dir_path.exists() {
            if let Some(parent) = dst_dir_path.parent() {
                if !parent.exists() {
                    warn!("Hydration: Missing parent directory {:?} for target dir {:?}. Queueing MKDIR for parent.", parent, rel);
                    let _ = fs::create_dir_all(&dst_dir_path).map_err(|e| {
                         warn!("Hydration: Failed to create missing directory {:?}: {}", dst_dir_path, e);
                         e
                    });
                }
            } else {
                 let _ = fs::create_dir_all(&dst_dir_path).map_err(|e| {
                     warn!("Hydration: Failed to create missing directory {:?}: {}", dst_dir_path, e);
                     e
                 });
            }
        }

        if dst_dir_path.exists() {
            if let Ok(hash) = security::calc_dir_integrity_hash_target(&dst_dir_path) {
                security::write_dir_integrity_hash(&dst_dir_path, hash);
            }
        }
        Ok(())
    }

    fn check_tuner_pause(&self, target_cfg: &TargetConfig) {
        if let Some(state) = self.tuner_board.get(&target_cfg.path) {
            match *state.value() {
                TunerState::Muted | TunerState::SpacePressure | TunerState::CriticalDrain => {
                    std::thread::yield_now();
                    std::thread::sleep(std::time::Duration::from_micros(100));
                },
                _ => {}
            }
        }
    }
}
