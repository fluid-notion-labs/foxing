use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use std::fs;
use std::time::Instant;
use std::os::unix::fs::{MetadataExt, FileTypeExt};
use walkdir::WalkDir;
use tracing::{info, warn, debug};
use notify::{Watcher, RecursiveMode, RecommendedWatcher, EventKind};
use crate::config::{TargetConfig};
use crate::event::{Event, EventType};
use crate::mirror::SourceInfo;
use crate::governor::Governor;
use crate::tuner::{TunerBoard, TunerState};
use crate::{security, identity, sidecar, metrics, Result};
use std::os::unix::io::AsRawFd;
use tokio::sync::mpsc;
use std::collections::HashMap;
use crate::consistency::SerializationEngine;
use crate::sidecar::WalState;

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
    pub targets: Vec<TargetConfig>,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
    repair_txs: Vec<mpsc::Sender<Arc<Event>>>,
    serialization_engines: HashMap<PathBuf, Arc<SerializationEngine>>,
    daemon_id: String,
}

impl Hydrator {
    pub fn new(
        source: Arc<SourceInfo>,
        targets: Vec<TargetConfig>,
        governor: Arc<Governor>,
        tuner_board: TunerBoard,
        repair_txs: Vec<mpsc::Sender<Arc<Event>>>,
        serialization_engines: HashMap<PathBuf, Arc<SerializationEngine>>,
        daemon_id: String,
    ) -> Self {
        Self { source, targets, governor, tuner_board, repair_txs, serialization_engines, daemon_id }
    }

    pub fn repair_path(&self, path: PathBuf) {
        debug!("Hydration: Targeted repair requested for {:?}", path);
        if let Ok(metadata) = fs::metadata(&path) {
            let inode = metadata.ino();
            if metadata.is_file() {
                match identity::resolve_and_update_path(&self.source, inode, 0, 0, 0) {
                    Ok(new_full_path) => {
                        for target_cfg in &self.targets {
                            if let Some(queue) = self.source.bulk_job_queue.lock().as_ref() {
                                if let Ok(stripped_path) = new_full_path.strip_prefix(&self.source.mount) {
                                     // FOXING FIX: Pass None for inode since we just resolved it
                                     queue.submit_job(stripped_path.to_path_buf(), target_cfg.clone(), None);
                                     info!("Hydration Fix: Dispatched immediate RENAME repair job for Inode {} at new path: {:?}", inode, stripped_path);
                                     return;
                                }
                            }
                        }
                    },
                    Err(e) => {
                         warn!("Hydration: Failed lookup for Inode {} (File: {:?}): {:?}", inode, path, e);
                    }
                }
            }
        }

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
            inode: 0, parent_inode: 0, new_parent_inode: 0,
            seq_num: 0, offset: 0, length: 0,
            timestamp_ns: 0,
            name: path_str.to_string(), new_name: None, generation: 0, projid: 0, mode: 0, flags: 0,
            process_name: "hydration_repair".into(), interactive: false, 
            created_at: Instant::now(),
        };

        if let Err(_) = self.repair_txs[idx].try_send(Arc::new(evt)) {
            warn!("Hydration: Priority Lane FULL for deletion of {:?}. This implies extreme overload.", rel);
        }
    }

    pub fn full_scan(&self) {
        if self.source.hydration.active.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            warn!("Hydration: Scan requested but ALREADY ACTIVE for {:?}. Skipping.", self.source.path);
            return;
        }

        let dev_str = self.source.dev.to_string();
        metrics::HYDRATION_ACTIVE.with_label_values(&[&dev_str]).set(1.0);
        warn!("Hydration: STARTED full background scan of {:?}", self.source.path);

        let walk = WalkDir::new(&self.source.path).sort_by_file_name().into_iter();
        let mut count = 0;

        for entry_result in walk {
            self.governor.pace_hydration();
            count += 1;
            
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

        // Deletion Sweep
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
                                        let _ = fs::create_dir_all(&parent_path);
                                    }
                                }
                                self.queue_deletion(rel);
                            }
                        }
                    }
                }
            }
        }

        warn!("Hydration: FINISHED full scan for {:?}. Scanned {} items.", self.source.path, count);
        self.source.hydration.active.store(false, Ordering::SeqCst);
        metrics::HYDRATION_ACTIVE.with_label_values(&[&dev_str]).set(0.0);
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
        let dev = m.dev() as u32;
        let is_dir = m.is_dir();

        identity::update_map(
            &self.source.inode_map,
            &self.source.dir_map,
            dev, 
            ino, 
            rel.clone(), 
            std::u32::MAX, // Unknown generation
            false,
            is_dir,
            u64::MAX,
            u64::MAX
        );

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
            if !urgent {
                self.check_tuner_pause(target_cfg);
            }

            let is_dir_meta = m.is_dir() || m.is_symlink();

            if m.is_file() && (m.len() > 0 || urgent) {
                if self.sync_file_needed(path, rel.as_path(), &m, ino, target_cfg)? {
                    // FOXING FIX: Pass None for inode here as path is confirmed valid by metadata check above
                    queue_sender.submit_job(rel.clone(), target_cfg.clone(), None);
                    self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                }
            } else if is_dir_meta {
                if m.is_symlink() {
                    self.sync_symlink(path, rel.as_path(), &m, ino, target_cfg)?;
                } else if m.is_dir() {
                    self.priority_mkdir(rel.as_path(), ino);
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

    fn priority_mkdir(&self, rel: &Path, ino: u64) {
        if self.repair_txs.is_empty() { return; }
        
        let evt = Event {
            event_type: EventType::Mkdir,
            dev_id: self.source.dev,
            inode: ino, parent_inode: 0, new_parent_inode: 0,
            seq_num: 0, offset: 0, length: 0,
            timestamp_ns: 0,
            name: rel.to_string_lossy().to_string(), new_name: None, generation: 0, projid: 0, mode: 0, flags: 0,
            process_name: "hydration_prio".into(), interactive: false, 
            created_at: Instant::now(),
        };

        let idx = (rel.as_os_str().len()) % self.repair_txs.len();
        if let Err(_) = self.repair_txs[idx].try_send(Arc::new(evt)) {
            warn!("Hydration: Priority Lane FULL for deletion of {:?}. This implies extreme overload.", rel);
        }
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
                    inode: ino, parent_inode: 0, new_parent_inode: 0,
                    seq_num: 0, offset: 0, length: 0,
                    timestamp_ns: 0,
                    name: path_str.to_string(), new_name: None, generation: 0, projid: 0, mode: 0, flags: 0,
                    process_name: "hydration".into(), interactive: false, 
                    created_at: Instant::now(),
                };
                let _ = self.repair_txs[idx].try_send(Arc::new(evt));
            }
        }
        Ok(())
    }

    fn sync_file_needed(&self, src_path: &Path, rel: &Path, m: &fs::Metadata, ino: u64, target_cfg: &TargetConfig) -> Result<bool> {
        let dst_path = target_cfg.path.join(rel);
        
        // WAL check (Write-Ahead-Log)
        if let Some(wal_entry) = sidecar::get_wal_state(&dst_path) {
            if wal_entry.daemon_id != self.daemon_id {
                warn!("Hydration: Found WAL from previous daemon instance (ID mismatch). Forcing repair for {:?}.", dst_path);
                return Ok(true);
            }

            let mut is_active_worker = false;
            if let Some(engine) = self.serialization_engines.get(&target_cfg.path) {
                if engine.is_active(ino) {
                    is_active_worker = true;
                }
            }

            if is_active_worker {
                debug!("Hydration: Skipping {:?} - active write in progress by worker", dst_path);
                return Ok(false);
            }

            match wal_entry.state {
                WalState::IntentPending => {
                    info!("Hydration: Clearing stale INTENT_PENDING flag for {:?}", dst_path);
                    sidecar::clear_wal_state(&dst_path);
                },
                WalState::InProgress => {
                    warn!("Hydration: Found STALE/CRASHED write ({:?}) for {:?}. Forcing repair.", wal_entry.state, dst_path);
                    return Ok(true);
                },
                WalState::CommitPending => {
                    if let Ok(meta) = std::fs::metadata(&dst_path) {
                        if meta.len() == m.len() && meta.mtime() == m.mtime() {
                             info!("Hydration: WAL CommitPending but file matches source. Assuming valid: {:?}", dst_path);
                             sidecar::clear_wal_state(&dst_path);
                             return Ok(false);
                        }
                    }
                    warn!("Hydration: Found UNCOMMITTED write for {:?}. Forcing repair.", dst_path);
                    return Ok(true);
                },
                _ => {
                    warn!("Hydration: Found Unknown WAL state for {:?}. Forcing repair.", dst_path);
                    return Ok(true);
                }
            }
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
                        let target_hash = security::calc_dir_integrity_hash_target(dst_parent).unwrap_or(0);

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
                    inode: ino, parent_inode: 0, new_parent_inode: 0,
                    seq_num: 0, offset: 0, length: 0,
                    timestamp_ns: 0,
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
                    let _ = fs::create_dir_all(&dst_dir_path);
                }
            } else {
                 let _ = fs::create_dir_all(&dst_dir_path);
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
