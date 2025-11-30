use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use std::fs;
use std::time::Instant;
use std::os::unix::fs::{MetadataExt, FileTypeExt};
use std::os::unix::io::AsRawFd;
use walkdir::WalkDir;
use tracing::{info, warn, debug};
use notify::{Watcher, RecursiveMode, RecommendedWatcher, EventKind};

use crate::config::TargetConfig;
use crate::event::{Event, EventType, EventQueue};
use crate::mirror::SourceInfo;
use crate::governor::Governor;
use crate::worker::{TunerBoard, TunerState};
use crate::{security, identity, sidecar, metrics, Result};

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
    source: Arc<SourceInfo>,
    targets: Vec<(TargetConfig, Arc<EventQueue>, Arc<EventQueue>)>,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
}

impl Hydrator {
    pub fn new(
        source: Arc<SourceInfo>,
        targets: Vec<(TargetConfig, Arc<EventQueue>, Arc<EventQueue>)>,
        governor: Arc<Governor>,
        tuner_board: TunerBoard,
    ) -> Self {
        Self { source, targets, governor, tuner_board }
    }

    pub fn full_scan(&self) {
        let dev_str = self.source.dev.to_string();
        self.source.hydration.active.store(true, Ordering::Relaxed);
        metrics::HYDRATION_ACTIVE.with_label_values(&[&dev_str]).set(1);

        info!("Hydration: Starting full background scan of {:?}", self.source.path);

        let walk = WalkDir::new(&self.source.path).into_iter();

        for entry_result in walk {
            self.governor.pace_hydration();

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

        info!("Hydration: Full scan complete for {:?}", self.source.path);
        self.source.hydration.active.store(false, Ordering::Relaxed);
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
                        debug!("Inotify hydration processing error for {:?}: {:?}", p, e);
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

        for (target_cfg, high_q, low_q) in &self.targets {
            let q = if urgent { high_q } else { low_q };

            if !urgent {
                self.check_tuner_pause(target_cfg);
            }

            if m.is_symlink() {
                self.sync_symlink(path, &rel, &m, ino, target_cfg, q)?;
            } else if m.is_file() {
                self.sync_file(path, &rel, &m, ino, target_cfg, q)?;
            } else if m.is_dir() {
                self.sync_dir_hash(&rel, target_cfg)?;
            } else {
                let ft = m.file_type();
                if ft.is_block_device() || ft.is_char_device() || ft.is_fifo() {
                     self.sync_special(path, &rel, &m, ino, target_cfg, q)?;
                }
            }
        }

        Ok(())
    }

    fn sync_file(&self, src_path: &Path, rel: &Path, m: &fs::Metadata, ino: u64, target_cfg: &TargetConfig, q: &Arc<EventQueue>) -> Result<()> {
        let dst_path = target_cfg.path.join(rel);
        
        let needs_sync = if sidecar::is_dirty(&dst_path) {
            true
        } else {
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
                        else if target_epoch == 0 || dm.mtime() < m.mtime() { true }
                        else { false }
                    }
                },
                Err(_) => true 
            }
        };

        if needs_sync {
            let evt = Event {
                event_type: EventType::Write,
                dev_id: self.source.dev,
                inode: ino, parent_inode: 0, seq_num: 0, offset: 0, length: m.len(),
                name: rel.to_string_lossy().to_string(), new_name: None, generation: 0, projid: 0, mode: m.mode(), flags: 0,
                process_name: "hydration".into(), interactive: false, open_count: 0,
                created_at: Instant::now(),
            };
            q.push(Arc::new(evt));
            self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
            
            if let Some(parent) = dst_path.parent() {
                 if let Ok(hash) = security::calc_dir_integrity_hash_target(parent) {
                      security::write_dir_integrity_hash(parent, hash);
                 }
            }
        }
        Ok(())
    }

    fn sync_symlink(&self, src_path: &Path, rel: &Path, _m: &fs::Metadata, ino: u64, target_cfg: &TargetConfig, q: &Arc<EventQueue>) -> Result<()> {
        let dst_path = target_cfg.path.join(rel);
        let needs_sync = if let Ok(target) = fs::read_link(src_path) {
            if let Ok(existing) = fs::read_link(&dst_path) {
                target != existing
            } else { true }
        } else { false };

        if needs_sync {
            let evt = Event {
                event_type: EventType::Symlink,
                dev_id: self.source.dev,
                inode: ino, parent_inode: 0, seq_num: 0, offset: 0, length: 0,
                name: rel.to_string_lossy().to_string(), new_name: None, generation: 0, projid: 0, mode: 0, flags: 0,
                process_name: "hydration".into(), interactive: false, open_count: 0,
                created_at: Instant::now(),
            };
            q.push(Arc::new(evt));
        }
        Ok(())
    }

    fn sync_special(&self, _src_path: &Path, rel: &Path, m: &fs::Metadata, ino: u64, target_cfg: &TargetConfig, q: &Arc<EventQueue>) -> Result<()> {
        let dst_path = target_cfg.path.join(rel);
        if !dst_path.exists() {
            let evt = Event {
                event_type: EventType::Mknod,
                dev_id: self.source.dev,
                inode: ino, parent_inode: 0, seq_num: 0, offset: 0, length: 0,
                name: rel.to_string_lossy().to_string(), new_name: None, generation: 0, projid: 0, mode: m.mode(), flags: 0,
                process_name: "hydration".into(), interactive: false, open_count: 0,
                created_at: Instant::now(),
            };
            q.push(Arc::new(evt));
        }
        Ok(())
    }

    fn sync_dir_hash(&self, rel: &Path, target_cfg: &TargetConfig) -> Result<()> {
        let dst_dir_path = target_cfg.path.join(rel);
        if dst_dir_path.exists() {
            if let Ok(hash) = security::calc_dir_integrity_hash_target(&dst_dir_path) {
                security::write_dir_integrity_hash(&dst_dir_path, hash);
            }
        }
        Ok(())
    }

    fn check_tuner_pause(&self, target_cfg: &TargetConfig) {
        if let Some(state) = self.tuner_board.get(&target_cfg.path) {
            match *state {
                TunerState::Muted | TunerState::SpacePressure => {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                },
                _ => {}
            }
        }
    }
}
