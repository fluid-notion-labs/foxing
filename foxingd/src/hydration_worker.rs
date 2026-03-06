use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use std::fs;
use std::os::unix::fs::MetadataExt;
use walkdir::WalkDir;
use tracing::{warn, debug, info, error};
use notify::{Watcher, RecursiveMode, RecommendedWatcher, EventKind};
use crate::config::{TargetConfig, Config};
use crate::mirror::{SourceInfo, SharedConfig};
use fxcp_core::governor::Governor;
use crate::tuner::{TunerBoard, TunerState, GLOBAL_TUNER_REGISTRY};
use fxcp_core::security;
use crate::Result;
use crate::error::FoxingError;
use crate::identity::{self};
use std::os::unix::io::AsRawFd;
use tokio::sync::{mpsc};
use std::collections::{HashMap, HashSet};
use fxcp_core::consistency::SerializationEngine;
use fxcp_core::sidecar;
use std::fs::OpenOptions;
use crate::event::{Event};
use fxcp_core::operations::{CopyStats, probe_capabilities};
use fxcp_core::buffer::BufferPool;
use fxcp_core::constants;
use dashmap::DashMap;
use rayon::prelude::*;
use tokio::io::unix::AsyncFd;
use crate::metrics;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use io_uring::IoUring;
use libc;
use std::io::ErrorKind;
use fxcp_core::sidecar::{SyncSignature, get_sync_signature, set_sync_signature};
use fxcp_core::hashing;
use rand::seq::IndexedRandom;
use serde::{Serialize, Deserialize};

/// Convert a raw st_dev value to the synthetic device ID used by mirror.rs.
/// Mirror uses (major << 20) | minor, but st_dev uses kernel encoding.
fn normalize_dev(raw_dev: u64) -> u32 {
    let maj = ((raw_dev >> 8) & 0xfff) as u32;
    let min = ((raw_dev & 0xff) | ((raw_dev >> 12) & 0xfff00)) as u32;
    (maj << 20) | min
}

/// Serializable frontier for resumable hydration scans.
/// Replaces recursive WalkDir with a BFS queue that can be
/// checkpointed to disk and resumed after daemon restart.
#[derive(Debug, Serialize, Deserialize)]
pub struct HydrationFrontier {
    pub pending_dirs: Vec<PathBuf>,
    pub completed_dirs: usize,
    pub files_queued: u64,
}

impl HydrationFrontier {
    pub fn new(root: PathBuf) -> Self {
        Self {
            pending_dirs: vec![root],
            completed_dirs: 0,
            files_queued: 0,
        }
    }

    /// Try to load a saved frontier checkpoint from disk.
    pub fn load(checkpoint_path: &Path) -> Option<Self> {
        let data = std::fs::read_to_string(checkpoint_path).ok()?;
        serde_json::from_str(&data).ok()
    }

    /// Save frontier state to disk for crash recovery.
    pub fn save(&self, checkpoint_path: &Path) -> std::io::Result<()> {
        let data = serde_json::to_string(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        std::fs::write(checkpoint_path, data)
    }
}

#[derive(Debug)]
pub struct HydrationState {
    pub active: AtomicBool,
    pub scanned: AtomicU64,
    pub synced: AtomicU64,
    pub shutdown_requested: AtomicBool,
    /// Set by workers after detecting target reconnection — triggers full rescan
    /// to discover files created during outage.
    pub request_rescan: AtomicBool,
}

impl Default for HydrationState {
    fn default() -> Self {
        Self {
            active: AtomicBool::new(false),
            scanned: AtomicU64::new(0),
            synced: AtomicU64::new(0),
            shutdown_requested: AtomicBool::new(false),
            request_rescan: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationMode {
    Streaming,
    PrioritizeStructure
}

#[derive(Debug, PartialEq, Clone)]
struct HydrationAllocation {
    chunk_size_bytes: usize,
    buffers_per_ring: usize,
    label: String,
}

fn calculate_hydration_allocation(
    cfg: &Config,
    _target_cfg: &TargetConfig,
    worker_count: usize,
    global_pending_events: u64,
    current_label: Option<&str>,
) -> HydrationAllocation {
    let global_limit_bytes = cfg.global_buffer_limit * 1024 * 1024;
    let storm_threshold = (cfg.global_buffer_limit * 20).clamp(2000, 100_000);
    
    let is_storming = match current_label {
        Some("hydration-primary") => global_pending_events > (storm_threshold as f64 * 1.1) as u64,
        Some("hydration-yield") => global_pending_events > (storm_threshold as f64 * 0.9) as u64,
        _ => global_pending_events > storm_threshold,
    };

    let (label, memory_share_pct) = if is_storming {
        ("hydration-yield", 0.20)
    } else {
        ("hydration-primary", 0.70)
    };

    let total_budget = (global_limit_bytes as f64 * memory_share_pct) as u64;
    let per_worker_budget = total_budget / worker_count.max(1) as u64;
    
    let chunk_size_bytes = (cfg.io_buffer_size_mib * 1024 * 1024).max(4096) as usize;
    let min_buffers = 2;
    let max_buffers = per_worker_budget / chunk_size_bytes as u64;
    // Cap hydration worker buffers — 716MB per worker is excessive.
    // Small files use std::fs::copy (no buffers). Large files need at most
    // 32 buffers for io_uring depth. This cuts 2.8GB → 128MB total.
    let buffers_per_ring = max_buffers.max(min_buffers as u64).min(32) as usize;

    HydrationAllocation {
        chunk_size_bytes,
        buffers_per_ring,
        label: label.to_string(),
    }
}

pub struct Hydrator {
    pub source: Arc<SourceInfo>,
    pub targets: Vec<TargetConfig>,
    governor: Arc<Governor>,
    _tuner_board: TunerBoard,
    _repair_txs: Vec<mpsc::Sender<Arc<Event>>>,
    _serialization_engines: HashMap<PathBuf, Arc<SerializationEngine>>,
    _daemon_id: String,
    pub mode: HydrationMode,
    pub global_buffer_limit: u64,
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
        mode: HydrationMode,
        global_buffer_limit: u64,
    ) -> Self {
        Self {
            source,
            targets,
            governor,
            _tuner_board: tuner_board,
            _repair_txs: repair_txs,
            _serialization_engines: serialization_engines,
            _daemon_id: daemon_id,
            mode,
            global_buffer_limit,
        }
    }

    pub fn repair_path(&self, path: PathBuf) {
        debug!("Hydration: Targeted repair requested for {:?}", path);
        if let Err(e) = self.process_path(&path, true, None, &mut None) {
            warn!("Hydration: Failed to repair specific path {:?}: {:?}", path, e);
        }
    }

    /// Check if any immediate child in a target directory has a dirty flag.
    fn any_child_dirty(target_dir: &Path) -> bool {
        if let Ok(entries) = std::fs::read_dir(target_dir) {
            for entry in entries.flatten() {
                if fxcp_core::sidecar::is_dirty(&entry.path()) {
                    return true;
                }
            }
        }
        false
    }

    /// Compute the current directory hash from immediate children's stat signatures.
    /// Uses size + mtime + type as per-child input — fast (stat-only, no file I/O).
    fn compute_current_dir_hash(src_dir: &Path) -> Option<[u8; 32]> {
        let entries = std::fs::read_dir(src_dir).ok()?;
        let mut children: Vec<(String, [u8; 32])> = Vec::new();

        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            // Skip sidecar files
            if name.starts_with('.') && name.ends_with(".foxing_meta") { continue; }
            // Skip foxing internal files
            if name.starts_with(".foxing") { continue; }

            let path = entry.path();
            if let Ok(meta) = std::fs::metadata(&path) {
                let mut hasher = blake3::Hasher::new();
                hasher.update(&meta.len().to_le_bytes());
                hasher.update(&meta.mtime().to_le_bytes());
                hasher.update(&meta.mtime_nsec().to_le_bytes());
                if meta.is_dir() {
                    hasher.update(b"d");
                } else {
                    hasher.update(b"f");
                }
                children.push((name, *hasher.finalize().as_bytes()));
            }
        }

        if children.is_empty() { return None; }
        Some(fxcp_core::hashing::compute_dir_hash(&mut children))
    }

    pub fn full_scan(&self, enable_watching: bool) {
        self.source.hydration.active.store(true, Ordering::SeqCst);
        info!("Hydration: Starting full scan for {:?} (Mode: {:?}, Enable Watch: {})", 
              self.source.path, self.mode, enable_watching);
        
        let root_dev = self.source.dev;
        let cross = self.source.cross_subvolumes;
        
        let mut job_buffer: Option<Vec<(PathBuf, TargetConfig, u64)>> = if self.mode == HydrationMode::PrioritizeStructure {
            Some(Vec::with_capacity(10_000))
        } else {
            None
        };

        let mut dirs = Vec::new();
        let mut files = Vec::new();
        
        let walker = WalkDir::new(&self.source.path).sort_by_file_name();
        let high_watermark = (self.global_buffer_limit * 20).clamp(2000, 100_000);

        for entry_res in walker.into_iter().filter_entry(move |e| {
            if cross { return true; }
            if let Ok(meta) = e.metadata() {
                if normalize_dev(meta.dev()) != root_dev {
                    return false;
                }
            }
            true
        }) {
            if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                info!("Hydration: Scan aborted by shutdown signal.");
                self.source.hydration.active.store(false, Ordering::SeqCst);
                return;
            }

            let pending = metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).get() as u64;
            if pending > high_watermark {
                // FIXED: Check shutdown flag during stall/sleep
                for _ in 0..5 {
                    if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                        info!("Hydration: Scan aborted by shutdown signal during backpressure pause.");
                        self.source.hydration.active.store(false, Ordering::SeqCst);
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }

            match entry_res {
                Ok(entry) => {
                    let path = entry.path().to_path_buf();
                    if entry.file_type().is_dir() {
                        dirs.push(path);
                    } else if entry.file_type().is_file() {
                        files.push(path);
                    }
                },
                Err(e) => warn!("Hydration: WalkDir error: {}", e),
            }
        }

        dirs.sort_by_key(|p| p.components().count());
        
        let targets_ref = &self.targets;
        let source_path = &self.source.path;
        let source_mount = &self.source.mount;

        info!("Hydration: Creating {} directory structures in parallel...", dirs.len());
        
        let dir_mappings: Vec<(PathBuf, PathBuf)> = dirs.iter().filter_map(|p| {
            let rel = if let Ok(r) = p.strip_prefix(source_path) {
                Some(r.to_path_buf())
            } else {
                p.strip_prefix(source_mount).ok().map(|r| r.to_path_buf())
            };
            rel.map(|r| (p.clone(), r))
        }).collect();

        dir_mappings.par_iter().for_each(|(src, rel)| {
            if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { return; }
            for target in targets_ref {
                let target_dir = target.path.join(rel);
                if !target_dir.exists() {
                    let _ = fs::create_dir_all(&target_dir);
                }
                if !security::is_dir_metadata_synced(src, &target_dir) {
                    if let Err(e) = security::apply_metadata(src, &target_dir) {
                        debug!("Hydration: Failed to apply directory metadata for {:?}: {}", target_dir, e);
                    }
                }
            }
        });

        if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            info!("Hydration: Scan aborted after directory creation.");
            self.source.hydration.active.store(false, Ordering::SeqCst);
            return;
        }

        // --- Tree pruning: skip directories whose hash matches on all targets ---
        let mut pruned_dirs: HashSet<PathBuf> = HashSet::new();

        for (src_dir, rel_dir) in &dir_mappings {
            // Skip root directory (always scan)
            if rel_dir.as_os_str().is_empty() { continue; }

            // No cascade — each directory must independently verify its hash.
            // File modifications inside subdirectories don't update parent dir mtime,
            // so cascading from parent to child would miss changes.

            // Compute current source dir hash from stat metadata
            let src_hash = match Self::compute_current_dir_hash(src_dir) {
                Some(h) => h,
                None => continue, // Empty or unreadable — don't prune
            };

            // Check against all targets
            let mut all_match = true;
            for target in targets_ref {
                let target_dir = target.path.join(rel_dir);

                // If target dir doesn't exist, can't prune
                if !target_dir.exists() { all_match = false; break; }

                // If any child has a dirty flag, can't prune
                if Self::any_child_dirty(&target_dir) { all_match = false; break; }

                // Compare stored hash
                match sidecar::get_dir_hash(&target_dir) {
                    Some(stored) if stored == src_hash => {
                        info!("Dir hash MATCH for {:?} (prunable) hash={}", rel_dir, hex::encode(&src_hash[..8]));
                    },
                    Some(stored) => {
                        info!("Dir hash MISMATCH for {:?}: src={} stored={}", rel_dir,
                              hex::encode(&src_hash[..8]), hex::encode(&stored[..8]));
                        all_match = false; break;
                    },
                    None => {
                        debug!("Dir hash ABSENT for {:?}", rel_dir);
                        all_match = false; break;
                    }
                }
            }

            if all_match {
                pruned_dirs.insert(rel_dir.clone());
                metrics::HYDRATION_DIR_PRUNED.inc();
            } else {
                // This dir has changes — remove any ancestor from pruned set
                // to prevent cascading prune from skipping modified children
                let mut ancestor = rel_dir.to_path_buf();
                while let Some(parent) = ancestor.parent() {
                    if parent.as_os_str().is_empty() { break; }
                    if pruned_dirs.remove(parent) {
                        info!("Unpruned ancestor {:?} due to mismatch in {:?}", parent, rel_dir);
                    }
                    ancestor = parent.to_path_buf();
                }
            }
        }

        let pruned_count = pruned_dirs.len();
        if pruned_count > 0 {
            info!("Hydration: Pruned {} directories via Merkle hash match", pruned_count);
        }

        // Filter out files in pruned directories
        let files_to_check: Vec<&PathBuf> = files.iter().filter(|file_path| {
            if let Ok(rel) = file_path.strip_prefix(source_path)
                .or_else(|_| file_path.strip_prefix(source_mount)) {
                // Check if any ancestor directory was pruned
                let mut current = rel.to_path_buf();
                while let Some(parent) = current.parent() {
                    if pruned_dirs.contains(parent) {
                        return false; // Skip — ancestor was pruned
                    }
                    if parent.as_os_str().is_empty() { break; }
                    current = parent.to_path_buf();
                }
            }
            true // Not pruned — include
        }).collect();

        info!("Hydration: Pre-computing signatures for {} files ({} skipped by tree pruning)...",
              files_to_check.len(), files.len() - files_to_check.len());

        let verification_results: Vec<(PathBuf, TargetConfig, u64)> = files_to_check
            .par_iter()
            .flat_map(|path| {
                if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { return Vec::new(); }
                let rel = match path.strip_prefix(source_path) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => match path.strip_prefix(source_mount) {
                        Ok(r) => r.to_path_buf(),
                        Err(_) => return Vec::new(),
                    }
                };
                
                let meta = match fs::metadata(path) {
                    Ok(m) => m,
                    Err(_) => return Vec::new(),
                };
                let ino = meta.ino();
                
                let mut jobs = Vec::new();
                for target in targets_ref {
                    metrics::HASH_VERIFICATIONS_TOTAL.inc();
                    
                    match self.sync_file_needed(path, &rel, &meta, ino, target) {
                        Ok(true) => {
                            jobs.push((rel.clone(), target.clone(), ino));
                        },
                        Ok(false) => {
                            crate::metrics::HYDRATION_HASH_SKIPPED.inc();
                            // Already synced — register as hydrated
                            self.source.hydrated_inodes.insert(ino);
                        },
                        Err(e) => {
                            warn!("Hydration: Verification failed for {:?}: {}. Queuing for retry/copy.", path, e);
                            jobs.push((rel.clone(), target.clone(), ino));
                        }
                    }
                }
                jobs
            })
            .collect();

        // Sort by (tier_priority, file_size) — fast targets first within each file,
        // small files first overall. This ensures the signature cache is populated
        // by fast local targets before slow NFS targets need the same signatures.
        let mut verification_results = verification_results;
        verification_results.sort_by_key(|(rel_path, target_cfg, _)| {
            let size = fs::metadata(source_path.join(rel_path))
                .map(|m| m.len())
                .unwrap_or(u64::MAX);
            (size, target_cfg.profile.tier_priority())
        });

        info!("Hydration: {} files require synchronization", verification_results.len());

        let low_watermark = high_watermark / 2;
        let mut submitted_count = 0u64;

        for (rel_path, target_cfg, ino) in verification_results {
            if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                break;
            }

            self.governor.pace_hydration();
            
            let pending = metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).get() as u64;
            if pending > low_watermark {
                // FIXED: Check shutdown flag during stall/sleep
                for _ in 0..5 {
                    if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }

            if let Some(buf) = &mut job_buffer {
                buf.push((rel_path, target_cfg, ino));
                if buf.len() >= 1_000 {
                    self.flush_buffer_partial(buf);
                }
            } else {
                let bulk_job_queue = self.source.bulk_job_queue.lock();
                if let Some(queue_sender) = bulk_job_queue.as_ref() {
                    // Handle both tokio and std::thread contexts
                    if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        handle.block_on(queue_sender.submit_job(rel_path, target_cfg, Some(ino)));
                    } else {
                        // No tokio runtime - create one for this call
                        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                        rt.block_on(queue_sender.submit_job(rel_path, target_cfg, Some(ino)));
                    }
                    submitted_count += 1;
                    self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                } else {
                    warn!("Hydration: bulk_job_queue is None! Cannot submit job.");
                }
            }
        }

        info!("Hydration: Submitted {} jobs to bulk queue", submitted_count);
        self.flush_buffer(job_buffer);

        // Store dir hashes on targets for future pruning
        for (src_dir, rel_dir) in &dir_mappings {
            if pruned_dirs.contains(rel_dir) { continue; } // Already stored
            if let Some(hash) = Self::compute_current_dir_hash(src_dir) {
                for target in targets_ref {
                    let target_dir = target.path.join(rel_dir);
                    if target_dir.exists() {
                        if let Err(e) = sidecar::set_dir_hash(&target_dir, &hash) {
                            warn!("Hydration: Failed to store dir hash for {:?}: {}", target_dir, e);
                        }
                    }
                }
            }
        }

        self.source.hydration.active.store(false, Ordering::SeqCst);
        info!("Hydration: Full scan complete for {:?}", self.source.path);

        // Clear hydrated_inodes after grace period to free memory
        let source_cleanup = self.source.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(30));
            source_cleanup.hydrated_inodes.clear();
            debug!("Hydration gate: cleared hydrated_inodes set");
        });

        if !self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            self.start_watcher_if_needed(enable_watching);
        }
    }

    /// Frontier-based BFS scan: iterative, checkpointable, and resumable.
    /// If a previous scan was interrupted, it resumes from the saved checkpoint.
    pub fn execute_frontier_scan(&self, enable_watching: bool) {
        self.source.hydration.active.store(true, Ordering::SeqCst);

        let checkpoint_path = self.source.path.join(".foxing_frontier.json");

        // Try to resume from saved checkpoint
        let mut frontier = match HydrationFrontier::load(&checkpoint_path) {
            Some(f) => {
                info!("Hydration: Resuming frontier scan ({} dirs pending, {} completed)",
                      f.pending_dirs.len(), f.completed_dirs);
                f
            }
            None => {
                info!("Hydration: Starting frontier scan from root {:?}", self.source.path);
                HydrationFrontier::new(self.source.path.clone())
            }
        };

        let root_dev = self.source.dev;
        let cross = self.source.cross_subvolumes;

        while let Some(current_dir) = frontier.pending_dirs.pop() {
            if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                info!("Hydration: Saving frontier checkpoint on shutdown...");
                let _ = frontier.save(&checkpoint_path);
                self.source.hydration.active.store(false, Ordering::SeqCst);
                return;
            }

            let entries = match std::fs::read_dir(&current_dir) {
                Ok(e) => e,
                Err(e) => {
                    debug!("Hydration frontier: skip {:?}: {}", current_dir, e);
                    continue;
                }
            };

            for entry in entries.flatten() {
                let path = entry.path();

                // Cross-subvolume filter
                if !cross {
                    if let Ok(meta) = entry.metadata() {
                        if normalize_dev(meta.dev()) != root_dev { continue; }
                    }
                }

                if entry.file_type().map(|f| f.is_dir()).unwrap_or(false) {
                    frontier.pending_dirs.push(path);
                } else if entry.file_type().map(|f| f.is_file()).unwrap_or(false) {
                    // Queue file for sync via existing bulk job infrastructure
                    let rel_path = match path.strip_prefix(&self.source.path) {
                        Ok(r) => r.to_path_buf(),
                        Err(_) => continue,
                    };
                    let bulk_queue = self.source.bulk_job_queue.lock();
                    if let Some(queue_sender) = bulk_queue.as_ref() {
                        if let Ok(meta) = entry.metadata() {
                            for target in &self.targets {
                                tokio::runtime::Handle::current().block_on(queue_sender.submit_job(rel_path.clone(), target.clone(), Some(meta.ino())));
                            }
                            frontier.files_queued += 1;
                        }
                    }
                }
            }

            frontier.completed_dirs += 1;
            self.source.hydration.scanned.fetch_add(1, Ordering::Relaxed);

            // Governor pacing
            self.governor.pace_hydration();

            // Checkpoint every 1000 directories
            if frontier.completed_dirs % 1000 == 0 {
                let _ = frontier.save(&checkpoint_path);
                debug!("Hydration frontier: checkpoint at {} dirs, {} files queued",
                       frontier.completed_dirs, frontier.files_queued);
            }
        }

        // Scan complete — remove checkpoint
        let _ = std::fs::remove_file(&checkpoint_path);
        self.source.hydration.active.store(false, Ordering::SeqCst);
        info!("Hydration: Frontier scan complete ({} dirs, {} files)",
              frontier.completed_dirs, frontier.files_queued);

        if !self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            self.start_watcher_if_needed(enable_watching);
        }
    }

    fn start_watcher_if_needed(&self, enable: bool) {
        if enable {
            info!("Hydration: Initializing inotify watcher post-scan...");
            match self.setup_watcher_internal() {
                Ok(watcher) => {
                    let mut w_guard = self.source.watchers.lock();
                    w_guard.push(watcher);
                    info!("Hydration: Watcher active.");
                },
                Err(e) => {
                    warn!("Hydration: Failed to start watcher after scan: {:?}", e);
                }
            }
        }
    }

    fn flush_buffer(&self, buffer: Option<Vec<(PathBuf, TargetConfig, u64)>>) {
        if let Some(jobs) = buffer {
            if !jobs.is_empty() && !self.source.hydration.shutdown_requested.load(Ordering::Relaxed) {
                info!("Hydration: Structure scan done. Flushing {} deferred data jobs...", jobs.len());
                let bulk_job_queue = self.source.bulk_job_queue.lock();
                if let Some(queue_sender) = bulk_job_queue.as_ref() {
                    for (rel_path, target_cfg, ino) in jobs {
                        if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { break; }
                        // Handle both tokio and std::thread contexts
                        if let Ok(handle) = tokio::runtime::Handle::try_current() {
                            handle.block_on(queue_sender.submit_job(rel_path, target_cfg, Some(ino)));
                        } else {
                            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                            rt.block_on(queue_sender.submit_job(rel_path, target_cfg, Some(ino)));
                        }
                        self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn flush_buffer_partial(&self, buffer: &mut Vec<(PathBuf, TargetConfig, u64)>) {
        if self.source.hydration.shutdown_requested.load(Ordering::Relaxed) { return; }
        let bulk_job_queue = self.source.bulk_job_queue.lock();
        if let Some(queue_sender) = bulk_job_queue.as_ref() {
            for (rel_path, target_cfg, ino) in buffer.drain(..) {
                // Handle both tokio and std::thread contexts
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.block_on(queue_sender.submit_job(rel_path, target_cfg, Some(ino)));
                } else {
                    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                    rt.block_on(queue_sender.submit_job(rel_path, target_cfg, Some(ino)));
                }
                self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn setup_watcher_internal(&self) -> Result<RecommendedWatcher> {
        let (tx, rx) = std::sync::mpsc::channel::<std::result::Result<notify::Event, notify::Error>>();
        let source_ref = self.source.clone();
        let targets_ref = self.targets.clone();
        
        let process_logic = move |path: PathBuf, kind: EventKind| {
            let _ = Self::process_path_static(&source_ref, &targets_ref, &path, true, Some(kind));
        };

        std::thread::spawn(move || {
            while let Ok(res) = rx.recv() {
                match res {
                    Ok(event) => {
                        match event.kind {
                            EventKind::Modify(_) | EventKind::Access(_) => continue,
                            _ => {}
                        }
                        for path in event.paths {
                            process_logic(path, event.kind);
                        }
                    },
                    Err(e) => warn!("Hydration: Watcher error: {:?}", e),
                }
            }
        });

        let mut watcher = notify::recommended_watcher(tx)
            .map_err(|e| crate::error::FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        
        watcher.watch(&self.source.path, RecursiveMode::Recursive)
            .map_err(|e| crate::error::FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
            
        Ok(watcher)
    }

    fn process_path_static(
        source: &Arc<SourceInfo>,
        targets: &[TargetConfig],
        path: &Path,
        _urgent: bool,
        _kind: Option<EventKind>
    ) -> Result<()> {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with(".foxing") || name.contains(".tmp.") || name.ends_with(".swap_tmp") {
                return Ok(());
            }
        }

        let rel = match path.strip_prefix(&source.path) {
            Ok(r) => r.to_path_buf(),
            Err(e) => {
                match path.strip_prefix(&source.mount) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => {
                        debug!("Hydration: Failed to strip prefix for {:?} (Source: {:?}, Mount: {:?}): {}", path, source.path, source.mount, e);
                        return Ok(());
                    }
                }
            }
        };

        let m = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(_) => return Ok(()),
        };
        let ino = m.ino();
        let dev = m.dev() as u32;
        let is_dir = m.is_dir();

        if m.nlink() > 1 || is_dir || _urgent {
            identity::update_map(
                &source.inode_map, &source.dir_map, dev, ino, 
                rel.clone(), std::u32::MAX, false, is_dir, u64::MAX, u64::MAX
            );
        }

        let bulk_job_queue = source.bulk_job_queue.lock();
        if let Some(queue_sender) = bulk_job_queue.as_ref() {
            for target_cfg in targets {
                if m.is_file() {
                    tokio::runtime::Handle::current().block_on(queue_sender.submit_job(rel.clone(), target_cfg.clone(), Some(ino)));
                    source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                } else if is_dir {
                    let dst_path = target_cfg.path.join(&rel);
                    if !dst_path.exists() {
                        let _ = fs::create_dir_all(&dst_path);
                    }
                    if !security::is_dir_metadata_synced(path, &dst_path) {
                        if let Err(e) = security::apply_metadata(path, &dst_path) {
                            debug!("Hydration: Metadata apply failed for {:?}: {}", dst_path, e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn process_path(
        &self,
        path: &Path,
        urgent: bool,
        _kind: Option<EventKind>,
        job_buffer: &mut Option<Vec<(PathBuf, TargetConfig, u64)>>
    ) -> Result<()> {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with(".foxing") || name.contains(".tmp.") || name.ends_with(".swap_tmp") {
                return Ok(());
            }
        }

        let rel = match path.strip_prefix(&self.source.path) {
            Ok(r) => r.to_path_buf(),
            Err(e) => {
                match path.strip_prefix(&self.source.mount) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => {
                        debug!("Hydration: Failed to strip prefix for {:?} (Source: {:?}, Mount: {:?}): {}", path, self.source.path, self.source.mount, e);
                        return Ok(());
                    }
                }
            }
        };

        let m = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(_) => return Ok(()),
        };
        let ino = m.ino();
        let dev = m.dev() as u32;
        let is_dir = m.is_dir();

        if m.nlink() > 1 || is_dir || urgent {
            identity::update_map(
                &self.source.inode_map, &self.source.dir_map, dev, ino, 
                rel.clone(), std::u32::MAX, false, is_dir, u64::MAX, u64::MAX
            );
        }

        let bulk_job_queue = self.source.bulk_job_queue.lock();
        if let Some(queue_sender) = bulk_job_queue.as_ref() {
            for target_cfg in &self.targets {
                if m.is_file() {
                    if urgent || self.sync_file_needed(path, rel.as_path(), &m, ino, target_cfg)? {
                        if let Some(buf) = job_buffer {
                            buf.push((rel.clone(), target_cfg.clone(), ino));
                        } else {
                            tokio::runtime::Handle::current().block_on(queue_sender.submit_job(rel.clone(), target_cfg.clone(), Some(ino)));
                            self.source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        crate::metrics::HYDRATION_HASH_SKIPPED.inc();
                    }
                } else if is_dir {
                    let dst_path = target_cfg.path.join(&rel);
                    if !dst_path.exists() {
                        let _ = fs::create_dir_all(&dst_path);
                    }
                    if !security::is_dir_metadata_synced(path, &dst_path) {
                        if let Err(e) = security::apply_metadata(path, &dst_path) {
                            debug!("Hydration: Metadata apply failed for {:?}: {}", dst_path, e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn sync_file_needed(&self, src_path: &Path, rel: &Path, src_meta: &fs::Metadata, _ino: u64, target_cfg: &TargetConfig) -> Result<bool> {
        let dst_path = target_cfg.path.join(rel);
        
        if sidecar::is_dirty(&dst_path) {
            if self.mode == HydrationMode::Streaming {
                debug!("Hydration: Skipping DIRTY file {:?} (BPF active).", dst_path);
                return Ok(false);
            }
            info!("Hydration: Found DIRTY flag for {:?}. Forcing repair (Recovery Mode).", dst_path);
            return Ok(true);
        }

        let src_sig = match SyncSignature::compute(src_path) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to compute source signature for {:?}: {}", src_path, e);
                return Ok(true);
            }
        };

        // Check cached signature ONLY if target file exists
        let dst_sig_opt = get_sync_signature(&dst_path);
        debug!("sync_file_needed {:?}: has_cached_sig={}", dst_path, dst_sig_opt.is_some());
        if let Some(dst_sig) = dst_sig_opt {
            // CRITICAL: Verify file exists before trusting cached signature
            if !dst_path.exists() {
                debug!("  cached signature exists but file missing — forcing sync");
                return Ok(true);
            }
            let sig_match = src_sig.matches(&dst_sig);
            let src_mr = src_sig.merkle_root.as_deref().unwrap_or("none");
            let dst_mr = dst_sig.merkle_root.as_deref().unwrap_or("none");
            debug!("  sig matches={} size={}vs{} mtime={}vs{} merkle_root={}vs{}",
                  sig_match, src_sig.size, dst_sig.size, src_sig.mtime_sec, dst_sig.mtime_sec,
                  &src_mr[..src_mr.len().min(8)], &dst_mr[..dst_mr.len().min(8)]);
            if sig_match {
                metrics::HASH_CACHE_HITS.inc();
                return Ok(false);
            }
        }

        if let Ok(df) = OpenOptions::new().read(true).open(&dst_path) {
            if !hashing::is_hashing_enabled() {
                if security::acquire_read_lock(df.as_raw_fd()).is_err() {
                    return Ok(true);
                }
                
                let dm = df.metadata()?;
                if dm.len() != src_meta.len() { return Ok(true); }
                if dm.mtime() != src_meta.mtime() { return Ok(true); }
                return Ok(false);
            }

            match hashing::verify_incremental(src_path, &dst_path, src_meta.len()) {
                Ok(true) => {
                    // Lite hash (head+tail) says match — but middle-of-file changes
                    // are invisible to it. If the target has a stored Merkle signature,
                    // ALWAYS compare roots to catch chunk-level changes.
                    let merkle_result = sidecar::get_merkle_signature(&dst_path);
                    debug!("sync_file_needed {:?}: get_merkle_signature={}", dst_path, merkle_result.is_some());
                    if let Some(stored_merkle) = merkle_result {
                        match hashing::verify_with_merkle(src_path, &stored_merkle) {
                            Ok(true) => {
                                // Merkle roots match — genuinely unchanged, safe to cache signature
                                info!("sync_file_needed {:?}: lite+merkle both match — skip", dst_path);
                                if let Err(e) = set_sync_signature(&dst_path, &src_sig) {
                                    warn!("Hydration: Failed to store sync signature for {:?}: {}", dst_path, e);
                                }
                                return Ok(false);
                            }
                            Ok(false) => {
                                // Merkle roots differ — middle-of-file change detected
                                info!("sync_file_needed {:?}: MERKLE ROOT MISMATCH — forcing delta resync", dst_path);
                                return Ok(true);
                            }
                            Err(e) => {
                                warn!("Merkle verification failed for {:?}: {}, forcing resync", dst_path, e);
                                return Ok(true);
                            }
                        }
                    }
                    // No Merkle signature stored.
                    // For small files below hash threshold, verify_incremental
                    // doesn't actually hash — it returns Ok(true) immediately.
                    // Fall back to size+mtime comparison.
                    let dm = df.metadata()?;
                    if dm.len() != src_meta.len() || dm.mtime() != src_meta.mtime() {
                        info!("sync_file_needed {:?}: size/mtime mismatch (src={}:{} dst={}:{}) — forcing resync",
                              dst_path, src_meta.len(), src_meta.mtime(), dm.len(), dm.mtime());
                        return Ok(true);
                    }
                    if let Err(e) = set_sync_signature(&dst_path, &src_sig) {
                        warn!("Hydration: Failed to store sync signature for {:?}: {}", dst_path, e);
                    }
                    Ok(false)
                },
                Ok(false) => Ok(true),
                Err(e) => {
                    warn!("Hash verification failed for {:?}: {}", dst_path, e);
                    Ok(true)
                }
            }
        } else {
            Ok(true)
        }
    }
}

pub async fn run_hydration_worker_loop(
    mut rx: Receiver<crate::hydration::HydrationJob>,
    source: Arc<SourceInfo>,
    config: SharedConfig,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_count: usize,
    worker_id: usize,
    tracker: Arc<DashMap<u64, (u32, std::time::Instant)>>,
    pending_count: Arc<std::sync::atomic::AtomicUsize>,
    stats_senders: Arc<HashMap<PathBuf, Vec<UnboundedSender<CopyStats>>>>,
    sig_cache: SignatureCache,
) -> Result<()> {
    
    use fxcp_core::operations::FsyncLatencyTracker;

    let mut current_alloc = {
        let cfg = config.read().await;
        calculate_hydration_allocation(&cfg, &TargetConfig {
            path: PathBuf::new(),
            profile: crate::config::TargetProfile::Auto,
            ..Default::default()
        }, worker_count, 0, None)
    };

    info!("Hydration Worker {}: Init [{}] - Buffers: {}", worker_id, current_alloc.label, current_alloc.buffers_per_ring);

    let mut ring = IoUring::new((current_alloc.buffers_per_ring + 8) as u32).map_err(|e| FoxingError::Io(e))?;
    
    // FIXED: Buffer allocation robustness and fallback
    let mut buffer_pool = match BufferPool::new(current_alloc.buffers_per_ring, current_alloc.chunk_size_bytes, constants::MINIMUM_ALIGNMENT_BYTES) {
        Ok(pool) => pool,
        Err(e) => {
            warn!("Hydration Worker {}: Allocation failed for optimal buffer count ({}), attempting minimal fallback. Error: {:?}", worker_id, current_alloc.buffers_per_ring, e);
            
            // Try minimal safe allocation (64MB)
            let minimal_buffers = 16;
            let minimal_chunk = 4 * 1024 * 1024;
            match BufferPool::new(minimal_buffers, minimal_chunk, constants::MINIMUM_ALIGNMENT_BYTES) {
                Ok(pool) => {
                    info!("Hydration Worker {}: Fallback allocation succeeded ({} x {}MB).", worker_id, minimal_buffers, minimal_chunk / 1024 / 1024);
                    pool
                },
                Err(critical) => {
                    error!("Hydration Worker {}: CRITICAL MEMORY FAILURE. Exiting worker thread. Error: {:?}", worker_id, critical);
                    return Err(critical.into());
                }
            }
        }
    };

    {
        let iovs = buffer_pool.as_io_vecs();
        unsafe { ring.submitter().register_buffers(&iovs) }.map_err(|e| FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
    }

    let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    if eventfd < 0 { return Err(FoxingError::Io(std::io::Error::last_os_error())); }
    
    ring.submitter().register_eventfd(eventfd).map_err(|e| FoxingError::Io(e))?;
    let async_fd = Arc::new(AsyncFd::new(eventfd).map_err(|e| FoxingError::Io(e))?);

    let source_caps = probe_capabilities(&source.path);
    let mut last_check = std::time::Instant::now();
    let mut fsync_tracker = FsyncLatencyTracker::default();
    let mut current_limit: Option<usize> = None;
    
    let skip_fsync = constants::ONE_SHOT_MODE.load(Ordering::Relaxed);
    if skip_fsync {
        info!("Hydration Worker {}: Running in One-Shot Mode (Fast Sync). Per-file fsync disabled.", worker_id);
    }

    let mut jobs_processed: u64 = 0;
    let mut jobs_skipped: u64 = 0;
    let mut jobs_failed: u64 = 0;
    // Cache probe_capabilities per target path — avoids 5000 NFS stat+ioctl for same target
    let mut caps_cache: HashMap<PathBuf, Arc<fxcp_core::operations::Capabilities>> = HashMap::new();

    loop {
        // FIXED: Check for shutdown signal in worker loop
        if source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            info!("Hydration Worker {}: Shutdown requested. Processed={}, skipped={}, failed={}",
                  worker_id, jobs_processed, jobs_skipped, jobs_failed);
            break;
        }

        if last_check.elapsed() > std::time::Duration::from_secs(5) {
            let pending_events = metrics::ORDERING_BUF_SIZE.with_label_values(&["ingress"]).get() as u64;
            let desired_alloc = {
                let cfg = config.read().await;
                calculate_hydration_allocation(
                    &cfg,
                    &TargetConfig {
                        path: PathBuf::new(),
                        profile: crate::config::TargetProfile::Auto,
                        ..Default::default()
                    },
                    worker_count,
                    pending_events,
                    Some(&current_alloc.label)
                )
            };

            let mut dynamic_limit_from_tuner = None;
            for r in GLOBAL_TUNER_REGISTRY.iter() {
                 let output = r.value();
                 let bdp = output.bdp_bytes;
                 if bdp > 0 {
                     let buffers_needed = (bdp / current_alloc.chunk_size_bytes as u64).max(4) as usize;
                     dynamic_limit_from_tuner = Some(buffers_needed);
                     break;
                 }
            }

            if desired_alloc.label == "hydration-yield" {
                current_limit = Some(buffer_pool.capacity() / 5);
                if current_alloc.label != "hydration-yield" {
                    info!("Hydration Worker {}: Entering Yield Mode (Limit: {} buffers)", worker_id, current_limit.unwrap());
                }
            } else if let Some(tuner_limit) = dynamic_limit_from_tuner {
                let safe_limit = tuner_limit.min(buffer_pool.capacity());
                current_limit = Some(safe_limit);
            } else {
                current_limit = None;
            }

            current_alloc = desired_alloc;
            last_check = std::time::Instant::now();
        }

        if buffer_pool.get_ptr(0).is_none() {
            error!("Hydration Worker {}: Buffer Pool invalid or exhausted. Exiting loop.", worker_id);
            break;
        }

        // Log first iteration and every 100 jobs to track worker liveness
        if jobs_processed == 0 || jobs_processed % 100 == 0 {
            info!("Hydration Worker {}: Waiting for job (processed={}, channel_len={})",
                  worker_id, jobs_processed, rx.len());
        }

        let job = match rx.recv().await {
            Some(j) => {
                if jobs_processed < 3 {
                    info!("Hydration Worker {}: Received job {:?}", worker_id, j.rel_path);
                }
                Some(j)
            },
            None => {
                info!("Hydration Worker {}: Channel closed. Processed={}, skipped={}, failed={}",
                      worker_id, jobs_processed, jobs_skipped, jobs_failed);
                None
            }
        };

        match job {
            Some(job) => {
                let target_path_root = job.target_cfg.path.clone();
                let target_caps = caps_cache.entry(target_path_root.clone())
                    .or_insert_with(|| probe_capabilities(&target_path_root))
                    .clone();

                match process_hydration_job(
                    job.clone(), &source, &governor, &tuner_board, &mut ring, &mut buffer_pool,
                    async_fd.clone(), &source_caps, &target_caps, &tracker, &mut fsync_tracker,
                    current_limit,
                    skip_fsync,
                    &sig_cache
                ).await {
                    Ok(Some(stats)) => {
                        metrics::EVENTS_REPAIR_COMPLETED.inc();
                        source.active_repairs.remove(&job.rel_path);
                        if let Some(senders) = stats_senders.get(&job.target_cfg.path) {
                            if let Some(sender) = senders.choose(&mut rand::rng()) {
                                let _ = sender.send(stats);
                            }
                        }
                        // Detect target recovery: success after failure streak
                        if jobs_failed > 5 && !source.hydration.request_rescan.load(Ordering::Relaxed) {
                            info!("Hydration Worker {}: Target recovered after {} failures — requesting rescan",
                                  worker_id, jobs_failed);
                            source.hydration.request_rescan.store(true, Ordering::SeqCst);
                        }
                    }
                    Ok(None) => {
                        jobs_skipped += 1;
                    }
                    Err(e) => {
                        metrics::EVENTS_REPAIR_FAILED.inc();
                        source.active_repairs.remove(&job.rel_path);
                        jobs_failed += 1;
                        warn!("Hydration Worker {}: Job failed for {:?} -> {:?}: {}. Continuing.",
                              worker_id, job.rel_path, job.target_cfg.path, e);
                    }
                }
                jobs_processed += 1;
                pending_count.fetch_sub(1, Ordering::SeqCst);

                // Log progress every 500 jobs
                if jobs_processed % 500 == 0 {
                    info!("Hydration Worker {}: Progress: processed={}, completed={}, skipped={}, failed={}, channel_len={}",
                          worker_id, jobs_processed, jobs_processed - jobs_skipped - jobs_failed,
                          jobs_skipped, jobs_failed, rx.len());
                }
            },
            None => {
                info!("Hydration Worker {}: No more jobs (processed {} total). Exiting.", worker_id, jobs_processed);
                break;
            }
        }
    }

    info!("Hydration Worker {}: Exited main loop. processed={}, completed={}, skipped={}, failed={}",
          worker_id, jobs_processed, jobs_processed - jobs_skipped - jobs_failed, jobs_skipped, jobs_failed);
    let _ = ring.submitter().unregister_buffers();
    Ok(())
}

async fn wait_for_reconnect(path: &Path) {
    let mut backoff = 1;
    while !path.exists() {
        warn!("Waiting for reconnect: {:?} not found. Retrying in {}s...", path, backoff);
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
    info!("Reconnect detected: {:?} is back.", path);
}

fn is_disconnect_error(err: &FoxingError) -> bool {
    if let FoxingError::Io(e) = err {
        if let Some(code) = e.raw_os_error() {
            return code == libc::ENODEV || code == libc::EIO || code == libc::EROFS || code == libc::ENOENT;
        }
        if e.kind() == ErrorKind::NotFound {
            return true;
        }
    }
    false
}

/// Cached signatures from a previous target's post-copy computation.
/// Shared across all hydration workers so the first target to complete
/// a file's copy caches the result, and subsequent targets skip Merkle.
pub type SignatureCache = Arc<DashMap<PathBuf, CachedSignatures>>;

#[derive(Clone, Debug)]
pub struct CachedSignatures {
    pub sync_sig: SyncSignature,
    pub merkle_bytes: Option<Vec<u8>>,
}

pub async fn process_hydration_job(
    job: crate::hydration::HydrationJob,
    source: &Arc<SourceInfo>,
    governor: &Arc<Governor>,
    tuner_board: &TunerBoard,
    ring: &mut io_uring::IoUring,
    buffer_pool: &mut BufferPool,
    async_fd: Arc<AsyncFd<std::os::unix::io::RawFd>>,
    source_caps: &Arc<fxcp_core::operations::Capabilities>,
    target_caps_ref: &Arc<fxcp_core::operations::Capabilities>,
    tracker: &Arc<DashMap<u64, (u32, std::time::Instant)>>,
    fsync_tracker: &mut fxcp_core::operations::FsyncLatencyTracker,
    buffer_limit: Option<usize>,
    skip_fsync: bool,
    sig_cache: &SignatureCache,
) -> Result<Option<CopyStats>> {
    use tokio::task::spawn_blocking;
    use fxcp_core::operations::{SmartCopier};
    use std::time::Duration;
    use fxcp_core::constants;
    use std::io::ErrorKind;
    use crate::identity;
    

    let crate::hydration::HydrationJob { mut rel_path, target_cfg, inode: job_inode } = job;
    
    let target_caps = target_caps_ref.clone();
    if !target_cfg.target_uncached { target_caps.atomic_writes.store(false, Ordering::Relaxed); }
    if !target_cfg.atomic_writes { target_caps.atomic_writes.store(false, Ordering::Relaxed); }

    let inode = if let Some(ino) = job_inode {
        ino
    } else {
        std::fs::metadata(&source.mount.join(&rel_path)).map(|m| m.ino()).unwrap_or(0)
    };

    let mut current_source_path = source.mount.join(&rel_path);
    let mut current_target_path = target_cfg.path.join(&rel_path);

    // Identity resolution: inline (DashMap lookup, no spawn_blocking needed)
    if inode != 0 {
        if let Ok(new_full_path) = identity::resolve_and_update_path(source, inode, 0, 0, 0) {
            let new_abs_path = source.mount.join(&new_full_path);
            if new_abs_path != current_source_path {
                if let Ok(new_rel) = new_abs_path.strip_prefix(&source.mount) {
                    debug!("Hydration: Path updated for inode {} from {:?} to {:?}.", inode, current_source_path, new_abs_path);
                    rel_path = new_rel.to_path_buf();
                    current_source_path = new_abs_path;
                    current_target_path = target_cfg.path.join(&rel_path);
                }
            }
        }
    }

    if let Ok(_metadata) = std::fs::metadata(&current_source_path) {
        tracker.remove(&inode);
    }

    let direct_io_ok = target_cfg.direct_io_ok.load(Ordering::Relaxed);
    let current_state = tuner_board.get(&target_cfg.path).map(|r| *r.value()).unwrap_or(TunerState::Startup);

    if governor.is_system_stressed() || matches!(current_state, TunerState::Muted) {
        tokio::task::yield_now().await;
    }

    // Small file threshold: files below this use std::fs::copy (no io_uring overhead)
    const HYDRATION_SMALL_FILE_THRESHOLD: u64 = 256 * 1024; // 256KB

    let mut attempts = 0;
    let max_attempts = constants::HYDRATION_COPY_MAX_ATTEMPTS;
    let mut success = false;
    let mut final_stats = None;

    while attempts < max_attempts && !success {
        if source.hydration.shutdown_requested.load(Ordering::Relaxed) {
            return Ok(None);
        }

        attempts += 1;

        // Use synchronous stat — avoids tokio::fs::metadata spawn_blocking overhead
        let metadata = match std::fs::metadata(&current_source_path) {
            Ok(m) => m,
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(FoxingError::Io(e));
                }
                continue;
            }
        };

        if metadata.is_dir() {
            if !current_target_path.exists() {
                let _ = fs::create_dir_all(&current_target_path);
            }
            return Ok(None);
        }
        if !metadata.is_file() {
            return Ok(None);
        }

        let file_size = metadata.len();

        // Look up adaptive timeouts from tuner
        let (adaptive_stall, adaptive_overall, adaptive_postcopy) = {
            #[allow(deprecated)]
            let mut stall = fxcp_core::constants::PROCESS_SEGMENT_STALL_SECS;
            #[allow(deprecated)]
            let mut overall = fxcp_core::constants::PROCESS_SEGMENT_TIMEOUT_SECS;
            #[allow(deprecated)]
            let mut postcopy = fxcp_core::constants::POSTCOPY_TIMEOUT_SECS;
            let target_label = target_cfg.path.to_string_lossy().to_string();
            for r in GLOBAL_TUNER_REGISTRY.iter() {
                if r.key().0 == target_label {
                    stall = r.value().segment_stall_timeout_secs;
                    overall = r.value().segment_overall_timeout_secs;
                    postcopy = r.value().postcopy_timeout_secs;
                    break;
                }
            }
            // Apply profile-based minimum floors — the tuner may misclassify NFS
            // as NVMe due to fast latency probes hitting write cache
            let (min_stall, min_overall, min_postcopy) = match target_cfg.profile {
                crate::config::TargetProfile::Network | crate::config::TargetProfile::NFS => (60, 600, 300),
                crate::config::TargetProfile::HDD => (30, 300, 120),
                crate::config::TargetProfile::SSD => (15, 120, 60),
                crate::config::TargetProfile::Auto => (30, 300, 120), // Conservative for Auto
                _ => (10, 60, 30),
            };
            stall = stall.max(min_stall);
            overall = overall.max(min_overall);
            postcopy = postcopy.max(min_postcopy);

            // Apply config overrides if set (these override everything)
            if let Some(v) = target_cfg.segment_stall_timeout_override {
                stall = v;
            }
            if let Some(v) = target_cfg.segment_overall_timeout_override {
                overall = v;
            }
            if let Some(v) = target_cfg.postcopy_timeout_override {
                postcopy = v;
            }
            (stall, overall, postcopy)
        };

        const MERKLE_DELTA_THRESHOLD: u64 = 1024 * 1024; // 1MB

        // --- Delta copy path: use Merkle diff for large files with stored signatures ---
        if file_size > MERKLE_DELTA_THRESHOLD {
            if let Some(target_merkle_sig) = fxcp_core::sidecar::get_merkle_signature(&current_target_path) {
                // Build source Merkle tree
                let src_clone = current_source_path.clone();
                let chunk_size = fxcp_core::hashing::CHUNK_SIZE as u64;

                let src_tree_result = tokio::task::spawn_blocking(move || {
                    fxcp_core::hashing::MerkleTree::from_file(&src_clone, chunk_size)
                }).await;

                if let Ok(Ok(src_tree)) = src_tree_result {
                    // Reconstruct target tree from stored signature
                    if let Some(tgt_tree) = fxcp_core::hashing::MerkleTree::from_signature(&target_merkle_sig) {
                        // Fast root comparison — skip if identical
                        if src_tree.root == tgt_tree.root {
                            debug!("Hydration: Delta skip — Merkle roots match for {:?}", current_target_path);
                            return Ok(Some(CopyStats { bytes_processed: 0, bytes_zeros: 0, io_duration: std::time::Duration::ZERO, ops_count: 0 }));
                        }

                        let dirty_ranges = fxcp_core::hashing::MerkleTree::diff(&src_tree, &tgt_tree);
                        let total_dirty_bytes: u64 = dirty_ranges.iter().map(|r| r.length).sum();

                        // Only use delta if it saves >=50% of data transfer
                        if !dirty_ranges.is_empty() && total_dirty_bytes < file_size / 2 {
                            debug!("Hydration: Delta copy — {}/{} bytes dirty for {:?}",
                                   total_dirty_bytes, file_size, current_target_path);

                            let mut smart_copier = SmartCopier {
                                ring: std::mem::replace(ring, io_uring::IoUring::new(64).expect("io_uring fallback")),
                                buffer_pool: std::mem::replace(buffer_pool, BufferPool::new(1, 4096, 4096).expect("placeholder BufferPool")),
                                atomic_buffer_pool: None,
                                async_fd: async_fd.clone(),
                                vdo_opt: target_cfg.vdo_optimization,
                                direct_io_ok,
                                source_caps: source_caps.clone(),
                                target_caps: target_caps.clone(),
                                vdo_stall_threshold: target_cfg.vdo_stall_threshold,
                                barrier_callback: None,
                                source_uncached: target_cfg.source_uncached,
                                target_uncached: target_cfg.target_uncached,
                                governor: Some(governor.clone()),
                                fsync_tracker: std::mem::take(fsync_tracker),
                                skip_fsync,
                                segment_stall_timeout_secs: adaptive_stall,
                                segment_overall_timeout_secs: adaptive_overall,
                            };

                            let delta_result = smart_copier.copy_delta(
                                &current_source_path, &current_target_path,
                                &dirty_ranges, file_size, &target_cfg.label.to_string()
                            ).await;

                            // Restore ring, buffer_pool, fsync_tracker
                            *ring = smart_copier.ring;
                            *buffer_pool = smart_copier.buffer_pool;
                            *fsync_tracker = smart_copier.fsync_tracker;

                            match delta_result {
                                Ok(stats) => {
                                    // Store updated Merkle signature
                                    let new_sig = src_tree.to_signature();
                                    let dst_clone = current_target_path.clone();
                                    let _ = tokio::task::spawn_blocking(move || {
                                        fxcp_core::sidecar::set_merkle_signature(&dst_clone, &new_sig)
                                    }).await;

                                    metrics::DELTA_COPY_ATTEMPTED.inc();
                                    metrics::DELTA_COPY_BYTES_SAVED.inc_by((file_size - total_dirty_bytes) as f64);
                                    return Ok(Some(stats));
                                },
                                Err(e) => {
                                    warn!("Hydration: Delta copy failed for {:?}: {}. Falling back to full copy.", current_target_path, e);
                                    metrics::DELTA_COPY_FELL_THROUGH.inc();
                                    // Fall through to full copy
                                }
                            }
                        } else {
                            metrics::DELTA_COPY_FELL_THROUGH.inc();
                        }
                    }
                }
            }
        }

        // --- Fast path: small files use std::fs::copy (no io_uring overhead) ---
        let copy_result: Result<Option<CopyStats>> = if file_size <= HYDRATION_SMALL_FILE_THRESHOLD {
            // Ensure parent directory exists
            if let Some(parent) = current_target_path.parent() {
                if !parent.exists() {
                    let _ = fs::create_dir_all(parent);
                }
            }

            let src_clone = current_source_path.clone();
            let dst_clone = current_target_path.clone();
            let perms = metadata.permissions();
            let atime = metadata.atime();
            let atime_nsec = metadata.atime_nsec();
            let mtime = metadata.mtime();
            let mtime_nsec = metadata.mtime_nsec();

            let copy_start = std::time::Instant::now();
            match tokio::time::timeout(
                Duration::from_secs(120),
                spawn_blocking(move || -> std::result::Result<u64, std::io::Error> {
                    let bytes = std::fs::copy(&src_clone, &dst_clone)?;
                    let _ = std::fs::set_permissions(&dst_clone, perms);
                    let times = [
                        libc::timespec { tv_sec: atime, tv_nsec: atime_nsec },
                        libc::timespec { tv_sec: mtime, tv_nsec: mtime_nsec },
                    ];
                    if let Ok(cstr) = std::ffi::CString::new(dst_clone.as_os_str().as_encoded_bytes()) {
                        unsafe { libc::utimensat(libc::AT_FDCWD, cstr.as_ptr(), times.as_ptr(), 0) };
                    }
                    Ok(bytes)
                })
            ).await {
                Ok(Ok(Ok(bytes))) => {
                    let copy_duration = copy_start.elapsed();
                    metrics::HYDRATION_WORKER_BLOCKED_MS
                        .with_label_values(&["0"])
                        .inc_by(copy_duration.as_millis() as f64);
                    Ok(Some(CopyStats { bytes_processed: bytes, bytes_zeros: 0, io_duration: Duration::ZERO, ops_count: 1 }))
                }
                Ok(Ok(Err(e))) => Err(FoxingError::Io(e)),
                Ok(Err(join_err)) => Err(FoxingError::Io(std::io::Error::new(ErrorKind::Other, format!("spawn_blocking join error: {}", join_err)))),
                Err(_elapsed) => {
                    warn!("Hydration: Small file copy timed out after 120s for {:?}", current_target_path);
                    Err(FoxingError::Io(std::io::Error::new(ErrorKind::TimedOut, "Hydration copy timed out")))
                }
            }
        } else {
            // --- Standard path: large files use io_uring for throughput ---
            let copy_start = std::time::Instant::now();
            match tokio::time::timeout(
                Duration::from_secs(600), // 10 min for large files
                SmartCopier::copy_with_limit(
                    &current_source_path,
                    &current_target_path,
                    ring,
                    buffer_pool,
                    None,
                    async_fd.clone(),
                    target_cfg.vdo_optimization,
                    0,
                    file_size,
                    direct_io_ok,
                    file_size,
                    source_caps,
                    &target_caps,
                    target_cfg.vdo_stall_threshold,
                    target_cfg.source_uncached,
                    target_cfg.target_uncached,
                    &None,
                    Some(governor.clone()),
                    target_cfg.path.to_string_lossy().to_string(),
                    fsync_tracker,
                    buffer_limit,
                    skip_fsync,
                    adaptive_stall,
                    adaptive_overall
                )
            ).await {
                Ok(inner) => {
                    let copy_duration = copy_start.elapsed();
                    metrics::HYDRATION_WORKER_BLOCKED_MS
                        .with_label_values(&["0"])
                        .inc_by(copy_duration.as_millis() as f64);
                    inner.map(Some).map_err(FoxingError::from)
                },
                Err(_) => {
                    warn!("Hydration: Large file copy timed out for {:?}", current_target_path);
                    Err(FoxingError::Io(std::io::Error::new(ErrorKind::TimedOut, "Large file copy timed out")))
                }
            }
        };

        match copy_result {
            Ok(Some(stats)) => {
                crate::metrics::BYTES_REPLICATED.with_label_values(&[&target_cfg.path.to_string_lossy()]).inc_by(stats.bytes_processed as f64);

                // Consolidated post-copy: single spawn_blocking for ALL metadata + hashing.
                // WI-1: Compute Merkle from TARGET (in page cache from write) — eliminates
                //        2 redundant source file reads. Merkle root IS the full hash.
                // WI-4: One spawn_blocking instead of 3-5 separate ones.
                // WI-5: Skip sync_xattrs when source has no user.* xattrs.
                let src_post = current_source_path.clone();
                let dst_post = current_target_path.clone();
                let chunk_size = fxcp_core::hashing::CHUNK_SIZE as u64;
                let is_large = file_size > HYDRATION_SMALL_FILE_THRESHOLD;
                let needs_merkle = file_size > MERKLE_DELTA_THRESHOLD;

                // Check signature cache — if another target already computed
                // signatures for this file, reuse them (tiered cloning).
                let cached = sig_cache.get(&rel_path).map(|r| r.clone());
                let sig_cache_ref = sig_cache.clone();
                let rel_path_for_cache = rel_path.clone();

                let postcopy_future = spawn_blocking(move || -> std::result::Result<(), FoxingError> {
                    // 1. Apply ownership/permissions/timestamps (large files only — small files done inline)
                    if is_large {
                        // WI-5: Only sync user.* xattrs, skip if none exist
                        let has_user_xattrs = xattr::list(&src_post)
                            .map(|attrs| attrs.into_iter().any(|a| a.to_string_lossy().starts_with("user.")))
                            .unwrap_or(false);
                        if has_user_xattrs {
                            security::sync_xattrs(&src_post, &dst_post);
                        }
                        security::apply_metadata(&src_post, &dst_post)?;
                    }

                    // 2. Check if another target already computed signatures for this file
                    let (sig, merkle_bytes_opt) = if let Some(cached_sigs) = cached {
                        debug!("Hydration: Reusing cached signatures for {:?} (tiered)", dst_post);
                        (cached_sigs.sync_sig, cached_sigs.merkle_bytes)
                    } else {
                        // Compute fresh: Merkle tree from TARGET + SyncSignature
                        let src_meta = std::fs::metadata(&src_post)?;
                        let src_size = src_meta.len();

                        let (merkle_root, merkle_sig) = if needs_merkle {
                            match fxcp_core::hashing::MerkleTree::from_file(&dst_post, chunk_size) {
                                Ok(tree) => {
                                    let root_hex = hex::encode(tree.root.as_bytes());
                                    let sig = tree.to_signature();
                                    (Some(root_hex), Some(sig))
                                }
                                Err(e) => {
                                    warn!("Hydration: Failed to compute Merkle from target {:?}: {}", dst_post, e);
                                    (None, None)
                                }
                            }
                        } else {
                            (None, None)
                        };

                        let lite_hash = if hashing::is_hashing_enabled() && src_size >= hashing::get_lite_threshold_bytes() {
                            hashing::hash_file_lite(&src_post, src_size)
                                .ok()
                                .flatten()
                                .map(|h| h.to_hex().to_string())
                        } else {
                            None
                        };

                        let sig = SyncSignature {
                            size: src_size,
                            mtime_sec: src_meta.mtime(),
                            mtime_nsec: src_meta.mtime_nsec(),
                            hash: lite_hash,
                            merkle_root,
                            chunk_size: Some(chunk_size),
                            leaf_count: merkle_sig.as_ref().map(|s| s.leaf_hashes.len() as u32),
                            version: SyncSignature::CURRENT_VERSION,
                        };

                        let merkle_bytes_opt = merkle_sig.as_ref()
                            .and_then(|msig| bincode::serialize(msig).ok())
                            .filter(|b| b.len() <= 64 * 1024);

                        // Cache for reuse by other targets (tiered cloning)
                        sig_cache_ref.insert(rel_path_for_cache, CachedSignatures {
                            sync_sig: sig.clone(),
                            merkle_bytes: merkle_bytes_opt.clone(),
                        });

                        (sig, merkle_bytes_opt)
                    };

                    // 3. Write signatures to this target's xattrs
                    let sig_bytes = sig.serialize();
                    if let Some(ref merkle_bytes) = merkle_bytes_opt {
                        if let Err(e) = sidecar::set_metadata_batch(&dst_post, &[
                            ("sig", &sig_bytes),
                            ("merkle", merkle_bytes),
                        ]) {
                            warn!("Hydration: Failed to batch-write metadata for {:?}: {}", dst_post, e);
                        }
                    } else {
                        if let Err(e) = set_sync_signature(&dst_post, &sig) {
                            warn!("Hydration: Failed to store sync signature for {:?}: {}", dst_post, e);
                        }
                    }

                    // 4. Clear dirty flag
                    let _ = sidecar::set_dirty_flag(&dst_post, false, "hydration_complete");

                    Ok(())
                });

                let postcopy_result = tokio::time::timeout(
                    Duration::from_secs(adaptive_postcopy),
                    postcopy_future
                ).await;

                let postcopy_result = match postcopy_result {
                    Ok(Ok(Ok(()))) => Ok(()),
                    Ok(Ok(Err(e))) => Err(e),
                    Ok(Err(join_err)) => Err(FoxingError::Join(join_err)),
                    Err(_timeout) => {
                        warn!("Hydration: Post-copy metadata timed out after {}s for {:?}",
                              adaptive_postcopy, current_target_path);
                        metrics::POSTCOPY_TIMEOUT_TOTAL
                            .with_label_values(&[&target_cfg.path.to_string_lossy()])
                            .inc();
                        Err(FoxingError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("Post-copy metadata timeout after {}s", adaptive_postcopy)
                        )))
                    }
                };

                if let Err(e) = postcopy_result {
                    if is_large {
                        warn!("Hydration: Post-copy failed for {:?}: {}. Retrying.", current_target_path, e);
                        continue; // retry
                    }
                    // For small files, metadata failure is non-fatal
                    warn!("Hydration: Post-copy metadata failed for {:?}: {} (non-fatal)", current_target_path, e);
                }

                success = true;
                final_stats = Some(stats);
                source.hydration.synced.fetch_add(1, Ordering::Relaxed);
                // Register as hydrated for the gate check
                if inode != 0 {
                    source.hydrated_inodes.insert(inode);
                }
            },
            Ok(None) => {
                success = true;
            }
            Err(e) => {
                if let FoxingError::Io(ref io_err) = e {
                    if io_err.kind() == ErrorKind::NotFound {
                        info!("Hydration: File vanished before replication (skipping): {:?}", current_source_path);
                        return Ok(None);
                    }
                }
                
                if is_disconnect_error(&e) {
                    warn!("Hydration: Disconnect detected for {:?}. Pausing worker to wait for reconnect...", current_source_path);
                    wait_for_reconnect(&source.mount).await;
                    if current_source_path.exists() {
                        info!("Hydration: Device reconnected. Resuming job for {:?}.", current_source_path);
                        attempts = 0;
                        continue;
                    }
                }

                let exponent = attempts.min(10) as u32;
                let backoff_ms = (constants::HYDRATION_COPY_BACKOFF_BASE_MS * 2u64.pow(exponent)).min(constants::HYDRATION_COPY_BACKOFF_MAX_MS);
                
                use rand::Rng;
                let jitter_ms = rand::rng().random_range(0..constants::HYDRATION_COPY_JITTER_MAX_MS);
                let delay = Duration::from_millis(backoff_ms + jitter_ms);
                
                tokio::time::sleep(delay).await;
                
                if attempts < max_attempts {
                    warn!("Hydration copy FAILED for {:?} (Attempt {}/{}) due to {:?}. Retrying in {:?}...", 
                          rel_path, attempts, max_attempts, e, delay);
                } else {
                    error!("Hydration copy POISONED after {} attempts for {:?}: {:?}", max_attempts, rel_path, e);
                    return Err(e);
                }
            }
        }
    }

    if !success {
        return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "Hydration job failed repeated attempts")));
    }

    // Dirty flag is now cleared inside the consolidated post-copy spawn_blocking above.
    Ok(final_stats)
}
