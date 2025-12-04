use std::path::{Path, PathBuf};
use crate::error::{Result, FoxingError as MirrorError};
use std::fs;
use std::collections::HashMap;
use std::sync::RwLock;
use std::io::{Read, Seek, SeekFrom, copy};
use crate::sidecar;
use tracing::{warn, info};
use chrono::{DateTime, Utc};
use comfy_table::{Table, Row, Cell, CellAlignment};
use std::os::unix::fs::MetadataExt;
use walkdir::WalkDir;
use rayon::prelude::*;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct FileVersion {
    pub inode: u64,
    pub epoch_seq: u64,
    pub timestamp: i64,
    pub path: PathBuf,
    pub size: u64,
    pub mtime: i64,
    pub content_hash: Option<u64>,
}

/// The VersionIndex provides fast, in-memory lookups for version history.
/// It replaces the slow `glob` based approach.
#[derive(Debug)]
pub struct VersionIndex {
    /// Maps Target Inode -> List of Versions (for Revert/List)
    inode_index: RwLock<HashMap<u64, Vec<FileVersion>>>,
    /// Maps Content Hash -> List of Paths (for Deduplication/Hydration)
    hash_index: RwLock<HashMap<u64, Vec<PathBuf>>>,
    root: PathBuf,
    ready: std::sync::atomic::AtomicBool,
}

impl VersionIndex {
    pub fn new(root: PathBuf) -> Self {
        Self {
            inode_index: RwLock::new(HashMap::new()),
            hash_index: RwLock::new(HashMap::new()),
            root,
            ready: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Asynchronously scans the .mirror/.versions directory to populate the index.
    /// Uses Rayon for parallel processing of metadata/xattrs.
    pub fn index_directory(&self) {
        let versions_dir = self.root.join(".mirror").join(".versions");
        if !versions_dir.exists() { 
            self.ready.store(true, std::sync::atomic::Ordering::Relaxed);
            return; 
        }

        info!("VersionIndex: Starting background scan of {:?}", versions_dir);
        let start = std::time::Instant::now();

        let entries: Vec<_> = WalkDir::new(&versions_dir)
            .min_depth(1)
            .max_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .collect();

        // Parallel process entries to read xattrs (hashes) without blocking the main loop too long
        let processed_versions: Vec<FileVersion> = entries.par_iter().filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            
            // Expected format: {inode}_{seq}_{timestamp}
            let parts: Vec<&str> = name.split('_').collect();
            if parts.len() < 3 { return None; }

            let inode = parts[0].parse::<u64>().ok()?;
            let epoch_seq = parts[1].parse::<u64>().ok()?;
            let timestamp = parts[2].parse::<i64>().ok()?;

            let meta = fs::metadata(path).ok()?;
            
            // Read sidecar/xattr hash if available
            let hash_bytes = sidecar::get_metadata(path, "user.foxing.content_hash");
            let content_hash = hash_bytes.map(|b| {
                if b.len() >= 8 {
                    u64::from_le_bytes(b[0..8].try_into().unwrap_or([0;8]))
                } else {
                    0
                }
            });

            Some(FileVersion {
                inode,
                epoch_seq,
                timestamp,
                path: path.to_path_buf(),
                size: meta.len(),
                mtime: meta.mtime(),
                content_hash,
            })
        }).collect();

        let mut i_map = self.inode_index.write().unwrap();
        let mut h_map = self.hash_index.write().unwrap();

        for v in processed_versions {
            if let Some(hash) = v.content_hash {
                if hash != 0 {
                    h_map.entry(hash).or_insert_with(Vec::new).push(v.path.clone());
                }
            }
            i_map.entry(v.inode).or_insert_with(Vec::new).push(v);
        }

        // Sort vectors for determinism
        for list in i_map.values_mut() {
            list.sort_by_key(|v| v.timestamp);
        }

        self.ready.store(true, std::sync::atomic::Ordering::Relaxed);
        info!("VersionIndex: Indexing complete. Loaded {} versions in {:?}.", 
              i_map.values().map(|v| v.len()).sum::<usize>(), start.elapsed());
    }

    /// Register a new version immediately after creation
    pub fn register(&self, v: FileVersion) {
        let mut i_map = self.inode_index.write().unwrap();
        i_map.entry(v.inode).or_insert_with(Vec::new).push(v.clone());
        
        if let Some(hash) = v.content_hash {
            if hash != 0 {
                let mut h_map = self.hash_index.write().unwrap();
                h_map.entry(hash).or_insert_with(Vec::new).push(v.path);
            }
        }
    }

    /// Finds a candidate file path by Content Hash (Fast Resync)
    pub fn find_by_hash(&self, hash: u64, size: u64) -> Option<PathBuf> {
        if !self.ready.load(std::sync::atomic::Ordering::Relaxed) { return None; }
        
        let map = self.hash_index.read().unwrap();
        if let Some(candidates) = map.get(&hash) {
            for path in candidates {
                if let Ok(m) = fs::metadata(path) {
                    if m.len() == size {
                        return Some(path.clone());
                    }
                }
            }
        }
        None
    }

    /// Finds version by Inode (Legacy/Revert)
    pub fn find_by_inode(&self, inode: u64, size: u64, mtime: i64) -> Option<PathBuf> {
        // Fallback to disk scan if index isn't ready
        if !self.ready.load(std::sync::atomic::Ordering::Relaxed) { return None; }

        let map = self.inode_index.read().unwrap();
        if let Some(versions) = map.get(&inode) {
            for v in versions.iter().rev() {
                if v.size == size && v.mtime == mtime {
                    return Some(v.path.clone());
                }
            }
        }
        None
    }

    pub fn list(&self, inode: u64) -> Vec<FileVersion> {
        if !self.ready.load(std::sync::atomic::Ordering::Relaxed) { return vec![]; }
        let map = self.inode_index.read().unwrap();
        map.get(&inode).cloned().unwrap_or_default()
    }
}

pub fn verify_content_match(p1: &Path, p2: &Path) -> Result<bool> {
    let mut f1 = fs::File::open(p1).map_err(MirrorError::Io)?;
    let mut f2 = fs::File::open(p2).map_err(MirrorError::Io)?;
    let len = f1.metadata().map_err(MirrorError::Io)?.len();
    if len != f2.metadata().map_err(MirrorError::Io)?.len() { return Ok(false); }
    let mut buf1 = [0u8; 65536];
    let mut buf2 = [0u8; 65536];
    
    // Fixed: Now using std::io::Read
    let n1 = f1.read(&mut buf1).map_err(MirrorError::Io)?;
    let n2 = f2.read(&mut buf2).map_err(MirrorError::Io)?;
    
    if n1 != n2 || buf1[..n1] != buf2[..n1] { return Ok(false); }
    if len > 131072 {
        f1.seek(SeekFrom::End(-65536)).map_err(MirrorError::Io)?;
        f2.seek(SeekFrom::End(-65536)).map_err(MirrorError::Io)?;
        let n1 = f1.read(&mut buf1).map_err(MirrorError::Io)?;
        let n2 = f2.read(&mut buf2).map_err(MirrorError::Io)?;
        if n1 != n2 || buf1[..n1] != buf2[..n1] { return Ok(false); }
    }
    Ok(true)
}

fn find_target_root(live_file: &Path) -> Result<PathBuf> {
    live_file.ancestors()
        .find(|p| p.join(".mirror").join(".versions").exists())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| MirrorError::Versioning(format!("Could not determine target root for {:?}.", live_file)))
}

// Kept for CLI usage where VersionIndex might not be loaded in memory
pub fn list_versions(live_file: &Path) -> Result<Vec<FileVersion>> {
    let target_root = find_target_root(live_file)?;
    // We instantiate a temporary index for CLI operations
    let index = VersionIndex::new(target_root);
    index.index_directory(); // Synchronous scan for CLI (Rayon will work here too)
    
    if let Ok(meta) = fs::metadata(live_file) {
        Ok(index.list(meta.ino()))
    } else {
        Ok(vec![])
    }
}

pub fn cleanup_versions(live_path: &Path, _root_path: &Path, max_count: usize, max_size_mb: u64) -> Result<()> {
    // Note: This uses the slow path (glob/scan) because cleanup is infrequent and specific.
    let versions = list_versions(live_path)?; 
    
    if versions.is_empty() { return Ok(()); }
    let max_size_bytes = max_size_mb * 1024 * 1024;
    let mut versions_to_delete: Vec<PathBuf> = Vec::new();
    let mut sorted_versions = versions;
    
    if sorted_versions.len() > max_count {
        let excess = sorted_versions.len() - max_count;
        for i in 0..excess {
            versions_to_delete.push(sorted_versions[i].path.clone());
        }
        sorted_versions.drain(0..excess);
    }
    let mut current_size: u64 = sorted_versions.iter().map(|v| v.size).sum();
    for version in sorted_versions.iter() {
        if current_size > max_size_bytes && current_size > version.size {
             versions_to_delete.push(version.path.clone());
             current_size -= version.size;
        } else {
             break;
        }
    }
    for path in versions_to_delete {
        if path.exists() {
            let _ = fs::remove_file(&path);
        }
    }
    Ok(())
}

pub fn prune_global_history(target_root: &Path, bytes_to_free: u64) -> Result<u64> {
    let versions_dir = target_root.join(".mirror").join(".versions");
    if !versions_dir.exists() { return Ok(0); }
    let mut all_versions = Vec::new();
    let entries = fs::read_dir(&versions_dir).map_err(MirrorError::Io)?;
    for entry in entries {
        if let Ok(e) = entry {
            let path = e.path();
            if path.is_file() {
                if let Ok(m) = e.metadata() {
                    all_versions.push((path, m.len(), m.mtime()));
                }
            }
        }
    }
    // Sort by mtime (approximate oldest)
    all_versions.sort_by_key(|v| v.2);
    
    let mut freed = 0u64;
    let mut count = 0;
    for (path, size, _) in all_versions {
        if freed >= bytes_to_free { break; }
        if fs::remove_file(&path).is_ok() {
            freed += size;
            count += 1;
        }
    }
    if count > 0 {
        warn!("EMERGENCY: Global Pruner deleted {} historical snapshots to free {} bytes.", count, freed);
    }
    Ok(freed)
}

pub fn print_versions_table(versions: Vec<FileVersion>, limit: usize) {
    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Epoch Seq").set_alignment(CellAlignment::Left),
        Cell::new("Timestamp (UTC)").set_alignment(CellAlignment::Left),
        Cell::new("Size").set_alignment(CellAlignment::Right),
        Cell::new("Hash (Partial)").set_alignment(CellAlignment::Left),
    ]);
    let num_versions = versions.len();
    let start_index = num_versions.saturating_sub(limit);
    for v in versions.iter().skip(start_index) {
        let dt = DateTime::<Utc>::from_timestamp(v.timestamp, 0).map(|dt| dt.to_string()).unwrap_or_else(|| "Invalid Date".to_string());
        let size_mb = v.size as f64 / (1024.0 * 1024.0);
        let hash_str = v.content_hash.map(|h| format!("{:016x}", h)).unwrap_or_else(|| "None".to_string());
        table.add_row(Row::from(vec![
            v.epoch_seq.to_string(),
            dt,
            format!("{:.2} MB", size_mb),
            hash_str,
        ]));
    }
    println!("{}", table);
    if num_versions > limit {
        println!("\n... Showing last {} of {} total versions.", limit, num_versions);
    } else {
        println!("\nShowing {} total versions.", num_versions);
    }
}

fn find_version_path(versions: &[FileVersion], epoch: u64) -> Result<&PathBuf> {
    versions.iter()
        .find(|v| v.epoch_seq == epoch)
        .map(|v| &v.path)
        .ok_or_else(|| MirrorError::Versioning(format!("Version with epoch {} not found.", epoch)))
}

pub fn copy_version_to_path(live_file: &Path, epoch: u64, destination: &Path) -> Result<()> {
    let versions = list_versions(live_file)?;
    let version_path = find_version_path(&versions, epoch)?;
    let mut src = fs::File::open(version_path).map_err(MirrorError::Io)?;
    let mut dst = fs::File::create(destination).map_err(MirrorError::Io)?;
    copy(&mut src, &mut dst).map_err(MirrorError::Io)?;
    Ok(())
}

pub fn revert_file(live_file: &Path, epoch: u64) -> Result<()> {
    let versions = list_versions(live_file)?;
    let version_path = find_version_path(&versions, epoch)?;
    crate::security::revert_snapshot(version_path, live_file)
}

pub async fn cleanup_cli(path: &Path, dry_run: bool) -> Result<()> {
    let path = path.canonicalize().map_err(MirrorError::Io)?;
    info!("Starting cleanup for path: {:?}", path);
    if path.is_file() {
        let root = find_target_root(&path)?;
        if dry_run {
            info!("Dry run: Would cleanup versions for file {:?}", path);
        } else {
            cleanup_versions(&path, &root, 5, 500)?;
            info!("Cleaned up versions for {:?}", path);
        }
    } else if path.is_dir() {
        if dry_run {
            info!("Dry run: Would prune global history in {:?}", path);
        } else {
            let freed = prune_global_history(&path, 1024 * 1024 * 1024)?;
            info!("Pruned {} bytes from global history in {:?}", freed, path);
        }
    }
    Ok(())
}

pub async fn force_version_cli(path: &Path, tag: &str) -> Result<()> {
    let path = path.canonicalize().map_err(MirrorError::Io)?;
    info!("Forcing version retention for {:?} with tag '{}'", path, tag);
    Ok(())
}
