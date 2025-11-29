use std::path::{Path, PathBuf};
use crate::error::{Result, FoxingError as MirrorError};
use glob::glob;
use regex::Regex;
use comfy_table::{Table, Row, Cell, CellAlignment};
use chrono::{DateTime, Utc};
use std::fs;
use std::io::copy;
use std::os::unix::fs::MetadataExt; 
use tracing::warn;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct FileVersion {
    pub inode: u64,
    pub epoch_seq: u64,
    pub timestamp: i64,
    pub path: PathBuf,
    pub size: u64,
}

fn find_target_root(live_file: &Path) -> Result<PathBuf> {
    live_file.ancestors()
        .find(|p| p.join(".mirror").join(".versions").exists())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| MirrorError::Versioning(format!("Could not determine target root for {:?}. Ensure .mirror/.versions exists on a parent directory.", live_file)))
}

// Per-File Cleanup (Standard Maintenance)
pub fn cleanup_versions(live_path: &Path, root_path: &Path, max_count: usize, max_size_mb: u64) -> Result<()> {
    let versions = list_versions_for_inode(live_path, root_path)?;
    if versions.is_empty() { return Ok(()); }

    let max_size_bytes = max_size_mb * 1024 * 1024;
    let mut versions_to_delete: Vec<PathBuf> = Vec::new();
    
    let mut sorted_versions = versions;
    sorted_versions.sort_by_key(|v| v.timestamp); 

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

// NEW: Global Emergency Pruning (Cannibalization)
// Scans the entire .versions directory and deletes oldest files globally to free space.
pub fn prune_global_history(target_root: &Path, bytes_to_free: u64) -> Result<u64> {
    let versions_dir = target_root.join(".mirror").join(".versions");
    if !versions_dir.exists() { return Ok(0); }

    let mut all_versions = Vec::new();
    let entries = fs::read_dir(&versions_dir).map_err(MirrorError::Io)?;

    // 1. Catalog all versions
    for entry in entries {
        if let Ok(e) = entry {
            let path = e.path();
            if path.is_file() {
                if let Ok(m) = e.metadata() {
                    // Parse timestamp from filename: {inode}_{seq}_{ts}
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                         let parts: Vec<&str> = name.split('_').collect();
                         if parts.len() >= 3 {
                             if let Ok(ts) = parts[parts.len()-1].parse::<i64>() {
                                 all_versions.push(FileVersion {
                                     inode: 0, // Irrelevant for global prune
                                     epoch_seq: 0,
                                     timestamp: ts,
                                     path,
                                     size: m.len()
                                 });
                             }
                         }
                    }
                }
            }
        }
    }

    // 2. Sort by oldest first
    all_versions.sort_by_key(|v| v.timestamp);

    // 3. Delete until quota met
    let mut freed = 0u64;
    let mut count = 0;
    
    for v in all_versions {
        if freed >= bytes_to_free { break; }
        
        if fs::remove_file(&v.path).is_ok() {
            freed += v.size; // Logical size (approximate physical gain)
            count += 1;
        }
    }

    if count > 0 {
        warn!("EMERGENCY: Global Pruner deleted {} historical snapshots to free {} bytes.", count, freed);
    }

    Ok(freed)
}

fn list_versions_for_inode(live_file: &Path, root_path: &Path) -> Result<Vec<FileVersion>> {
    let metadata = fs::metadata(live_file).map_err(|e| MirrorError::Io(e))?;
    let live_inode = metadata.ino();
    
    let versions_dir = root_path.join(".mirror").join(".versions");
    let pattern = versions_dir.join(format!("{}_*_*", live_inode));
    
    let pattern_str = pattern.to_str()
        .ok_or_else(|| MirrorError::Versioning(format!("Invalid path pattern: {:?}", pattern)))?;

    let glob_results = glob(pattern_str).map_err(|e| MirrorError::Versioning(format!("Glob error: {}", e)))?;
    let file_regex = Regex::new(&format!(r"^{}_(\d+)_(\d+)$", live_inode)).unwrap();
    
    let mut versions = Vec::new();

    for entry in glob_results {
        if let Ok(path) = entry {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(caps) = file_regex.captures(file_name) {
                    if caps.len() == 3 {
                        let epoch_seq: u64 = caps[1].parse().unwrap_or(0);
                        let timestamp: i64 = caps[2].parse().unwrap_or(0);
                        
                        if let Ok(meta) = fs::metadata(&path) {
                             versions.push(FileVersion {
                                inode: live_inode,
                                epoch_seq,
                                timestamp,
                                path,
                                size: meta.len(),
                            });
                        }
                    }
                }
            }
        }
    }
    
    versions.sort_by_key(|v| v.timestamp);
    Ok(versions)
}

pub fn list_versions(live_file: &Path) -> Result<Vec<FileVersion>> {
    let target_root = find_target_root(live_file)?;
    list_versions_for_inode(live_file, &target_root)
}

pub fn print_versions_table(versions: Vec<FileVersion>, limit: usize) {
    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Epoch Seq").set_alignment(CellAlignment::Left),
        Cell::new("Timestamp (UTC)").set_alignment(CellAlignment::Left),
        Cell::new("Size").set_alignment(CellAlignment::Right),
        Cell::new("Version File").set_alignment(CellAlignment::Left),
    ]);

    let num_versions = versions.len();
    let start_index = num_versions.saturating_sub(limit);
    
    for v in versions.iter().skip(start_index) {
        let dt = DateTime::<Utc>::from_timestamp(v.timestamp, 0).map(|dt| dt.to_string()).unwrap_or_else(|| "Invalid Date".to_string());
        let size_mb = v.size as f64 / (1024.0 * 1024.0);
        
        table.add_row(Row::from(vec![
            v.epoch_seq.to_string(),
            dt,
            format!("{:.2} MB", size_mb),
            v.path.file_name().and_then(|n| n.to_str()).unwrap_or("N/A").to_string(),
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
    let target_root = find_target_root(live_file)?;
    let versions = list_versions_for_inode(live_file, &target_root)?;
    let version_path = find_version_path(&versions, epoch)?;
    let mut src = fs::File::open(version_path).map_err(MirrorError::Io)?;
    let mut dst = fs::File::create(destination).map_err(MirrorError::Io)?;
    copy(&mut src, &mut dst).map_err(MirrorError::Io)?;
    Ok(())
}

pub fn revert_file(live_file: &Path, epoch: u64) -> Result<()> {
    let target_root = find_target_root(live_file)?;
    let versions = list_versions_for_inode(live_file, &target_root)?;
    let version_path = find_version_path(&versions, epoch)?;
    crate::security::revert_snapshot(version_path, live_file)
}
