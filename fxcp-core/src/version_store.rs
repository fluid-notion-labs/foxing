// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/version_store.rs — Versioning presentation layer with JSON index

//! Hybrid point-in-time + per-file version store with JSON manifest.
//! Wraps the core versioning engine to provide human-readable naming,
//! dirvish-style browsable tree snapshots, and machine-readable indexes.

use std::path::{Path, PathBuf};
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use tracing::{info, warn, debug};

const VERSIONS_DIR: &str = ".foxing_versions";
const LIVE_DIR: &str = "live";
const INDEX_FILE: &str = "index.json";
const SUMMARY_FILE: &str = "summary.json";
const TREE_DIR: &str = "tree";
const FILES_DIR: &str = "files";

/// Filesystem-safe timestamp format (no colons).
fn format_timestamp(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let dt = chrono::DateTime::from_timestamp(secs as i64, 0)
        .unwrap_or_default();
    dt.format("%Y-%m-%dT%H%M%S").to_string()
}

/// ISO 8601 timestamp for JSON.
fn format_iso(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let dt = chrono::DateTime::from_timestamp(secs as i64, 0)
        .unwrap_or_default();
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Parse a filesystem-safe timestamp back to SystemTime.
fn parse_timestamp(s: &str) -> Option<SystemTime> {
    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H%M%S").ok()?;
    let ts = dt.and_utc().timestamp();
    Some(UNIX_EPOCH + Duration::from_secs(ts as u64))
}

// -----------------------------------------------------------------------
// JSON schema types
// -----------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionIndex {
    pub version: u32,
    pub target: String,
    pub created: String,
    pub snapshots: Vec<SnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub timestamp: String,
    #[serde(rename = "type")]
    pub snap_type: String,
    pub tag: Option<String>,
    pub files: u64,
    pub size_bytes: u64,
    #[serde(default)]
    pub disk_usage_bytes: u64,
    #[serde(default)]
    pub savings_pct: f64,
    pub source: String,
    pub trigger: String,
    pub retention: Option<RetentionPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub expires: Option<String>,
    pub policy: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub timestamp: String,
    pub status: String,
    #[serde(rename = "type")]
    pub snap_type: String,
    pub tag: Option<String>,
    pub source: String,
    pub trigger: String,
    pub files: u64,
    pub size_bytes: u64,
    #[serde(default)]
    pub disk_usage_bytes: u64,
    #[serde(default)]
    pub savings_pct: f64,
    pub reference: Option<String>,
    pub elapsed_ms: u64,
}

/// Aggregate storage statistics across all snapshots.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoreStats {
    pub snapshots: usize,
    pub total_apparent_bytes: u64,
    pub total_disk_bytes: u64,
    pub savings_pct: f64,
    pub oldest: Option<String>,
    pub newest: Option<String>,
}

#[derive(Debug, Default)]
pub struct PruneStats {
    pub snapshots_removed: u64,
    pub bytes_freed: u64,
}

/// Compute apparent size (st_size) and actual disk usage (st_blocks * 512) for a directory tree.
/// Returns (apparent_bytes, disk_bytes).
pub fn compute_disk_usage(dir: &Path) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    let mut apparent = 0u64;
    let mut disk = 0u64;
    for entry in walkdir::WalkDir::new(dir).follow_links(false) {
        let entry = match entry { Ok(e) => e, Err(_) => continue };
        if !entry.file_type().is_file() { continue; }
        if let Ok(meta) = entry.metadata() {
            apparent += meta.len();
            disk += meta.blocks() * 512;
        }
    }
    (apparent, disk)
}

fn savings_percent(apparent: u64, disk: u64) -> f64 {
    if apparent == 0 { return 0.0; }
    ((1.0 - (disk as f64 / apparent as f64)) * 1000.0).round() / 10.0
}

// -----------------------------------------------------------------------
// VersionStore
// -----------------------------------------------------------------------

pub struct VersionStore {
    root: PathBuf,
}

impl VersionStore {
    /// Open or create a version store at `{target}/.foxing_versions/`.
    pub fn open(target_root: &Path) -> Self {
        let root = target_root.join(VERSIONS_DIR);
        Self { root }
    }

    fn ensure_dirs(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.root)?;
        fs::create_dir_all(self.root.join(FILES_DIR))?;
        Ok(())
    }

    // ---- Point-in-time operations ----

    /// Create a point-in-time snapshot by reflinking all files from source→target
    /// into a timestamped tree directory.
    pub fn create_snapshot(
        &self,
        source: &Path,
        target: &Path,
        tag: Option<&str>,
        trigger: &str,
    ) -> crate::Result<SnapshotEntry> {
        let now = SystemTime::now();
        let ts_dir = format_timestamp(now);
        let ts_iso = format_iso(now);
        let snap_dir = self.root.join(&ts_dir);
        let tree_dir = snap_dir.join(TREE_DIR);

        self.ensure_dirs().map_err(crate::error::FxcpError::Io)?;
        fs::create_dir_all(&tree_dir).map_err(crate::error::FxcpError::Io)?;

        let start = std::time::Instant::now();
        let mut files = 0u64;
        let mut size_bytes = 0u64;

        // Walk target and reflink each file into the snapshot tree
        for entry in walkdir::WalkDir::new(target).follow_links(false) {
            let entry = match entry { Ok(e) => e, Err(_) => continue };
            let rel = match entry.path().strip_prefix(target) { Ok(r) => r, Err(_) => continue };
            if rel.as_os_str().is_empty() { continue; }

            // Skip our own version store
            let rel_str = rel.to_string_lossy();
            if rel_str.starts_with(VERSIONS_DIR) || rel_str.starts_with(".foxing") { continue; }

            let snap_path = tree_dir.join(rel);

            if entry.file_type().is_dir() {
                let _ = fs::create_dir_all(&snap_path);
            } else if entry.file_type().is_file() {
                if let Some(parent) = snap_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                // Try reflink (zero-cost CoW), fall back to copy
                let src_file = fs::File::open(entry.path());
                let reflinked = src_file.as_ref().ok().and_then(|f| {
                    use std::os::unix::io::AsRawFd;
                    crate::security::ioctl_ficlone(f.as_raw_fd(), &snap_path).ok()
                });
                if reflinked.is_some() {
                    files += 1;
                    size_bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
                } else if fs::copy(entry.path(), &snap_path).is_ok() {
                    files += 1;
                    size_bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;

        // Compute actual disk usage (CoW savings)
        let (apparent, disk_used) = compute_disk_usage(&tree_dir);
        let savings = savings_percent(apparent, disk_used);

        // Write summary.json
        let summary = SnapshotSummary {
            timestamp: ts_iso.clone(),
            status: "success".into(),
            snap_type: "full".into(),
            tag: tag.map(|t| t.to_string()),
            source: source.to_string_lossy().into(),
            trigger: trigger.into(),
            files,
            size_bytes,
            disk_usage_bytes: disk_used,
            savings_pct: savings,
            reference: None,
            elapsed_ms,
        };
        let summary_json = serde_json::to_string_pretty(&summary)
            .unwrap_or_default();
        let _ = fs::write(snap_dir.join(SUMMARY_FILE), summary_json);

        // Create per-file symlinks
        self.create_file_symlinks(&ts_dir, &tree_dir, target);

        // Update index
        let entry = SnapshotEntry {
            timestamp: ts_iso,
            snap_type: "full".into(),
            tag: tag.map(|t| t.to_string()),
            files,
            size_bytes,
            disk_usage_bytes: disk_used,
            savings_pct: savings,
            source: source.to_string_lossy().into(),
            trigger: trigger.into(),
            retention: None,
        };
        self.append_to_index(&entry);

        info!("Snapshot created: {} ({} files, {} bytes, {}ms)",
              ts_dir, files, size_bytes, elapsed_ms);

        Ok(entry)
    }

    fn create_file_symlinks(&self, ts_dir: &str, tree_dir: &Path, _target: &Path) {
        let files_root = self.root.join(FILES_DIR);
        for entry in walkdir::WalkDir::new(tree_dir).follow_links(false) {
            let entry = match entry { Ok(e) => e, Err(_) => continue };
            if !entry.file_type().is_file() { continue; }
            let rel = match entry.path().strip_prefix(tree_dir) { Ok(r) => r, Err(_) => continue };

            let symlink_name = format!("{}~{}", rel.to_string_lossy(), ts_dir);
            let symlink_path = files_root.join(&symlink_name);
            if let Some(parent) = symlink_path.parent() {
                let _ = fs::create_dir_all(parent);
            }

            // Relative symlink back to the tree
            let target_rel = pathdiff::diff_paths(entry.path(), symlink_path.parent().unwrap_or(&files_root));
            if let Some(target_rel) = target_rel {
                let _ = std::os::unix::fs::symlink(&target_rel, &symlink_path);
            }
        }
    }

    /// List all snapshots (point-in-time directories).
    pub fn list_snapshots(&self) -> Vec<SnapshotEntry> {
        // Try index.json first
        if let Ok(index) = self.read_index() {
            return index.snapshots;
        }
        // Fallback: scan directory
        self.scan_snapshots()
    }

    fn scan_snapshots(&self) -> Vec<SnapshotEntry> {
        let mut snapshots = Vec::new();
        let entries = match fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(_) => return snapshots,
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if parse_timestamp(&name).is_none() { continue; }
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) { continue; }

            // Read summary.json if available
            let summary_path = entry.path().join(SUMMARY_FILE);
            if let Ok(data) = fs::read_to_string(&summary_path) {
                if let Ok(summary) = serde_json::from_str::<SnapshotSummary>(&data) {
                    let (disk_usage, savings) = if summary.disk_usage_bytes > 0 {
                        (summary.disk_usage_bytes, summary.savings_pct)
                    } else {
                        // Compute on demand for old-format summaries
                        let tree = entry.path().join(TREE_DIR);
                        if tree.exists() {
                            let (app, dsk) = compute_disk_usage(&tree);
                            (dsk, savings_percent(app, dsk))
                        } else {
                            (0, 0.0)
                        }
                    };
                    snapshots.push(SnapshotEntry {
                        timestamp: summary.timestamp,
                        snap_type: summary.snap_type,
                        tag: summary.tag,
                        files: summary.files,
                        size_bytes: summary.size_bytes,
                        disk_usage_bytes: disk_usage,
                        savings_pct: savings,
                        source: summary.source,
                        trigger: summary.trigger,
                        retention: None,
                    });
                    continue;
                }
            }

            // Minimal entry — compute disk usage from tree if available
            let tree = entry.path().join(TREE_DIR);
            let (apparent, disk, files_count) = if tree.exists() {
                let (a, d) = compute_disk_usage(&tree);
                let fc = walkdir::WalkDir::new(&tree).into_iter()
                    .filter_map(|e| e.ok()).filter(|e| e.file_type().is_file()).count() as u64;
                (a, d, fc)
            } else {
                (0, 0, 0)
            };
            snapshots.push(SnapshotEntry {
                timestamp: name,
                snap_type: "unknown".into(),
                tag: None,
                files: files_count,
                size_bytes: apparent,
                disk_usage_bytes: disk,
                savings_pct: savings_percent(apparent, disk),
                source: String::new(),
                trigger: "unknown".into(),
                retention: None,
            });
        }
        snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        snapshots
    }

    /// Delete a snapshot by timestamp.
    pub fn delete_snapshot(&self, timestamp: &str) -> crate::Result<u64> {
        // Find directory matching timestamp (filesystem-safe format)
        let snap_dir = self.root.join(timestamp);
        if !snap_dir.exists() {
            // Try ISO format → filesystem-safe conversion
            let fs_safe = timestamp.replace(':', "").replace("-T", "T");
            let alt_dir = self.root.join(&fs_safe);
            if alt_dir.exists() {
                return self.remove_snapshot_dir(&alt_dir);
            }
            return Err(crate::error::FxcpError::Config(
                format!("Snapshot not found: {}", timestamp)
            ));
        }
        self.remove_snapshot_dir(&snap_dir)
    }

    fn remove_snapshot_dir(&self, dir: &Path) -> crate::Result<u64> {
        let mut freed = 0u64;
        for entry in walkdir::WalkDir::new(dir).contents_first(true) {
            let entry = match entry { Ok(e) => e, Err(_) => continue };
            if entry.file_type().is_file() || entry.file_type().is_symlink() {
                freed += entry.metadata().map(|m| m.len()).unwrap_or(0);
                let _ = fs::remove_file(entry.path());
            } else if entry.file_type().is_dir() {
                let _ = fs::remove_dir(entry.path());
            }
        }
        // Rebuild index
        let _ = self.rebuild_index();
        Ok(freed)
    }

    // ---- Index management ----

    pub fn read_index(&self) -> crate::Result<VersionIndex> {
        let path = self.root.join(INDEX_FILE);
        let data = fs::read_to_string(&path).map_err(crate::error::FxcpError::Io)?;
        serde_json::from_str(&data).map_err(|e|
            crate::error::FxcpError::Config(format!("Invalid index.json: {}", e))
        )
    }

    fn append_to_index(&self, entry: &SnapshotEntry) {
        let mut index = self.read_index().unwrap_or(VersionIndex {
            version: 1,
            target: self.root.parent()
                .map(|p| p.to_string_lossy().into())
                .unwrap_or_default(),
            created: format_iso(SystemTime::now()),
            snapshots: vec![],
        });
        index.snapshots.push(entry.clone());
        let _ = self.write_index(&index);
    }

    pub fn write_index(&self, index: &VersionIndex) -> crate::Result<()> {
        let _ = self.ensure_dirs();
        let json = serde_json::to_string_pretty(index)
            .map_err(|e| crate::error::FxcpError::Config(format!("JSON error: {}", e)))?;
        fs::write(self.root.join(INDEX_FILE), json).map_err(crate::error::FxcpError::Io)
    }

    pub fn rebuild_index(&self) -> crate::Result<VersionIndex> {
        let snapshots = self.scan_snapshots();
        let index = VersionIndex {
            version: 1,
            target: self.root.parent()
                .map(|p| p.to_string_lossy().into())
                .unwrap_or_default(),
            created: format_iso(SystemTime::now()),
            snapshots,
        };
        self.write_index(&index)?;
        Ok(index)
    }

    // ---- Maintenance / Pruning ----

    /// Remove snapshots older than `max_age`.
    pub fn prune_by_age(&self, max_age: Duration) -> crate::Result<PruneStats> {
        let cutoff = SystemTime::now() - max_age;
        let mut stats = PruneStats::default();
        let snapshots = self.scan_snapshots();
        for snap in &snapshots {
            let ts_fs = snap.timestamp.replace(':', "").replace("-T", "T")
                .trim_end_matches('Z').to_string();
            if let Some(t) = parse_timestamp(&ts_fs) {
                if t < cutoff {
                    // Skip tagged snapshots
                    if snap.tag.is_some() {
                        debug!("Skipping tagged snapshot: {}", snap.timestamp);
                        continue;
                    }
                    match self.delete_snapshot(&ts_fs) {
                        Ok(freed) => {
                            stats.snapshots_removed += 1;
                            stats.bytes_freed += freed;
                            info!("Pruned snapshot: {}", snap.timestamp);
                        }
                        Err(e) => warn!("Failed to prune {}: {}", snap.timestamp, e),
                    }
                }
            }
        }
        let _ = self.rebuild_index();
        Ok(stats)
    }

    /// Keep only the last `max_count` snapshots (oldest removed first).
    pub fn prune_by_count(&self, max_count: usize) -> crate::Result<PruneStats> {
        let mut stats = PruneStats::default();
        let mut snapshots = self.scan_snapshots();
        if snapshots.len() <= max_count { return Ok(stats); }

        // Sort oldest first, remove from front
        snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        let to_remove = snapshots.len() - max_count;
        for snap in snapshots.iter().take(to_remove) {
            if snap.tag.is_some() { continue; }
            let ts_fs = snap.timestamp.replace(':', "").replace("-T", "T")
                .trim_end_matches('Z').to_string();
            match self.delete_snapshot(&ts_fs) {
                Ok(freed) => {
                    stats.snapshots_removed += 1;
                    stats.bytes_freed += freed;
                    info!("Pruned snapshot: {}", snap.timestamp);
                }
                Err(e) => warn!("Failed to prune {}: {}", snap.timestamp, e),
            }
        }
        let _ = self.rebuild_index();
        Ok(stats)
    }

    /// Remove oldest snapshots until total size is under `max_bytes`.
    pub fn prune_by_size(&self, max_bytes: u64) -> crate::Result<PruneStats> {
        let mut stats = PruneStats::default();
        let mut snapshots = self.scan_snapshots();
        snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let total: u64 = snapshots.iter().map(|s| s.size_bytes).sum();
        if total <= max_bytes { return Ok(stats); }

        let mut remaining = total;
        for snap in &snapshots {
            if remaining <= max_bytes { break; }
            if snap.tag.is_some() { continue; }
            let ts_fs = snap.timestamp.replace(':', "").replace("-T", "T")
                .trim_end_matches('Z').to_string();
            match self.delete_snapshot(&ts_fs) {
                Ok(freed) => {
                    stats.snapshots_removed += 1;
                    stats.bytes_freed += freed;
                    remaining = remaining.saturating_sub(snap.size_bytes);
                    info!("Pruned snapshot: {} (freed {} bytes)", snap.timestamp, freed);
                }
                Err(e) => warn!("Failed to prune {}: {}", snap.timestamp, e),
            }
        }
        let _ = self.rebuild_index();
        Ok(stats)
    }

    // ---- Export / Import ----

    /// Export snapshots as a .fxar archive (content-addressable chunk-dedup).
    /// Writes to the provided writer (file or stdout).
    pub fn export<W: std::io::Write + 'static>(
        &self,
        writer: W,
        compress: &str,
        timestamp_filter: Option<&str>,
    ) -> crate::Result<ExportStats> {
        use std::io::Read;

        let mut stats = ExportStats::default();
        let mut builder = tar::Builder::new(wrap_compressor(writer, compress));

        // Collect snapshot directories to export
        let snap_dirs: Vec<_> = if let Some(ts) = timestamp_filter {
            let dir = self.root.join(ts);
            if dir.exists() { vec![dir] } else {
                return Err(crate::error::FxcpError::Config(format!("Snapshot not found: {}", ts)));
            }
        } else {
            fs::read_dir(&self.root).ok()
                .map(|entries| entries.flatten()
                    .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                    .filter(|e| parse_timestamp(&e.file_name().to_string_lossy()).is_some())
                    .map(|e| e.path())
                    .collect())
                .unwrap_or_default()
        };

        // Track unique chunks for dedup
        let mut seen_chunks: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Add index.json
        let index_path = self.root.join(INDEX_FILE);
        if index_path.exists() {
            builder.append_path_with_name(&index_path, INDEX_FILE)
                .map_err(|e| crate::error::FxcpError::Io(e.into()))?;
        }

        for snap_dir in &snap_dirs {
            let snap_name = snap_dir.file_name().unwrap_or_default().to_string_lossy().to_string();

            // Add summary.json
            let summary_path = snap_dir.join(SUMMARY_FILE);
            if summary_path.exists() {
                let archive_path = format!("meta/{}/{}", snap_name, SUMMARY_FILE);
                builder.append_path_with_name(&summary_path, &archive_path)
                    .map_err(|e| crate::error::FxcpError::Io(e.into()))?;
            }

            // Walk tree and add files as chunks
            let tree_dir = snap_dir.join(TREE_DIR);
            if !tree_dir.exists() { continue; }

            for entry in walkdir::WalkDir::new(&tree_dir).follow_links(false) {
                let entry = match entry { Ok(e) => e, Err(_) => continue };
                if !entry.file_type().is_file() { continue; }

                let rel = match entry.path().strip_prefix(&self.root) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => continue,
                };

                stats.total_files += 1;
                let file_size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                stats.total_apparent_bytes += file_size;

                // Try to get BLAKE3 hash for chunk dedup
                let hash = crate::hashing::hash_file_full(entry.path())
                    .ok()
                    .flatten()
                    .map(|h| hex::encode(h.as_bytes()))
                    .unwrap_or_default();

                if !hash.is_empty() && seen_chunks.contains(&hash) {
                    // Chunk already in archive — just record in manifest
                    stats.dedup_chunks += 1;
                    stats.dedup_bytes += file_size;
                    // Add a small manifest entry instead of file data
                    let manifest_entry = format!("{}|{}|{}\n", rel.display(), hash, file_size);
                    let data = manifest_entry.as_bytes();
                    let mut header = tar::Header::new_gnu();
                    header.set_size(data.len() as u64);
                    header.set_mode(0o644);
                    header.set_cksum();
                    let dedup_path = format!("dedup/{}", rel.display());
                    builder.append_data(&mut header, &dedup_path, data)
                        .map_err(|e| crate::error::FxcpError::Io(e.into()))?;
                } else {
                    // New unique chunk — add file data
                    if !hash.is_empty() { seen_chunks.insert(hash.clone()); }
                    stats.unique_chunks += 1;
                    stats.unique_bytes += file_size;

                    let mut file = fs::File::open(entry.path()).map_err(crate::error::FxcpError::Io)?;
                    let mut header = tar::Header::new_gnu();
                    header.set_size(file_size);
                    header.set_mode(entry.metadata().map(|m| {
                        use std::os::unix::fs::PermissionsExt;
                        m.permissions().mode()
                    }).unwrap_or(0o644));
                    header.set_mtime(entry.metadata().map(|m| {
                        use std::os::unix::fs::MetadataExt;
                        m.mtime() as u64
                    }).unwrap_or(0));
                    header.set_cksum();
                    builder.append_data(&mut header, rel.to_string_lossy().as_ref(), &mut file)
                        .map_err(|e| crate::error::FxcpError::Io(e.into()))?;
                }
            }
            stats.snapshots_exported += 1;
        }

        builder.finish().map_err(|e| crate::error::FxcpError::Io(e.into()))?;

        info!("Export: {} snapshots, {} files ({} unique, {} deduped), apparent {} → archive {}",
              stats.snapshots_exported, stats.total_files, stats.unique_chunks, stats.dedup_chunks,
              format_size(stats.total_apparent_bytes), format_size(stats.unique_bytes));

        Ok(stats)
    }

    /// Import snapshots from a .fxar archive.
    pub fn import<R: std::io::Read + 'static>(
        &self,
        reader: R,
        compress: &str,
    ) -> crate::Result<ImportStats> {
        let mut stats = ImportStats::default();
        let decompressed = wrap_import_decompressor(reader, compress);
        let mut archive = tar::Archive::new(decompressed);

        self.ensure_dirs().map_err(crate::error::FxcpError::Io)?;

        for entry in archive.entries().map_err(|e| crate::error::FxcpError::Io(e.into()))? {
            let mut entry = match entry { Ok(e) => e, Err(_) => continue };
            let path = entry.path()
                .map(|p| p.to_path_buf())
                .unwrap_or_default();
            let path_str = path.to_string_lossy().to_string();

            if path_str == INDEX_FILE {
                // Restore index.json
                entry.unpack(self.root.join(INDEX_FILE))
                    .map_err(|e| crate::error::FxcpError::Io(e.into()))?;
                stats.metadata_files += 1;
            } else if path_str.starts_with("meta/") {
                // Restore summary.json files
                let dest = self.root.join(path_str.trim_start_matches("meta/")
                    .split('/').next().unwrap_or(""))
                    .join(SUMMARY_FILE);
                if let Some(parent) = dest.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                entry.unpack(&dest).map_err(|e| crate::error::FxcpError::Io(e.into()))?;
                stats.metadata_files += 1;
            } else if path_str.starts_with("dedup/") {
                // Dedup reference — skip (file already exists from another snapshot)
                stats.dedup_refs += 1;
            } else {
                // Regular file data — restore to version store
                let dest = self.root.join(&path_str);
                if let Some(parent) = dest.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                entry.unpack(&dest).map_err(|e| crate::error::FxcpError::Io(e.into()))?;
                stats.files_restored += 1;
                stats.bytes_restored += entry.header().size().unwrap_or(0);
            }
        }

        // Rebuild index from restored state
        let _ = self.rebuild_index();

        info!("Import: {} files restored, {} bytes, {} dedup refs, {} metadata files",
              stats.files_restored, stats.bytes_restored, stats.dedup_refs, stats.metadata_files);

        Ok(stats)
    }

    /// List contents of a .fxar archive without extracting.
    pub fn inspect_archive<R: std::io::Read + 'static>(reader: R, compress: &str) -> crate::Result<Vec<ArchiveEntry>> {
        let decompressed = wrap_import_decompressor(reader, compress);
        let mut archive = tar::Archive::new(decompressed);
        let mut entries = Vec::new();

        for entry in archive.entries().map_err(|e| crate::error::FxcpError::Io(e.into()))? {
            let entry = match entry { Ok(e) => e, Err(_) => continue };
            let path = entry.path().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
            let size = entry.header().size().unwrap_or(0);
            let is_dedup = path.starts_with("dedup/");
            let is_meta = path.starts_with("meta/") || path == INDEX_FILE;

            entries.push(ArchiveEntry {
                path,
                size,
                is_dedup_ref: is_dedup,
                is_metadata: is_meta,
            });
        }

        Ok(entries)
    }
}

// -----------------------------------------------------------------------
// Export / Import types and compression helpers
// -----------------------------------------------------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ExportStats {
    pub snapshots_exported: u64,
    pub total_files: u64,
    pub unique_chunks: u64,
    pub dedup_chunks: u64,
    pub total_apparent_bytes: u64,
    pub unique_bytes: u64,
    pub dedup_bytes: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ImportStats {
    pub files_restored: u64,
    pub bytes_restored: u64,
    pub dedup_refs: u64,
    pub metadata_files: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveEntry {
    pub path: String,
    pub size: u64,
    pub is_dedup_ref: bool,
    pub is_metadata: bool,
}

fn wrap_compressor<W: std::io::Write + 'static>(writer: W, compress: &str) -> Box<dyn std::io::Write> {
    match compress {
        "none" => Box::new(writer),
        s if s.starts_with("zstd") => {
            let level = s.strip_prefix("zstd:").and_then(|l| l.parse().ok()).unwrap_or(3);
            let enc = zstd::stream::Encoder::new(writer, level).expect("zstd encoder init failed");
            Box::new(enc.auto_finish())
        }
        "lz4" => Box::new(lz4_flex::frame::FrameEncoder::new(writer)),
        "gzip" => Box::new(flate2::write::GzEncoder::new(writer, flate2::Compression::default())),
        s if s.starts_with("xz") => {
            let level = s.strip_prefix("xz:").and_then(|l| l.parse().ok()).unwrap_or(6);
            Box::new(xz2::write::XzEncoder::new(writer, level))
        }
        _ => {
            // Default to zstd level 3
            let enc = zstd::stream::Encoder::new(writer, 3).expect("zstd encoder init failed");
            Box::new(enc.auto_finish())
        }
    }
}

pub fn wrap_import_decompressor<R: std::io::Read + 'static>(reader: R, compress: &str) -> Box<dyn std::io::Read> {
    match compress {
        "none" => Box::new(reader),
        "zstd" => {
            let dec = zstd::stream::Decoder::new(reader).expect("zstd decoder init failed");
            Box::new(dec)
        }
        "lz4" => Box::new(lz4_flex::frame::FrameDecoder::new(reader)),
        "gzip" => Box::new(flate2::read::GzDecoder::new(reader)),
        "xz" => Box::new(xz2::read::XzDecoder::new(reader)),
        "auto" | _ => {
            // For auto-detect, we'd need to peek at magic bytes.
            // Default to raw (caller should specify or use file extension).
            Box::new(reader)
        }
    }
}

// -----------------------------------------------------------------------
// Display helpers
// -----------------------------------------------------------------------

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// Print snapshots as a formatted table with CoW storage stats.
pub fn print_snapshot_table(snapshots: &[SnapshotEntry]) {
    use comfy_table::{Table, Cell, CellAlignment};

    if snapshots.is_empty() {
        println!("No snapshots found.");
        return;
    }

    let mut table = Table::new();
    table.set_header(vec![
        Cell::new("Timestamp").set_alignment(CellAlignment::Left),
        Cell::new("Type").set_alignment(CellAlignment::Left),
        Cell::new("Files").set_alignment(CellAlignment::Right),
        Cell::new("Apparent").set_alignment(CellAlignment::Right),
        Cell::new("On-Disk").set_alignment(CellAlignment::Right),
        Cell::new("Savings").set_alignment(CellAlignment::Right),
        Cell::new("Tag").set_alignment(CellAlignment::Left),
    ]);

    let mut total_apparent = 0u64;
    let mut total_disk = 0u64;

    for snap in snapshots {
        total_apparent += snap.size_bytes;
        total_disk += snap.disk_usage_bytes;

        table.add_row(vec![
            Cell::new(&snap.timestamp),
            Cell::new(&snap.snap_type),
            Cell::new(snap.files.to_string()),
            Cell::new(format_size(snap.size_bytes)),
            Cell::new(format_size(snap.disk_usage_bytes)),
            Cell::new(format!("{:.1}%", snap.savings_pct)),
            Cell::new(snap.tag.as_deref().unwrap_or("")),
        ]);
    }

    println!("{table}");
    let total_savings = savings_percent(total_apparent, total_disk);
    println!("\n{} snapshots | Apparent: {} | On-Disk: {} | Savings: {:.1}%",
             snapshots.len(), format_size(total_apparent), format_size(total_disk), total_savings);
}

/// Compute aggregate storage statistics across all snapshots.
pub fn compute_store_stats(snapshots: &[SnapshotEntry]) -> StoreStats {
    let total_apparent: u64 = snapshots.iter().map(|s| s.size_bytes).sum();
    let total_disk: u64 = snapshots.iter().map(|s| s.disk_usage_bytes).sum();
    StoreStats {
        snapshots: snapshots.len(),
        total_apparent_bytes: total_apparent,
        total_disk_bytes: total_disk,
        savings_pct: savings_percent(total_apparent, total_disk),
        oldest: snapshots.first().map(|s| s.timestamp.clone()),
        newest: snapshots.last().map(|s| s.timestamp.clone()),
    }
}

/// Print detailed storage stats.
pub fn print_store_stats(stats: &StoreStats, path: &Path) {
    println!("Snapshot Store: {}/.foxing_versions/", path.display());
    println!("  Snapshots:      {}", stats.snapshots);
    println!("  Total Apparent: {}  (if all copies were independent)", format_size(stats.total_apparent_bytes));
    println!("  Total On-Disk:  {}   (actual exclusive storage)", format_size(stats.total_disk_bytes));
    let saved = stats.total_apparent_bytes.saturating_sub(stats.total_disk_bytes);
    println!("  CoW Savings:    {:.1}%    ({} saved via reflinks)", stats.savings_pct, format_size(saved));
    if let Some(ref oldest) = stats.oldest {
        println!("  Oldest:         {}", oldest);
    }
    if let Some(ref newest) = stats.newest {
        println!("  Newest:         {}", newest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_timestamp() {
        let t = UNIX_EPOCH + Duration::from_secs(1773484800); // 2026-03-12T08:00:00 UTC
        let s = format_timestamp(t);
        assert!(s.starts_with("2026"), "expected 2026, got: {}", s);
        assert!(!s.contains(':'), "should not contain colons: {}", s);
        assert!(s.contains('T'), "should contain T separator: {}", s);
    }

    #[test]
    fn test_parse_timestamp_roundtrip() {
        let t = SystemTime::now();
        let s = format_timestamp(t);
        let parsed = parse_timestamp(&s);
        assert!(parsed.is_some());
    }

    #[test]
    fn test_format_iso() {
        let t = UNIX_EPOCH + Duration::from_secs(1773484800);
        let s = format_iso(t);
        assert!(s.contains(':'));
        assert!(s.ends_with('Z'));
    }

    #[test]
    fn test_version_store_open() {
        let store = VersionStore::open(Path::new("/tmp/test"));
        assert_eq!(store.root, PathBuf::from("/tmp/test/.foxing_versions"));
    }

    #[test]
    fn test_snapshot_entry_json_roundtrip() {
        let entry = SnapshotEntry {
            timestamp: "2026-03-12T08:45:00Z".into(),
            snap_type: "full".into(),
            tag: Some("pre-migration".into()),
            files: 100,
            size_bytes: 1048576,
            disk_usage_bytes: 4096,
            savings_pct: 99.6,
            source: "/mnt/source".into(),
            trigger: "fxcp --snapshot".into(),
            retention: None,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: SnapshotEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.files, 100);
        assert_eq!(parsed.tag, Some("pre-migration".into()));
    }

    #[test]
    fn test_index_json_roundtrip() {
        let index = VersionIndex {
            version: 1,
            target: "/mnt/backup".into(),
            created: "2026-03-12T08:00:00Z".into(),
            snapshots: vec![],
        };
        let json = serde_json::to_string_pretty(&index).unwrap();
        let parsed: VersionIndex = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, 1);
    }
}
