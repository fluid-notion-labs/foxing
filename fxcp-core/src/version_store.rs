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
    pub reference: Option<String>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Default)]
pub struct PruneStats {
    pub snapshots_removed: u64,
    pub bytes_freed: u64,
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
                    snapshots.push(SnapshotEntry {
                        timestamp: summary.timestamp,
                        snap_type: summary.snap_type,
                        tag: summary.tag,
                        files: summary.files,
                        size_bytes: summary.size_bytes,
                        source: summary.source,
                        trigger: summary.trigger,
                        retention: None,
                    });
                    continue;
                }
            }

            // Minimal entry from directory name
            snapshots.push(SnapshotEntry {
                timestamp: name,
                snap_type: "unknown".into(),
                tag: None,
                files: 0,
                size_bytes: 0,
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
}

// -----------------------------------------------------------------------
// Display helpers
// -----------------------------------------------------------------------

/// Print snapshots as a formatted table.
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
        Cell::new("Size").set_alignment(CellAlignment::Right),
        Cell::new("Tag").set_alignment(CellAlignment::Left),
        Cell::new("Trigger").set_alignment(CellAlignment::Left),
    ]);

    for snap in snapshots {
        let size_str = if snap.size_bytes > 1_073_741_824 {
            format!("{:.1} GB", snap.size_bytes as f64 / 1_073_741_824.0)
        } else if snap.size_bytes > 1_048_576 {
            format!("{:.1} MB", snap.size_bytes as f64 / 1_048_576.0)
        } else {
            format!("{} KB", snap.size_bytes / 1024)
        };

        table.add_row(vec![
            Cell::new(&snap.timestamp),
            Cell::new(&snap.snap_type),
            Cell::new(snap.files.to_string()),
            Cell::new(&size_str),
            Cell::new(snap.tag.as_deref().unwrap_or("")),
            Cell::new(&snap.trigger),
        ]);
    }

    println!("{table}");
    println!("\n{} snapshots.", snapshots.len());
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
