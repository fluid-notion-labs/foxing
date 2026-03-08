// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/tombstone.rs — Persistent tombstone journal for delete propagation

//! Append-only JSONL journal recording file/directory deletions.
//!
//! Used by foxingd to persist deletion events across daemon restarts, and by
//! fxcp `--delete` to avoid full target tree walks. Entries are idempotent —
//! replaying a tombstone for an already-deleted path is a no-op.
//!
//! Format: one JSON object per line (JSONL), crash-resilient for lines < PIPE_BUF.
//! ```json
//! {"p":"relative/path.txt","d":false,"t":1741400000,"s":42}
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, warn};

/// A single tombstone entry recording a deletion event.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TombstoneEntry {
    /// Relative path from source/target root.
    #[serde(rename = "p")]
    pub rel_path: PathBuf,
    /// Whether this was a directory removal (Rmdir) vs file (Unlink).
    #[serde(rename = "d")]
    pub is_dir: bool,
    /// Wall-clock timestamp of the deletion (UTC epoch seconds).
    #[serde(rename = "t")]
    pub timestamp: i64,
    /// BPF sequence number at time of capture (0 if from fxcp scan).
    #[serde(rename = "s")]
    pub seq: u64,
}

/// Persistent append-only tombstone journal backed by a JSONL file.
pub struct TombstoneJournal {
    path: PathBuf,
    entry_count: AtomicU64,
}

impl TombstoneJournal {
    /// Open or create a tombstone journal at the given path.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let count = if path.exists() {
            let file = File::open(path)?;
            BufReader::new(file).lines().count() as u64
        } else {
            0
        };
        Ok(Self {
            path: path.to_path_buf(),
            entry_count: AtomicU64::new(count),
        })
    }

    /// Append a tombstone entry to the journal.
    ///
    /// Uses append mode — atomic for lines shorter than PIPE_BUF (4096 bytes)
    /// on POSIX systems when the journal is on a local filesystem.
    pub fn append(&self, entry: &TombstoneEntry) -> std::io::Result<()> {
        let mut line = serde_json::to_string(entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        line.push('\n');

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Read all valid entries from the journal, skipping malformed lines.
    pub fn read_all(&self) -> std::io::Result<Vec<TombstoneEntry>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for (line_num, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    warn!("tombstone journal line {}: read error: {}", line_num + 1, e);
                    continue;
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<TombstoneEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    debug!("tombstone journal line {}: malformed, skipping: {}", line_num + 1, e);
                }
            }
        }
        Ok(entries)
    }

    /// Remove entries older than `max_age_secs` and atomically replace the journal.
    ///
    /// Also removes entries whose path has been re-created (nullification):
    /// if a Create event with a later timestamp exists for the same path,
    /// the tombstone is stale.
    pub fn compact(&self, max_age_secs: i64) -> std::io::Result<u64> {
        let entries = self.read_all()?;
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - max_age_secs;

        let retained: Vec<&TombstoneEntry> = entries.iter()
            .filter(|e| e.timestamp >= cutoff)
            .collect();

        let removed = entries.len() as u64 - retained.len() as u64;

        // Atomic replace: write to temp file, then rename
        let tmp_path = self.path.with_extension("jsonl.tmp");
        {
            let mut tmp = File::create(&tmp_path)?;
            for entry in &retained {
                let mut line = serde_json::to_string(entry)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                line.push('\n');
                tmp.write_all(line.as_bytes())?;
            }
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, &self.path)?;
        self.entry_count.store(retained.len() as u64, Ordering::Relaxed);

        Ok(removed)
    }

    /// Clear the journal (truncate to empty).
    pub fn clear(&self) -> std::io::Result<()> {
        if self.path.exists() {
            File::create(&self.path)?; // truncate
            self.entry_count.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Current number of entries (approximate — may lag slightly under concurrent appends).
    pub fn len(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Whether the journal is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Path to the journal file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Replay tombstone entries against a target directory, removing files/dirs.
///
/// Returns the number of successfully removed entries.
pub fn replay_tombstones(
    target: &Path,
    entries: &[TombstoneEntry],
    excludes: &[glob::Pattern],
) -> crate::Result<u64> {
    let mut deleted = 0u64;
    for entry in entries {
        if excludes.iter().any(|p| p.matches_path(&entry.rel_path)) {
            continue;
        }
        let target_path = target.join(&entry.rel_path);
        let result = if entry.is_dir {
            fs::remove_dir(&target_path)
        } else {
            fs::remove_file(&target_path)
        };
        match result {
            Ok(()) => { deleted += 1; }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // already gone
            Err(e) => {
                debug!("tombstone replay {:?}: {}", entry.rel_path, e);
            }
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_journal() -> (tempfile::TempDir, TombstoneJournal) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".foxing_tombstones.jsonl");
        let journal = TombstoneJournal::open(&path).unwrap();
        (dir, journal)
    }

    #[test]
    fn test_append_and_read() {
        let (_dir, journal) = tmp_journal();

        journal.append(&TombstoneEntry {
            rel_path: PathBuf::from("foo/bar.txt"),
            is_dir: false,
            timestamp: 1000,
            seq: 1,
        }).unwrap();

        journal.append(&TombstoneEntry {
            rel_path: PathBuf::from("baz/"),
            is_dir: true,
            timestamp: 1001,
            seq: 2,
        }).unwrap();

        assert_eq!(journal.len(), 2);

        let entries = journal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].rel_path, PathBuf::from("foo/bar.txt"));
        assert!(!entries[0].is_dir);
        assert_eq!(entries[1].rel_path, PathBuf::from("baz/"));
        assert!(entries[1].is_dir);
    }

    #[test]
    fn test_malformed_lines_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".foxing_tombstones.jsonl");

        // Write a mix of valid and invalid lines
        let mut f = File::create(&path).unwrap();
        writeln!(f, r#"{{"p":"good.txt","d":false,"t":1000,"s":1}}"#).unwrap();
        writeln!(f, "this is not json").unwrap();
        writeln!(f, r#"{{"p":"also-good.txt","d":false,"t":1001,"s":2}}"#).unwrap();
        writeln!(f, "").unwrap(); // empty line
        drop(f);

        let journal = TombstoneJournal::open(&path).unwrap();
        let entries = journal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].rel_path, PathBuf::from("good.txt"));
        assert_eq!(entries[1].rel_path, PathBuf::from("also-good.txt"));
    }

    #[test]
    fn test_clear() {
        let (_dir, journal) = tmp_journal();

        journal.append(&TombstoneEntry {
            rel_path: PathBuf::from("x.txt"),
            is_dir: false,
            timestamp: 1000,
            seq: 1,
        }).unwrap();
        assert_eq!(journal.len(), 1);

        journal.clear().unwrap();
        assert_eq!(journal.len(), 0);
        assert!(journal.read_all().unwrap().is_empty());
    }

    #[test]
    fn test_compact_removes_old() {
        let (_dir, journal) = tmp_journal();
        let now = chrono::Utc::now().timestamp();

        // Old entry (beyond TTL)
        journal.append(&TombstoneEntry {
            rel_path: PathBuf::from("old.txt"),
            is_dir: false,
            timestamp: now - 100_000,
            seq: 1,
        }).unwrap();

        // Recent entry (within TTL)
        journal.append(&TombstoneEntry {
            rel_path: PathBuf::from("new.txt"),
            is_dir: false,
            timestamp: now - 10,
            seq: 2,
        }).unwrap();

        let removed = journal.compact(43200).unwrap(); // 12 hour TTL
        assert_eq!(removed, 1);

        let entries = journal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].rel_path, PathBuf::from("new.txt"));
    }

    #[test]
    fn test_replay_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path();

        // Create files to delete
        fs::write(target.join("keep.txt"), "keep").unwrap();
        fs::write(target.join("delete.txt"), "delete").unwrap();
        fs::create_dir(target.join("rmdir")).unwrap();

        let entries = vec![
            TombstoneEntry { rel_path: PathBuf::from("delete.txt"), is_dir: false, timestamp: 1000, seq: 1 },
            TombstoneEntry { rel_path: PathBuf::from("rmdir"), is_dir: true, timestamp: 1001, seq: 2 },
            TombstoneEntry { rel_path: PathBuf::from("nonexistent.txt"), is_dir: false, timestamp: 1002, seq: 3 },
        ];

        let deleted = replay_tombstones(target, &entries, &[]).unwrap();
        assert_eq!(deleted, 2);
        assert!(target.join("keep.txt").exists());
        assert!(!target.join("delete.txt").exists());
        assert!(!target.join("rmdir").exists());
    }

    #[test]
    fn test_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.jsonl");
        let journal = TombstoneJournal::open(&path).unwrap();
        assert_eq!(journal.len(), 0);
        assert!(journal.is_empty());
        assert!(journal.read_all().unwrap().is_empty());
    }
}
