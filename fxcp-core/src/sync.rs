// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/sync.rs — Shared copy/sync engine for fxcp CLI and foxingd sync

//! Shared copy/sync engine used by both fxcp CLI and foxingd sync command.
//!
//! Provides recursive directory copy with auto-adaptive strategy selection
//! (reflink → copy_file_range → sendfile → io_uring), stdin pipe mode with
//! compression auto-detection, delta copy via BLAKE3 Merkle trees, and
//! foxingd-compatible signature generation.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::unix::AsyncFd;
use tracing::{info, warn, debug, error};

use crate::constants;
use crate::error::CopyErrorKind;
use crate::operations::{
    SmartCopier, CopyStats, probe_capabilities, OptimizedFs, FsyncLatencyTracker,
};
use crate::buffer::BufferPool;
use crate::governor::Governor;
use crate::hashing::{self, MerkleTree};
use crate::sidecar;

// Auto-adaptive thresholds
const SMALL_FILE_THRESHOLD: u64 = 64 * 1024;
const FICLONE: u64 = 0x40049409;
const STDIN_CHUNK_SIZE: usize = 1024 * 1024;

// -----------------------------------------------------------------------
// Public types
// -----------------------------------------------------------------------

/// Options for a sync/copy operation (replaces CLI flags).
#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub archive: bool,
    pub recursive: bool,
    pub delete: bool,
    pub dry_run: bool,
    pub exclude: Vec<String>,
    pub include: Vec<String>,
    pub generate_sigs: bool,
    pub snapshot: bool,
    pub throttle: bool,
    pub cleanup: bool,
    // stdin-specific
    pub size: Option<u64>,
    pub checkpoint_interval: Option<u64>,
    pub checkpoint_keep: usize,
    pub zero_copy: bool,
    pub verify: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            source: PathBuf::new(),
            destination: PathBuf::new(),
            archive: false,
            recursive: false,
            delete: false,
            dry_run: false,
            exclude: vec![],
            include: vec![],
            generate_sigs: false,
            snapshot: false,
            throttle: false,
            cleanup: false,
            size: None,
            checkpoint_interval: None,
            checkpoint_keep: 5,
            zero_copy: false,
            verify: false,
        }
    }
}

/// Progress tracking for live reporting during copy operations.
pub struct ProgressTracker {
    pub files_done: std::sync::atomic::AtomicU64,
    pub files_total: std::sync::atomic::AtomicU64,
    pub bytes_done: std::sync::atomic::AtomicU64,
    pub bytes_total: std::sync::atomic::AtomicU64,
    pub active: std::sync::atomic::AtomicBool,
}

impl ProgressTracker {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            files_done: std::sync::atomic::AtomicU64::new(0),
            files_total: std::sync::atomic::AtomicU64::new(0),
            bytes_done: std::sync::atomic::AtomicU64::new(0),
            bytes_total: std::sync::atomic::AtomicU64::new(0),
            active: std::sync::atomic::AtomicBool::new(true),
        })
    }
}

/// Global progress tracker — set by cli_main when --progress is used.
lazy_static::lazy_static! {
    static ref ACTIVE_PROGRESS: parking_lot::Mutex<Option<std::sync::Arc<ProgressTracker>>> = parking_lot::Mutex::new(None);
}

/// Record a file completion for progress reporting.
fn record_file_progress(bytes: u64) {
    if let Some(ref progress) = *ACTIVE_PROGRESS.lock() {
        progress.files_done.fetch_add(1, Ordering::Relaxed);
        progress.bytes_done.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Spawn a progress reporter thread that prints updates to stderr.
/// Returns a join handle. Set active=false to stop.
fn spawn_progress_reporter(progress: std::sync::Arc<ProgressTracker>, json_mode: bool) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        while progress.active.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(if json_mode { 1000 } else { 200 }));

            let done = progress.files_done.load(Ordering::Relaxed);
            let total = progress.files_total.load(Ordering::Relaxed);
            let bytes_done = progress.bytes_done.load(Ordering::Relaxed);
            let bytes_total = progress.bytes_total.load(Ordering::Relaxed);

            if total == 0 { continue; }

            let pct = if bytes_total > 0 { (bytes_done as f64 / bytes_total as f64) * 100.0 } else { 0.0 };
            let elapsed = start.elapsed().as_secs_f64();
            let throughput = if elapsed > 0.0 { bytes_done as f64 / elapsed / 1_048_576.0 } else { 0.0 };
            let eta = if throughput > 0.0 && bytes_total > bytes_done {
                ((bytes_total - bytes_done) as f64 / (throughput * 1_048_576.0)) as u64
            } else { 0 };

            if json_mode {
                let json = serde_json::json!({
                    "progress_pct": (pct * 10.0).round() / 10.0,
                    "bytes_done": bytes_done,
                    "bytes_total": bytes_total,
                    "files_done": done,
                    "files_total": total,
                    "throughput_mbps": (throughput * 10.0).round() / 10.0,
                    "eta_secs": eta,
                });
                eprintln!("{}", serde_json::to_string(&json).unwrap_or_default());
            } else {
                let done_str = format_size_compact(bytes_done);
                let total_str = format_size_compact(bytes_total);
                eprint!("\r[{:>5.1}%] {} / {} | {} / {} files | {:.0} MB/s | ETA {}s   ",
                        pct, done_str, total_str, done, total, throughput, eta);
            }
        }
        if !json_mode { eprintln!(); } // Final newline
    })
}

/// Check available disk space on the filesystem containing `path`.
/// Returns None if statvfs fails (e.g., FUSE without statvfs support).
fn check_available_space(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    let c_path = CString::new(path.to_string_lossy().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc == 0 {
        Some(stat.f_bavail * stat.f_bsize)
    } else {
        None
    }
}

fn format_size_compact(bytes: u64) -> String {
    if bytes >= 1_073_741_824 { format!("{:.1} GB", bytes as f64 / 1_073_741_824.0) }
    else if bytes >= 1_048_576 { format!("{:.1} MB", bytes as f64 / 1_048_576.0) }
    else if bytes >= 1024 { format!("{:.0} KB", bytes as f64 / 1024.0) }
    else { format!("{} B", bytes) }
}

/// Copy statistics returned from sync operations.
#[derive(Debug, Default, serde::Serialize)]
pub struct SyncStats {
    pub files_copied: u64,
    pub files_reflinked: u64,
    pub files_cfr: u64,
    pub files_small: u64,
    pub files_skipped: u64,
    pub files_delta: u64,
    pub files_deleted: u64,
    pub dirs_created: u64,
    pub dirs_pruned: u64,
    pub bytes_copied: u64,
    pub bytes_reflinked: u64,
    pub bytes_cfr: u64,
    pub bytes_small: u64,
    pub bytes_delta: u64,
    pub errors: u64,
    pub sigs_stored: u64,
    pub dirs_hashed: u64,
    pub files_verified: u64,
    pub verify_failures: u64,
    #[cfg(feature = "nfs-bypass")]
    pub files_nfs_bypass: u64,
    #[cfg(feature = "nfs-bypass")]
    pub bytes_nfs_bypass: u64,
}

impl SyncStats {
    /// Accumulate stats from another run (for multi-source operations).
    pub fn merge(&mut self, other: &SyncStats) {
        self.files_copied += other.files_copied;
        self.files_reflinked += other.files_reflinked;
        self.files_cfr += other.files_cfr;
        self.files_small += other.files_small;
        self.files_skipped += other.files_skipped;
        self.files_delta += other.files_delta;
        self.files_deleted += other.files_deleted;
        self.dirs_created += other.dirs_created;
        self.dirs_pruned += other.dirs_pruned;
        self.bytes_copied += other.bytes_copied;
        self.bytes_reflinked += other.bytes_reflinked;
        self.bytes_cfr += other.bytes_cfr;
        self.bytes_small += other.bytes_small;
        self.bytes_delta += other.bytes_delta;
        self.errors += other.errors;
        self.sigs_stored += other.sigs_stored;
        self.dirs_hashed += other.dirs_hashed;
        self.files_verified += other.files_verified;
        self.verify_failures += other.verify_failures;
        #[cfg(feature = "nfs-bypass")]
        {
            self.files_nfs_bypass += other.files_nfs_bypass;
            self.bytes_nfs_bypass += other.bytes_nfs_bypass;
        }
    }
}

// -----------------------------------------------------------------------
// Main entry point
// -----------------------------------------------------------------------

/// Run a sync operation with the given options.
pub async fn run(opts: SyncOptions) -> crate::Result<SyncStats> {
    if opts.cleanup {
        run_cleanup(&opts.source);
        return Ok(SyncStats::default());
    }

    if opts.source.as_os_str() == "-" {
        return run_stdin_to_file(&opts).await;
    }

    run_sync(&opts).await
}

// -----------------------------------------------------------------------
// CLI definition (public for man page / completion generation)
// -----------------------------------------------------------------------

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "fxcp", version, about = "Smart filesystem copy with CoW/reflink/io_uring support")]
pub struct FxcpCli {
    /// Source path(s) and destination — last argument is destination (use '-' for stdin)
    #[arg(required = true, num_args = 2..)]
    pub paths: Vec<PathBuf>,
    #[arg(short = 'a', long, help = "Archive mode (recursive, preserve attributes)")]
    pub archive: bool,
    #[arg(short = 'r', long, help = "Recursive copy (implied by -a)")]
    pub recursive: bool,
    #[arg(short = 'v', long, help = "BLAKE3 verification after copy")]
    pub verify: bool,
    #[arg(long, help = "Delete files in target not present in source")]
    pub delete: bool,
    #[arg(short = 'n', long, help = "Dry run — show what would be copied")]
    pub dry_run: bool,
    #[arg(short = 'e', long, help = "Exclude pattern (glob)")]
    pub exclude: Vec<String>,
    #[arg(long, help = "Include pattern — override excludes (glob)")]
    pub include: Vec<String>,
    #[arg(long, help = "Read exclude patterns from FILE (one per line)")]
    pub exclude_from: Option<PathBuf>,
    #[arg(long, help = "Read include patterns from FILE (one per line)")]
    pub include_from: Option<PathBuf>,
    #[arg(long, help = "Create reflink snapshots of target files before overwriting (versioning)")]
    pub snapshot: bool,
    #[arg(long, help = "Enable PSI-based system stress throttling")]
    pub throttle: bool,
    #[arg(long, help = "Clean orphaned .tmp files and stale dirty flags")]
    pub cleanup: bool,
    #[arg(long, help = "Expected size in bytes (for stdin pre-allocation)")]
    pub size: Option<u64>,
    #[arg(long, help = "Interval in seconds to create CoW checkpoints of stdin stream")]
    pub checkpoint_interval: Option<u64>,
    #[arg(long, default_value = "5", help = "Number of stream checkpoints to keep")]
    pub checkpoint_keep: usize,
    #[arg(long, help = "Use zero-copy splice (mutually exclusive with sparse detection)")]
    pub zero_copy: bool,
    #[arg(long, default_value_t = false, help = "Increase verbosity")]
    pub debug: bool,
    #[arg(long, help = "Output copy results as JSON (machine-readable)")]
    pub json: bool,
    #[arg(long, help = "Show live progress during copy")]
    pub progress: bool,
    #[arg(long, help = "Generate foxingd-compatible sync signatures (xattr/sidecar) for fast resync")]
    pub generate_sigs: bool,
}

/// Snapshot management subcommands (MARS versioning)
#[derive(Parser)]
#[command(name = "fxcp", version, about = "Smart filesystem copy with CoW/reflink/io_uring support")]
pub struct FxcpSnapCli {
    #[command(subcommand)]
    pub command: SnapCommand,
    #[arg(long, default_value_t = false, help = "Increase verbosity")]
    pub debug: bool,
}

#[derive(Subcommand)]
pub enum SnapCommand {
    /// List snapshots or versions of a specific file
    List {
        /// Target directory or specific file path
        path: Option<String>,
        /// Output as JSON (machine-readable)
        #[arg(long)]
        json: bool,
    },
    /// Show details of a specific snapshot
    Show { timestamp: String },
    /// Revert a file to a previous version (atomic reflink swap)
    Revert { path: String, epoch: u64 },
    /// Copy a specific version to a new file
    Copy { path: String, epoch: u64, destination: String },
    /// Remove old snapshots by age, count, or size
    Prune {
        /// Delete snapshots older than this duration (e.g. 30d, 7d, 24h)
        #[arg(long)]
        older_than: Option<String>,
        /// Keep only the last N snapshots
        #[arg(long)]
        keep_last: Option<usize>,
        /// Delete oldest until total size is under this limit (e.g. 50G, 10G)
        #[arg(long)]
        max_size: Option<String>,
        /// Target directory containing .foxing_versions
        #[arg(default_value = ".")]
        path: String,
    },
    /// Tag a snapshot (tagged snapshots are exempt from auto-pruning)
    Tag { timestamp: String, tag: String },
    /// Rebuild index.json from on-disk snapshot state
    RebuildIndex {
        /// Target directory containing .foxing_versions
        #[arg(default_value = ".")]
        path: String,
    },
    /// Show aggregate storage statistics (apparent vs on-disk, CoW savings)
    Stats {
        /// Target directory containing .foxing_versions
        #[arg(default_value = ".")]
        path: String,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Export snapshots as a .fxar archive (content-addressable, deduped)
    Export {
        /// Source directory containing .foxing_versions
        path: String,
        /// Output file (default: stdout)
        #[arg(short, long)]
        output: Option<String>,
        /// Compression: zstd (default), zstd:N, lz4, gzip, xz, xz:N, none
        #[arg(long, default_value = "zstd")]
        compress: String,
        /// Export only a specific snapshot timestamp
        #[arg(long)]
        timestamp: Option<String>,
        /// Archive format: fxar2 (default, chunk-dedup) or tar (legacy whole-file)
        #[arg(long, default_value = "fxar2")]
        format: String,
    },
    /// Import snapshots from a .fxar archive
    Import {
        /// Target directory to restore into
        path: String,
        /// Input file (default: stdin)
        #[arg(short, long)]
        input: Option<String>,
        /// Decompression: auto (default), zstd, lz4, gzip, xz, none
        #[arg(long, default_value = "auto")]
        compress: String,
    },
    /// Inspect a .fxar archive without extracting
    Inspect {
        /// Archive file
        archive: String,
        /// Show full file listing
        #[arg(long)]
        list: bool,
        /// Output as JSON
        #[arg(long)]
        json: bool,
        /// Show versions of a specific file
        #[arg(long)]
        file: Option<String>,
    },
    /// Restore specific files/snapshots from a .fxar archive
    Restore {
        /// Archive file
        archive: String,
        /// File path pattern to restore (glob)
        #[arg(long)]
        file: Option<String>,
        /// Snapshot date to restore from (prefix match)
        #[arg(long)]
        date: Option<String>,
        /// Restore most recent version
        #[arg(long)]
        latest: bool,
        /// Restore all versions of matched files
        #[arg(long)]
        all_versions: bool,
        /// Output directory
        #[arg(short, long, default_value = ".")]
        output: String,
    },
    /// Interactive MC-style dual-pane browser for snapshots and archives
    #[cfg(feature = "tui")]
    Browse {
        /// Directory with .foxing_versions or .fxar archive file
        #[arg(default_value = ".")]
        path: String,
    },
}

// -----------------------------------------------------------------------
// CLI entry point (for symlink dispatch from foxingd)
// -----------------------------------------------------------------------

/// Parse CLI args and run — used when foxingd is called as `fxcp` via symlink.
pub fn cli_main() -> anyhow::Result<()> {
    // Pre-parse: check if first arg is "snap" for subcommand dispatch
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "snap" {
        return cli_snap_main();
    }

    let cli = FxcpCli::parse();

    let log_filter = if cli.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filter))
        .with_target(false)
        .init();

    // Split positional args: all-but-last = sources, last = destination
    let (sources, destination) = crate::filter::split_paths(cli.paths)?;

    // Merge file-based patterns into CLI patterns
    let mut excludes = cli.exclude;
    if let Some(ref file) = cli.exclude_from {
        excludes.extend(crate::filter::read_patterns(file)?);
    }
    let mut includes = cli.include;
    if let Some(ref file) = cli.include_from {
        includes.extend(crate::filter::read_patterns(file)?);
    }

    // Stdin mode: only valid with a single source
    if sources.len() > 1 && sources.iter().any(|s| s.as_os_str() == "-") {
        anyhow::bail!("stdin (-) cannot be used with multiple sources");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    // Set up progress reporting if requested
    let progress_handle = if cli.progress {
        let tracker = ProgressTracker::new();
        *ACTIVE_PROGRESS.lock() = Some(tracker.clone());
        Some(spawn_progress_reporter(tracker, cli.json))
    } else {
        None
    };

    let mut total_stats = SyncStats::default();

    for source in &sources {
        let opts = SyncOptions {
            source: source.clone(),
            destination: destination.clone(),
            archive: cli.archive,
            recursive: cli.recursive || cli.archive,
            delete: cli.delete,
            dry_run: cli.dry_run,
            exclude: excludes.clone(),
            include: includes.clone(),
            generate_sigs: cli.generate_sigs,
            snapshot: cli.snapshot,
            throttle: cli.throttle,
            cleanup: cli.cleanup,
            size: cli.size,
            checkpoint_interval: cli.checkpoint_interval,
            checkpoint_keep: cli.checkpoint_keep,
            zero_copy: cli.zero_copy,
            verify: cli.verify,
        };

        match rt.block_on(run(opts)) {
            Ok(stats) => total_stats.merge(&stats),
            Err(e) => {
                error!("fxcp failed for {}: {}", source.display(), e);
                total_stats.errors += 1;
            }
        }
    }

    // Stop progress reporter
    if let Some(handle) = progress_handle {
        if let Some(ref progress) = *ACTIVE_PROGRESS.lock() {
            progress.active.store(false, Ordering::Relaxed);
        }
        let _ = handle.join();
        *ACTIVE_PROGRESS.lock() = None;
    }

    if cli.json {
        // Machine-readable JSON output
        let total_files = total_stats.files_copied + total_stats.files_skipped;
        let status = if total_stats.errors == 0 { "success" }
                     else if total_stats.files_copied > 0 { "partial" }
                     else { "failed" };
        let json_output = serde_json::json!({
            "status": status,
            "files_copied": total_stats.files_copied,
            "files_reflinked": total_stats.files_reflinked,
            "files_skipped": total_stats.files_skipped,
            "files_deleted": total_stats.files_deleted,
            "dirs_created": total_stats.dirs_created,
            "dirs_pruned": total_stats.dirs_pruned,
            "bytes_copied": total_stats.bytes_copied,
            "bytes_reflinked": total_stats.bytes_reflinked,
            "errors": total_stats.errors,
            "method_breakdown": {
                "reflink": total_stats.files_reflinked,
                "copy_file_range": total_stats.files_cfr,
                "sendfile": total_stats.files_small,
                "io_uring": total_stats.files_copied.saturating_sub(
                    total_stats.files_reflinked + total_stats.files_cfr + total_stats.files_small
                ),
            },
        });
        println!("{}", serde_json::to_string_pretty(&json_output).unwrap_or_default());
    } else {
        print_summary(&total_stats);
    }

    // Granular exit codes
    if total_stats.errors > 0 {
        if total_stats.files_copied > 0 {
            std::process::exit(1);  // Partial success
        } else {
            std::process::exit(2);  // Complete failure
        }
    }
    Ok(())
}

/// Snapshot management subcommand handler.
fn cli_snap_main() -> anyhow::Result<()> {
    // Re-parse with snap-aware CLI (skip argv[0], "snap" is the subcommand)
    let snap_cli = FxcpSnapCli::parse_from(
        std::iter::once("fxcp-snap".to_string())
            .chain(std::env::args().skip(2))
    );

    let log_filter = if snap_cli.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(log_filter))
        .with_target(false)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    match snap_cli.command {
        SnapCommand::List { path, json } => {
            if let Some(ref p) = path {
                let p = std::path::PathBuf::from(p);
                if p.is_file() {
                    // Per-file version listing (legacy versioning.rs)
                    let versions = crate::versioning::list_versions(&p)?;
                    if json {
                        // FileVersion doesn't derive Serialize, convert manually
                        let json_versions: Vec<serde_json::Value> = versions.iter().map(|v| {
                            serde_json::json!({
                                "epoch": v.epoch_seq,
                                "timestamp": v.timestamp,
                                "size": v.size,
                                "path": v.path.to_string_lossy(),
                            })
                        }).collect();
                        println!("{}", serde_json::to_string_pretty(&json_versions).unwrap_or_default());
                    } else if versions.is_empty() {
                        println!("No versions found for {:?}", p);
                    } else {
                        crate::versioning::print_versions_table(versions, 50);
                    }
                } else {
                    // Directory: list point-in-time snapshots
                    let store = crate::version_store::VersionStore::open(&p);
                    let snapshots = store.list_snapshots();
                    if json {
                        println!("{}", serde_json::to_string_pretty(&snapshots).unwrap_or_default());
                    } else {
                        crate::version_store::print_snapshot_table(&snapshots);
                    }
                }
            } else {
                // No path: list snapshots in current directory
                let store = crate::version_store::VersionStore::open(&std::env::current_dir().unwrap_or_default());
                let snapshots = store.list_snapshots();
                if json {
                    println!("{}", serde_json::to_string_pretty(&snapshots).unwrap_or_default());
                } else {
                    crate::version_store::print_snapshot_table(&snapshots);
                }
            }
        }
        SnapCommand::Show { timestamp } => {
            let store = crate::version_store::VersionStore::open(&std::env::current_dir().unwrap_or_default());
            let snapshots = store.list_snapshots();
            if let Some(snap) = snapshots.iter().find(|s| s.timestamp.contains(&timestamp)) {
                println!("{}", serde_json::to_string_pretty(snap).unwrap_or_default());
            } else {
                anyhow::bail!("Snapshot not found: {}", timestamp);
            }
        }
        SnapCommand::Revert { path, epoch } => {
            let p = std::path::PathBuf::from(&path);
            crate::versioning::revert_file(&p, epoch)?;
            info!("Reverted {:?} to epoch {}", path, epoch);
        }
        SnapCommand::Copy { path, epoch, destination } => {
            let p = std::path::PathBuf::from(&path);
            let d = std::path::PathBuf::from(&destination);
            crate::versioning::copy_version_to_path(&p, epoch, &d)?;
            info!("Copied version {} of {:?} to {:?}", epoch, path, destination);
        }
        SnapCommand::Prune { older_than, keep_last, max_size, path } => {
            let p = std::path::PathBuf::from(&path);
            let store = crate::version_store::VersionStore::open(&p);
            let mut total = crate::version_store::PruneStats::default();

            if let Some(ref age_str) = older_than {
                let duration = parse_duration(age_str)?;
                let stats = store.prune_by_age(duration)?;
                total.snapshots_removed += stats.snapshots_removed;
                total.bytes_freed += stats.bytes_freed;
            }
            if let Some(count) = keep_last {
                let stats = store.prune_by_count(count)?;
                total.snapshots_removed += stats.snapshots_removed;
                total.bytes_freed += stats.bytes_freed;
            }
            if let Some(ref size_str) = max_size {
                let bytes = parse_size(size_str)?;
                let stats = store.prune_by_size(bytes)?;
                total.snapshots_removed += stats.snapshots_removed;
                total.bytes_freed += stats.bytes_freed;
            }

            if total.snapshots_removed > 0 {
                info!("Pruned {} snapshots, freed {} bytes", total.snapshots_removed, total.bytes_freed);
            } else {
                info!("Nothing to prune.");
            }
        }
        SnapCommand::Tag { timestamp, tag } => {
            info!("Tagged snapshot {} as '{}'", timestamp, tag);
            // TODO: write tag to summary.json
        }
        SnapCommand::RebuildIndex { path } => {
            let p = std::path::PathBuf::from(&path);
            let store = crate::version_store::VersionStore::open(&p);
            let index = store.rebuild_index()?;
            info!("Rebuilt index: {} snapshots", index.snapshots.len());
        }
        SnapCommand::Stats { path, json } => {
            let p = std::path::PathBuf::from(&path);
            let store = crate::version_store::VersionStore::open(&p);
            let snapshots = store.list_snapshots();
            let versions_root = p.join(".foxing_versions");
            let stats = crate::version_store::compute_store_stats(
                &snapshots,
                if versions_root.exists() { Some(&versions_root) } else { None }
            );
            if json {
                println!("{}", serde_json::to_string_pretty(&stats).unwrap_or_default());
            } else {
                crate::version_store::print_store_stats(&stats, &p);
            }
        }
        SnapCommand::Export { path, output, compress, timestamp, format } => {
            let p = std::path::PathBuf::from(&path);
            let store = crate::version_store::VersionStore::open(&p);
            let ts_filter = timestamp.as_deref();

            if format == "tar" {
                // Legacy tar format
                if let Some(ref out_path) = output {
                    let file = std::fs::File::create(out_path)
                        .map_err(|e| anyhow::anyhow!("Cannot create {}: {}", out_path, e))?;
                    let stats = store.export(file, &compress, ts_filter)?;
                    info!("Exported to {} (tar): {} snapshots, {} unique files",
                          out_path, stats.snapshots_exported, stats.unique_chunks);
                } else {
                    let stdout = std::io::stdout().lock();
                    let stats = store.export(stdout, &compress, ts_filter)?;
                    eprintln!("Exported {} snapshots, {} unique files ({} deduped)",
                              stats.snapshots_exported, stats.unique_chunks, stats.dedup_chunks);
                }
            } else {
                // FXAR v2 (default)
                if let Some(ref out_path) = output {
                    let file = std::fs::File::create(out_path)
                        .map_err(|e| anyhow::anyhow!("Cannot create {}: {}", out_path, e))?;
                    let stats = crate::fxar::write_archive_seekable(&store, file, &compress, ts_filter)?;
                    info!("Exported to {} (FXAR v2): {} snapshots, {} files, {} unique chunks, {:.1}% dedup",
                          out_path, stats.snapshots_exported, stats.total_files,
                          stats.unique_chunks, stats.dedup_ratio() * 100.0);
                } else {
                    let stdout = std::io::stdout().lock();
                    let stats = crate::fxar::write_archive(&store, stdout, &compress, ts_filter)?;
                    eprintln!("Exported {} snapshots, {} files ({} unique chunks, {:.1}% dedup)",
                              stats.snapshots_exported, stats.total_files,
                              stats.unique_chunks, stats.dedup_ratio() * 100.0);
                }
            }
        }
        SnapCommand::Import { path, input, compress } => {
            let p = std::path::PathBuf::from(&path);

            if let Some(ref in_path) = input {
                // Auto-detect: peek at magic bytes
                let mut file = std::fs::File::open(in_path)
                    .map_err(|e| anyhow::anyhow!("Cannot open {}: {}", in_path, e))?;
                let mut magic = [0u8; 4];
                use std::io::Read;
                file.read_exact(&mut magic)
                    .map_err(|e| anyhow::anyhow!("Cannot read {}: {}", in_path, e))?;

                if &magic == b"FXAR" {
                    // FXAR v2 format — re-open for seekable reader
                    drop(file);
                    let file = std::fs::File::open(in_path)
                        .map_err(|e| anyhow::anyhow!("Cannot reopen {}: {}", in_path, e))?;
                    let mut reader = crate::fxar::FxarReader::open(file)
                        .map_err(|e| anyhow::anyhow!("FXAR open: {}", e))?;
                    let stats = reader.restore_all(&p)
                        .map_err(|e| anyhow::anyhow!("FXAR import: {}", e))?;
                    info!("Imported from {} (FXAR v2): {} files, {} bytes",
                          in_path, stats.files_restored, stats.bytes_restored);
                } else {
                    // Legacy tar format — re-open
                    drop(file);
                    let file = std::fs::File::open(in_path)
                        .map_err(|e| anyhow::anyhow!("Cannot reopen {}: {}", in_path, e))?;
                    let store = crate::version_store::VersionStore::open(&p);
                    let comp = if compress == "auto" {
                        if in_path.ends_with(".zst") || in_path.ends_with(".zstd") { "zstd" }
                        else if in_path.ends_with(".lz4") { "lz4" }
                        else if in_path.ends_with(".gz") { "gzip" }
                        else if in_path.ends_with(".xz") { "xz" }
                        else { "none" }
                    } else { &compress };
                    let stats = store.import(file, comp)?;
                    info!("Imported from {} (tar): {} files restored", in_path, stats.files_restored);
                }
            } else {
                // stdin — try streaming FXAR first, fall back to tar
                let stdin = std::io::stdin().lock();
                let mut buf_reader = std::io::BufReader::new(stdin);
                let mut magic = [0u8; 4];
                use std::io::Read;
                buf_reader.read_exact(&mut magic)
                    .map_err(|e| anyhow::anyhow!("Cannot read stdin: {}", e))?;

                if &magic == b"FXAR" {
                    // Chain magic bytes back with remaining stream
                    let chain = std::io::Cursor::new(magic.to_vec()).chain(buf_reader);
                    let stats = crate::fxar::read_archive_stream(chain, &p)
                        .map_err(|e| anyhow::anyhow!("FXAR stream import: {}", e))?;
                    info!("Imported from stdin (FXAR v2): {} files", stats.files_restored);
                } else {
                    // tar fallback
                    let store = crate::version_store::VersionStore::open(&p);
                    let comp = if compress == "auto" { "none" } else { &compress };
                    let chain = std::io::Cursor::new(magic.to_vec()).chain(buf_reader);
                    let stats = store.import(chain, comp)?;
                    info!("Imported from stdin (tar): {} files", stats.files_restored);
                }
            }
        }
        SnapCommand::Inspect { archive, list, json, file } => {
            // Auto-detect format
            let mut f = std::fs::File::open(&archive)
                .map_err(|e| anyhow::anyhow!("Cannot open {}: {}", archive, e))?;
            let mut magic = [0u8; 4];
            use std::io::Read;
            f.read_exact(&mut magic)
                .map_err(|e| anyhow::anyhow!("Cannot read {}: {}", archive, e))?;
            drop(f);

            if &magic == b"FXAR" {
                // FXAR v2 inspect
                let f = std::fs::File::open(&archive)
                    .map_err(|e| anyhow::anyhow!("Cannot reopen {}: {}", archive, e))?;
                let result = crate::fxar::inspect_archive(f)
                    .map_err(|e| anyhow::anyhow!("FXAR inspect: {}", e))?;

                let filtered: Vec<_> = if let Some(ref pattern) = file {
                    result.files.iter().filter(|f| f.path.contains(pattern)).collect()
                } else {
                    result.files.iter().collect()
                };

                if json {
                    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
                } else if list {
                    for f in &filtered {
                        println!("{:>10}  {}  [{}]", f.size, f.path, &f.blake3[..16]);
                    }
                    println!("\n{} files, {} chunks ({:.1}% dedup ratio)",
                             filtered.len(), result.chunk_count, result.dedup_ratio * 100.0);
                } else {
                    println!("Archive: {} (FXAR v2)", archive);
                    println!("  Format:      FXAR v{}", result.version);
                    println!("  Snapshots:   {}", result.snapshots.len());
                    println!("  Files:       {}", result.total_files);
                    println!("  Unique files:{}", result.unique_files);
                    println!("  Chunks:      {}", result.chunk_count);
                    println!("  Apparent:    {} bytes", result.total_apparent_bytes);
                    println!("  Chunk data:  {} bytes", result.total_chunk_bytes);
                    println!("  Compressed:  {} bytes", result.total_compressed_bytes);
                    println!("  Dedup ratio: {:.1}%", result.dedup_ratio * 100.0);
                    for snap in &result.snapshots {
                        println!("  Snapshot:    {}", snap);
                    }
                }
            } else {
                // Legacy tar inspect
                let f = std::fs::File::open(&archive)
                    .map_err(|e| anyhow::anyhow!("Cannot reopen {}: {}", archive, e))?;
                let comp = if archive.ends_with(".zst") { "zstd" }
                           else if archive.ends_with(".lz4") { "lz4" }
                           else if archive.ends_with(".gz") { "gzip" }
                           else if archive.ends_with(".xz") { "xz" }
                           else { "none" };
                let entries = crate::version_store::VersionStore::inspect_archive(f, comp)?;

                let filtered: Vec<_> = if let Some(ref pattern) = file {
                    entries.into_iter().filter(|e| e.path.contains(pattern)).collect()
                } else {
                    entries
                };

                if json {
                    println!("{}", serde_json::to_string_pretty(&filtered).unwrap_or_default());
                } else if list {
                    for e in &filtered {
                        let marker = if e.is_dedup_ref { " [dedup]" }
                                     else if e.is_metadata { " [meta]" }
                                     else { "" };
                        println!("{:>10}  {}{}", e.size, e.path, marker);
                    }
                    let total_files = filtered.iter().filter(|e| !e.is_dedup_ref && !e.is_metadata).count();
                    let dedup_refs = filtered.iter().filter(|e| e.is_dedup_ref).count();
                    println!("\n{} files, {} dedup references", total_files, dedup_refs);
                } else {
                    let total_files = filtered.iter().filter(|e| !e.is_dedup_ref && !e.is_metadata).count();
                    let dedup_refs = filtered.iter().filter(|e| e.is_dedup_ref).count();
                    let total_size: u64 = filtered.iter().map(|e| e.size).sum();
                    println!("Archive: {} (tar)", archive);
                    println!("  Files:       {}", total_files);
                    println!("  Dedup refs:  {}", dedup_refs);
                    println!("  Total size:  {} bytes", total_size);
                }
            }
        }
        SnapCommand::Restore { archive, file, date, latest, all_versions: _, output } => {
            // Auto-detect format
            let mut f = std::fs::File::open(&archive)
                .map_err(|e| anyhow::anyhow!("Cannot open {}: {}", archive, e))?;
            let mut magic = [0u8; 4];
            use std::io::Read;
            f.read_exact(&mut magic)
                .map_err(|e| anyhow::anyhow!("Cannot read {}: {}", archive, e))?;
            drop(f);

            let out_dir = std::path::PathBuf::from(&output);

            if &magic == b"FXAR" {
                // FXAR v2 restore
                let f = std::fs::File::open(&archive)
                    .map_err(|e| anyhow::anyhow!("Cannot reopen {}: {}", archive, e))?;
                let mut reader = crate::fxar::FxarReader::open(f)
                    .map_err(|e| anyhow::anyhow!("FXAR open: {}", e))?;
                let manifest = reader.read_manifest()
                    .map_err(|e| anyhow::anyhow!("FXAR manifest: {}", e))?;

                let mut matched: Vec<_> = manifest.files.iter()
                    .filter(|e| {
                        if let Some(ref pattern) = file {
                            glob::Pattern::new(pattern).map(|p| p.matches(&e.path)).unwrap_or(false)
                                || e.path.contains(pattern)
                        } else { true }
                    })
                    .filter(|e| {
                        if let Some(ref d) = date { e.path.contains(d) }
                        else { true }
                    })
                    .cloned()
                    .collect();

                if latest {
                    matched.sort_by(|a, b| b.path.cmp(&a.path));
                    matched.truncate(1);
                }

                if matched.is_empty() {
                    anyhow::bail!("No matching files found in archive");
                }

                info!("Restoring {} files to {}", matched.len(), output);

                for entry in &matched {
                    let data = reader.restore_file(&entry.path)
                        .map_err(|e| anyhow::anyhow!("FXAR restore {}: {}", entry.path, e))?;
                    let dest = out_dir.join(std::path::Path::new(&entry.path).file_name().unwrap_or_default());
                    if let Some(parent) = dest.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    std::fs::write(&dest, &data)?;
                    info!("Restored: {} ({} bytes, BLAKE3 verified)", dest.display(), data.len());
                }
            } else {
                // Legacy tar restore
                let f = std::fs::File::open(&archive)
                    .map_err(|e| anyhow::anyhow!("Cannot reopen {}: {}", archive, e))?;
                let comp = if archive.ends_with(".zst") { "zstd" }
                           else if archive.ends_with(".lz4") { "lz4" }
                           else if archive.ends_with(".gz") { "gzip" }
                           else if archive.ends_with(".xz") { "xz" }
                           else { "none" };
                let entries = crate::version_store::VersionStore::inspect_archive(f, comp)?;

                let mut matched: Vec<_> = entries.into_iter()
                    .filter(|e| !e.is_metadata && !e.is_dedup_ref)
                    .filter(|e| {
                        if let Some(ref pattern) = file {
                            glob::Pattern::new(pattern).map(|p| p.matches(&e.path)).unwrap_or(false)
                                || e.path.contains(pattern)
                        } else { true }
                    })
                    .filter(|e| {
                        if let Some(ref d) = date { e.path.contains(d) }
                        else { true }
                    })
                    .collect();

                if latest {
                    matched.sort_by(|a, b| b.path.cmp(&a.path));
                    matched.truncate(1);
                }

                if matched.is_empty() {
                    anyhow::bail!("No matching files found in archive");
                }

                info!("Restoring {} files to {}", matched.len(), output);

                let f2 = std::fs::File::open(&archive)
                    .map_err(|e| anyhow::anyhow!("Cannot reopen {}: {}", archive, e))?;
                let decompressed = crate::version_store::wrap_import_decompressor(f2, comp);
                let mut tar_archive = tar::Archive::new(decompressed);
                let match_paths: std::collections::HashSet<String> = matched.iter().map(|e| e.path.clone()).collect();

                for entry in tar_archive.entries().map_err(|e| anyhow::anyhow!("tar error: {}", e))? {
                    let mut entry = match entry { Ok(e) => e, Err(_) => continue };
                    let path = entry.path().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    if match_paths.contains(&path) {
                        let dest = out_dir.join(std::path::Path::new(&path).file_name().unwrap_or_default());
                        if let Some(parent) = dest.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        entry.unpack(&dest).map_err(|e| anyhow::anyhow!("unpack error: {}", e))?;
                        info!("Restored: {}", dest.display());
                    }
                }
            }
        }
        #[cfg(feature = "tui")]
        SnapCommand::Browse { path } => {
            let p = std::path::PathBuf::from(&path);
            let mut app = crate::browser::BrowserApp::new(&p);
            app.run().map_err(|e| anyhow::anyhow!("TUI error: {}", e))?;
        }
    }
    Ok(())
}

/// Parse a human-readable duration string (e.g., "30d", "7d", "24h", "2w").
fn parse_duration(s: &str) -> anyhow::Result<std::time::Duration> {
    let s = s.trim();
    let (num_str, unit) = s.split_at(s.len().saturating_sub(1));
    let num: u64 = num_str.parse().map_err(|_| anyhow::anyhow!("Invalid duration: {}", s))?;
    let secs = match unit {
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        "w" => num * 604800,
        _ => anyhow::bail!("Unknown duration unit '{}' (use s/m/h/d/w)", unit),
    };
    Ok(std::time::Duration::from_secs(secs))
}

/// Parse a human-readable size string (e.g., "50G", "10G", "500M").
fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    let (num_str, unit) = s.split_at(s.len().saturating_sub(1));
    let num: u64 = num_str.parse().map_err(|_| anyhow::anyhow!("Invalid size: {}", s))?;
    let bytes = match unit.to_uppercase().as_str() {
        "K" => num * 1024,
        "M" => num * 1024 * 1024,
        "G" => num * 1024 * 1024 * 1024,
        "T" => num * 1024 * 1024 * 1024 * 1024,
        _ => anyhow::bail!("Unknown size unit '{}' (use K/M/G/T)", unit),
    };
    Ok(bytes)
}

// -----------------------------------------------------------------------
// stdin → file mode
// -----------------------------------------------------------------------

async fn run_stdin_to_file(opts: &SyncOptions) -> crate::Result<SyncStats> {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;

    crate::metrics::initialize_metrics(512);

    let dst = &opts.destination;
    if let Some(parent) = dst.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let is_block_device = dst.exists() && {
        use std::os::unix::fs::FileTypeExt;
        let m = std::fs::metadata(dst).map(|m| m.file_type()).ok();
        m.map(|ft| ft.is_block_device()).unwrap_or(false)
    };

    let file = if is_block_device {
        info!("Block device detected: {:?} (skipping truncate/create)", dst);
        std::fs::OpenOptions::new().read(true).write(true).open(dst)?
    } else {
        std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dst)?
    };
    let fd = file.as_raw_fd();

    if !is_block_device {
        if let Some(size) = opts.size {
            let ret = unsafe { libc::fallocate(fd, 0, 0, size as i64) };
            if ret != 0 {
                debug!("fallocate pre-allocation failed (non-fatal): {}", std::io::Error::last_os_error());
            }
        }
    }

    let use_sparse = !opts.zero_copy;
    let checkpoint_interval = opts.checkpoint_interval.map(std::time::Duration::from_secs);
    let mut last_checkpoint = std::time::Instant::now();
    let mut checkpoint_count = 0u64;

    // Auto-detect compression from magic bytes
    let raw_stdin = std::io::stdin().lock();
    let mut header_buf = [0u8; 6];
    let mut header_reader: Box<dyn std::io::Read> = Box::new(raw_stdin);
    let header_len = {
        let mut n = 0;
        while n < 6 {
            match std::io::Read::read(&mut header_reader, &mut header_buf[n..]) {
                Ok(0) => break,
                Ok(r) => n += r,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(crate::FxcpError::Io(e)),
            }
        }
        n
    };

    let compression = detect_compression(&header_buf[..header_len]);
    let chained: Box<dyn std::io::Read> = Box::new(
        std::io::Cursor::new(header_buf[..header_len].to_vec()).chain(header_reader)
    );
    let (mut reader, _comp_name): (Box<dyn std::io::Read>, &str) = if let Some(fmt) = compression {
        info!("Detected {} compressed input — decompressing inline", fmt);
        wrap_decompressor(chained, fmt)
    } else {
        (chained, "raw")
    };

    let mut buf = vec![0u8; STDIN_CHUNK_SIZE];
    let mut offset: u64 = 0;
    let mut bytes_written: u64 = 0;
    let mut bytes_sparse: u64 = 0;
    let mut chunks_zero: u64 = 0;

    loop {
        let mut filled = 0;
        while filled < STDIN_CHUNK_SIZE {
            let n = match reader.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(crate::FxcpError::Io(e)),
            };
            filled += n;
        }
        if filled == 0 { break; }

        if use_sparse && crate::operations::is_zero_block(&buf[..filled]) {
            bytes_sparse += filled as u64;
            chunks_zero += 1;
        } else {
            let mut written = 0;
            while written < filled {
                let ret = unsafe {
                    libc::pwrite(fd, buf[written..filled].as_ptr() as *const _,
                                 filled - written, (offset + written as u64) as i64)
                };
                if ret < 0 {
                    return Err(crate::FxcpError::Io(std::io::Error::last_os_error()));
                }
                written += ret as usize;
            }
            bytes_written += filled as u64;
        }
        offset += filled as u64;

        if let Some(interval) = checkpoint_interval {
            if last_checkpoint.elapsed() >= interval {
                file.sync_all()?;
                let ts = chrono::Utc::now().timestamp();
                let snap_path = dst.with_extension(format!("snap.{}", ts));
                if let Ok(snap_file) = std::fs::File::create(&snap_path) {
                    let ret = unsafe {
                        libc::ioctl(snap_file.as_raw_fd(), FICLONE, fd)
                    };
                    if ret == 0 {
                        checkpoint_count += 1;
                        info!("Stream checkpoint #{}: {:?} ({})", checkpoint_count, snap_path, format_bytes(offset));
                        if checkpoint_count > opts.checkpoint_keep as u64 {
                            prune_stream_checkpoints(dst, opts.checkpoint_keep);
                        }
                    }
                }
                last_checkpoint = std::time::Instant::now();
            }
        }
    }

    if !is_block_device {
        unsafe { libc::ftruncate(fd, offset as i64) };
    }
    file.sync_all()?;

    info!("stdin → {:?}: {} total, {} data, {} sparse ({} zero chunks punched)",
          dst, format_bytes(offset), format_bytes(bytes_written),
          format_bytes(bytes_sparse), chunks_zero);

    Ok(SyncStats {
        files_copied: 1,
        bytes_copied: bytes_written,
        bytes_small: bytes_sparse,
        ..Default::default()
    })
}

// -----------------------------------------------------------------------
// Recursive directory sync
// -----------------------------------------------------------------------

async fn run_sync(opts: &SyncOptions) -> crate::Result<SyncStats> {
    let source = opts.source.canonicalize().map_err(|e| {
        crate::FxcpError::Config(format!("source {:?}: {}", opts.source, e))
    })?;
    let destination = &opts.destination;
    let recursive = opts.archive || opts.recursive;

    crate::metrics::initialize_metrics(512);

    if source.is_file() && !recursive {
        if !destination.exists() {
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let dst = if destination.is_dir() {
            destination.join(source.file_name().unwrap())
        } else {
            destination.clone()
        };
        let mut copier = create_copier(&source, &dst).await?;
        let src_meta = std::fs::metadata(&source)?;
        copier.optimized_copy(
            source.clone(), dst.clone(), src_meta.len(),
            "default".into(), None, true,
        ).await?;
        if opts.archive {
            preserve_metadata(&source, &dst)?;
        }
        return Ok(SyncStats { files_copied: 1, bytes_copied: src_meta.len(), ..Default::default() });
    }

    if !source.is_dir() {
        return Err(crate::FxcpError::Config(format!(
            "{:?} is not a directory (use -r for recursive)", source
        )));
    }

    std::fs::create_dir_all(destination)?;
    let destination = destination.canonicalize()?;

    // Pre-flight disk space check — abort early if target is critically low
    if let Some(avail) = check_available_space(&destination) {
        const MIN_FREE_BYTES: u64 = 10 * 1024 * 1024; // 10 MB minimum
        if avail < MIN_FREE_BYTES {
            return Err(crate::FxcpError::Config(format!(
                "target {:?} has only {} bytes free (need at least 10 MB)",
                destination, avail
            )));
        }
    }

    let _src_caps = probe_capabilities(&source);
    let dst_caps = probe_capabilities(&destination);

    if let Some(ref container) = dst_caps.container {
        if container.in_container {
            info!("Container: {} (rootless={})",
                  container.engine.as_deref().unwrap_or("unknown"), container.rootless);
        }
    }
    if let Some(ref dm) = dst_caps.dm_stack {
        let mut layers = Vec::new();
        if dm.has_crypt { layers.push(format!("dm-crypt({}B sectors)", dm.crypt_sector_size)); }
        if dm.has_integrity { layers.push("dm-integrity".into()); }
        if dm.has_thin { layers.push("dm-thin".into()); }
        if dm.has_vdo { layers.push("kvdo".into()); }
        if dm.has_cache { layers.push("dm-cache".into()); }
        if dm.has_stratis { layers.push("stratis".into()); }
        info!("Storage: {} on {} [depth={}, phys_blk={}B]",
              layers.join(" + "),
              dm.base_device.as_deref().unwrap_or("unknown"),
              dm.stack_depth,
              dm.physical_block_size);
        crate::metrics::DM_STACK_DEPTH.set(dm.stack_depth as f64);
        crate::metrics::DM_CRYPT_DETECTED.set(if dm.has_crypt { 1.0 } else { 0.0 });
        crate::metrics::STORAGE_PHYSICAL_BLOCK_SIZE.set(dm.physical_block_size as f64);
    }

    // NFS compound bypass: lazy-init TCP client if target is NFSv4.2
    #[cfg(feature = "nfs-bypass")]
    let mut nfs_client: Option<crate::nfs::NfsCompoundClient> = None;
    #[cfg(feature = "nfs-bypass")]
    let mut _nfs_bypass_disabled = false;
    #[cfg(feature = "nfs-bypass")]
    {
        let nfs_bypass_env = std::env::var("FOXING_NFS_BYPASS").unwrap_or_else(|_| "1".to_string());
        let is_nfs = dst_caps.is_nfs.load(Ordering::Relaxed);
        debug!("NFS bypass init: is_nfs={}, env={}", is_nfs, nfs_bypass_env);
        if is_nfs && nfs_bypass_env != "0" {
            match crate::nfs::mount::probe_nfs_bypass(&destination) {
                Some(info) => {
                    match crate::nfs::NfsCompoundClient::connect(&info) {
                        Ok(client) => {
                            info!("NFS bypass: connected to {} for compound RPCs", info.server_addr);
                            nfs_client = Some(client);
                        }
                        Err(e) => {
                            debug!("NFS bypass unavailable: {} — using VFS path", e);
                            _nfs_bypass_disabled = true;
                        }
                    }
                }
                None => {
                    debug!("NFS bypass: target not eligible (not v4.2 or kerberos)");
                }
            }
        }
    }

    let filter_rules = crate::filter::FilterRules::new(&opts.exclude, &opts.include);

    constants::ONE_SHOT_MODE.store(true, Ordering::Relaxed);

    // Pre-count files and bytes for progress reporting
    if let Some(ref progress) = *ACTIVE_PROGRESS.lock() {
        let mut total_files = 0u64;
        let mut total_bytes = 0u64;
        for entry in walkdir::WalkDir::new(&source).follow_links(false) {
            if let Ok(e) = entry {
                if e.file_type().is_file() {
                    total_files += 1;
                    total_bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
        progress.files_total.store(total_files, Ordering::Relaxed);
        progress.bytes_total.store(total_bytes, Ordering::Relaxed);
    }

    let mut copier = create_copier(&source, &destination).await?;
    let mut stats = SyncStats::default();

    let walker = walkdir::WalkDir::new(&source)
        .follow_links(false)
        .sort_by_file_name();

    // Dir-hash pruning: skip unchanged directory subtrees
    let mut pruned_dirs: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => { warn!("walk error: {}", e); stats.errors += 1; continue; }
        };

        let src_path = entry.path();
        let rel = match src_path.strip_prefix(&source) {
            Ok(r) => r,
            Err(_) => continue,
        };

        if filter_rules.should_skip(rel) { continue; }

        let dst_path = destination.join(rel);

        if entry.file_type().is_dir() {
            // Dir-hash pruning: if target dir has a stored hash matching source, skip subtree
            if dst_path.exists() && !rel.as_os_str().is_empty() {
                if let Some(src_hash) = crate::hashing::compute_dir_hash_from_path(src_path) {
                    if let Some(dst_hash) = crate::sidecar::get_dir_hash(&dst_path) {
                        if src_hash == dst_hash {
                            pruned_dirs.insert(rel.to_path_buf());
                            stats.dirs_pruned += 1;
                            continue;
                        }
                    }
                }
            }
            if !dst_path.exists() {
                if opts.dry_run {
                    info!("mkdir {:?}", dst_path);
                } else {
                    std::fs::create_dir_all(&dst_path)?;
                    stats.dirs_created += 1;
                }
            }
            continue;
        }

        if !entry.file_type().is_file() { continue; }

        // Skip files in pruned directory subtrees
        if pruned_dirs.iter().any(|p| rel.starts_with(p)) {
            stats.files_skipped += 1;
            continue;
        }

        let src_meta = match std::fs::metadata(src_path) {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    debug!("Source vanished before stat: {:?}", src_path);
                    stats.files_skipped += 1;
                } else {
                    warn!("stat {:?}: {}", src_path, e);
                    stats.errors += 1;
                }
                continue;
            }
        };

        let needs_copy = if let Ok(dst_meta) = std::fs::metadata(&dst_path) {
            !(dst_meta.len() == src_meta.len() && dst_meta.mtime() == src_meta.mtime()
                && dst_meta.mtime_nsec() == src_meta.mtime_nsec())
        } else {
            true
        };

        if !needs_copy { stats.files_skipped += 1; record_file_progress(0); continue; }

        if opts.dry_run {
            info!("copy {:?} -> {:?} ({})", src_path, dst_path, src_meta.len());
            stats.files_copied += 1;
            stats.bytes_copied += src_meta.len();
            record_file_progress(src_meta.len());
            continue;
        }

        if let Some(parent) = dst_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
                stats.dirs_created += 1;
            }
        }

        // Delta copy for large files with existing target
        let dst_exists = dst_path.exists();
        let same_size = dst_exists && std::fs::metadata(&dst_path)
            .map(|m| m.len() == src_meta.len()).unwrap_or(false);

        if same_size && src_meta.len() > hashing::CHUNK_SIZE as u64 * 4 {
            match try_delta_copy(&mut copier, src_path, &dst_path, src_meta.len()).await {
                Ok(Some(delta_stats)) => {
                    stats.files_delta += 1;
                    stats.bytes_delta += delta_stats.bytes_processed;
                    if opts.archive { let _ = preserve_metadata(src_path, &dst_path); }
                    continue;
                }
                Ok(None) => {}
                Err(e) => { debug!("delta copy failed for {:?}: {}", src_path, e); }
            }
        }

        // Auto-adaptive copy strategy selection
        let file_size = src_meta.len();
        let same_device = src_meta.dev() == std::fs::metadata(&destination)
            .map(|m| m.dev()).unwrap_or(0);
        let dst_is_nfs = dst_caps.is_nfs.load(Ordering::Relaxed);
        let is_sparse = file_size > 4096
            && (src_meta.blocks() as u64 * 512) < file_size / 2;

        // Tier 0.5: NFSv4.2 compound RPC bypass (small files on NFS targets)
        #[cfg(feature = "nfs-bypass")]
        if dst_is_nfs && !is_sparse && file_size > 0
            && file_size <= crate::nfs::NFS_BYPASS_MAX_SIZE
            && nfs_client.is_some()
        {
            match std::fs::read(src_path) {
                Ok(data) => {
                    let client = nfs_client.as_mut().unwrap();
                    let rel_parent = dst_path.parent()
                        .and_then(|p| p.strip_prefix(&destination).ok())
                        .unwrap_or(std::path::Path::new(""));
                    let full_parent = destination.join(rel_parent);
                    match client.get_or_resolve_handle(&full_parent) {
                        Ok(handle) => {
                            let fname = dst_path.file_name().unwrap().to_string_lossy();
                            match client.write_file(
                                &handle, &fname, &data,
                                src_meta.mode(), src_meta.uid(), src_meta.gid(),
                                (src_meta.mtime(), src_meta.mtime_nsec()),
                            ) {
                                Ok(()) => {
                                    stats.files_nfs_bypass += 1;
                                    stats.bytes_nfs_bypass += file_size;
                                    if opts.generate_sigs {
                                        let sig = sidecar::SyncSignature::compute_from_buffer(
                                            &data, src_meta.mtime(), src_meta.mtime_nsec(),
                                        );
                                        let _ = sidecar::set_sync_signature(&dst_path, &sig);
                                        stats.sigs_stored += 1;
                                    }
                                    continue;
                                }
                                Err(e) => {
                                    debug!("NFS bypass write failed for {:?}: {} — VFS fallback", src_path, e);
                                }
                            }
                        }
                        Err(e) => {
                            debug!("NFS handle resolve failed: {} — VFS fallback", e);
                        }
                    }
                }
                Err(e) => {
                    debug!("NFS bypass read failed for {:?}: {} — VFS fallback", src_path, e);
                }
            }
        }

        // Tier 1: Reflink/FICLONE
        if (same_device || dst_is_nfs) && file_size > 0 {
            if try_reflink_copy(src_path, &dst_path) {
                stats.files_reflinked += 1;
                stats.bytes_reflinked += file_size;
                if opts.archive { let _ = preserve_metadata(src_path, &dst_path); }
                if opts.generate_sigs {
                    if store_foxing_signatures(&dst_path).is_ok() { stats.sigs_stored += 1; }
                }
                continue;
            }
        }

        // Tier 1.5: copy_file_range (NFS 4.2 server-side)
        if file_size > 0 && !is_sparse {
            match try_copy_file_range(src_path, &dst_path, file_size) {
                Ok(bytes) if bytes == file_size => {
                    stats.files_cfr += 1;
                    stats.bytes_cfr += bytes;
                    if opts.archive { let _ = preserve_metadata(src_path, &dst_path); }
                    if opts.generate_sigs {
                        if store_foxing_signatures(&dst_path).is_ok() { stats.sigs_stored += 1; }
                    }
                    continue;
                }
                Ok(_) => { let _ = std::fs::remove_file(&dst_path); }
                Err(e) => { debug!("copy_file_range {:?}: {} — falling back", src_path, e); }
            }
        }

        // Tier 2: Small file fast path
        if file_size <= SMALL_FILE_THRESHOLD && !is_sparse {
            match copy_small_file(src_path, &dst_path) {
                Ok(bytes) => {
                    stats.files_small += 1;
                    stats.bytes_small += bytes;
                    if opts.archive { let _ = preserve_metadata(src_path, &dst_path); }
                    if opts.generate_sigs {
                        if store_foxing_signatures(&dst_path).is_ok() { stats.sigs_stored += 1; }
                    }
                    continue;
                }
                Err(e) => { debug!("small file copy failed for {:?}: {}", src_path, e); }
            }
        }

        // Tier 3: io_uring
        match copier.optimized_copy(
            src_path.to_path_buf(), dst_path.clone(), file_size,
            "default".into(), None, true,
        ).await {
            Ok(copy_stats) => {
                stats.files_copied += 1;
                stats.bytes_copied += copy_stats.bytes_processed;
                record_file_progress(copy_stats.bytes_processed);
                if opts.archive { let _ = preserve_metadata(src_path, &dst_path); }
                if opts.generate_sigs {
                    if store_foxing_signatures(&dst_path).is_ok() { stats.sigs_stored += 1; }
                }
            }
            Err(e) => {
                match e.copy_error_kind() {
                    CopyErrorKind::SourceNotFound | CopyErrorKind::TargetNotFound => {
                        debug!("Path vanished during copy: {:?}: {}", src_path, e);
                        stats.files_skipped += 1;
                    }
                    CopyErrorKind::Timeout => { warn!("Copy timed out: {:?}: {}", src_path, e); stats.errors += 1; }
                    CopyErrorKind::Transient => { warn!("Transient error: {:?}: {}", src_path, e); stats.errors += 1; }
                    CopyErrorKind::Permanent => { error!("Permanent error: {:?}: {}", src_path, e); stats.errors += 1; }
                    #[cfg(feature = "nfs-bypass")]
                    CopyErrorKind::NfsTransient | CopyErrorKind::NfsBypassUnavailable => {
                        debug!("NFS bypass error: {:?}: {}", src_path, e); stats.errors += 1;
                    }
                }
            }
        }
    }

    // Post-copy BLAKE3 verification pass
    if opts.verify && !opts.dry_run {
        info!("Running post-copy BLAKE3 verification...");
        let verify_walker = walkdir::WalkDir::new(&source)
            .follow_links(false)
            .sort_by_file_name();
        for entry in verify_walker.into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() { continue; }
            let src_path = entry.path();
            let rel = match src_path.strip_prefix(&source) { Ok(r) => r, Err(_) => continue };
            if filter_rules.should_skip(rel) { continue; }
            let dst_path = destination.join(rel);
            if !dst_path.exists() { continue; }
            match verify_blake3(src_path, &dst_path) {
                Ok(true) => { stats.files_verified += 1; }
                Ok(false) => {
                    error!("BLAKE3 mismatch: {:?}", src_path);
                    stats.verify_failures += 1;
                }
                Err(e) => {
                    warn!("verify error {:?}: {}", src_path, e);
                    stats.verify_failures += 1;
                }
            }
        }
        if stats.verify_failures > 0 {
            warn!("{} files failed BLAKE3 verification", stats.verify_failures);
        } else {
            info!("{} files verified OK", stats.files_verified);
        }
    }

    if opts.delete && !opts.dry_run {
        stats.files_deleted = delete_extra_files(&source, &destination, &filter_rules)?;
    }

    // Generate directory hashes for foxingd tree pruning
    // Always store dir hashes for non-pruned directories so future syncs can prune.
    // File signatures (SyncSignature + MerkleSignature) only stored with --generate-sigs.
    if !opts.dry_run {
        let dir_walker = walkdir::WalkDir::new(&destination)
            .contents_first(true)
            .follow_links(false);
        for entry in dir_walker.into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_dir() {
                let dst_dir = entry.path();
                if let Ok(rel) = dst_dir.strip_prefix(&destination) {
                    // Skip dirs that were already pruned (hash is still valid)
                    if pruned_dirs.contains(rel) { continue; }
                    let src_dir = source.join(rel);
                    if src_dir.is_dir() {
                        if let Some(hash) = hashing::compute_dir_hash_from_path(&src_dir) {
                            if sidecar::set_dir_hash(dst_dir, &hash).is_ok() {
                                stats.dirs_hashed += 1;
                            }
                        }
                    }
                }
            }
        }
        if stats.dirs_hashed > 0 || stats.dirs_pruned > 0 {
            info!("{} dir hashes stored, {} dirs pruned", stats.dirs_hashed, stats.dirs_pruned);
        }
        if opts.generate_sigs && stats.sigs_stored > 0 {
            info!("{} file signatures stored (foxingd-compatible)", stats.sigs_stored);
        }
    }

    Ok(stats)
}

// -----------------------------------------------------------------------
// Helper functions
// -----------------------------------------------------------------------

/// Store foxingd-compatible sync signatures on the destination file.
pub fn store_foxing_signatures(dst: &Path) -> crate::Result<()> {
    let sig = sidecar::SyncSignature::compute(dst)?;
    sidecar::set_sync_signature(dst, &sig)?;
    let file_size = std::fs::metadata(dst)?.len();
    if file_size > (hashing::CHUNK_SIZE * 4) as u64 {
        let tree = MerkleTree::from_file(dst, hashing::calculate_adaptive_chunk_size(dst.metadata()?.len()))?;
        let merkle_sig = tree.to_signature();
        sidecar::set_merkle_signature(dst, &merkle_sig)?;
    }
    Ok(())
}

fn try_reflink_copy(src: &Path, dst: &Path) -> bool {
    use std::os::unix::io::AsRawFd;
    let src_file = match std::fs::File::open(src) { Ok(f) => f, Err(_) => return false };
    let dst_file = match std::fs::File::create(dst) { Ok(f) => f, Err(_) => return false };
    unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) == 0 }
}

fn try_copy_file_range(src: &Path, dst: &Path, size: u64) -> std::io::Result<u64> {
    use std::os::unix::io::AsRawFd;
    let src_file = std::fs::File::open(src)?;
    let dst_file = std::fs::File::create(dst)?;
    let sfd = src_file.as_raw_fd();
    let dfd = dst_file.as_raw_fd();
    let mut total = 0u64;
    let mut off_in = 0i64;
    let mut off_out = 0i64;
    while total < size {
        let chunk = ((size - total) as usize).min(1024 * 1024 * 1024);
        let ret = unsafe { libc::copy_file_range(sfd, &mut off_in, dfd, &mut off_out, chunk, 0) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if total == 0 { drop(dst_file); let _ = std::fs::remove_file(dst); }
            return Err(err);
        }
        if ret == 0 { break; }
        total += ret as u64;
    }
    Ok(total)
}

fn copy_small_file(src: &Path, dst: &Path) -> std::io::Result<u64> {
    std::fs::copy(src, dst)
}

async fn try_delta_copy(
    copier: &mut SmartCopier,
    src: &Path, dst: &Path, file_size: u64,
) -> crate::Result<Option<CopyStats>> {
    let chunk_size = hashing::calculate_adaptive_chunk_size(file_size);
    let src_tree = MerkleTree::from_file(src, chunk_size)?;
    let dst_tree = MerkleTree::from_file(dst, chunk_size)?;
    if src_tree.root == dst_tree.root {
        return Ok(Some(CopyStats::default()));
    }
    let dirty = MerkleTree::diff(&src_tree, &dst_tree);
    if dirty.is_empty() {
        return Ok(Some(CopyStats::default()));
    }
    let stats = copier.copy_delta(src, dst, &dirty, file_size, "delta").await?;
    Ok(Some(stats))
}

pub(crate) async fn create_copier(src: &Path, dst: &Path) -> crate::Result<SmartCopier> {
    let src_caps = probe_capabilities(src);
    let dst_caps = probe_capabilities(dst);
    let ring = io_uring::IoUring::new(256)?;
    let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    if eventfd < 0 {
        return Err(crate::FxcpError::Io(std::io::Error::last_os_error()));
    }
    ring.submitter().register_eventfd(eventfd)?;
    let async_fd = Arc::new(AsyncFd::new(eventfd)?);
    let mut buffer_pool = BufferPool::new(64, 4096, 512)?;
    let iovs = buffer_pool.as_io_vecs();
    // Try to register buffers for zero-copy I/O — falls back gracefully on
    // tmpfs/ramfs/hugetlbfs where page pinning fails with EINVAL.
    if unsafe { ring.submitter().register_buffers(&iovs) }.is_err() {
        debug!("io_uring buffer registration failed (tmpfs/ramfs?) — using unregistered I/O");
    }
    let governor = Governor::new(4.0, 0, 10.0, 10.0);
    Ok(SmartCopier {
        ring, buffer_pool,
        atomic_buffer_pool: None, async_fd,
        vdo_opt: false, direct_io_ok: false,
        source_caps: src_caps, target_caps: dst_caps,
        vdo_stall_threshold: 0, barrier_callback: None,
        source_uncached: false, target_uncached: false,
        governor: Some(Arc::new(governor)),
        fsync_tracker: FsyncLatencyTracker::default(),
        skip_fsync: true,
        #[allow(deprecated)]
        segment_stall_timeout_secs: constants::PROCESS_SEGMENT_STALL_SECS,
        #[allow(deprecated)]
        segment_overall_timeout_secs: constants::PROCESS_SEGMENT_TIMEOUT_SECS,
    })
}

pub fn preserve_metadata(src: &Path, dst: &Path) -> std::io::Result<()> {
    let src_meta = std::fs::metadata(src)?;
    std::fs::set_permissions(dst, src_meta.permissions())?;
    let times = [
        libc::timespec { tv_sec: src_meta.atime(), tv_nsec: src_meta.atime_nsec() },
        libc::timespec { tv_sec: src_meta.mtime(), tv_nsec: src_meta.mtime_nsec() },
    ];
    let dst_cstr = std::ffi::CString::new(dst.as_os_str().as_encoded_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let ret = unsafe { libc::utimensat(libc::AT_FDCWD, dst_cstr.as_ptr(), times.as_ptr(), 0) };
    if ret != 0 { return Err(std::io::Error::last_os_error()); }
    Ok(())
}

fn delete_extra_files(source: &Path, target: &Path, filter: &crate::filter::FilterRules) -> crate::Result<u64> {
    // Fast path: if a tombstone journal exists on the target, replay it
    // instead of walking the entire target tree.
    let journal_path = target.join(".foxing_tombstones.jsonl");
    if journal_path.exists() {
        if let Ok(journal) = crate::tombstone::TombstoneJournal::open(&journal_path) {
            if let Ok(entries) = journal.read_all() {
                if !entries.is_empty() {
                    info!("Replaying {} tombstones (skipping full target walk)", entries.len());
                    let deleted = crate::tombstone::replay_tombstones(target, &entries, &filter.excludes)?;
                    let _ = journal.clear();
                    return Ok(deleted);
                }
            }
        }
    }

    // Slow path: full target walk (no journal available)
    let mut deleted = 0u64;
    for entry in walkdir::WalkDir::new(target).contents_first(true) {
        let entry = match entry { Ok(e) => e, Err(_) => continue };
        let tgt_path = entry.path();
        let rel = match tgt_path.strip_prefix(target) { Ok(r) => r, Err(_) => continue };
        if rel.as_os_str().is_empty() { continue; }
        if filter.should_skip(rel) { continue; }
        if rel.to_string_lossy().contains(".foxing_tombstones") { continue; }
        let src_path = source.join(rel);
        if !src_path.exists() {
            if entry.file_type().is_dir() { let _ = std::fs::remove_dir(tgt_path); }
            else { let _ = std::fs::remove_file(tgt_path); deleted += 1; }
        }
    }
    Ok(deleted)
}

fn run_cleanup(path: &Path) {
    let mut orphaned = 0u64;
    let mut dirty = 0u64;
    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    for entry in walkdir::WalkDir::new(path) {
        let entry = match entry { Ok(e) => e, Err(_) => continue };
        if !entry.file_type().is_file() { continue; }
        let name = entry.file_name().to_string_lossy();
        if name.starts_with(".tmp.") {
            if let Ok(meta) = entry.metadata() {
                if let Ok(mtime) = meta.modified() {
                    if mtime < cutoff {
                        info!("removing orphaned: {:?}", entry.path());
                        let _ = std::fs::remove_file(entry.path());
                        orphaned += 1;
                    }
                }
            }
        }
        if sidecar::is_dirty(entry.path()) {
            info!("dirty flag: {:?}", entry.path());
            dirty += 1;
        }
    }
    println!("Cleanup: {} orphaned files removed, {} dirty flags found", orphaned, dirty);
}

fn detect_compression(header: &[u8]) -> Option<&'static str> {
    if header.len() < 4 { return None; }
    if header[0] == 0xFD && header[1] == 0x2F && header[2] == 0xB5 && header[3] == 0x28 { return Some("zstd"); }
    if header[0] == 0x1F && header[1] == 0x8B { return Some("gzip"); }
    if header[0] == 0x04 && header[1] == 0x22 && header[2] == 0x4D && header[3] == 0x18 { return Some("lz4"); }
    if header.len() >= 6 && header[0] == 0xFD && header[1] == 0x37
        && header[2] == 0x7A && header[3] == 0x58 && header[4] == 0x5A && header[5] == 0x00 {
        return Some("xz");
    }
    None
}

fn wrap_decompressor<'a>(
    reader: Box<dyn std::io::Read + 'a>, format: &str,
) -> (Box<dyn std::io::Read + 'a>, &'static str) {
    match format {
        "zstd" => {
            let dec = zstd::stream::Decoder::new(reader).expect("zstd decoder init failed");
            (Box::new(dec), "zstd")
        }
        "gzip" => (Box::new(flate2::read::GzDecoder::new(reader)), "gzip"),
        "lz4" => (Box::new(lz4_flex::frame::FrameDecoder::new(reader)), "lz4"),
        "xz" => (Box::new(xz2::read::XzDecoder::new(reader)), "xz"),
        _ => (reader, "raw")
    }
}

fn prune_stream_checkpoints(base_path: &Path, keep: usize) {
    let parent = match base_path.parent() { Some(p) => p, None => return };
    let stem = base_path.file_name().unwrap_or_default().to_string_lossy();
    let mut snaps: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(parent) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&*stem) && name.contains(".snap.") {
                snaps.push(entry.path());
            }
        }
    }
    snaps.sort();
    while snaps.len() > keep {
        if let Some(oldest) = snaps.first() {
            let _ = std::fs::remove_file(oldest);
            snaps.remove(0);
        }
    }
}

/// BLAKE3 verification: hash both files and compare.
fn verify_blake3(src: &Path, dst: &Path) -> std::io::Result<bool> {
    let src_hash = blake3::hash(&std::fs::read(src)?);
    let dst_hash = blake3::hash(&std::fs::read(dst)?);
    Ok(src_hash == dst_hash)
}

fn format_bytes(b: u64) -> String {
    if b >= 1024 * 1024 * 1024 { format!("{:.1} GB", b as f64 / 1024.0 / 1024.0 / 1024.0) }
    else if b >= 1024 * 1024 { format!("{:.1} MB", b as f64 / 1024.0 / 1024.0) }
    else if b >= 1024 { format!("{:.1} KB", b as f64 / 1024.0) }
    else { format!("{} B", b) }
}

/// Print a human-readable summary of sync results.
pub fn print_summary(stats: &SyncStats) {
    #[allow(unused_mut)]
    let mut total_bytes = stats.bytes_copied + stats.bytes_reflinked + stats.bytes_cfr + stats.bytes_small + stats.bytes_delta;
    #[allow(unused_mut)]
    let mut total_files = stats.files_copied + stats.files_reflinked + stats.files_cfr + stats.files_small + stats.files_delta;
    #[cfg(feature = "nfs-bypass")]
    {
        total_bytes += stats.bytes_nfs_bypass;
        total_files += stats.files_nfs_bypass;
    }
    println!("fxcp sync complete:");
    println!("  Files total:   {}", total_files);
    if stats.files_reflinked > 0 {
        println!("  - reflinked:   {} ({:.1} MB, instant CoW)", stats.files_reflinked, stats.bytes_reflinked as f64 / 1024.0 / 1024.0);
    }
    if stats.files_cfr > 0 {
        println!("  - server copy: {} ({:.1} MB, copy_file_range)", stats.files_cfr, stats.bytes_cfr as f64 / 1024.0 / 1024.0);
    }
    if stats.files_small > 0 {
        println!("  - small copy:  {} ({:.1} MB, sendfile)", stats.files_small, stats.bytes_small as f64 / 1024.0 / 1024.0);
    }
    if stats.files_copied > 0 {
        println!("  - io_uring:    {} ({:.1} MB)", stats.files_copied, stats.bytes_copied as f64 / 1024.0 / 1024.0);
    }
    if stats.files_delta > 0 {
        println!("  - delta:       {} ({:.1} MB)", stats.files_delta, stats.bytes_delta as f64 / 1024.0 / 1024.0);
    }
    #[cfg(feature = "nfs-bypass")]
    if stats.files_nfs_bypass > 0 {
        println!("  - NFS bypass:  {} ({:.1} MB, compound RPC)", stats.files_nfs_bypass, stats.bytes_nfs_bypass as f64 / 1024.0 / 1024.0);
    }
    println!("  Files skipped: {}", stats.files_skipped);
    if stats.files_deleted > 0 { println!("  Files deleted: {}", stats.files_deleted); }
    println!("  Dirs created:  {}", stats.dirs_created);
    if stats.dirs_pruned > 0 { println!("  Dirs pruned:   {} (unchanged, skipped)", stats.dirs_pruned); }
    println!("  Bytes total:   {} ({:.1} MB)", total_bytes, total_bytes as f64 / 1024.0 / 1024.0);
    if stats.files_verified > 0 { println!("  Verified:      {} (BLAKE3)", stats.files_verified); }
    if stats.verify_failures > 0 { println!("  Verify FAIL:   {}", stats.verify_failures); }
    if stats.errors > 0 { println!("  Errors:        {}", stats.errors); }
}
