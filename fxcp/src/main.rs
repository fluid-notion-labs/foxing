use clap::Parser;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::unix::AsyncFd;
use tracing::{info, warn, debug, error};

use fxcp_core::constants;
use fxcp_core::operations::{
    SmartCopier, CopyStats, probe_capabilities, OptimizedFs, FsyncLatencyTracker,
};
use fxcp_core::buffer::BufferPool;
use fxcp_core::governor::Governor;
use fxcp_core::hashing::{self, MerkleTree};
use fxcp_core::sidecar;

#[derive(Parser)]
#[command(name = "fxcp", version, about = "Smart filesystem copy with CoW/reflink/io_uring support")]
struct Cli {
    /// Source path
    source: PathBuf,
    /// Destination path
    destination: PathBuf,

    #[arg(short = 'a', long, help = "Archive mode (recursive, preserve attributes)")]
    archive: bool,
    #[arg(short = 'r', long, help = "Recursive copy (implied by -a)")]
    recursive: bool,
    #[arg(short = 'v', long, help = "BLAKE3 verification after copy")]
    verify: bool,
    #[arg(long, help = "Delete files in target not present in source")]
    delete: bool,
    #[arg(short = 'n', long, help = "Dry run — show what would be copied")]
    dry_run: bool,
    #[arg(short = 'e', long, help = "Exclude pattern (glob)")]
    exclude: Vec<String>,
    #[arg(long, help = "Clean orphaned .tmp files and stale dirty flags")]
    cleanup: bool,
    #[arg(long, help = "Force full hash verification, ignore stored signatures")]
    strict_hash: bool,
    #[arg(long, default_value_t = false, help = "Increase verbosity")]
    debug: bool,
}

// Auto-adaptive thresholds
const SMALL_FILE_THRESHOLD: u64 = 64 * 1024; // Files below 64KB use std::fs::copy (no io_uring)
const FICLONE: u64 = 0x40049409;              // btrfs/xfs/nfs reflink ioctl

struct SyncStats {
    files_copied: u64,
    files_reflinked: u64,
    files_cfr: u64,       // copy_file_range (NFS server-side copy)
    files_small: u64,
    files_skipped: u64,
    files_delta: u64,
    files_deleted: u64,
    dirs_created: u64,
    bytes_copied: u64,
    bytes_reflinked: u64,
    bytes_cfr: u64,
    bytes_small: u64,
    bytes_delta: u64,
    errors: u64,
}

impl Default for SyncStats {
    fn default() -> Self {
        Self { files_copied: 0, files_reflinked: 0, files_cfr: 0, files_small: 0,
               files_skipped: 0, files_delta: 0, files_deleted: 0,
               dirs_created: 0, bytes_copied: 0, bytes_reflinked: 0,
               bytes_cfr: 0, bytes_small: 0, bytes_delta: 0, errors: 0 }
    }
}

fn main() {
    let cli = Cli::parse();

    let filter = if cli.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_target(false)
        .init();

    if cli.cleanup {
        run_cleanup(&cli.source);
        return;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let result = rt.block_on(run_sync(&cli));
    match result {
        Ok(stats) => print_summary(&stats),
        Err(e) => {
            error!("fxcp failed: {}", e);
            std::process::exit(1);
        }
    }
}

async fn run_sync(cli: &Cli) -> fxcp_core::Result<SyncStats> {
    let source = cli.source.canonicalize().map_err(|e| {
        fxcp_core::FxcpError::Config(format!("source {:?}: {}", cli.source, e))
    })?;
    let destination = &cli.destination;
    let recursive = cli.archive || cli.recursive;

    // Initialize global buffer limit (512MB for fxcp one-shot mode)
    fxcp_core::metrics::initialize_metrics(512);

    if source.is_file() && !recursive {
        // Single file copy
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
        if cli.archive {
            preserve_metadata(&source, &dst)?;
        }
        return Ok(SyncStats { files_copied: 1, bytes_copied: src_meta.len(), ..Default::default() });
    }

    if !source.is_dir() {
        return Err(fxcp_core::FxcpError::Config(format!(
            "{:?} is not a directory (use -r for recursive)", source
        )));
    }

    // Directory sync
    std::fs::create_dir_all(destination)?;
    let destination = destination.canonicalize()?;

    let _src_caps = probe_capabilities(&source);
    let dst_caps = probe_capabilities(&destination);

    // Log storage stack detection
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
        // Set metrics
        fxcp_core::metrics::DM_STACK_DEPTH.set(dm.stack_depth as f64);
        fxcp_core::metrics::DM_CRYPT_DETECTED.set(if dm.has_crypt { 1.0 } else { 0.0 });
        fxcp_core::metrics::STORAGE_PHYSICAL_BLOCK_SIZE.set(dm.physical_block_size as f64);
    }

    // Compile exclude patterns
    let exclude_patterns: Vec<glob::Pattern> = cli.exclude.iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect();

    constants::ONE_SHOT_MODE.store(true, Ordering::Relaxed);

    let mut copier = create_copier(&source, &destination).await?;
    let mut stats = SyncStats::default();

    // Walk source tree
    let walker = walkdir::WalkDir::new(&source)
        .follow_links(false)
        .sort_by_file_name();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                warn!("walk error: {}", e);
                stats.errors += 1;
                continue;
            }
        };

        let src_path = entry.path();
        let rel = match src_path.strip_prefix(&source) {
            Ok(r) => r,
            Err(_) => continue,
        };

        // Skip excluded
        if exclude_patterns.iter().any(|p| p.matches_path(rel)) {
            continue;
        }

        let dst_path = destination.join(rel);

        if entry.file_type().is_dir() {
            if !dst_path.exists() {
                if cli.dry_run {
                    info!("mkdir {:?}", dst_path);
                } else {
                    std::fs::create_dir_all(&dst_path)?;
                    stats.dirs_created += 1;
                }
            }
            continue;
        }

        if !entry.file_type().is_file() {
            continue; // Skip symlinks, pipes, etc for now
        }

        let src_meta = match std::fs::metadata(src_path) {
            Ok(m) => m,
            Err(e) => {
                warn!("stat {:?}: {}", src_path, e);
                stats.errors += 1;
                continue;
            }
        };

        // Decide if copy is needed
        let needs_copy = if let Ok(dst_meta) = std::fs::metadata(&dst_path) {
            // Target exists — compare size and mtime
            if dst_meta.len() == src_meta.len() && dst_meta.mtime() == src_meta.mtime()
                && dst_meta.mtime_nsec() == src_meta.mtime_nsec() {
                // Size+mtime match — check Merkle if available
                false
            } else {
                true
            }
        } else {
            true // Target doesn't exist
        };

        if !needs_copy {
            stats.files_skipped += 1;
            continue;
        }

        if cli.dry_run {
            info!("copy {:?} -> {:?} ({})", src_path, dst_path, src_meta.len());
            stats.files_copied += 1;
            stats.bytes_copied += src_meta.len();
            continue;
        }

        // Ensure parent directory exists
        if let Some(parent) = dst_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
                stats.dirs_created += 1;
            }
        }

        // Try delta copy if target exists and sizes match
        let dst_exists = dst_path.exists();
        let same_size = dst_exists && std::fs::metadata(&dst_path)
            .map(|m| m.len() == src_meta.len()).unwrap_or(false);

        if same_size && src_meta.len() > hashing::CHUNK_SIZE as u64 * 4 {
            // Files large enough to benefit from delta copy
            match try_delta_copy(&mut copier, src_path, &dst_path, src_meta.len()).await {
                Ok(Some(delta_stats)) => {
                    stats.files_delta += 1;
                    stats.bytes_delta += delta_stats.bytes_processed;
                    if cli.archive {
                        let _ = preserve_metadata(src_path, &dst_path);
                    }
                    continue;
                }
                Ok(None) => {} // Fall through to full copy
                Err(e) => {
                    debug!("delta copy failed for {:?}: {}, falling back to full copy", src_path, e);
                }
            }
        }

        // --- Auto-adaptive copy strategy selection ---
        let file_size = src_meta.len();
        let same_device = src_meta.dev() == std::fs::metadata(&destination)
            .map(|m| m.dev()).unwrap_or(0);
        let dst_is_nfs = dst_caps.is_nfs.load(Ordering::Relaxed);

        // Sparse detection: if actual disk usage is <50% of logical size, it's sparse.
        // Sparse files must use io_uring (Tier 3) which has SEEK_HOLE/PUNCH_HOLE support.
        let is_sparse = file_size > 4096
            && (src_meta.blocks() as u64 * 512) < file_size / 2;

        // Tier 1: Reflink/FICLONE (instant CoW) — preserves sparsity
        // On NFS 4.2: try even cross-device — server handles clone internally
        if (same_device || dst_is_nfs) && file_size > 0 {
            if try_reflink_copy(src_path, &dst_path) {
                stats.files_reflinked += 1;
                stats.bytes_reflinked += file_size;
                if cli.archive {
                    let _ = preserve_metadata(src_path, &dst_path);
                }
                continue;
            }
        }

        // Tier 1.5: copy_file_range (NFS 4.2 server-side copy, also works on local fs)
        // Skip for sparse files — copy_file_range writes zeros into holes
        if file_size > 0 && !is_sparse {
            match try_copy_file_range(src_path, &dst_path, file_size) {
                Ok(bytes) if bytes == file_size => {
                    stats.files_cfr += 1;
                    stats.bytes_cfr += bytes;
                    if cli.archive {
                        let _ = preserve_metadata(src_path, &dst_path);
                    }
                    continue;
                }
                Ok(_) => {
                    // Partial copy — fall through to other methods
                    let _ = std::fs::remove_file(&dst_path);
                }
                Err(e) => {
                    debug!("copy_file_range {:?}: {} — falling back", src_path, e);
                }
            }
        }

        // Tier 2: Small file fast path (std::fs::copy, no io_uring overhead)
        // Skip for sparse files — sendfile writes zeros into holes
        if file_size <= SMALL_FILE_THRESHOLD && !is_sparse {
            match copy_small_file(src_path, &dst_path) {
                Ok(bytes) => {
                    stats.files_small += 1;
                    stats.bytes_small += bytes;
                    if cli.archive {
                        let _ = preserve_metadata(src_path, &dst_path);
                    }
                    continue;
                }
                Err(e) => {
                    debug!("small file copy failed for {:?}: {}, falling back to io_uring", src_path, e);
                }
            }
        }

        // Tier 3: io_uring copy (large files, cross-device, or fallback)
        match copier.optimized_copy(
            src_path.to_path_buf(), dst_path.clone(), file_size,
            "default".into(), None, true,
        ).await {
            Ok(copy_stats) => {
                stats.files_copied += 1;
                stats.bytes_copied += copy_stats.bytes_processed;
                if cli.archive {
                    let _ = preserve_metadata(src_path, &dst_path);
                }
            }
            Err(e) => {
                warn!("copy {:?}: {}", src_path, e);
                stats.errors += 1;
            }
        }
    }

    // Handle --delete
    if cli.delete && !cli.dry_run {
        stats.files_deleted = delete_extra_files(&source, &destination, &exclude_patterns)?;
    }

    Ok(stats)
}

/// Reflink fast path: FICLONE ioctl for instant CoW copy.
/// Returns true if reflink succeeded, false if not supported/failed.
fn try_reflink_copy(src: &Path, dst: &Path) -> bool {
    use std::os::unix::io::AsRawFd;
    let src_file = match std::fs::File::open(src) {
        Ok(f) => f,
        Err(_) => return false,
    };
    // Create or truncate destination
    let dst_file = match std::fs::File::create(dst) {
        Ok(f) => f,
        Err(_) => return false,
    };
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };
    ret == 0
}

/// NFS server-side copy via copy_file_range().
/// On NFS 4.2, this triggers a server-side COPY operation — data never traverses the network.
/// Works for both same-server and cross-mount copies if the server supports it.
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
        let remaining = (size - total) as usize;
        let chunk = remaining.min(1024 * 1024 * 1024); // 1GB max per call
        let ret = unsafe {
            libc::copy_file_range(sfd, &mut off_in, dfd, &mut off_out, chunk, 0)
        };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if total == 0 {
                // First call failed — not supported, clean up
                drop(dst_file);
                let _ = std::fs::remove_file(dst);
                return Err(err);
            }
            return Err(err);
        }
        if ret == 0 { break; }
        total += ret as u64;
    }
    Ok(total)
}

/// Small file fast path: use std::fs::copy (kernel sendfile/splice internally).
/// Avoids io_uring ring submission overhead for tiny files.
fn copy_small_file(src: &Path, dst: &Path) -> std::io::Result<u64> {
    std::fs::copy(src, dst)
}

async fn try_delta_copy(
    copier: &mut SmartCopier,
    src: &Path,
    dst: &Path,
    file_size: u64,
) -> fxcp_core::Result<Option<CopyStats>> {
    let chunk_size = hashing::CHUNK_SIZE as u64;

    let src_tree = MerkleTree::from_file(src, chunk_size)?;
    let dst_tree = MerkleTree::from_file(dst, chunk_size)?;

    if src_tree.root == dst_tree.root {
        return Ok(Some(CopyStats::default())); // Files identical
    }

    let dirty = MerkleTree::diff(&src_tree, &dst_tree);
    if dirty.is_empty() {
        return Ok(Some(CopyStats::default()));
    }

    let stats = copier.copy_delta(src, dst, &dirty, file_size, "delta").await?;
    Ok(Some(stats))
}

async fn create_copier(src: &Path, dst: &Path) -> fxcp_core::Result<SmartCopier> {
    let src_caps = probe_capabilities(src);
    let dst_caps = probe_capabilities(dst);

    let ring = io_uring::IoUring::new(256)?;
    let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    if eventfd < 0 {
        return Err(fxcp_core::FxcpError::Io(std::io::Error::last_os_error()));
    }
    ring.submitter().register_eventfd(eventfd)?;
    let async_fd = Arc::new(AsyncFd::new(eventfd)?);

    let num_buffers = 64;
    let chunk_size = 4096;
    let mut buffer_pool = BufferPool::new(num_buffers, chunk_size, 512)?;

    let iovs = buffer_pool.as_io_vecs();
    unsafe { ring.submitter().register_buffers(&iovs) }?;

    let governor = Governor::new(4.0, 0, 10.0, 10.0);

    Ok(SmartCopier {
        ring,
        buffer_pool,
        atomic_buffer_pool: None,
        async_fd,
        vdo_opt: false,
        direct_io_ok: false,
        source_caps: src_caps,
        target_caps: dst_caps,
        vdo_stall_threshold: 0,
        barrier_callback: None,
        source_uncached: false,
        target_uncached: false,
        governor: Some(Arc::new(governor)),
        fsync_tracker: FsyncLatencyTracker::default(),
        skip_fsync: true,
    })
}

fn preserve_metadata(src: &Path, dst: &Path) -> std::io::Result<()> {
    let src_meta = std::fs::metadata(src)?;
    std::fs::set_permissions(dst, src_meta.permissions())?;
    // Preserve timestamps via utimensat syscall
    let times = [
        libc::timespec { tv_sec: src_meta.atime(), tv_nsec: src_meta.atime_nsec() },
        libc::timespec { tv_sec: src_meta.mtime(), tv_nsec: src_meta.mtime_nsec() },
    ];
    let dst_cstr = std::ffi::CString::new(dst.as_os_str().as_encoded_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let ret = unsafe { libc::utimensat(libc::AT_FDCWD, dst_cstr.as_ptr(), times.as_ptr(), 0) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn delete_extra_files(source: &Path, target: &Path, excludes: &[glob::Pattern]) -> fxcp_core::Result<u64> {
    let mut deleted = 0u64;
    for entry in walkdir::WalkDir::new(target).contents_first(true) {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let tgt_path = entry.path();
        let rel = match tgt_path.strip_prefix(target) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if rel.as_os_str().is_empty() { continue; }
        if excludes.iter().any(|p| p.matches_path(rel)) { continue; }

        let src_path = source.join(rel);
        if !src_path.exists() {
            if entry.file_type().is_dir() {
                let _ = std::fs::remove_dir(tgt_path);
            } else {
                let _ = std::fs::remove_file(tgt_path);
                deleted += 1;
            }
        }
    }
    Ok(deleted)
}

fn run_cleanup(path: &Path) {
    let mut orphaned = 0u64;
    let mut dirty = 0u64;
    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);

    for entry in walkdir::WalkDir::new(path) {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !entry.file_type().is_file() { continue; }
        let name = entry.file_name().to_string_lossy();

        // Orphaned .tmp files
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

        // Dirty flags
        if sidecar::is_dirty(entry.path()) {
            info!("dirty flag: {:?}", entry.path());
            dirty += 1;
        }
    }

    println!("Cleanup: {} orphaned files removed, {} dirty flags found", orphaned, dirty);
}

fn print_summary(stats: &SyncStats) {
    let total_bytes = stats.bytes_copied + stats.bytes_reflinked + stats.bytes_cfr + stats.bytes_small + stats.bytes_delta;
    let total_files = stats.files_copied + stats.files_reflinked + stats.files_cfr + stats.files_small + stats.files_delta;
    println!("fxcp sync complete:");
    println!("  Files total:   {}", total_files);
    if stats.files_reflinked > 0 {
        println!("  - reflinked:   {} ({:.1} MB, instant CoW)", stats.files_reflinked,
                 stats.bytes_reflinked as f64 / 1024.0 / 1024.0);
    }
    if stats.files_cfr > 0 {
        println!("  - server copy: {} ({:.1} MB, copy_file_range)", stats.files_cfr,
                 stats.bytes_cfr as f64 / 1024.0 / 1024.0);
    }
    if stats.files_small > 0 {
        println!("  - small copy:  {} ({:.1} MB, sendfile)", stats.files_small,
                 stats.bytes_small as f64 / 1024.0 / 1024.0);
    }
    if stats.files_copied > 0 {
        println!("  - io_uring:    {} ({:.1} MB)", stats.files_copied,
                 stats.bytes_copied as f64 / 1024.0 / 1024.0);
    }
    if stats.files_delta > 0 {
        println!("  - delta:       {} ({:.1} MB)", stats.files_delta,
                 stats.bytes_delta as f64 / 1024.0 / 1024.0);
    }
    println!("  Files skipped: {}", stats.files_skipped);
    if stats.files_deleted > 0 {
        println!("  Files deleted: {}", stats.files_deleted);
    }
    println!("  Dirs created:  {}", stats.dirs_created);
    println!("  Bytes total:   {} ({:.1} MB)", total_bytes, total_bytes as f64 / 1024.0 / 1024.0);
    if stats.errors > 0 {
        println!("  Errors:        {}", stats.errors);
    }
}
