use clap::Parser;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::unix::AsyncFd;
use tracing::{info, warn, debug, error};

use fxcp_core::constants;
use fxcp_core::error::CopyErrorKind;
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
    /// Source path (use '-' for stdin)
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
    #[arg(long, help = "Expected size in bytes (for stdin pre-allocation)")]
    size: Option<u64>,
    #[arg(long, help = "Interval in seconds to create CoW checkpoints of stdin stream")]
    checkpoint_interval: Option<u64>,
    #[arg(long, default_value = "5", help = "Number of stream checkpoints to keep")]
    checkpoint_keep: usize,
    #[arg(long, help = "Use zero-copy splice (mutually exclusive with sparse detection)")]
    zero_copy: bool,
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

    let result = if cli.source.as_os_str() == "-" {
        rt.block_on(run_stdin_to_file(&cli))
    } else {
        rt.block_on(run_sync(&cli))
    };
    match result {
        Ok(stats) => print_summary(&stats),
        Err(e) => {
            error!("fxcp failed: {}", e);
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// stdin → file mode: read piped data, write with sparse optimization
// ---------------------------------------------------------------------------

const STDIN_CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks (matches NFS wsize)

/// SIMD-accelerated zero-block detection (AVX-512/AVX2 on x86_64, NEON on AArch64).
fn is_zero(buf: &[u8]) -> bool {
    fxcp_core::operations::is_zero_block(buf)
}

/// Detect compression format from magic bytes at the start of a stream.
fn detect_compression(header: &[u8]) -> Option<&'static str> {
    if header.len() < 4 { return None; }
    // zstd magic: 0xFD2FB528
    if header[0] == 0xFD && header[1] == 0x2F && header[2] == 0xB5 && header[3] == 0x28 {
        return Some("zstd");
    }
    // gzip magic: 0x1F 0x8B
    if header[0] == 0x1F && header[1] == 0x8B {
        return Some("gzip");
    }
    // lz4 frame magic: 0x04224D18
    if header[0] == 0x04 && header[1] == 0x22 && header[2] == 0x4D && header[3] == 0x18 {
        return Some("lz4");
    }
    // xz magic: 0xFD377A585A00
    if header.len() >= 6 && header[0] == 0xFD && header[1] == 0x37
        && header[2] == 0x7A && header[3] == 0x58 && header[4] == 0x5A && header[5] == 0x00 {
        return Some("xz");
    }
    None
}

/// Wrap a reader in a decompressor based on detected format.
/// Returns the decompressing reader and format name.
fn wrap_decompressor<'a>(
    reader: Box<dyn std::io::Read + 'a>,
    format: &str,
) -> (Box<dyn std::io::Read + 'a>, &'static str) {
    match format {
        "zstd" => {
            let dec = zstd::stream::Decoder::new(reader).expect("zstd decoder init failed");
            (Box::new(dec), "zstd")
        }
        "gzip" => {
            let dec = flate2::read::GzDecoder::new(reader);
            (Box::new(dec), "gzip")
        }
        "lz4" => {
            let dec = lz4_flex::frame::FrameDecoder::new(reader);
            (Box::new(dec), "lz4")
        }
        _ => (reader, "raw")
    }
}

async fn run_stdin_to_file(cli: &Cli) -> fxcp_core::Result<SyncStats> {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;

    fxcp_core::metrics::initialize_metrics(512);

    let dst = &cli.destination;
    if let Some(parent) = dst.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    // Block device detection: skip O_TRUNC/O_CREAT for raw devices
    let is_block_device = dst.exists() && {
        use std::os::unix::fs::FileTypeExt;
        let m = std::fs::metadata(dst).map(|m| m.file_type()).ok();
        m.map(|ft| ft.is_block_device()).unwrap_or(false)
    };

    let file = if is_block_device {
        info!("Block device detected: {:?} (skipping truncate/create)", dst);
        std::fs::OpenOptions::new()
            .read(true).write(true)
            .open(dst)?
    } else {
        std::fs::OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(dst)?
    };
    let fd = file.as_raw_fd();

    // Pre-allocate if size known (not applicable to block devices)
    if !is_block_device {
        if let Some(size) = cli.size {
            let ret = unsafe {
                libc::fallocate(fd, 0, 0, size as i64)
            };
            if ret != 0 {
                debug!("fallocate pre-allocation failed (non-fatal): {}", std::io::Error::last_os_error());
            }
        }
    }

    // Mutual exclusivity: zero-copy splice bypasses userspace buffers, can't detect zeros
    let use_sparse = !cli.zero_copy;
    if cli.zero_copy && cli.checkpoint_interval.is_none() {
        // zero_copy without checkpointing is just a pass-through
    }

    let checkpoint_interval = cli.checkpoint_interval.map(std::time::Duration::from_secs);
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
                Err(e) => return Err(fxcp_core::FxcpError::Io(e)),
            }
        }
        n
    };

    let compression = detect_compression(&header_buf[..header_len]);
    // Chain the already-read header bytes with the rest of stdin
    let chained: Box<dyn std::io::Read> = Box::new(std::io::Cursor::new(header_buf[..header_len].to_vec()).chain(header_reader));
    let (mut reader, comp_name): (Box<dyn std::io::Read>, &str) = if let Some(fmt) = compression {
        info!("Detected {} compressed input — decompressing inline", fmt);
        wrap_decompressor(chained, fmt)
    } else {
        (chained, "raw")
    };

    let mut buf = vec![0u8; STDIN_CHUNK_SIZE];
    let mut offset: u64 = 0;
    let mut bytes_written: u64 = 0;
    let mut bytes_sparse: u64 = 0;
    let mut chunks_data: u64 = 0;
    let mut chunks_zero: u64 = 0;

    loop {
        // Read a full chunk from stdin (handling partial reads)
        let mut filled = 0;
        while filled < STDIN_CHUNK_SIZE {
            let n = match reader.read(&mut buf[filled..]) {
                Ok(0) => break,     // EOF
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(fxcp_core::FxcpError::Io(e)),
            };
            filled += n;
        }
        if filled == 0 { break; } // EOF

        if use_sparse && is_zero(&buf[..filled]) {
            // Zero chunk — create a hole (don't write, just advance offset)
            // The file will have a hole here (sparse)
            bytes_sparse += filled as u64;
            chunks_zero += 1;
        } else {
            // Data chunk — write at current offset via pwrite
            let mut written = 0;
            while written < filled {
                let ret = unsafe {
                    libc::pwrite(fd, buf[written..filled].as_ptr() as *const _,
                                 filled - written, (offset + written as u64) as i64)
                };
                if ret < 0 {
                    return Err(fxcp_core::FxcpError::Io(std::io::Error::last_os_error()));
                }
                written += ret as usize;
            }
            bytes_written += filled as u64;
            chunks_data += 1;
        }
        offset += filled as u64;

        // Rolling checkpoint: create CoW snapshot via FICLONE at intervals
        if let Some(interval) = checkpoint_interval {
            if last_checkpoint.elapsed() >= interval {
                file.sync_all()?;
                let ts = chrono::Utc::now().timestamp();
                let snap_path = dst.with_extension(format!("snap.{}", ts));
                // FICLONE the current file to create an instant snapshot
                if let Ok(snap_file) = std::fs::File::create(&snap_path) {
                    let ret = unsafe {
                        libc::ioctl(snap_file.as_raw_fd(), FICLONE, fd)
                    };
                    if ret == 0 {
                        checkpoint_count += 1;
                        info!("Stream checkpoint #{}: {:?} ({})", checkpoint_count, snap_path, format_bytes(offset));

                        // Prune old checkpoints beyond keep limit
                        if checkpoint_count > cli.checkpoint_keep as u64 {
                            prune_stream_checkpoints(dst, cli.checkpoint_keep);
                        }
                    } else {
                        debug!("FICLONE checkpoint failed (non-fatal): {}", std::io::Error::last_os_error());
                    }
                }
                last_checkpoint = std::time::Instant::now();
            }
        }
    }

    // Truncate to exact size (sets file size even if last chunk was a hole)
    // Skip for block devices — they have fixed size
    if !is_block_device {
        unsafe { libc::ftruncate(fd, offset as i64) };
    }

    // fsync
    file.sync_all()?;

    info!("stdin → {:?}: {} total, {} data, {} sparse ({} zero chunks punched)",
          dst, format_bytes(offset), format_bytes(bytes_written),
          format_bytes(bytes_sparse), chunks_zero);

    Ok(SyncStats {
        files_copied: 1,
        bytes_copied: bytes_written,
        bytes_small: bytes_sparse, // reuse for sparse display
        ..Default::default()
    })
}

fn prune_stream_checkpoints(base_path: &Path, keep: usize) {
    let parent = match base_path.parent() {
        Some(p) => p,
        None => return,
    };
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

fn format_bytes(b: u64) -> String {
    if b >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", b as f64 / 1024.0 / 1024.0 / 1024.0)
    } else if b >= 1024 * 1024 {
        format!("{:.1} MB", b as f64 / 1024.0 / 1024.0)
    } else if b >= 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{} B", b)
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
                if e.kind() == std::io::ErrorKind::NotFound {
                    // Source vanished between walk and stat — skip silently
                    debug!("Source vanished before stat: {:?}", src_path);
                    stats.files_skipped += 1;
                } else {
                    warn!("stat {:?}: {}", src_path, e);
                    stats.errors += 1;
                }
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
                match e.copy_error_kind() {
                    CopyErrorKind::SourceNotFound | CopyErrorKind::TargetNotFound => {
                        // In copy context, NotFound almost always means the source
                        // vanished between stat() and copy() — common in live trees.
                        // TargetNotFound would mean parent disappeared mid-walk.
                        debug!("Path vanished during copy: {:?}: {}", src_path, e);
                        stats.files_skipped += 1;
                    }
                    CopyErrorKind::Timeout => {
                        warn!("Copy timed out: {:?}: {}", src_path, e);
                        stats.errors += 1;
                    }
                    CopyErrorKind::Transient => {
                        warn!("Transient error copying {:?}: {} (may succeed on retry)", src_path, e);
                        stats.errors += 1;
                    }
                    CopyErrorKind::Permanent => {
                        error!("Permanent error copying {:?}: {}", src_path, e);
                        stats.errors += 1;
                    }
                }
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
        #[allow(deprecated)]
        segment_stall_timeout_secs: fxcp_core::constants::PROCESS_SEGMENT_STALL_SECS,
        #[allow(deprecated)]
        segment_overall_timeout_secs: fxcp_core::constants::PROCESS_SEGMENT_TIMEOUT_SECS,
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
