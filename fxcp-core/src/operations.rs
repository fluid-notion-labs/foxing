// Core copy engine — SmartCopier, Capabilities, io_uring I/O
use std::path::{Path, PathBuf};
use std::os::unix::io::{AsRawFd, RawFd, FromRawFd, IntoRawFd};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::io;
use std::ffi::CString;
use tokio::task::spawn_blocking;
use io_uring::{opcode, types, IoUring, squeue};
use uuid::Uuid;
use libc;
use nix::sys::statfs;
use std::time::{Duration, Instant};
use tracing::{warn, debug, error, trace, info};
use crate::buffer::{BufferPool};
use crate::error::{FxcpError, Result};
use crate::security;
use crate::metrics;
use std::os::unix::fs::MetadataExt;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use dashmap::DashMap;
use crate::governor::Governor;
use tokio::fs::File;
use tokio::io::unix::AsyncFd;
use lazy_static::lazy_static;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use std::arch::x86_64::*;

const RWF_UNCACHED: i32 = 0x00000040;
const RWF_ATOMIC: i32 = 0x00000080;
const STATX_WRITE_ATOMIC: u32 = 0x00010000;
const OP_TYPE_MASK: u64 = 0xFFFF_0000_0000_0000;
const INDEX_MASK: u64 = 0x0000_0000_0000_FFFF;
const READ_OP: u64 = 1 << 48;
const WRITE_OP: u64 = 2 << 48;
const FALLOC_OP: u64 = 3 << 48;
const HOLE_OP: u64 = 4 << 48;
const FICLONE: u64 = 0x40049409;
const FICLONERANGE: u64 = 0x4020940D;
pub const NFS_SUPER_MAGIC: i64 = 0x6969;
pub const BTRFS_SUPER_MAGIC: i64 = 0x9123683E;
pub const FS_IOC_GETFLAGS: u64 = 0x80086601;
pub const FS_IOC_SETFLAGS: u64 = 0x40086602;
pub const FS_IMMUTABLE_FL: u32 = 0x00000010;
pub const FS_APPEND_FL: u32 = 0x00000020;
pub const FS_NOCOW_FL: u32 = 0x00800000;
const F2FS_SUPER_MAGIC: i64 = 0xF2F52010;
const F2FS_IOC_START_ATOMIC_WRITE: u64 = 0xF501;
const F2FS_IOC_COMMIT_ATOMIC_WRITE: u64 = 0xF502;
const F2FS_IOC_ABORT_ATOMIC_WRITE: u64 = 0xF505;
const F2FS_IOC_SET_PIN_FILE: u64 = 0xF50D;
const BTRFS_IOC_SNAP_CREATE_V2: u64 = 0x50009417;
const BTRFS_IOC_SUBVOL_CREATE_V2: u64 = 0x50009418;
const BTRFS_IOC_SCRUB: u64 = 0xC400941B;
const BTRFS_IOC_SEND: u64 = 0x40489426;
const BTRFS_IOC_INO_LOOKUP: u64 = 0xD0009412;
const BTRFS_IOC_FS_INFO: u64 = 0x8400941F;

lazy_static! {
    static ref GLOBAL_CAPS_CACHE: DashMap<PathBuf, Arc<Capabilities>> = DashMap::new();
}

#[derive(Debug, Clone)]
pub struct FsyncLatencyTracker {
    avg_us: u64,
    deviation_us: u64,
}

impl Default for FsyncLatencyTracker {
    fn default() -> Self {
        Self {
            avg_us: 1_000_000,
            deviation_us: 500_000,
        }
    }
}

impl FsyncLatencyTracker {
    pub fn record_success(&mut self, duration: Duration) {
        let sample = duration.as_micros() as u64;
        let diff = if sample > self.avg_us { sample - self.avg_us } else { self.avg_us - sample };
        self.deviation_us = (self.deviation_us * 3 + diff) / 4;
        self.avg_us = (self.avg_us * 7 + sample) / 8;
    }

    pub fn record_timeout(&mut self) {
        self.avg_us = self.avg_us.saturating_mul(2).min(30_000_000);
        self.deviation_us = self.deviation_us.saturating_mul(2);
    }

    pub fn get_timeout(&self) -> Duration {
        let timeout_us = self.avg_us + (4 * self.deviation_us);
        Duration::from_micros(timeout_us.clamp(5_000_000, 60_000_000))
    }
}

#[repr(C)]
struct FileCloneRange {
    src_fd: i64,
    src_offset: u64,
    src_length: u64,
    dest_offset: u64,
}

#[repr(C)]
pub struct FileHandle {
    pub handle_bytes: u32,
    pub handle_type: i32,
    pub f_handle: [u8; 128],
}

#[repr(C)]
struct BtrfsIoctlVolArgsV2 {
    fd: i64,
    transid: u64,
    flags: u64,
    union_reserved: [u64; 4],
    name: [i8; 4040],
}

#[repr(C)]
struct BtrfsScrubArgs {
    devid: u64,
    start: u64,
    end: u64,
    flags: u64,
    progress: [u64; 16],
}

#[repr(C)]
struct BtrfsIoctlSendArgs {
    send_fd: i64,
    clone_sources_count: u64,
    clone_sources: u64,
    parent_root: u64,
    flags: u64,
    reserved: [u64; 4],
}

#[repr(C)]
struct BtrfsIoctlInoLookupArgs {
    treeid: u64,
    objectid: u64,
    name: [u8; 4080],
}

#[repr(C)]
struct BtrfsIoctlFsInfoArgs {
    max_id: u64,
    num_devices: u64,
    fsid: [u8; 16],
    nodesize: u32,
    sectorsize: u32,
    clone_alignment: u32,
    reserved32: u32,
    reserved: [u64; 122],
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CopyStats {
    pub bytes_processed: u64,
    pub bytes_zeros: u64,
    pub io_duration: Duration,
    pub ops_count: u64,
}

#[derive(Debug, Clone)]
enum FileSegment {
    Data { offset: u64, len: u64 },
    Hole { offset: u64, len: u64 },
}

pub enum Operation {
    CopyFile { src: PathBuf, dst: PathBuf, src_file_size: u64, target_label: String },
    CopyRange { src: PathBuf, dst: PathBuf, offset: u64, length: u64, src_file_size: u64, target_label: String },
    Truncate { dst: PathBuf, size: u64 },
    Rename { src: PathBuf, dst: PathBuf, flags: u32 },
    Fallocate { dst: PathBuf, mode: i32, offset: u64, length: u64 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyStrategy {
    Reflink,
    StandardCopy,
}

pub trait OptimizedFs {
    fn optimized_copy(&mut self, src: PathBuf, dst: PathBuf, src_file_size: u64, target_label: String, buffer_limit: Option<usize>, skip_fsync: bool) -> impl std::future::Future<Output = Result<CopyStats>> + Send;
    fn optimized_copy_range(&mut self, src: PathBuf, dst: PathBuf, offset: u64, length: u64, src_file_size: u64, target_label: String, buffer_limit: Option<usize>, skip_fsync: bool) -> impl std::future::Future<Output = Result<CopyStats>> + Send;
    fn optimized_truncate(&mut self, dst: PathBuf, size: u64) -> impl std::future::Future<Output = Result<CopyStats>> + Send;
    fn optimized_rename(&mut self, src: PathBuf, dst: PathBuf, flags: u32) -> impl std::future::Future<Output = Result<CopyStats>> + Send;
    fn optimized_fallocate(&mut self, dst: PathBuf, mode: i32, offset: u64, length: u64) -> impl std::future::Future<Output = Result<CopyStats>> + Send;
}

// ---------------------------------------------------------------------------
// Container and storage stack detection
// ---------------------------------------------------------------------------

/// Container runtime information.
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub in_container: bool,
    pub engine: Option<String>,
    pub rootless: bool,
}

/// Device-mapper stack layers detected beneath a filesystem.
#[derive(Debug, Clone)]
pub struct DmStackInfo {
    pub has_crypt: bool,
    pub has_integrity: bool,
    pub has_cache: bool,
    pub has_thin: bool,
    pub has_vdo: bool,
    pub has_stratis: bool,
    pub crypt_sector_size: u32,
    pub integrity_tag_size: u32,
    pub thin_pool_data_pct: f64,
    pub thin_pool_meta_pct: f64,
    pub stack_depth: u8,
    pub physical_block_size: u32,
    pub optimal_io_size: u32,
    pub base_device: Option<String>,
}

impl Default for DmStackInfo {
    fn default() -> Self {
        Self {
            has_crypt: false, has_integrity: false, has_cache: false,
            has_thin: false, has_vdo: false, has_stratis: false,
            crypt_sector_size: 0, integrity_tag_size: 0,
            thin_pool_data_pct: 0.0, thin_pool_meta_pct: 0.0,
            stack_depth: 0, physical_block_size: 512, optimal_io_size: 0,
            base_device: None,
        }
    }
}

pub struct Capabilities {
    pub atomic_writes: AtomicBool,
    pub atomic_min_bytes: AtomicU32,
    pub atomic_max_bytes: AtomicU32,
    pub uncached_io: AtomicBool,
    pub exchange_range: AtomicBool,
    pub reflink: AtomicBool,
    pub seek_hole: AtomicBool,
    pub btrfs_subvol: AtomicBool,
    pub btrfs_quotas: AtomicBool,
    pub f2fs_atomic_legacy: AtomicBool,
    pub is_nfs: AtomicBool,
    pub dm_stack: Option<DmStackInfo>,
    pub container: Option<ContainerInfo>,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            atomic_writes: AtomicBool::new(false),
            atomic_min_bytes: AtomicU32::new(0),
            atomic_max_bytes: AtomicU32::new(0),
            uncached_io: AtomicBool::new(false),
            exchange_range: AtomicBool::new(true),
            reflink: AtomicBool::new(false),
            seek_hole: AtomicBool::new(false),
            btrfs_subvol: AtomicBool::new(false),
            btrfs_quotas: AtomicBool::new(false),
            f2fs_atomic_legacy: AtomicBool::new(false),
            is_nfs: AtomicBool::new(false),
            dm_stack: None,
            container: None,
        }
    }
}

#[repr(C)]
struct StatxAtomic {
    stx_mask: u32,
    stx_blksize: u32,
    stx_attributes: u64,
    stx_nlink: u32,
    stx_uid: u32,
    stx_gid: u32,
    stx_mode: u16,
    __spare0: [u16; 1],
    stx_ino: u64,
    stx_size: u64,
    stx_blocks: u64,
    stx_attributes_mask: u64,
    stx_atime: libc::statx_timestamp,
    stx_btime: libc::statx_timestamp,
    stx_ctime: libc::statx_timestamp,
    stx_mtime: libc::statx_timestamp,
    stx_rdev_major: u32,
    stx_rdev_minor: u32,
    stx_dev_major: u32,
    stx_dev_minor: u32,
    stx_mnt_id: u64,
    stx_atomic_write_unit_min: u32,
    stx_atomic_write_unit_max: u32,
    stx_atomic_write_segments_max: u32,
    __spare1: [u64; 10],
}

pub struct CleanupGuard {
    files: Vec<PathBuf>,
}

impl CleanupGuard {
    pub fn new(files: Vec<PathBuf>) -> Self {
        Self { files }
    }

    pub fn empty() -> Self {
        Self { files: Vec::new() }
    }

    pub fn register(&mut self, path: PathBuf) {
        self.files.push(path);
    }

    /// Remove a path from cleanup list (call after successful rename/commit).
    pub fn disarm(&mut self, path: &Path) {
        self.files.retain(|p| p != path);
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        for p in &self.files {
            let _ = std::fs::remove_file(p);
        }
    }
}

pub fn probe_reflink_support(target_root: &Path) -> bool {
    let uuid = Uuid::new_v4();
    let src_path = target_root.join(format!(".foxing_probe_src_{}", uuid));
    let dst_path = target_root.join(format!(".foxing_probe_dst_{}", uuid));
    
    let _guard = CleanupGuard::new(vec![src_path.clone(), dst_path.clone()]);
    
    let mut src_file = match std::fs::File::create(&src_path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    use std::io::Write;
    let buf = [0xAAu8; 4096];
    if src_file.write_all(&buf).is_err() { return false; }
    if src_file.sync_all().is_err() { return false; }
    
    let dst_file = match std::fs::File::create(&dst_path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    
    let src_fd = src_file.as_raw_fd();
    let dst_fd = dst_file.as_raw_fd();
    
    let ret = unsafe { libc::ioctl(dst_fd, FICLONE, src_fd) };
    
    let supported = if ret == 0 {
        if let Ok(meta) = dst_file.metadata() {
            meta.len() == 4096
        } else {
            false
        }
    } else {
        false
    };
    
    drop(src_file);
    drop(dst_file);
    
    supported
}

fn has_nocow_flag(path: &Path) -> bool {
    if let Ok(f) = std::fs::File::open(path) {
        let fd = f.as_raw_fd();
        let mut flags: u32 = 0;
        let ret = unsafe { libc::ioctl(fd, FS_IOC_GETFLAGS, &mut flags) };
        if ret == 0 {
            return (flags & FS_NOCOW_FL) != 0;
        }
    }
    false
}

pub fn determine_copy_strategy(
    src_path: &Path,
    dst_path: &Path,
    src_meta: &std::fs::Metadata,
    caps: &Capabilities
) -> CopyStrategy {
    if !caps.reflink.load(Ordering::Relaxed) {
        return CopyStrategy::StandardCopy;
    }
    
    let dst_dev = if let Ok(m) = std::fs::metadata(dst_path) {
        m.dev()
    } else if let Some(parent) = dst_path.parent() {
        if let Ok(m) = std::fs::metadata(parent) {
            m.dev()
        } else {
            return CopyStrategy::StandardCopy;
        }
    } else {
        return CopyStrategy::StandardCopy;
    };

    if src_meta.dev() != dst_dev {
        return CopyStrategy::StandardCopy;
    }

    if has_nocow_flag(src_path) {
        debug!("Strategy: Skipping reflink for NOCOW file: {:?}", src_path);
        return CopyStrategy::StandardCopy;
    }

    if src_meta.len() < 4096 {
        return CopyStrategy::StandardCopy;
    }

    CopyStrategy::Reflink
}

pub fn probe_capabilities(path: &Path) -> Arc<Capabilities> {
    let cache_key = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if let Some(cached) = GLOBAL_CAPS_CACHE.get(&cache_key) {
        return cached.clone();
    }

    debug!("probe_capabilities: START {:?}", path);
    let mut caps_inner = Capabilities::default();

    if let Ok(c_path) = CString::new(path.to_string_lossy().as_bytes()) {
        let mut stx: StatxAtomic = unsafe { std::mem::zeroed() };
        let ret = unsafe {
            libc::syscall(
                libc::SYS_statx,
                libc::AT_FDCWD,
                c_path.as_ptr(),
                libc::AT_EMPTY_PATH | libc::AT_NO_AUTOMOUNT,
                STATX_WRITE_ATOMIC,
                &mut stx as *mut _
            )
        };
        
        if ret == 0 && (stx.stx_mask & STATX_WRITE_ATOMIC) != 0 {
            if stx.stx_atomic_write_unit_max > 0 {
                caps_inner.atomic_min_bytes.store(stx.stx_atomic_write_unit_min, Ordering::Relaxed);
                caps_inner.atomic_max_bytes.store(stx.stx_atomic_write_unit_max, Ordering::Relaxed);
                caps_inner.atomic_writes.store(true, Ordering::Relaxed);
                debug!("probe_capabilities: Atomic writes detected");
            }
        }
    }

    debug!("probe_capabilities: checking uncached_io");
    caps_inner.uncached_io.store(crate::security::probe_rwf_uncached(path), Ordering::Relaxed);

    debug!("probe_capabilities: checking statfs magic");
    if let Ok(s) = statfs::statfs(path) {
        let magic = s.filesystem_type().0 as i64;
        if magic == NFS_SUPER_MAGIC {
            caps_inner.is_nfs.store(true, Ordering::Relaxed);
            debug!("probe_capabilities: NFS detected");
        }
        if magic == F2FS_SUPER_MAGIC {
            caps_inner.f2fs_atomic_legacy.store(true, Ordering::Relaxed);
            if !caps_inner.atomic_writes.load(Ordering::Relaxed) {
                caps_inner.atomic_writes.store(true, Ordering::Relaxed);
                caps_inner.atomic_min_bytes.store(4096, Ordering::Relaxed);
                caps_inner.atomic_max_bytes.store(u32::MAX, Ordering::Relaxed);
                debug!("Probe: F2FS Detected. Enabling Legacy Atomic Writes (IOCTL).");
            }
        }
    }

    debug!("probe_capabilities: checking file IOCTLs");
    if let Ok(f) = std::fs::File::open(path) {
        let fd = f.as_raw_fd();
        if unsafe { libc::lseek(fd, 0, libc::SEEK_DATA) } >= 0 {
             caps_inner.seek_hole.store(true, Ordering::Relaxed);
        }
        if probe_btrfs_quotas(fd) {
            caps_inner.btrfs_quotas.store(true, Ordering::Relaxed);
            debug!("Probe: Btrfs Qgroups detected on {:?}", path);
        }
    }

    debug!("probe_capabilities: checking reflink");
    if probe_reflink_support(path) {
        caps_inner.reflink.store(true, Ordering::Relaxed);
        debug!("probe_capabilities: Reflink supported");
    }

    debug!("probe_capabilities: checking dm-stack");
    caps_inner.dm_stack = probe_dm_stack(path);
    caps_inner.container = Some(detect_container());
    if let Some(ref dm) = caps_inner.dm_stack {
        debug!("probe_capabilities: dm-stack depth={} crypt={} integrity={} base={:?}",
               dm.stack_depth, dm.has_crypt, dm.has_integrity, dm.base_device);
    }

    let caps = Arc::new(caps_inner);
    debug!("probe_capabilities: END {:?}", path);
    GLOBAL_CAPS_CACHE.insert(cache_key, caps.clone());
    caps
}

// ---------------------------------------------------------------------------
// Container detection
// ---------------------------------------------------------------------------

pub fn detect_container() -> ContainerInfo {
    // podman/toolbx: /run/.containerenv
    if let Ok(content) = std::fs::read_to_string("/run/.containerenv") {
        let engine = content.lines()
            .find(|l| l.starts_with("engine="))
            .map(|l| l.trim_start_matches("engine=").trim_matches('"').to_string());
        let rootless = content.contains("rootless=1");
        return ContainerInfo { in_container: true, engine, rootless };
    }
    // docker: /.dockerenv
    if Path::new("/.dockerenv").exists() {
        return ContainerInfo { in_container: true, engine: Some("docker".into()), rootless: false };
    }
    ContainerInfo { in_container: false, engine: None, rootless: false }
}

// ---------------------------------------------------------------------------
// Device-mapper stack probing via /proc/self/mountinfo + sysfs
// ---------------------------------------------------------------------------

/// Resolve the backing block device for a path by parsing /proc/self/mountinfo.
/// Works inside containers where stat().st_dev returns virtual device numbers.
fn resolve_backing_device(path: &Path) -> Option<String> {
    let canonical = path.canonicalize().ok()?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;

    let mut best_mount = String::new();
    let mut best_source = String::new();
    let mut best_len = 0;

    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 10 { continue; }
        let mount_point = fields[4];
        // Fields after the " - " separator: fs_type source super_options
        let sep_pos = fields.iter().position(|&f| f == "-");
        let sep_pos = match sep_pos {
            Some(p) => p,
            None => continue,
        };
        if sep_pos + 2 >= fields.len() { continue; }
        let source = fields[sep_pos + 2];

        let mount_path = Path::new(mount_point);
        if canonical.starts_with(mount_path) {
            let mlen = mount_point.len();
            if mlen > best_len {
                best_len = mlen;
                best_mount = mount_point.to_string();
                best_source = source.to_string();
            }
        }
    }

    if best_source.is_empty() || best_source == "none" || best_source == "overlay" {
        return None;
    }
    debug!("resolve_backing_device: {:?} → mount={} source={}", path, best_mount, best_source);
    Some(best_source)
}

/// Resolve a /dev/mapper/NAME or /dev/dm-N path to the sysfs block device name (e.g. "dm-0").
fn resolve_sysfs_block_name(device_path: &str) -> Option<String> {
    let dev_path = Path::new(device_path);
    // /dev/mapper/NAME → readlink to /dev/dm-N
    let resolved = if device_path.starts_with("/dev/mapper/") {
        std::fs::read_link(dev_path).ok()?
    } else {
        dev_path.to_path_buf()
    };
    // Extract "dm-0" from "/dev/dm-0"
    resolved.file_name()?.to_str().map(String::from)
}

/// Probe the device-mapper stack beneath a filesystem path.
pub fn probe_dm_stack(path: &Path) -> Option<DmStackInfo> {
    let device = resolve_backing_device(path)?;
    let block_name = resolve_sysfs_block_name(&device)?;

    let mut info = DmStackInfo::default();
    let mut current_dev = block_name.clone();

    // Walk the dm stack
    loop {
        let dm_uuid_path = format!("/sys/block/{}/dm/uuid", current_dev);
        if let Ok(uuid) = std::fs::read_to_string(&dm_uuid_path) {
            let uuid = uuid.trim();
            info.stack_depth += 1;

            if uuid.starts_with("CRYPT-LUKS2-") {
                info.has_crypt = true;
                info.crypt_sector_size = 4096;
            } else if uuid.starts_with("CRYPT-LUKS1-") || uuid.starts_with("CRYPT-") {
                info.has_crypt = true;
                info.crypt_sector_size = 512;
            } else if uuid.starts_with("INTEGRITY-") {
                info.has_integrity = true;
                // Read tag size from dm table if available
                info.integrity_tag_size = 4096;
            } else if uuid.starts_with("LVM-") {
                // Check if thin pool by looking for pool target
                let table_path = format!("/sys/block/{}/dm/name", current_dev);
                if let Ok(name) = std::fs::read_to_string(&table_path) {
                    if name.trim().contains("tpool") || name.trim().contains("thin") {
                        info.has_thin = true;
                    }
                }
            } else if uuid.starts_with("VDO-") {
                info.has_vdo = true;
            }

            // Check for Stratis naming
            let name_path = format!("/sys/block/{}/dm/name", current_dev);
            if let Ok(name) = std::fs::read_to_string(&name_path) {
                if name.trim().contains("stratis") {
                    info.has_stratis = true;
                }
            }
        }

        // Walk slaves to find underlying device
        let slaves_path = format!("/sys/block/{}/slaves", current_dev);
        if let Ok(entries) = std::fs::read_dir(&slaves_path) {
            let slaves: Vec<String> = entries
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect();
            if slaves.len() == 1 {
                current_dev = slaves[0].clone();
                continue; // Keep walking the stack
            } else if slaves.is_empty() {
                // Reached the base device
                info.base_device = Some(current_dev.clone());
                break;
            } else {
                // Multiple slaves (RAID, multipath) — take first, stop recursion
                info.base_device = Some(slaves[0].clone());
                break;
            }
        } else {
            // No slaves directory — this is a physical device
            info.base_device = Some(current_dev.clone());
            break;
        }
    }

    // Read queue properties from base device
    if let Some(ref base) = info.base_device {
        let pbs_path = format!("/sys/block/{}/queue/physical_block_size", base);
        if let Ok(val) = std::fs::read_to_string(&pbs_path) {
            info.physical_block_size = val.trim().parse().unwrap_or(512);
        }
        let oio_path = format!("/sys/block/{}/queue/optimal_io_size", base);
        if let Ok(val) = std::fs::read_to_string(&oio_path) {
            info.optimal_io_size = val.trim().parse().unwrap_or(0);
        }
    }

    Some(info)
}

fn probe_btrfs_quotas(fd: RawFd) -> bool {
    let mut info_args: BtrfsIoctlFsInfoArgs = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(fd, BTRFS_IOC_FS_INFO, &mut info_args) } < 0 {
        return false;
    }
    
    let uuid_bytes = info_args.fsid;
    let uuid_str = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        uuid_bytes[0], uuid_bytes[1], uuid_bytes[2], uuid_bytes[3],
        uuid_bytes[4], uuid_bytes[5], uuid_bytes[6], uuid_bytes[7],
        uuid_bytes[8], uuid_bytes[9], uuid_bytes[10], uuid_bytes[11],
        uuid_bytes[12], uuid_bytes[13], uuid_bytes[14], uuid_bytes[15]
    );
    
    let sysfs_path = PathBuf::from(format!("/sys/fs/btrfs/{}/qgroups", uuid_str));
    if sysfs_path.exists() {
        return true;
    }
    
    let sysfs_path_old = PathBuf::from(format!("/sys/fs/btrfs/{}/quota_override", uuid_str));
    if sysfs_path_old.exists() {
        return true;
    }
    
    false
}

pub fn btrfs_resolve_inode(fd: RawFd, inode: u64) -> Result<PathBuf> {
    let mut args = BtrfsIoctlInoLookupArgs { treeid: 0, objectid: inode, name: [0; 4080] };
    let ret = unsafe { libc::ioctl(fd, BTRFS_IOC_INO_LOOKUP, &mut args) };
    if ret < 0 { return Err(FxcpError::Io(std::io::Error::last_os_error())); }
    
    let name_slice = match args.name.iter().position(|&c| c == 0) {
        Some(pos) => &args.name[..pos],
        None => &args.name[..],
    };
    
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(name_slice)))
}

pub fn btrfs_create_snapshot(src_fd: RawFd, dest_dir: &Path, name: &str) -> Result<()> {
    let dest_dir_file = std::fs::File::open(dest_dir).map_err(FxcpError::Io)?;
    let cname = CString::new(name).map_err(|_| FxcpError::Config("Invalid snapshot name".into()))?;
    
    if cname.as_bytes().len() > 4039 {
        return Err(FxcpError::Config("Snapshot name too long".into()));
    }

    let mut args = BtrfsIoctlVolArgsV2 {
        fd: src_fd as i64,
        transid: 0,
        flags: 0,
        union_reserved: [0; 4],
        name: [0; 4040]
    };

    unsafe {
        std::ptr::copy_nonoverlapping(
            cname.as_ptr(), 
            args.name.as_mut_ptr() as *mut i8, 
            cname.as_bytes().len()
        );
    }

    let ret = unsafe { libc::ioctl(dest_dir_file.as_raw_fd(), BTRFS_IOC_SNAP_CREATE_V2, &args) };
    if ret < 0 {
        return Err(FxcpError::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

pub fn btrfs_create_subvol(dest_dir: &Path, name: &str) -> Result<()> {
    let dir_file = std::fs::File::open(dest_dir).map_err(FxcpError::Io)?;
    let cname = CString::new(name).map_err(|_| FxcpError::Config("Invalid subvol name".into()))?;
    
    if cname.as_bytes().len() > 4039 {
        return Err(FxcpError::Config("Subvolume name too long".into()));
    }

    let mut args = BtrfsIoctlVolArgsV2 {
        fd: 0,
        transid: 0,
        flags: 0,
        union_reserved: [0; 4],
        name: [0; 4040]
    };

    unsafe {
        std::ptr::copy_nonoverlapping(
            cname.as_ptr(), 
            args.name.as_mut_ptr() as *mut i8, 
            cname.as_bytes().len()
        );
    }

    let ret = unsafe { libc::ioctl(dir_file.as_raw_fd(), BTRFS_IOC_SUBVOL_CREATE_V2, &args) };
    if ret < 0 {
        return Err(FxcpError::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

pub fn btrfs_scrub_start(mount_point: &Path) -> Result<()> {
    let f = std::fs::File::open(mount_point).map_err(FxcpError::Io)?;
    let mut args: BtrfsScrubArgs = unsafe { std::mem::zeroed() };
    args.devid = 0;
    
    let ret = unsafe { libc::ioctl(f.as_raw_fd(), BTRFS_IOC_SCRUB, &mut args) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINPROGRESS) {
            return Ok(());
        }
        return Err(FxcpError::Io(err));
    }
    Ok(())
}

pub fn btrfs_send_stream(
    subvol_fd: RawFd, 
    parent_root_id: u64, 
    clone_sources: &[u64]
) -> Result<std::fs::File> {
    let (pipe_r, pipe_w) = nix::unistd::pipe().map_err(FxcpError::System)?;
    let clone_sources_vec = clone_sources.to_vec();
    
    let fd_dup = nix::unistd::dup(subvol_fd).map_err(FxcpError::System)?;
    let pipe_w_fd = pipe_w.into_raw_fd();

    std::thread::spawn(move || {
        let ptr = if !clone_sources_vec.is_empty() {
            clone_sources_vec.as_ptr() as u64
        } else {
            0
        };
        
        let mut args = BtrfsIoctlSendArgs {
            send_fd: pipe_w_fd as i64,
            clone_sources_count: clone_sources_vec.len() as u64,
            clone_sources: ptr,
            parent_root: parent_root_id,
            flags: 0,
            reserved: [0; 4]
        };

        let ret = unsafe { libc::ioctl(fd_dup, BTRFS_IOC_SEND, &mut args) };
        let _ = nix::unistd::close(fd_dup);
        let _ = nix::unistd::close(pipe_w_fd);
        
        if ret < 0 {
            error!("Btrfs Send Failed: {}", std::io::Error::last_os_error());
        } else {
            debug!("Btrfs Send Completed successfully.");
        }
    });

    let file = unsafe { std::fs::File::from_raw_fd(pipe_r.into_raw_fd()) };
    Ok(file)
}

pub fn open_by_handle_at(mount_fd: RawFd, handle: &FileHandle) -> Result<std::fs::File> {
    let fd = unsafe {
        libc::syscall(
            libc::SYS_open_by_handle_at,
            mount_fd,
            handle as *const _ as *const libc::c_void,
            libc::O_RDONLY | libc::O_NOATIME
        )
    };
    if fd < 0 {
        return Err(FxcpError::Io(std::io::Error::last_os_error()));
    }
    Ok(unsafe { std::fs::File::from_raw_fd(fd as i32) })
}

pub struct SmartCopier {
    pub ring: IoUring,
    pub buffer_pool: BufferPool,
    pub atomic_buffer_pool: Option<BufferPool>,
    pub async_fd: Arc<AsyncFd<RawFd>>,
    pub vdo_opt: bool,
    pub direct_io_ok: bool,
    pub source_caps: Arc<Capabilities>,
    pub target_caps: Arc<Capabilities>,
    pub vdo_stall_threshold: u32,
    pub barrier_callback: Option<Box<dyn Fn(u64) + Send + Sync>>,
    pub source_uncached: bool,
    pub target_uncached: bool,
    pub governor: Option<Arc<Governor>>,
    pub fsync_tracker: FsyncLatencyTracker,
    pub skip_fsync: bool, // NEW: Optimization flag
}

impl OptimizedFs for SmartCopier {
    fn optimized_copy(&mut self, src: PathBuf, dst: PathBuf, src_file_size: u64, target_label: String, buffer_limit: Option<usize>, skip_fsync: bool) -> impl std::future::Future<Output = Result<CopyStats>> + Send {
        async move {
            let op = Operation::CopyFile { src, dst, src_file_size, target_label };
            Self::dispatch(op, &mut self.ring, &mut self.buffer_pool, self.atomic_buffer_pool.as_mut(), self.async_fd.clone(), self.vdo_opt, self.direct_io_ok, &self.source_caps, &self.target_caps, self.vdo_stall_threshold, &self.barrier_callback, self.source_uncached, self.target_uncached, self.governor.clone(), &mut self.fsync_tracker, buffer_limit, skip_fsync).await
        }
    }

    fn optimized_rename(&mut self, src: PathBuf, dst: PathBuf, flags: u32) -> impl std::future::Future<Output = Result<CopyStats>> + Send {
        async move {
            let op = Operation::Rename { src, dst, flags };
            Self::dispatch(op, &mut self.ring, &mut self.buffer_pool, self.atomic_buffer_pool.as_mut(), self.async_fd.clone(), self.vdo_opt, self.direct_io_ok, &self.source_caps, &self.target_caps, self.vdo_stall_threshold, &self.barrier_callback, self.source_uncached, self.target_uncached, self.governor.clone(), &mut self.fsync_tracker, None, false).await
        }
    }

    fn optimized_copy_range(&mut self, src: PathBuf, dst: PathBuf, offset: u64, length: u64, src_file_size: u64, target_label: String, buffer_limit: Option<usize>, skip_fsync: bool) -> impl std::future::Future<Output = Result<CopyStats>> + Send {
        async move {
            let op = Operation::CopyRange { src, dst, offset, length, src_file_size, target_label };
            Self::dispatch(op, &mut self.ring, &mut self.buffer_pool, self.atomic_buffer_pool.as_mut(), self.async_fd.clone(), self.vdo_opt, self.direct_io_ok, &self.source_caps, &self.target_caps, self.vdo_stall_threshold, &self.barrier_callback, self.source_uncached, self.target_uncached, self.governor.clone(), &mut self.fsync_tracker, buffer_limit, skip_fsync).await
        }
    }

    fn optimized_truncate(&mut self, dst: PathBuf, size: u64) -> impl std::future::Future<Output = Result<CopyStats>> + Send {
        async move {
            let op = Operation::Truncate { dst, size };
            Self::dispatch(op, &mut self.ring, &mut self.buffer_pool, self.atomic_buffer_pool.as_mut(), self.async_fd.clone(), self.vdo_opt, self.direct_io_ok, &self.source_caps, &self.target_caps, self.vdo_stall_threshold, &self.barrier_callback, self.source_uncached, self.target_uncached, self.governor.clone(), &mut self.fsync_tracker, None, false).await
        }
    }

    fn optimized_fallocate(&mut self, dst: PathBuf, mode: i32, offset: u64, length: u64) -> impl std::future::Future<Output = Result<CopyStats>> + Send {
        async move {
            let op = Operation::Fallocate { dst, mode, offset, length };
            Self::dispatch(op, &mut self.ring, &mut self.buffer_pool, self.atomic_buffer_pool.as_mut(), self.async_fd.clone(), self.vdo_opt, self.direct_io_ok, &self.source_caps, &self.target_caps, self.vdo_stall_threshold, &self.barrier_callback, self.source_uncached, self.target_uncached, self.governor.clone(), &mut self.fsync_tracker, None, false).await
        }
    }
}

impl SmartCopier {
    async fn open_source_noatime(path: &Path) -> Result<File> {
        let mut opts = tokio::fs::OpenOptions::new();
        opts.read(true);
        opts.custom_flags(libc::O_NOATIME);
        match opts.open(path).await {
            Ok(f) => Ok(f),
            Err(e) => {
                if let Some(raw) = e.raw_os_error() {
                    if raw == libc::EPERM || raw == libc::EACCES {
                        return File::open(path).await.map_err(FxcpError::Io);
                    }
                }
                Err(FxcpError::Io(e))
            }
        }
    }

    fn set_f2fs_pinning(fd: RawFd, enable: bool) {
        let val: u32 = if enable { 1 } else { 0 };
        let _ = unsafe { libc::ioctl(fd, F2FS_IOC_SET_PIN_FILE, &val) };
    }

    pub async fn copy(
        src: &Path,
        dst: &Path,
        ring: &mut IoUring,
        buffer_pool: &mut BufferPool,
        atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        vdo_opt: bool,
        offset: u64,
        length: u64,
        direct_io_ok: bool,
        src_file_size: u64,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        vdo_stall_threshold: u32,
        source_uncached: bool,
        target_uncached: bool,
        barrier_callback: &Option<Box<dyn Fn(u64) + Send + Sync>>,
        governor: Option<Arc<Governor>>,
        target_label: String,
        fsync_tracker: &mut FsyncLatencyTracker,
        skip_fsync: bool,
    ) -> Result<CopyStats> {
        Self::copy_with_limit(src, dst, ring, buffer_pool, atomic_pool, async_fd, vdo_opt, offset, length, direct_io_ok, src_file_size, source_caps, target_caps, vdo_stall_threshold, source_uncached, target_uncached, barrier_callback, governor, target_label, fsync_tracker, None, skip_fsync).await
    }

    pub async fn copy_with_limit(
        src: &Path,
        dst: &Path,
        ring: &mut IoUring,
        buffer_pool: &mut BufferPool,
        atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        vdo_opt: bool,
        offset: u64,
        length: u64,
        direct_io_ok: bool,
        src_file_size: u64,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        vdo_stall_threshold: u32,
        source_uncached: bool,
        target_uncached: bool,
        barrier_callback: &Option<Box<dyn Fn(u64) + Send + Sync>>,
        governor: Option<Arc<Governor>>,
        target_label: String,
        fsync_tracker: &mut FsyncLatencyTracker,
        buffer_limit: Option<usize>,
        skip_fsync: bool,
    ) -> Result<CopyStats> {
        if offset == 0 && length == src_file_size {
             Self::execute_copy_file(
                src, dst, ring, buffer_pool, atomic_pool, async_fd, vdo_opt, direct_io_ok, 
                source_caps, target_caps, src_file_size, vdo_stall_threshold, 
                barrier_callback, source_uncached, target_uncached, governor, 
                target_label, fsync_tracker, buffer_limit, skip_fsync
             ).await
        } else {
             Self::execute_copy_range(
                src, dst, ring, buffer_pool, atomic_pool, async_fd, vdo_opt, offset, length, 
                direct_io_ok, src_file_size, source_caps, target_caps, 
                vdo_stall_threshold, source_uncached, target_uncached, governor, 
                target_label, fsync_tracker, buffer_limit, skip_fsync
             ).await
        }
    }

    pub async fn dispatch(
        op: Operation,
        ring: &mut IoUring,
        buffer_pool: &mut BufferPool,
        atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        vdo_opt: bool,
        direct_io_ok: bool,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        vdo_stall_threshold: u32,
        barrier_callback: &Option<Box<dyn Fn(u64) + Send + Sync>>,
        source_uncached: bool,
        target_uncached: bool,
        governor: Option<Arc<Governor>>,
        fsync_tracker: &mut FsyncLatencyTracker,
        buffer_limit: Option<usize>,
        skip_fsync: bool,
    ) -> Result<CopyStats> {
        match op {
            Operation::CopyFile { src, dst, src_file_size, target_label } => {
                Self::execute_copy_file(
                    &src, &dst, ring, buffer_pool, atomic_pool, async_fd, vdo_opt, 
                    direct_io_ok, source_caps, target_caps, 
                    src_file_size, vdo_stall_threshold, barrier_callback, 
                    source_uncached, target_uncached, governor, 
                    target_label, fsync_tracker, buffer_limit, skip_fsync
                ).await
            },
            Operation::CopyRange { src, dst, offset, length, src_file_size, target_label } => {
                Self::execute_copy_range(
                    &src, &dst, ring, buffer_pool, atomic_pool, async_fd, vdo_opt, 
                    offset, length, direct_io_ok, src_file_size, 
                    source_caps, target_caps, vdo_stall_threshold, 
                    source_uncached, target_uncached, governor, 
                    target_label, fsync_tracker, buffer_limit, skip_fsync
                ).await
            },
            Operation::Truncate { dst, size } => {
                spawn_blocking(move || {
                    security::truncate_file(&dst, size).map(|_| CopyStats::default())
                }).await.map_err(FxcpError::Join)?
            },
            Operation::Rename { src, dst, flags } => {
                spawn_blocking(move || {
                    crate::consistency::atomic_rename(&src, &dst, Some(flags)).map(|_| CopyStats::default()).map_err(FxcpError::Io)
                }).await.map_err(FxcpError::Join)?.map_err(FxcpError::from)
            },
            Operation::Fallocate { dst, mode, offset, length } => {
                spawn_blocking(move || {
                    security::do_fallocate(&dst, offset, length, mode).map(|_| CopyStats::default())
                }).await.map_err(FxcpError::Join)?
            }
        }
    }

    fn prepare_destination_file(
        path: PathBuf,
        direct_io: bool,
        f2fs_atomic: bool,
        size: u64,
    ) -> Result<(std::fs::File, RawFd)> {
        debug!("prepare_destination_file: {:?}", path);
        let mut open_opts = std::fs::OpenOptions::new();
        open_opts.read(true).write(true).create(true).truncate(true);
        use std::os::unix::fs::OpenOptionsExt;
        open_opts.custom_flags(libc::O_NOFOLLOW);
        if direct_io {
            open_opts.custom_flags(libc::O_DIRECT | libc::O_NOFOLLOW);
        }
        
        let file = match open_opts.open(&path) {
            Ok(f) => f,
            Err(e) => {
                error!("prepare_destination_file: Failed to open {:?}: {}", path, e);
                return Err(FxcpError::Io(e));
            }
        };
        let fd = file.as_raw_fd();

        if f2fs_atomic {
            Self::set_f2fs_pinning(fd, true);
            if unsafe { libc::ioctl(fd, F2FS_IOC_START_ATOMIC_WRITE) } < 0 {
                warn!("F2FS: Failed to start atomic write transaction. Proceeding non-atomically.");
            }
        }

        if size > 0 {
            debug!("prepare_destination_file: Fallocating {} bytes for {:?}", size, path);
            let ret = unsafe { libc::fallocate(fd, 0, 0, size as i64) };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EOPNOTSUPP) {
                    warn!("Prepare: fallocate failed: {}. Performance may degrade.", err);
                }
            }
        }
        debug!("prepare_destination_file: Ready {:?}", path);
        Ok((file, fd))
    }

    async fn execute_copy_file(
        src: &Path,
        dst: &Path,
        ring: &mut IoUring,
        buffer_pool: &mut BufferPool,
        atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        vdo_opt: bool,
        direct_io_ok: bool,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        src_file_size: u64,
        vdo_stall_threshold: u32,
        barrier_callback: &Option<Box<dyn Fn(u64) + Send + Sync>>,
        source_uncached: bool,
        target_uncached: bool,
        governor: Option<Arc<Governor>>,
        target_label: String,
        fsync_tracker: &mut FsyncLatencyTracker,
        buffer_limit: Option<usize>,
        skip_fsync: bool,
    ) -> Result<CopyStats> {
        let target_path = dst.with_extension(format!("tmp.{}", Uuid::new_v4()));
        debug!("execute_copy_file: {:?} -> {:?}", src, target_path);

        let res = Self::execute_full_copy_logic(
            src, &target_path, ring, buffer_pool, atomic_pool, async_fd, vdo_opt, direct_io_ok, 
            source_caps, target_caps, false, src_file_size, vdo_stall_threshold, 
            barrier_callback, source_uncached, target_uncached, governor, 
            target_label, fsync_tracker, buffer_limit, skip_fsync
        ).await?;

        match res {
            Ok(stats) => {
                let tp1 = target_path.clone();
                
                // If skipping fsync, we rely on rename to be atomic enough for crash consistency
                // relative to the application, but data might still be in page cache.
                if !skip_fsync {
                    spawn_blocking(move || {
                        let f = std::fs::File::open(&tp1)?;
                        f.sync_all()
                    }).await.map_err(FxcpError::Join)??;
                }

                let src_owned = src.to_path_buf();
                let tp_meta = target_path.clone();
                
                spawn_blocking(move || {
                    if let Err(e) = security::apply_metadata(&src_owned, &tp_meta) {
                        debug!("SmartCopier: Metadata apply failed for temp file {:?}: {:?}", tp_meta, e);
                    }
                    security::sync_xattrs(&src_owned, &tp_meta);
                }).await.map_err(FxcpError::Join)?;

                let tp2 = target_path.clone();
                let dst_owned = dst.to_path_buf();
                let target_caps_clone = target_caps.clone();

                spawn_blocking(move || {
                    let can_exchange = target_caps_clone.exchange_range.load(Ordering::Relaxed);
                    let dst_exists = dst_owned.exists();
                    
                    if can_exchange && dst_exists {
                        let is_btrfs = if let Ok(s) = statfs::statfs(&tp2) {
                            s.filesystem_type().0 as i64 == BTRFS_SUPER_MAGIC
                        } else {
                            false
                        };

                        if is_btrfs {
                            crate::consistency::atomic_rename(&tp2, &dst_owned, Some(libc::RENAME_EXCHANGE as u32))
                                .or_else(|_| crate::consistency::atomic_rename(&tp2, &dst_owned, None))
                                .map_err(FxcpError::Io)
                        } else {
                            match crate::consistency::exchange::atomic_exchange(&tp2, &dst_owned) {
                                Ok(_) => Ok(()),
                                Err(e) => {
                                    let raw_err = e.raw_os_error();
                                    if raw_err == Some(libc::EOPNOTSUPP) || raw_err == Some(libc::ENOTTY) {
                                        debug!("Atomic Exchange unsupported on target ({}). Disabling optimization.", e);
                                        target_caps_clone.exchange_range.store(false, Ordering::Relaxed);
                                    } else {
                                        warn!("Atomic Exchange failed: {}. Falling back to rename.", e);
                                    }
                                    crate::consistency::atomic_rename(&tp2, &dst_owned, None).map_err(FxcpError::Io)
                                }
                            }
                        }
                    } else {
                        crate::consistency::atomic_rename(&tp2, &dst_owned, None).map_err(FxcpError::Io)
                    }
                }).await.map_err(FxcpError::Join)??;

                if let Some(cb) = barrier_callback {
                    if let Ok(meta) = std::fs::metadata(dst) { cb(meta.ino()); }
                }
                debug!("execute_copy_file: Success {:?}", dst);
                Ok(stats)
            },
            Err(e) => {
                error!("execute_copy_file: Failed {:?} - {}", target_path, e);
                let _ = std::fs::remove_file(&target_path);
                Err(e)
            }
        }
    }

    async fn execute_copy_range(
        src: &Path,
        dst: &Path,
        ring: &mut IoUring,
        buffer_pool: &mut BufferPool,
        atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        vdo_opt: bool,
        offset: u64,
        length: u64,
        direct_io_ok: bool,
        _src_file_size: u64,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        vdo_stall_threshold: u32,
        source_uncached: bool,
        target_uncached: bool,
        governor: Option<Arc<Governor>>,
        target_label: String,
        fsync_tracker: &mut FsyncLatencyTracker,
        buffer_limit: Option<usize>,
        skip_fsync: bool,
    ) -> Result<CopyStats> {
        let src_meta = std::fs::metadata(src).map_err(FxcpError::Io)?;
        let strategy = determine_copy_strategy(src, dst, &src_meta, target_caps);
        
        let sf = Self::open_source_noatime(src).await?;
        let sfd = sf.as_raw_fd();
        
        let mut open_opts = tokio::fs::OpenOptions::new();
        open_opts.read(true).write(true).create(false);
        
        let mut use_direct_io = direct_io_ok;
        if use_direct_io && (length % 4096 != 0 || offset % 4096 != 0) {
            use_direct_io = false;
        }
        if use_direct_io { open_opts.custom_flags(libc::O_DIRECT); }

        let df = open_opts.open(dst).await.map_err(FxcpError::Io)?;
        let dfd = df.as_raw_fd();

        let mut reflink_done = false;
        let mut stats = CopyStats::default();

        if strategy == CopyStrategy::Reflink {
             let reflink_res = Self::try_reflink_range(sfd, dfd, offset, length, offset, target_label.clone()).await;
             if let Ok(s) = reflink_res { stats = s; reflink_done = true; }
        }

        let res = if reflink_done { Ok(stats) } else {
            Self::perform_delta_uring_pipelined(
                ring, sfd, dfd, offset, length, vdo_opt, buffer_pool, atomic_pool, async_fd, 
                dst.to_path_buf(), source_caps, target_caps, false, vdo_stall_threshold, 
                source_uncached, target_uncached, governor, target_label, fsync_tracker, buffer_limit, skip_fsync
            ).await
        };
        
        drop(sf); drop(df);
        res
    }

    async fn execute_full_copy_logic(
        src: &Path,
        target_path: &Path,
        ring: &mut IoUring,
        buffer_pool: &mut BufferPool,
        atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        vdo_opt: bool,
        direct_io_ok: bool,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        use_atomic: bool,
        src_file_size: u64,
        vdo_stall_threshold: u32,
        _barrier_callback: &Option<Box<dyn Fn(u64) + Send + Sync>>,
        source_uncached: bool,
        target_uncached: bool,
        governor: Option<Arc<Governor>>,
        target_label: String,
        fsync_tracker: &mut FsyncLatencyTracker,
        buffer_limit: Option<usize>,
        skip_fsync: bool,
    ) -> Result<std::result::Result<CopyStats, FxcpError>> {
        debug!("execute_full_copy_logic: Start {:?} -> {:?}", src, target_path);
        
        let sf = Self::open_source_noatime(src).await?;
        let sfd = sf.as_raw_fd();
        
        let mut use_direct_io = direct_io_ok;
        if use_direct_io && src_file_size % 4096 != 0 {
            use_direct_io = false;
        }

        let use_f2fs = target_caps.f2fs_atomic_legacy.load(Ordering::Relaxed);
        let path_clone = target_path.to_path_buf();
        
        debug!("execute_full_copy_logic: Preparing destination...");
        let (_file_handle, dfd) = spawn_blocking(move || {
            Self::prepare_destination_file(path_clone, use_direct_io, use_f2fs, src_file_size)
        }).await.map_err(FxcpError::Join)??;

        let mut transfer_done = false;
        let mut stats = CopyStats::default();
        let src_meta = std::fs::metadata(src).map_err(FxcpError::Io)?;
        let strategy = determine_copy_strategy(src, target_path, &src_meta, target_caps);

        if !use_atomic && !use_f2fs && strategy == CopyStrategy::Reflink {
            debug!("execute_full_copy_logic: Attempting reflink...");
            let src_path_owned = src.to_path_buf();
            let reflink_res = Self::try_reflink(sfd, dfd, src_file_size, src_path_owned, target_label.clone()).await;
            if reflink_res.is_ok() {
                stats = reflink_res.unwrap();
                transfer_done = true;
                debug!("execute_full_copy_logic: Reflink success");
            } 
            else {
                debug!("execute_full_copy_logic: Reflink failed/partial, falling back. Error: {:?}", reflink_res.err());
                unsafe { libc::lseek(dfd, 0, libc::SEEK_SET); libc::lseek(sfd, 0, libc::SEEK_SET); };
            }
        }

        let mut result_val = Ok(stats);

        if !transfer_done {
            debug!("execute_full_copy_logic: Starting io_uring pipeline...");
            let res = Self::perform_delta_uring_pipelined(
                ring, sfd, dfd, 0, src_file_size, vdo_opt, buffer_pool, atomic_pool, async_fd, 
                target_path.to_path_buf(), source_caps, target_caps, use_atomic, 
                vdo_stall_threshold, source_uncached, target_uncached, governor, 
                target_label, fsync_tracker, buffer_limit, skip_fsync
            ).await;
            result_val = res;
        }

        if use_f2fs {
            let should_commit = result_val.is_ok();
            let commit_res = spawn_blocking(move || {
                let res = if should_commit {
                    if unsafe { libc::ioctl(dfd, F2FS_IOC_COMMIT_ATOMIC_WRITE) } < 0 {
                        error!("F2FS: Atomic Commit Failed!");
                        Err(FxcpError::Io(std::io::Error::last_os_error()))
                    } else {
                        Ok(())
                    }
                } else {
                    let _ = unsafe { libc::ioctl(dfd, F2FS_IOC_ABORT_ATOMIC_WRITE) };
                    Ok(())
                };
                Self::set_f2fs_pinning(dfd, false);
                res
            }).await.map_err(FxcpError::Join)?;
            
            if let Err(e) = commit_res {
                if result_val.is_ok() { result_val = Err(e); }
            }
        }

        drop(sf);
        debug!("execute_full_copy_logic: Finished");
        Ok(result_val)
    }

    async fn try_reflink(sfd: i32, dfd: i32, src_file_size: u64, src_path: PathBuf, target_label: String) -> Result<CopyStats> {
        spawn_blocking(move || {
            let start = Instant::now();
            let ret = unsafe { libc::ioctl(dfd, FICLONE, sfd) };
            if ret == 0 {
                 metrics::COPY_METHOD_REFLINK.with_label_values(&[&target_label]).inc();
                 return Ok(CopyStats { bytes_processed: src_file_size, bytes_zeros: 0, io_duration: start.elapsed(), ops_count: 1 });
            }
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EOPNOTSUPP) && err.raw_os_error() != Some(libc::EXDEV) {
                 warn!("Reflink failed on supported target ({}). Fallback copy active. Path: {:?}", err, src_path);
            } else {
                 debug!("Reflink not supported or cross-device ({}), falling back.", err);
            }
            
            // Fallback: Copy File Range
            let mut total_reflinked = 0usize;
            let size_usize: usize = src_file_size.try_into().unwrap_or(usize::MAX);
            let mut ops = 0;
            
            if src_file_size > 0 {
                let mut off_in = 0i64;
                let mut off_out = 0i64;
                while total_reflinked < size_usize {
                    let remaining = size_usize - total_reflinked;
                    let chunk = std::cmp::min(remaining, 1024 * 1024 * 1024);
                    let ret = unsafe { libc::copy_file_range(sfd, &mut off_in, dfd, &mut off_out, chunk, 0) };
                    if ret < 0 {
                        return Err(FxcpError::Io(std::io::Error::last_os_error()));
                    } else if ret == 0 {
                        break;
                    }
                    total_reflinked += ret as usize;
                    ops += 1;
                }
            }
            
            if total_reflinked == size_usize {
                 let is_network_fs = match statfs::statfs(&src_path) {
                    Ok(s) => { let magic = s.filesystem_type().0 as i64; magic == NFS_SUPER_MAGIC || magic == 0x517B || magic == 0xFF534D42 },
                    Err(_) => false,
                };
                if is_network_fs { metrics::COPY_METHOD_OFFLOAD.with_label_values(&[&target_label]).inc(); } else { metrics::COPY_METHOD_REFLINK.with_label_values(&[&target_label]).inc(); }
                Ok(CopyStats { bytes_processed: src_file_size, bytes_zeros: 0, io_duration: start.elapsed(), ops_count: ops })
            } else {
                Err(FxcpError::Io(io::Error::new(io::ErrorKind::Other, "Reflink partial copy or EOF")))
            }
        }).await.map_err(FxcpError::Join)?
    }

    async fn try_reflink_range(sfd: i32, dfd: i32, src_offset: u64, len: u64, dst_offset: u64, target_label: String) -> Result<CopyStats> {
        spawn_blocking(move || {
            let start = Instant::now();
            let args = FileCloneRange { src_fd: sfd as i64, src_offset, src_length: len, dest_offset: dst_offset };
            let ret = unsafe { libc::ioctl(dfd, FICLONERANGE, &args) };
            if ret == 0 {
                 metrics::COPY_METHOD_REFLINK.with_label_values(&[&target_label]).inc();
                 Ok(CopyStats { bytes_processed: len, bytes_zeros: 0, io_duration: start.elapsed(), ops_count: 1 })
            } else {
                 let err = std::io::Error::last_os_error();
                 if err.raw_os_error() != Some(libc::EOPNOTSUPP) && err.raw_os_error() != Some(libc::EXDEV) {
                     warn!("Reflink range failed on supported target ({}). Fallback copy active.", err);
                 }
                 Err(FxcpError::Io(err))
            }
        }).await.map_err(FxcpError::Join)?
    }

    fn check_atomic_invariants(caps: &Arc<Capabilities>, offset: u64, length: u32, buffer_addr: usize) -> Result<()> {
        let min = caps.atomic_min_bytes.load(Ordering::Relaxed) as u64;
        let max = caps.atomic_max_bytes.load(Ordering::Relaxed) as u64;
        
        if min == 0 || max == 0 {
            return Err(FxcpError::Io(io::Error::new(io::ErrorKind::Unsupported, "Atomic writes not supported by hardware")));
        }
        
        let len_u64 = length as u64;
        if !len_u64.is_power_of_two() {
            return Err(FxcpError::Io(io::Error::new(io::ErrorKind::InvalidInput, format!("Atomic write length {} is not power of 2", len_u64))));
        }
        
        if len_u64 < min || len_u64 > max {
            return Err(FxcpError::Io(io::Error::new(io::ErrorKind::InvalidInput, format!("Atomic write length {} out of bounds ({}-{})", len_u64, min, max))));
        }
        
        if offset % len_u64 != 0 {
            return Err(FxcpError::Io(io::Error::new(io::ErrorKind::InvalidInput, format!("Atomic write offset {} not aligned to length {}", offset, len_u64))));
        }
        
        if (buffer_addr as u64) % len_u64 != 0 {
            return Err(FxcpError::Io(io::Error::new(io::ErrorKind::InvalidInput, "Buffer memory not aligned for atomic write")));
        }
        
        Ok(())
    }

    fn map_sparse_segments(
        sfd: RawFd,
        offset: u64,
        length: u64
    ) -> Result<Vec<FileSegment>> {
        trace!("map_sparse_segments: sfd={}, offset={}, len={}", sfd, offset, length);
        let mut segments = Vec::new();
        let mut current_offset = offset;
        let end_offset = offset + length;

        while current_offset < end_offset {
            let data_offset_res = unsafe { libc::lseek(sfd, current_offset as i64, libc::SEEK_DATA) };
            if data_offset_res < 0 {
                let err = std::io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::ENXIO) => {
                        // EOF or no more data
                        let hole_len = end_offset - current_offset;
                        if hole_len > 0 {
                            segments.push(FileSegment::Hole { offset: current_offset, len: hole_len });
                        }
                        break;
                    },
                    _ => return Err(FxcpError::Io(err)),
                }
            }
            let data_offset = data_offset_res as u64;
            
            if data_offset >= end_offset {
                // Hole until end
                let hole_len = end_offset - current_offset;
                if hole_len > 0 {
                    segments.push(FileSegment::Hole { offset: current_offset, len: hole_len });
                }
                break;
            }

            if data_offset > current_offset {
                // Hole before data
                let hole_len = data_offset - current_offset;
                segments.push(FileSegment::Hole { offset: current_offset, len: hole_len });
                current_offset = data_offset;
            }

            let hole_offset_res = unsafe { libc::lseek(sfd, current_offset as i64, libc::SEEK_HOLE) };
            let hole_offset = if hole_offset_res < 0 {
                end_offset
            } else {
                hole_offset_res as u64
            };

            let segment_end = hole_offset.min(end_offset);
            let segment_len = segment_end - current_offset;
            
            if segment_len > 0 {
                segments.push(FileSegment::Data { offset: current_offset, len: segment_len });
                current_offset += segment_len;
            } else {
                warn!("map_sparse_segments: Infinite loop guard triggered. off={} len={}", current_offset, segment_len);
                break;
            }
        }
        trace!("map_sparse_segments: found {} segments", segments.len());
        Ok(segments)
    }

    async fn perform_delta_uring_pipelined(
        ring: &mut IoUring,
        sfd: i32,
        dfd: i32,
        offset: u64,
        length: u64,
        vdo_opt: bool,
        buffer_pool: &mut BufferPool,
        mut atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        _debug_path: PathBuf,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        use_atomic: bool,
        _vdo_stall_threshold: u32,
        source_uncached: bool,
        target_uncached: bool,
        governor: Option<Arc<Governor>>,
        _target_label: String,
        fsync_tracker: &mut FsyncLatencyTracker,
        buffer_limit: Option<usize>,
        skip_fsync: bool, // NEW: Optimization flag
    ) -> Result<CopyStats> {
        debug!("perform_delta_uring_pipelined: Start offset={} len={}", offset, length);
        let mut stats = CopyStats::default();
        let start_time = Instant::now();
        
        let use_seek_hole = source_caps.seek_hole.load(Ordering::Relaxed);

        if use_seek_hole {
            debug!("perform_delta_uring_pipelined: Mapping sparse segments...");
            let segments = spawn_blocking(move || {
                Self::map_sparse_segments(sfd, offset, length)
            }).await.map_err(FxcpError::Join)??;
            debug!("perform_delta_uring_pipelined: Mapped {} segments", segments.len());

            for segment in segments {
                match segment {
                    FileSegment::Hole { offset, len } => {
                        stats.bytes_zeros += len;
                        if len >= 16 * 1024 {
                            let falloc_op = opcode::Fallocate::new(types::Fd(dfd), len)
                                .offset(offset)
                                .mode(libc::FALLOC_FL_PUNCH_HOLE as i32 | libc::FALLOC_FL_KEEP_SIZE as i32)
                                .build()
                                .user_data(HOLE_OP);
                            
                            let mut pushed = false;
                            for _ in 0..10 {
                                if unsafe { ring.submission().push(&falloc_op) }.is_ok() {
                                    pushed = true;
                                    break;
                                }
                                let _ = ring.submit();
                                if ring.submission().is_full() {
                                    match tokio::time::timeout(Duration::from_millis(100), async_fd.readable()).await {
                                        Ok(Ok(mut guard)) => {
                                            let mut buf = [0u8; 8];
                                            let _ = unsafe { libc::read(async_fd.get_ref().as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, 8) };
                                            guard.clear_ready();
                                        },
                                        Ok(Err(e)) => return Err(FxcpError::Io(e)),
                                        Err(_) => {
                                            if let Some(_) = ring.completion().next() {
                                                // drain
                                            }
                                        }
                                    }
                                }
                            }
                            if !pushed {
                                 return Err(FxcpError::Io(io::Error::new(io::ErrorKind::Other, "Submission queue full (fallocate)")));
                            }
                        }
                    },
                    FileSegment::Data { offset, len } => {
                        let atomic_pool_ref = atomic_pool.as_mut().map(|p| &mut **p);
                        let seg_stats = Self::process_data_segment(
                            ring, sfd, dfd, offset, len, vdo_opt, buffer_pool, atomic_pool_ref, async_fd.clone(), 
                            source_caps, target_caps, use_atomic, 
                            source_uncached, target_uncached, &governor,
                            &_target_label, buffer_limit
                        ).await?;
                        stats.bytes_processed += seg_stats.bytes_processed;
                        stats.bytes_zeros += seg_stats.bytes_zeros;
                        stats.ops_count += seg_stats.ops_count;
                    }
                }
            }
        } else {
            let segment_stats = Self::process_data_segment(
                ring, sfd, dfd, offset, length, vdo_opt, buffer_pool, atomic_pool, async_fd.clone(), 
                source_caps, target_caps, use_atomic, 
                source_uncached, target_uncached, &governor,
                &_target_label, buffer_limit
            ).await?;
            stats.bytes_processed += segment_stats.bytes_processed;
            stats.bytes_zeros += segment_stats.bytes_zeros;
            stats.ops_count += segment_stats.ops_count;
        }

        // FSYNC OPTIMIZATION
        if !skip_fsync {
            let fsync_op = opcode::Fsync::new(types::Fd(dfd)).build().user_data(0);
            let mut pushed = false;
            let target_path = _debug_path.clone();
            
            for _ in 0..10 {
                if unsafe { ring.submission().push(&fsync_op) }.is_ok() {
                    pushed = true;
                    break;
                }
                let _ = ring.submit();
                if ring.submission().is_full() {
                    match tokio::time::timeout(Duration::from_millis(100), async_fd.readable()).await {
                        Ok(Ok(mut guard)) => {
                            let mut buf = [0u8; 8];
                            let _ = unsafe { libc::read(async_fd.get_ref().as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, 8) };
                            guard.clear_ready();
                        },
                        Ok(Err(e)) => return Err(FxcpError::Io(e)),
                        Err(_) => {
                            if let Some(_) = ring.completion().next() {
                                // drain
                            }
                        }
                    }
                }
            }
            
            if !pushed {
                 let _ = ring.submit();
                 unsafe { ring.submission().push(&fsync_op) }.map_err(|e| FxcpError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;
            }

            ring.submit()?;
            
            let mut fsync_found = false;
            let mut last_log = Instant::now();
            let fsync_start = Instant::now();
            let timeout_duration = fsync_tracker.get_timeout();
            let fsync_deadline = Instant::now() + timeout_duration;

            while !fsync_found {
                 if let Some(c) = ring.completion().next() {
                     if c.user_data() == 0 {
                         if c.result() < 0 {
                             return Err(FxcpError::Io(io::Error::from_raw_os_error(-c.result())));
                         }
                         fsync_found = true;
                     }
                     continue;
                 }
                 
                 if Instant::now() > fsync_deadline {
                     warn!("io_uring fsync timed out on {:?} (> {:?}). Ramp-up triggered. Falling back to blocking fsync.", target_path, timeout_duration);
                     fsync_tracker.record_timeout();
                     unsafe { libc::fsync(dfd) };
                     break;
                 }

                 match tokio::time::timeout(Duration::from_millis(100), async_fd.readable()).await {
                    Ok(Ok(mut guard)) => {
                        let mut buf = [0u8; 8];
                        let _ = unsafe { libc::read(async_fd.get_ref().as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, 8) };
                        guard.clear_ready();
                    },
                    Ok(Err(e)) => return Err(FxcpError::Io(e)),
                    Err(_) => {
                        let _ = ring.submit();
                        if let Some(_) = ring.completion().next() {
                            // drain
                        }
                    }
                 }
                 
                 if last_log.elapsed() > Duration::from_secs(10) {
                     info!("Still waiting for fsync completion on {:?} (Elapsed: {:?}, Timeout: {:?})...", target_path, fsync_start.elapsed(), timeout_duration);
                     last_log = Instant::now();
                 }
            }
            if fsync_found {
                fsync_tracker.record_success(fsync_start.elapsed());
            }
        } else {
            // Ensure all writes are submitted even if we skip fsync
            ring.submit()?;
        }

        metrics::COPY_METHOD_STANDARD.with_label_values(&[&_target_label]).inc();
        stats.io_duration = start_time.elapsed();
        debug!("perform_delta_uring_pipelined: Finished");
        Ok(stats)
    }

    async fn process_data_segment(
        ring: &mut IoUring,
        sfd: i32,
        dfd: i32,
        start_offset: u64,
        total_len: u64,
        vdo_opt: bool,
        buffer_pool: &mut BufferPool,
        atomic_pool: Option<&mut BufferPool>,
        async_fd: Arc<AsyncFd<RawFd>>,
        source_caps: &Arc<Capabilities>,
        target_caps: &Arc<Capabilities>,
        use_atomic: bool,
        source_uncached: bool,
        target_uncached: bool,
        governor: &Option<Arc<Governor>>,
        _target_label: &String,
        buffer_limit: Option<usize>,
    ) -> Result<CopyStats> {
        debug!("process_data_segment ENTRY: offset={}, len={}", start_offset, total_len);
        
        let mut bytes_processed = 0u64;
        let mut bytes_zeros = 0u64;
        let mut ops_count = 0u64;
        
        let (active_pool, is_atomic_buffer) = if use_atomic && atomic_pool.is_some() {
            (atomic_pool.unwrap(), true)
        } else if use_atomic {
            (buffer_pool, true)
        } else {
            (buffer_pool, false)
        };

        let chunk_size = active_pool.chunk_size() as u64;
        let capacity = active_pool.capacity();
        let buffers_count_u64 = buffer_limit.unwrap_or(capacity).min(capacity) as u64;
        
        let mut inflight_ops = 0;
        let mut offset = start_offset;
        let mut length = total_len;
        let mut atomic_intent = vec![false; capacity];
        let mut context_map = vec![0u64; capacity];
        let mut encountered_error: Option<FxcpError> = None;
        let atomic_max = target_caps.atomic_max_bytes.load(Ordering::Relaxed) as u64;
        let mut pending_submissions = 0;
        
        let mut batched_fallback_counter = metrics::BatchedCounter::new(
            metrics::ATOMIC_WRITE_FALLBACKS.clone(),
            64
        );

        let use_linked_sqe = !vdo_opt && !use_atomic;

        while length > 0 || inflight_ops > 0 {
            if let Some(gov) = &governor {
                if gov.current_memory_usage_pct() > 0.90 { tokio::task::yield_now().await; }
            }

            // Submit new operations
            while inflight_ops < buffers_count_u64 && length > 0 && encountered_error.is_none() {
                 if ring.submission().is_full() {
                     if pending_submissions > 0 {
                         if let Err(e) = ring.submit() {
                             encountered_error = Some(FxcpError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())));
                             break;
                         }
                         pending_submissions = 0;
                     }
                     if ring.submission().is_full() {
                         break;
                     }
                 }

                 let index = if let Some(idx) = active_pool.acquire() { idx } else {
                     break;
                 };
                 
                 if index as usize >= active_pool.capacity() {
                     error!("BufferPool Index Out of Bounds: {} (Capacity: {})", index, active_pool.capacity());
                     return Err(FxcpError::MemoryExhausted("BufferPool index corrupted".into()));
                 }

                 let mut current_len = (length as usize).min(chunk_size as usize) as u32;
                 
                 // Atomic write constraint
                 if use_atomic && is_atomic_buffer && atomic_max > 0 {
                     current_len = current_len.min(atomic_max as u32);
                 }
                 let len = current_len;

                 // Prepare buffer
                 if use_atomic && is_atomic_buffer {
                    let ptr_opt = active_pool.get_ptr(index);
                    if let Some(ptr) = ptr_opt {
                        let ptr_addr = ptr as usize;
                        if let Err(e) = Self::check_atomic_invariants(target_caps, offset, len, ptr_addr) {
                            active_pool.release(index);
                            return Err(e);
                        }
                    } else {
                        active_pool.release(index);
                        error!("BufferPool returned null pointer for index {}", index);
                        return Err(FxcpError::MemoryExhausted("BufferPool null pointer".into()));
                    }
                    atomic_intent[index as usize] = true;
                 } else {
                    atomic_intent[index as usize] = false;
                 }

                 let mut read_flags: i32 = 0;
                 if source_uncached && source_caps.uncached_io.load(Ordering::Relaxed) { read_flags |= RWF_UNCACHED; }
                 
                 context_map[index as usize] = offset;
                 let buf_ptr = match active_pool.get_ptr(index) {
                     Some(p) => p,
                     None => {
                         active_pool.release(index);
                         error!("BufferPool returned null pointer for index {}", index);
                         return Err(FxcpError::MemoryExhausted("BufferPool null pointer".into()));
                     }
                 };

                 if use_linked_sqe {
                     // READ OP
                     let read_user_data = READ_OP | (index as u64);
                     let mut read_op = opcode::ReadFixed::new(types::Fd(sfd), buf_ptr, len, index as u16)
                        .offset(offset)
                        .rw_flags(read_flags)
                        .build()
                        .user_data(read_user_data);
                     read_op = read_op.flags(squeue::Flags::IO_LINK);

                     // WRITE OP
                     let mut write_flags: i32 = 0;
                     if target_uncached && target_caps.uncached_io.load(Ordering::Relaxed) { write_flags |= RWF_UNCACHED; }
                     
                     let write_user_data = WRITE_OP | (index as u64);
                     let write_op = opcode::WriteFixed::new(types::Fd(dfd), buf_ptr, len, index as u16)
                        .offset(offset)
                        .rw_flags(write_flags)
                        .build()
                        .user_data(write_user_data);

                     unsafe {
                         if ring.submission().push(&read_op).is_err() {
                             active_pool.release(index);
                             break;
                         }
                         if ring.submission().push(&write_op).is_err() {
                             active_pool.release(index);
                             let _ = ring.submit(); // Force flush partially
                             break;
                         }
                     }
                     inflight_ops += 2;
                     pending_submissions += 2;
                 } else {
                     // Standard decoupled or VDO mode
                     let user_data = READ_OP | (index as u64);
                     let read_op = opcode::ReadFixed::new(types::Fd(sfd), buf_ptr, len, index as u16)
                        .offset(offset)
                        .rw_flags(read_flags)
                        .build()
                        .user_data(user_data);

                     if unsafe { ring.submission().push(&read_op) }.is_err() {
                         active_pool.release(index);
                         if pending_submissions > 0 {
                             let _ = ring.submit();
                             pending_submissions = 0;
                         }
                         break;
                     }
                     inflight_ops += 1;
                     pending_submissions += 1;
                 }

                 offset += len as u64;
                 length -= len as u64;
                 ops_count += 1;
            }

            if pending_submissions > 0 {
                if let Err(e) = ring.submit() {
                    if encountered_error.is_none() {
                        encountered_error = Some(FxcpError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())));
                    }
                }
                pending_submissions = 0;
            }

            // Process completions
            loop {
                let peek_cqe = ring.completion().next();
                
                let cqe = if let Some(c) = peek_cqe {
                    c
                } else {
                    // Nothing ready immediately
                    if length > 0 && active_pool.free_count() > 0 && !ring.submission().is_full() {
                        // Go back to filling submission queue
                        break;
                    }
                    
                    if inflight_ops > 0 {
                        // Must wait
                        match tokio::time::timeout(Duration::from_millis(100), async_fd.readable()).await {
                            Ok(Ok(mut guard)) => {
                                let mut buf = [0u8; 8];
                                let _ = unsafe { libc::read(async_fd.get_ref().as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, 8) };
                                guard.clear_ready();
                                if let Some(c) = ring.completion().next() { c } else { continue; }
                            },
                            Ok(Err(e)) => return Err(FxcpError::Io(e)),
                            Err(_) => {
                                // Timeout, force submit
                                ring.submit()?;
                                if let Some(c) = ring.completion().next() { c } else { continue; }
                            }
                        }
                    } else {
                        // Nothing inflight, nothing to submit
                        break;
                    }
                };

                let user_data = cqe.user_data();
                if user_data == 0 {
                    // Fsync completion or barrier
                    continue;
                }

                let op_type = user_data & OP_TYPE_MASK;
                if op_type == HOLE_OP {
                    continue;
                }

                let index = (user_data & INDEX_MASK) as u16;
                let original_offset = if (index as usize) < context_map.len() {
                    context_map[index as usize]
                } else {
                    0
                };

                if cqe.result() < 0 {
                    let raw_err = -cqe.result();
                    if raw_err == libc::ECANCELED && use_linked_sqe && op_type == WRITE_OP {
                        // Linked read failed, so write was canceled.
                        // We handle the read failure below.
                        inflight_ops -= 1;
                        active_pool.release(index);
                        continue;
                    }

                    // Atomic Fallback Logic
                    if op_type == WRITE_OP && atomic_intent[index as usize] {
                        if raw_err == libc::EOPNOTSUPP || raw_err == libc::EINVAL {
                            if raw_err == libc::EOPNOTSUPP {
                                warn!("Atomic Write Failed (EOPNOTSUPP). Disabling Atomic Writes for this session.");
                                target_caps.atomic_writes.store(false, Ordering::Relaxed);
                            } else {
                                warn!("Atomic Write Failed for offset {}: {} (Errno {}). Downgrading to Buffered I/O.", context_map[index as usize], io::Error::from_raw_os_error(raw_err), raw_err);
                            }
                            
                            batched_fallback_counter.inc();
                            atomic_intent[index as usize] = false;
                            
                            // Retry as normal write
                            let retry_len = active_pool.get_len(index) as u32;
                            let retry_offset = context_map[index as usize];
                            
                            let mut write_flags: i32 = 0;
                            if target_uncached && target_caps.uncached_io.load(Ordering::Relaxed) { write_flags |= RWF_UNCACHED; }
                            
                            let user_data_write = WRITE_OP | (index as u64);
                            let buf_ptr = active_pool.get_ptr(index).unwrap();
                            let write_op = opcode::WriteFixed::new(types::Fd(dfd), buf_ptr, retry_len, index as u16)
                                .offset(retry_offset)
                                .rw_flags(write_flags)
                                .build()
                                .user_data(user_data_write);
                            
                            if unsafe { ring.submission().push(&write_op) }.is_ok() {
                                continue; // Successfully requeued
                            } else {
                                encountered_error = Some(FxcpError::Io(io::Error::new(io::ErrorKind::Other, "SQ full during atomic retry")));
                            }
                        }
                    }

                    // General Error Handling
                    if op_type != FALLOC_OP {
                        if !use_linked_sqe || op_type == WRITE_OP {
                            active_pool.release(index);
                        }
                    }
                    
                    if encountered_error.is_none() {
                        let err = io::Error::from_raw_os_error(-cqe.result());
                        if err.kind() != io::ErrorKind::Interrupted {
                             encountered_error = Some(FxcpError::Io(err));
                        }
                    }
                    inflight_ops -= 1;
                    continue;
                }

                if encountered_error.is_some() {
                    // Drain
                    if op_type != FALLOC_OP {
                        if !use_linked_sqe || op_type == WRITE_OP {
                            active_pool.release(index);
                        }
                    }
                    inflight_ops -= 1;
                    continue;
                }

                let bytes_transferred = cqe.result() as u64;

                match op_type {
                    READ_OP => {
                        inflight_ops -= 1;
                        active_pool.set_len(index, bytes_transferred as usize);
                        
                        if use_linked_sqe {
                            // Write is already linked and submitted
                            continue;
                        }

                        let ptr = active_pool.get_ptr(index).unwrap();
                        
                        // VDO Zero-Block Optimization
                        let is_zero_block = if vdo_opt && bytes_transferred > 0 {
                            let s = unsafe { std::slice::from_raw_parts(ptr, bytes_transferred as usize) };
                            Self::is_block_zero(s)
                        } else { false };

                        if is_zero_block {
                             if bytes_transferred >= 16 * 1024 {
                                 // Hole Punch
                                 let user_data_falloc = FALLOC_OP | (index as u64);
                                 let falloc_op = opcode::Fallocate::new(types::Fd(dfd), bytes_transferred)
                                    .offset(original_offset)
                                    .mode(libc::FALLOC_FL_PUNCH_HOLE as i32 | libc::FALLOC_FL_KEEP_SIZE as i32)
                                    .build()
                                    .user_data(user_data_falloc);
                                 
                                 if unsafe { ring.submission().push(&falloc_op) }.is_ok() {
                                     pending_submissions += 1;
                                     bytes_zeros += bytes_transferred;
                                     bytes_processed += bytes_transferred;
                                     inflight_ops += 1;
                                 } else {
                                     encountered_error = Some(FxcpError::Io(io::Error::new(io::ErrorKind::Other, "Submission queue full during zero-block fallocate")));
                                     active_pool.release(index);
                                 }
                             } else {
                                // Small zero block -> Write as normal to avoid frag
                                let mut write_flags: i32 = 0;
                                if target_uncached && target_caps.uncached_io.load(Ordering::Relaxed) { write_flags |= RWF_UNCACHED; }
                                if target_caps.atomic_writes.load(Ordering::Relaxed) && atomic_intent[index as usize] {
                                    write_flags |= RWF_ATOMIC;
                                }
                                
                                let user_data_write = WRITE_OP | (index as u64);
                                let write_op = opcode::WriteFixed::new(types::Fd(dfd), ptr, bytes_transferred as u32, index as u16)
                                    .offset(original_offset)
                                    .rw_flags(write_flags)
                                    .build()
                                    .user_data(user_data_write);
                                
                                if unsafe { ring.submission().push(&write_op) }.is_ok() {
                                    pending_submissions += 1;
                                    inflight_ops += 1;
                                } else {
                                    encountered_error = Some(FxcpError::Io(io::Error::new(io::ErrorKind::Other, "Submission queue full during write")));
                                    active_pool.release(index);
                                }
                             }
                        } else {
                             // Standard Write
                             let mut write_flags: i32 = 0;
                             if target_uncached && target_caps.uncached_io.load(Ordering::Relaxed) { write_flags |= RWF_UNCACHED; }
                             if target_caps.atomic_writes.load(Ordering::Relaxed) && atomic_intent[index as usize] {
                                 write_flags |= RWF_ATOMIC;
                             }
                             
                             let user_data_write = WRITE_OP | (index as u64);
                             let write_op = opcode::WriteFixed::new(types::Fd(dfd), ptr, bytes_transferred as u32, index as u16)
                                .offset(original_offset)
                                .rw_flags(write_flags)
                                .build()
                                .user_data(user_data_write);
                             
                             if unsafe { ring.submission().push(&write_op) }.is_ok() {
                                 pending_submissions += 1;
                                 inflight_ops += 1;
                             } else {
                                 encountered_error = Some(FxcpError::Io(io::Error::new(io::ErrorKind::Other, "Submission queue full during write")));
                                 active_pool.release(index);
                             }
                        }
                    },
                    WRITE_OP => {
                        inflight_ops -= 1;
                        bytes_processed += bytes_transferred;
                        active_pool.release(index);
                    },
                    FALLOC_OP => {
                        inflight_ops -= 1;
                        active_pool.release(index);
                    }
                    _ => {
                        debug!("process_data_segment: Encountered unknown Op Type in CQE user_data: {:x}", user_data);
                    }
                }
            }
        }

        if let Some(err) = encountered_error {
            debug!("process_data_segment EXIT with ERROR: {:?}", err);
            return Err(err);
        }

        debug!("process_data_segment EXIT SUCCESS");
        Ok(CopyStats { bytes_processed, bytes_zeros, io_duration: Duration::ZERO, ops_count })
    }

    #[inline(always)]
    fn is_block_zero(buf: &[u8]) -> bool {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx512f") {
                return unsafe { Self::is_block_zero_avx512(buf) };
            }
            if is_x86_feature_detected!("avx2") {
                return unsafe { Self::is_block_zero_avx2(buf) };
            }
        }
        
        // Fallback / Generic
        let (prefix, chunks, suffix) = unsafe { buf.align_to::<u128>() };
        chunks.iter().all(|&x| x == 0) && prefix.iter().all(|&x| x == 0) && suffix.iter().all(|&x| x == 0)
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx512f")]
    unsafe fn is_block_zero_avx512(buf: &[u8]) -> bool {
        unsafe {
            let len = buf.len();
            let ptr = buf.as_ptr();
            let mut i = 0;

            while i + 256 <= len {
                let a = _mm512_loadu_si512(ptr.add(i) as *const _);
                let b = _mm512_loadu_si512(ptr.add(i + 64) as *const _);
                let c = _mm512_loadu_si512(ptr.add(i + 128) as *const _);
                let d = _mm512_loadu_si512(ptr.add(i + 192) as *const _);

                let combined = _mm512_or_si512(_mm512_or_si512(a, b), _mm512_or_si512(c, d));
                if _mm512_test_epi64_mask(combined, combined) != 0 {
                    return false;
                }
                i += 256;
            }

            while i + 64 <= len {
                let a = _mm512_loadu_si512(ptr.add(i) as *const _);
                if _mm512_test_epi64_mask(a, a) != 0 {
                    return false;
                }
                i += 64;
            }

            // Fallback for remaining bytes
            buf[i..].iter().all(|&b| b == 0)
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx2")]
    unsafe fn is_block_zero_avx2(buf: &[u8]) -> bool {
        unsafe {
            let len = buf.len();
            let ptr = buf.as_ptr();
            let mut i = 0;

            while i + 32 <= len {
                let a = _mm256_loadu_si256(ptr.add(i) as *const _);
                if _mm256_testz_si256(a, a) == 0 {
                    return false;
                }
                i += 32;
            }

            buf[i..].iter().all(|&b| b == 0)
        }
    }
}

pub fn apply_lock(path: &Path, lock_type: u32, lock_map: &DashMap<u64, std::fs::File>) -> Result<()> {
    if (lock_type & libc::LOCK_UN as u32) != 0 {
         if let Ok(meta) = std::fs::metadata(path) { lock_map.remove(&meta.ino()); }
         return Ok(());
    }
    
    let file = std::fs::File::open(path).map_err(FxcpError::Io)?;
    let fd = file.as_raw_fd();
    
    let ret = unsafe { libc::flock(fd, (lock_type as i32) | libc::LOCK_NB) };
    
    if ret == 0 { if let Ok(meta) = file.metadata() { lock_map.insert(meta.ino(), file); } }
    else { debug!("Failed to apply lock on {:?}: {}", path, std::io::Error::last_os_error()); }

    Ok(())
}

// ---------------------------------------------------------------------------
// Delta copy — transfer only dirty ranges identified by Merkle tree diff
// ---------------------------------------------------------------------------
impl SmartCopier {
    /// Copy only the specified dirty ranges from src to dst.
    /// Each DirtyRange triggers an `optimized_copy_range()` call.
    pub async fn copy_delta(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
        dirty_ranges: &[crate::hashing::DirtyRange],
        file_size: u64,
        label: &str,
    ) -> Result<CopyStats> {
        let mut total_stats = CopyStats::default();

        for range in dirty_ranges {
            let stats = self.optimized_copy_range(
                src.to_path_buf(),
                dst.to_path_buf(),
                range.offset,
                range.length,
                file_size,
                label.to_string(),
                None,
                self.skip_fsync,
            ).await?;
            total_stats.bytes_processed += stats.bytes_processed;
            total_stats.ops_count += stats.ops_count;
        }

        Ok(total_stats)
    }
}
