// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/security.rs — Metadata sync, permissions, ownership, xattr operations

// Security — metadata sync, capability checks, fallocate, xattr preservation
use std::path::Path;
use crate::error::{FxcpError, Result};
use std::os::unix::io::{BorrowedFd, AsRawFd};
use nix::fcntl::{fallocate, FallocateFlags};
use libc;
use nix::sys::statvfs::statvfs;
use std::fs::File;
use crate::operations;
use nix::unistd::{chown, Uid, Gid};
use xattr;
use tracing::{debug, warn};
use std::fs::OpenOptions;
use std::ffi::CString;
use crate::buffer::AlignedBuffer;
use crate::sidecar;
use std::io::{Read, Seek, SeekFrom};
use walkdir::WalkDir;
use std::os::unix::fs::MetadataExt;
use std::hash::Hasher;
use std::collections::hash_map::DefaultHasher;
use fxhash::FxHasher;
use std::sync::Arc;
use dashmap::DashMap;
use lazy_static::lazy_static;
use crate::operations::Capabilities;

const FS_COMPR_FL: u32 = 0x00000004;
const FICLONE: u64 = 0x40049409;

lazy_static! {
    static ref GLOBAL_CAPS_CACHE: DashMap<u64, Arc<Capabilities>> = DashMap::new();
}

pub fn ioctl_ficlone(src_fd: i32, dst_path: &Path) -> Result<()> {
    let dst_file = File::create(dst_path).map_err(FxcpError::Io)?;
    let dst_fd = dst_file.as_raw_fd();
    let ret = unsafe {
        libc::ioctl(dst_fd, FICLONE, src_fd)
    };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINVAL) {
            return Err(FxcpError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("FICLONE (CoW) not supported by filesystem: {}", dst_path.to_string_lossy())
            )));
        }
        return Err(FxcpError::System(nix::Error::last()));
    }
    Ok(())
}

pub fn check_capacity(path: &Path, threshold_mb: u64) -> bool {
    if let Ok(s) = statvfs(path) {
        let avail = s.blocks_available() * s.block_size();
        let avail_inodes = s.files_available();
        let threshold_bytes = threshold_mb * 1024 * 1024;
        
        if avail < threshold_bytes { return false; }
        if avail_inodes < 1000 { return false; }
    }
    true
}

pub fn is_filesystem_compressed(path: &Path) -> bool {
    if let Ok(s) = nix::sys::statfs::statfs(path) {
        let magic = s.filesystem_type().0 as i64;
        return magic == 0x9123683E || magic == 0xF2F52010;
    }
    false
}

pub fn preallocate(fd: i32, size: u64) {
    if size > 0 {
        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(fd) };
        let _ = fallocate(borrowed_fd.as_raw_fd(), FallocateFlags::FALLOC_FL_KEEP_SIZE, 0, size as i64);
    }
}

pub fn enable_compression(fd: i32) -> Result<()> {
    let flags: u32 = FS_COMPR_FL;
    let ret = unsafe { libc::ioctl(fd, 0x40086602, &flags) };
    if ret != 0 { return Err(FxcpError::System(nix::Error::last())); }
    Ok(())
}

pub fn enable_f2fs_pinning(fd: i32) -> Result<()> {
    let pin: u32 = 1;
    let ret = unsafe { libc::ioctl(fd, 0xF50D, &pin) };
    if ret != 0 { return Err(FxcpError::System(nix::Error::last())); }
    Ok(())
}

pub fn set_project_id(fd: i32, projid: u32) -> Result<()> {
    if projid == 0 { return Ok(()); }
    
    #[repr(C)]
    #[derive(Default)]
    struct FsxAttr {
        fsx_xflags: u32,
        fsx_extsize: u32,
        fsx_nextents: u32,
        fsx_projid: u32,
        fsx_cowextsize: u32,
        fsx_pad: [u8; 8]
    }

    let mut attr: FsxAttr = Default::default();
    attr.fsx_projid = projid;
    
    let ret = unsafe { libc::ioctl(fd, 0x40205820, &attr) };
    if ret != 0 { return Err(FxcpError::System(nix::Error::last())); }
    Ok(())
}

pub fn acquire_mandatory_lock(fd: i32) -> Result<()> {
    let lock = libc::flock { l_type: libc::F_WRLCK as i16, l_whence: libc::SEEK_SET as i16, l_start: 0, l_len: 0, l_pid: 0 };
    if unsafe { libc::fcntl(fd, libc::F_OFD_SETLK, &lock) } < 0 {
        return Err(FxcpError::System(nix::Error::last()));
    }
    Ok(())
}

pub fn acquire_read_lock(fd: i32) -> Result<()> {
    let lock = libc::flock { l_type: libc::F_RDLCK as i16, l_whence: libc::SEEK_SET as i16, l_start: 0, l_len: 0, l_pid: 0 };
    if unsafe { libc::fcntl(fd, libc::F_OFD_SETLK, &lock) } < 0 {
        return Err(FxcpError::System(nix::Error::last()));
    }
    Ok(())
}

pub fn set_ownership(path: &Path, uid: u32, gid: u32) -> Result<()> {
    match chown(
        path,
        Some(Uid::from_raw(uid)),
        Some(Gid::from_raw(gid)),
    ) {
        Ok(_) => Ok(()),
        Err(e) => Err(FxcpError::System(e))
    }
}

pub fn copy_timestamps(src: &Path, dst: &Path) -> Result<()> {
    let meta = std::fs::metadata(src).map_err(FxcpError::Io)?;
    let atime = libc::timespec { tv_sec: meta.atime(), tv_nsec: meta.atime_nsec() };
    let mtime = libc::timespec { tv_sec: meta.mtime(), tv_nsec: meta.mtime_nsec() };
    let times = [atime, mtime];
    
    let c_path = CString::new(dst.to_string_lossy().as_bytes()).map_err(|_| FxcpError::Config("Invalid path".into()))?;
    
    let ret = unsafe {
        libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0)
    };
    
    if ret != 0 {
        return Err(FxcpError::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

pub fn sync_xattrs(src: &Path, dst: &Path) {
    if let Ok(iter) = xattr::list(src) {
        for name in iter {
            if let Ok(Some(value)) = xattr::get(src, &name) {
                let _ = xattr::set(dst, &name, &value);
            }
        }
    }
}

/// Stores a strict signature of the source file state into the target's xattrs.
/// This allows us to skip re-copying even if the target filesystem mangles timestamps.
fn write_sync_marker(dst: &Path, len: u64, mtime: i64, mtime_nsec: i64) {
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&mtime.to_le_bytes());
    buf.extend_from_slice(&mtime_nsec.to_le_bytes());
    let _ = sidecar::set_metadata(dst, "sync_sig", &buf);
}

/// Verifies if the target has a signature matching the current source state.
pub fn check_sync_marker(dst: &Path, src_len: u64, src_mtime: i64, src_mtime_nsec: i64) -> bool {
    if let Some(val) = sidecar::get_metadata(dst, "sync_sig") {
        if val.len() == 24 {
            let stored_len = u64::from_le_bytes(val[0..8].try_into().unwrap());
            let stored_mtime = i64::from_le_bytes(val[8..16].try_into().unwrap());
            let stored_nsec = i64::from_le_bytes(val[16..24].try_into().unwrap());
            
            if stored_len == src_len && stored_mtime == src_mtime && stored_nsec == src_mtime_nsec {
                return true;
            }
        }
    }
    false
}

pub fn apply_metadata(src: &Path, dst: &Path) -> Result<()> {
    let meta = std::fs::metadata(src).map_err(FxcpError::Io)?;
    use std::os::unix::fs::MetadataExt;
    
    set_ownership(dst, meta.uid(), meta.gid())?;
    
    let permissions = meta.permissions();
    if let Err(e) = std::fs::set_permissions(dst, permissions) {
         return Err(FxcpError::Io(e));
    }
    
    copy_timestamps(src, dst)?;
    
    // Write the sync signature to authorize skipping next time
    write_sync_marker(dst, meta.len(), meta.mtime(), meta.mtime_nsec());
    
    Ok(())
}

pub fn is_dir_metadata_synced(src: &Path, dst: &Path) -> bool {
    if let (Ok(sm), Ok(dm)) = (std::fs::metadata(src), std::fs::metadata(dst)) {
        if sm.uid() != dm.uid() { return false; }
        if sm.gid() != dm.gid() { return false; }
        if sm.mode() != dm.mode() { return false; }
        if sm.mtime() != dm.mtime() { return false; }
        return true;
    }
    false
}

pub fn get_target_epoch(path: &Path) -> u64 {
    if let Some(val) = sidecar::get_metadata(path, "epoch") {
         if val.len() == 8 {
            return u64::from_le_bytes(val.clone().try_into().unwrap_or_default());
        }
    }
    0
}

pub fn get_valid_dir_hash(path: &Path) -> u64 {
    let hash_bytes = match sidecar::get_metadata(path, "dir_hash") {
        Some(b) if b.len() == 8 => b,
        _ => return 0,
    };
    let hash = u64::from_le_bytes(hash_bytes.try_into().unwrap());
    
    let guard_bytes = match sidecar::get_metadata(path, "dir_guard") {
        Some(b) if b.len() == 8 => b,
        _ => return 0,
    };
    let guard_mtime = i64::from_le_bytes(guard_bytes.try_into().unwrap());
    
    if let Ok(meta) = std::fs::metadata(path) {
        if meta.mtime() == guard_mtime {
            return hash;
        }
    }
    0
}

pub fn write_dir_integrity_hash(target_dir: &Path, hash: u64) {
    let hash_bytes = hash.to_le_bytes();
    let _ = sidecar::set_metadata(target_dir, "dir_hash_pending", &hash_bytes);
    if let Ok(dir_file) = File::open(target_dir) {
        let _ = dir_file.sync_all();
    }
    let _ = sidecar::set_metadata(target_dir, "dir_hash", &hash_bytes);
    if let Ok(meta) = std::fs::metadata(target_dir) {
        let mtime_bytes = meta.mtime().to_le_bytes();
        let _ = sidecar::set_metadata(target_dir, "dir_guard", &mtime_bytes);
    }
    if let Ok(dir_file) = File::open(target_dir) {
        let _ = dir_file.sync_all();
    }
    let _ = sidecar::remove_metadata(target_dir, "dir_hash_pending");
}

pub fn calc_dir_integrity_hash_target(dir_path: &Path) -> Result<u64> {
    if !dir_path.is_dir() { return Ok(0); }
    let mut hasher = DefaultHasher::new();
    if let Some(name) = dir_path.file_name() { hasher.write(name.to_string_lossy().as_bytes()); }
    if let Ok(meta) = dir_path.metadata() {
         hasher.write_u64(meta.ino());
         hasher.write_u32(meta.mode());
         hasher.write_u32(meta.uid());
         hasher.write_u32(meta.gid());
    }
    for entry in WalkDir::new(dir_path).max_depth(1).min_depth(1) {
        if let Ok(entry) = entry {
            if let Ok(meta) = entry.metadata() {
                if let Some(name) = entry.file_name().to_str() {
                    hasher.write(name.as_bytes());
                    hasher.write_u64(meta.len());
                    hasher.write_i64(meta.mtime());
                    hasher.write_u32(meta.mode());
                }
            }
        }
    }
    Ok(hasher.finish())
}

pub fn check_and_lock_dir_integrity(dir_path: &Path, lock_file: &File) -> Result<u64> {
    if let Err(e) = acquire_mandatory_lock(lock_file.as_raw_fd()) {
        return Err(e);
    }
    let _meta = lock_file.metadata().map_err(FxcpError::Io)?;
    let hash_bytes = match sidecar::get_metadata(dir_path, "dir_hash") {
        Some(b) if b.len() == 8 => b,
        _ => return Ok(0),
    };
    let hash = u64::from_le_bytes(hash_bytes.try_into().unwrap());
    let guard_bytes = match sidecar::get_metadata(dir_path, "dir_guard") {
        Some(b) if b.len() == 8 => b,
        _ => return Ok(0),
    };
    let guard_mtime = i64::from_le_bytes(guard_bytes.try_into().unwrap());
    if let Ok(meta) = std::fs::metadata(dir_path) {
        if meta.mtime() == guard_mtime {
            return Ok(hash);
        }
    }
    Ok(0)
}

pub fn probe_xattr_support(target_root: &Path) -> bool {
    let probe_file = target_root.join(".xfs_mirror_probe_xattr");
    if probe_file.exists() { let _ = std::fs::remove_file(&probe_file); }
    if let Ok(_f) = File::create(&probe_file) {
        let key = "probe";
        let val = b"1";
        let res = sidecar::set_metadata(&probe_file, key, val);
        let _ = std::fs::remove_file(&probe_file);
        res.is_ok()
    } else {
        false
    }
}

pub fn probe_direct_io(target_root: &Path) -> bool {
    let probe_file = target_root.join(".xfs_mirror_probe_dio");
    let buf = match AlignedBuffer::try_new(4096, 4096) {
        Ok(b) => b,
        Err(_) => return false
    };
    buf.set_len(4096);
    let flags = libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_DIRECT;
    let path_c = match CString::new(probe_file.to_string_lossy().as_bytes()) { Ok(c) => c, Err(_) => return false };
    let fd = unsafe { libc::open(path_c.as_ptr(), flags, 0o644) };
    if fd < 0 { return false; }
    let ret = unsafe { libc::write(fd, buf.ptr() as *const _, 4096) };
    unsafe { libc::close(fd); }
    let _ = std::fs::remove_file(&probe_file);
    ret == 4096
}

pub fn probe_rwf_uncached(target_root: &Path) -> bool {
    let probe_file = target_root.join(".xfs_mirror_probe_dontcache");
    let buf = match AlignedBuffer::try_new(4096, 4096) {
        Ok(b) => b,
        Err(_) => return false
    };
    buf.set_len(4096);
    let file_res = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&probe_file);
    let sfd = match file_res.as_ref() {
        Ok(f) => f.as_raw_fd(),
        Err(_e) => {
            let _ = std::fs::remove_file(probe_file);
            return false;
        }
    };
    let mut ring = match io_uring::IoUring::new(1) {
        Ok(r) => r,
        Err(_e) => { return false; }
    };
    let iov = [libc::iovec { iov_base: buf.ptr() as _, iov_len: 4096 }];
    if unsafe { ring.submitter().register_buffers(&iov) }.is_err() {
        let _ = ring.submitter().unregister_buffers();
        let _ = std::fs::remove_file(&probe_file);
        return false;
    }
    let r_op = io_uring::opcode::ReadFixed::new(io_uring::types::Fd(sfd), buf.ptr(), 4096, 0)
        .offset(0)
        .rw_flags(0x40 as i32)
        .build()
        .user_data(1);
    if unsafe { ring.submission().push(&r_op) }.is_err() {
        let _ = ring.submitter().unregister_buffers();
        let _ = std::fs::remove_file(&probe_file);
        return false;
    }
    let result = match ring.submit_and_wait(1) {
        Ok(_) => {
            let mut cqe_success = false;
            for cqe in ring.completion().take(1) {
                if cqe.result() >= 0 { cqe_success = true; }
            }
            cqe_success
        },
        Err(_e) => {
            false
        }
    };
    let _ = ring.submitter().unregister_buffers();
    let _ = std::fs::remove_file(&probe_file);
    result
}

fn calculate_partial_hash(path: &Path) -> Result<u64> {
    let mut file = File::open(path).map_err(FxcpError::Io)?;
    let len = file.metadata().map_err(FxcpError::Io)?.len();
    let mut hasher = FxHasher::default();
    hasher.write_u64(len);
    let mut buf = [0u8; 65536];
    let n = file.read(&mut buf).map_err(FxcpError::Io)?;
    hasher.write(&buf[..n]);
    if len > 131072 {
        file.seek(SeekFrom::End(-65536)).map_err(FxcpError::Io)?;
        let n = file.read(&mut buf).map_err(FxcpError::Io)?;
        hasher.write(&buf[..n]);
    }
    Ok(hasher.finish())
}

pub fn create_version_snapshot(path: &Path, epoch_seq: u64, root_path: &Path, inode: u64) -> Result<Option<crate::versioning::FileVersion>> {
    use std::fs;
    let version_dir = root_path.join(".foxing_versions").join("live");
    if let Err(e) = fs::create_dir_all(&version_dir) {
        warn!("Versioning: Failed to create version directory {:?}: {}", version_dir, e);
        return Ok(None);
    }
    if !path.exists() {
        debug!("Versioning: Source path {:?} does not exist, skipping snapshot.", path);
        return Ok(None);
    }
    let metadata = match fs::metadata(path) {
        Ok(m) => m,
        Err(e) => {
            debug!("Versioning: Failed to stat {:?}: {}", path, e);
            return Ok(None);
        }
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    let file_size = metadata.len();
    let mtime = metadata.mtime();
    let actual_inode = metadata.ino();
    if inode != 0 && actual_inode != inode {
        warn!("Versioning: Inode mismatch for {:?}. Expected {}, got {}. Skipping snapshot.", path, inode, actual_inode);
        return Ok(None);
    }
    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let snapshot_name = format!("{}_{}_{}", actual_inode, epoch_seq, timestamp);
    let snapshot_path = version_dir.join(&snapshot_name);
    if snapshot_path.exists() {
        debug!("Versioning: Snapshot {:?} already exists, skipping duplicate.", snapshot_path);
        return Ok(None);
    }
    let src_file = File::open(path)?;
    let src_fd = src_file.as_raw_fd();
    match ioctl_ficlone(src_fd, &snapshot_path) {
        Ok(_) => {
            debug!("Versioning: Created FFI FICLONE snapshot {:?} for epoch {}", snapshot_path, epoch_seq);
            let content_hash = match calculate_partial_hash(&snapshot_path) {
                Ok(h) => Some(h),
                Err(_) => None,
            };
            if let Some(hash) = content_hash {
                let hash_bytes = hash.to_le_bytes();
                let _ = sidecar::set_metadata(&snapshot_path, "content_hash", &hash_bytes);
            }
            crate::metrics::VERSIONING_SUCCESS.inc();
            Ok(Some(crate::versioning::FileVersion {
                inode: actual_inode,
                epoch_seq,
                timestamp,
                path: snapshot_path,
                size: file_size,
                mtime,
                content_hash,
            }))
        }
        Err(FxcpError::Io(e)) if e.kind() == std::io::ErrorKind::Unsupported => {
            warn!("Versioning: FICLONE (CoW) failed on {:?}: {}. Snapshot skipped.", path, e);
            crate::metrics::VERSIONING_FAILURES.inc();
            Ok(None)
        }
        Err(e) => {
            warn!("Versioning: Failed to execute FICLONE ioctl for {:?}: {}", path, e);
            crate::metrics::VERSIONING_FAILURES.inc();
            Ok(None)
        }
    }
}

pub fn revert_snapshot(_version_path: &Path, _live_path: &Path) -> Result<()> {
    Ok(())
}

pub fn commit_epoch(_path: &Path, _seq: u64, _projid: u32) -> Result<()> {
    Ok(())
}

pub fn truncate_file(dst: &Path, size: u64) -> Result<()> {
    let f = OpenOptions::new().write(true).open(dst).map_err(FxcpError::Io)?;
    f.set_len(size).map_err(FxcpError::Io)
}

pub fn do_fallocate(dst: &Path, offset: u64, len: u64, mode: i32) -> Result<()> {
    let f = OpenOptions::new().write(true).open(dst).map_err(FxcpError::Io)?;
    let fd = f.as_raw_fd();
    let ret = unsafe { libc::fallocate(fd, mode, offset as i64, len as i64) };
    if ret < 0 { return Err(FxcpError::Io(std::io::Error::last_os_error())); }
    Ok(())
}

pub fn create_symlink(target: &str, link: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link).map_err(FxcpError::Io)
}

pub fn create_hard_link(original: &Path, link: &Path) -> Result<()> {
    std::fs::hard_link(original, link).map_err(FxcpError::Io)
}

pub fn create_mknod(dst: &Path, mode: u32, dev: u64) -> Result<()> {
    let cpath = CString::new(dst.to_string_lossy().as_bytes()).map_err(|_| FxcpError::Config("Invalid path".into()))?;
    let ret = unsafe { libc::mknod(cpath.as_ptr(), mode, dev) };
    if ret < 0 { return Err(FxcpError::Io(std::io::Error::last_os_error())); }
    Ok(())
}

pub fn set_file_attr(path: &Path, flags: u32) -> Result<()> {
    let file = File::open(path).map_err(FxcpError::Io)?;
    let fd = file.as_raw_fd();
    let flags_long = flags as libc::c_long;
    let ret = unsafe {
        libc::ioctl(fd, operations::FS_IOC_SETFLAGS, &flags_long)
    };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EOPNOTSUPP) | Some(libc::ENOTTY) | Some(libc::EINVAL) => {
                debug!("Security: FS_IOC_SETFLAGS not supported or invalid on {:?} (Error: {})", path, err);
                Ok(())
            },
            _ => Err(FxcpError::Io(err))
        }
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Path sanitization — prevent path traversal and symlink escape
// ---------------------------------------------------------------------------

/// Check that `path` is within `root` after canonicalization.
/// Rejects symlinks that escape the root directory.
pub fn path_within_root(path: &Path, root: &Path) -> crate::error::Result<bool> {
    let canonical = path.canonicalize().map_err(|e| {
        FxcpError::Security(format!("cannot canonicalize {:?}: {}", path, e))
    })?;
    let root_canonical = root.canonicalize().map_err(|e| {
        FxcpError::Security(format!("cannot canonicalize root {:?}: {}", root, e))
    })?;
    Ok(canonical.starts_with(&root_canonical))
}

/// Canonicalize `path` and verify it stays within `root`.
/// Returns the canonical path or a security error.
pub fn canonicalize_safe(path: &Path, root: &Path) -> crate::error::Result<std::path::PathBuf> {
    let canonical = path.canonicalize().map_err(|e| {
        FxcpError::Security(format!("cannot canonicalize {:?}: {}", path, e))
    })?;
    let root_canonical = root.canonicalize().map_err(|e| {
        FxcpError::Security(format!("cannot canonicalize root {:?}: {}", root, e))
    })?;
    if !canonical.starts_with(&root_canonical) {
        return Err(FxcpError::Security(format!(
            "path {:?} escapes root {:?} (resolved to {:?})", path, root, canonical
        )));
    }
    Ok(canonical)
}

// ---------------------------------------------------------------------------
// openat2 RESOLVE_BENEATH — kernel-enforced path containment (Linux 5.6+)
// ---------------------------------------------------------------------------

const RESOLVE_BENEATH: u64 = 0x08;
const SYS_OPENAT2: i64 = 437;

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

/// Open a file beneath `root` using openat2(2) with RESOLVE_BENEATH.
/// The kernel rejects any path that escapes `root` via `..` or symlinks.
/// Falls back to canonicalize_safe + standard open if openat2 unavailable.
pub fn open_beneath(
    root: &Path,
    relative_path: &Path,
    flags: i32,
    mode: u32,
) -> crate::error::Result<std::fs::File> {
    use std::os::unix::io::{FromRawFd, AsRawFd};

    // Open the root directory
    let root_dir = std::fs::File::open(root).map_err(|e| {
        FxcpError::Security(format!("cannot open root {:?}: {}", root, e))
    })?;
    let root_fd = root_dir.as_raw_fd();

    let rel_cstr = std::ffi::CString::new(
        relative_path.as_os_str().as_encoded_bytes()
    ).map_err(|e| FxcpError::Security(format!("invalid path: {}", e)))?;

    let how = OpenHow {
        flags: flags as u64,
        mode: mode as u64,
        resolve: RESOLVE_BENEATH,
    };

    let fd = unsafe {
        libc::syscall(
            SYS_OPENAT2,
            root_fd,
            rel_cstr.as_ptr(),
            &how as *const OpenHow,
            std::mem::size_of::<OpenHow>(),
        )
    };

    if fd >= 0 {
        Ok(unsafe { std::fs::File::from_raw_fd(fd as i32) })
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOSYS) {
            // openat2 not available — fall back to canonicalize_safe
            let full_path = root.join(relative_path);
            let safe = canonicalize_safe(&full_path, root)?;
            let file = std::fs::OpenOptions::new()
                .read((flags & libc::O_RDONLY) == libc::O_RDONLY || (flags & libc::O_RDWR) != 0)
                .write((flags & libc::O_WRONLY) != 0 || (flags & libc::O_RDWR) != 0)
                .create((flags & libc::O_CREAT) != 0)
                .truncate((flags & libc::O_TRUNC) != 0)
                .open(&safe)?;
            Ok(file)
        } else if err.raw_os_error() == Some(libc::EXDEV) {
            Err(FxcpError::Security(format!(
                "path {:?} escapes root {:?} (RESOLVE_BENEATH rejected)", relative_path, root
            )))
        } else {
            Err(FxcpError::Io(err))
        }
    }
}
