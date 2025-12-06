use std::path::Path;
use crate::error::{FoxingError, Result};
use std::os::unix::io::{AsRawFd, BorrowedFd};
use nix::fcntl::{fallocate, FallocateFlags};
use libc;
use xattr;
use nix::sys::statvfs::statvfs;
use std::ffi::CString;
use std::hash::Hasher;
use walkdir::WalkDir;
use std::os::unix::fs::MetadataExt;
use std::fs::{File, OpenOptions};
use chrono::Utc;
use crate::sidecar;
use crate::buffer::AlignedBuffer;
use io_uring::{IoUring, opcode, types};
use std::collections::hash_map::DefaultHasher;
use std::io::{Read, Seek, SeekFrom};
use fxhash::FxHasher;
use tracing::warn;

const FS_IOC_FSSETXATTR: u64 = 0x40205820;
const FS_IOC_SETFLAGS: u64 = 0x40086602;
const FS_COMPR_FL: u32 = 0x00000004;
const F2FS_IOC_SET_PIN_FILE: u64 = 0xF50D;
const FIOCLONERANGE: u64 = 0x4020940D;
const RWF_UNCACHED: i32 = 0x00000008;
const F2FS_SUPER_MAGIC: i64 = 0xF2F52010;
const BTRFS_SUPER_MAGIC: i64 = 0x9123683E;

#[repr(C)] struct FileCloneRange { s: i64, so: u64, l: u64, do_: u64 }
#[repr(C)] #[derive(Default)]
struct FsxAttr { fsx_xflags: u32, fsx_extsize: u32, fsx_nextents: u32, fsx_projid: u32, fsx_cowextsize: u32, fsx_pad: [u8; 8] }

pub fn calculate_partial_hash(path: &Path) -> Result<u64> {
    let mut file = File::open(path).map_err(FoxingError::Io)?;
    let len = file.metadata().map_err(FoxingError::Io)?.len();
    let mut hasher = FxHasher::default();
    hasher.write_u64(len);
    let mut buf = [0u8; 65536];
    let n = file.read(&mut buf).map_err(FoxingError::Io)?;
    hasher.write(&buf[..n]);
    if len > 131072 {
        file.seek(SeekFrom::End(-65536)).map_err(FoxingError::Io)?;
        let n = file.read(&mut buf).map_err(FoxingError::Io)?;
        hasher.write(&buf[..n]);
    }
    Ok(hasher.finish())
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
        return magic == BTRFS_SUPER_MAGIC || magic == F2FS_SUPER_MAGIC;
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
    let ret = unsafe { libc::ioctl(fd, FS_IOC_SETFLAGS, &flags) };
    if ret != 0 { return Err(FoxingError::System(nix::Error::last())); }
    Ok(())
}

pub fn enable_f2fs_pinning(fd: i32) -> Result<()> {
    let pin: u32 = 1;
    let ret = unsafe { libc::ioctl(fd, F2FS_IOC_SET_PIN_FILE, &pin) };
    if ret != 0 { return Err(FoxingError::System(nix::Error::last())); }
    Ok(())
}

pub fn set_project_id(fd: i32, projid: u32) -> Result<()> {
    if projid == 0 { return Ok(()); }
    let mut attr: FsxAttr = Default::default();
    attr.fsx_projid = projid;
    let ret = unsafe { libc::ioctl(fd, FS_IOC_FSSETXATTR, &attr) };
    if ret != 0 { return Err(FoxingError::System(nix::Error::last())); }
    Ok(())
}

pub fn acquire_mandatory_lock(fd: i32) -> Result<()> {
    let lock = libc::flock { l_type: libc::F_WRLCK as i16, l_whence: libc::SEEK_SET as i16, l_start: 0, l_len: 0, l_pid: 0 };
    if unsafe { libc::fcntl(fd, libc::F_OFD_SETLKW, &lock) } < 0 {
        return Err(FoxingError::System(nix::Error::last()));
    }
    Ok(())
}

pub fn get_target_epoch(path: &Path) -> u64 {
    if let Some(val) = sidecar::get_metadata(path, "user.foxing_epoch") {
         if val.len() == 8 {
            return u64::from_le_bytes(val.clone().try_into().unwrap_or_default());
        }
    }
    0
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

pub fn get_valid_dir_hash(path: &Path) -> u64 {
    let hash_bytes = match sidecar::get_metadata(path, "user.foxing_dir_hash") {
        Some(b) if b.len() == 8 => b,
        _ => return 0,
    };
    let hash = u64::from_le_bytes(hash_bytes.try_into().unwrap());
    let guard_bytes = match sidecar::get_metadata(path, "user.foxing_dir_guard") {
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
    let _ = sidecar::set_metadata(target_dir, "user.foxing_dir_hash_pending", &hash_bytes);
    if let Ok(dir_file) = File::open(target_dir) {
        let _ = dir_file.sync_all();
    }
    let _ = sidecar::set_metadata(target_dir, "user.foxing_dir_hash", &hash_bytes);
    if let Ok(meta) = std::fs::metadata(target_dir) {
        let mtime_bytes = meta.mtime().to_le_bytes();
        let _ = sidecar::set_metadata(target_dir, "user.foxing_dir_guard", &mtime_bytes);
    }
    if let Ok(dir_file) = File::open(target_dir) {
        let _ = dir_file.sync_all();
    }
    let _ = sidecar::remove_metadata(target_dir, "user.foxing_dir_hash_pending");
}

pub fn probe_xattr_support(target_root: &Path) -> bool {
    let probe_file = target_root.join(".xfs_mirror_probe_xattr");
    if probe_file.exists() { let _ = std::fs::remove_file(&probe_file); }
    if let Ok(_f) = File::create(&probe_file) {
        let key = "user.foxing_probe";
        let val = b"1";
        let res = xattr::set(&probe_file, key, val);
        let _ = std::fs::remove_file(&probe_file);
        res.is_ok()
    } else {
        false
    }
}

pub fn probe_direct_io(target_root: &Path) -> bool {
    let probe_file = target_root.join(".xfs_mirror_probe_dio");
    let mut buf = AlignedBuffer::new(4096);
    unsafe { buf.capacity_slice_mut()[0] = 1; }
    let flags = libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_DIRECT;
    let path_c = match CString::new(probe_file.to_string_lossy().as_bytes()) { Ok(c) => c, Err(_) => return false };
    let fd = unsafe { libc::open(path_c.as_ptr(), flags, 0o644) };
    if fd < 0 { return false; }
    let ret = unsafe { libc::write(fd, buf.as_ptr() as *const _, 4096) };
    let _ = std::fs::remove_file(probe_file);
    ret == 4096
}

pub fn probe_rwf_uncached(target_root: &Path) -> bool {
    let probe_file = target_root.join(".xfs_mirror_probe_uncached");
    let mut buf = AlignedBuffer::new(4096);
    unsafe { buf.capacity_slice_mut().copy_from_slice(&[1u8; 4096]); }
    let file_res = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&probe_file);
    let sfd = match file_res {
        Ok(f) => f.as_raw_fd(),
        Err(_e) => {
            let _ = std::fs::remove_file(probe_file);
            return false;
        }
    };
    let ret = unsafe { libc::write(sfd, buf.as_ptr() as *const _, 4096) };
    if ret != 4096 {
        let _ = std::fs::remove_file(probe_file);
        return false;
    }
    let mut ring = match IoUring::new(1) {
        Ok(r) => r,
        Err(_e) => { return false; }
    };
    let iov = [libc::iovec { iov_base: buf.ptr() as _, iov_len: 4096 }];
    if unsafe { ring.submitter().register_buffers(&iov) }.is_err() {
        let _ = ring.submitter().unregister_buffers();
        let _ = std::fs::remove_file(probe_file);
        return false;
    }
    let r_op = opcode::ReadFixed::new(types::Fd(sfd), buf.ptr(), 4096, 0).offset(0).rw_flags(RWF_UNCACHED).build().user_data(1);
    if unsafe { ring.submission().push(&r_op) }.is_err() {
        let _ = ring.submitter().unregister_buffers();
        let _ = std::fs::remove_file(probe_file);
        return false;
    }
    let result = match ring.submit_and_wait(1) {
        Ok(_) => {
            let mut cqe_success = false;
            for cqe in ring.completion().take(1) {
                let res = cqe.result();
                if res >= 0 { cqe_success = true; }
            }
            cqe_success
        },
        Err(_e) => {
            false
        }
    };
    let _ = ring.submitter().unregister_buffers();
    let _ = std::fs::remove_file(probe_file);
    result
}

pub fn create_version_snapshot(path: &Path, epoch_seq: u64, root_path: &Path, inode: u64) -> Result<()> {
    let version_dir = root_path.join(".mirror").join(".versions");
    std::fs::create_dir_all(&version_dir)?;
    let src_file = std::fs::File::open(path)?;
    let src_fd = src_file.as_raw_fd();
    let size = src_file.metadata()?.len();
    let content_hash = calculate_partial_hash(path).unwrap_or(0);
    let version_path = version_dir.join(format!("{}_{}_{}", inode, epoch_seq, Utc::now().timestamp()));
    let dst_file = std::fs::OpenOptions::new().write(true).create_new(true).open(&version_path)?;
    let dst_fd = dst_file.as_raw_fd();
    let range = FileCloneRange { s: src_fd as i64, so: 0, l: size, do_: 0 };
    let ret = unsafe { libc::ioctl(dst_fd, FIOCLONERANGE, &range) };
    if let Ok(s) = nix::sys::statfs::statfs(path) {
        if s.filesystem_type().0 as i64 == F2FS_SUPER_MAGIC {
            dst_file.sync_all()?;
        }
    }
    if ret != 0 {
        warn!("IOCTL FICLONERANGE failed (errno: {} / {:?}). Fallback copy.", ret, std::io::Error::last_os_error());
        let mut off_in = 0i64;
        let mut off_out = 0i64;
        let ret = unsafe { libc::copy_file_range(src_fd, &mut off_in, dst_fd, &mut off_out, size as usize, 0) };
        if ret != size as isize {
             return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Fallback copy failed size mismatch"))));
        }
    }
    let _ = apply_metadata(path, &version_path);
    if content_hash != 0 {
        let _ = sidecar::set_metadata(&version_path, "user.foxing.content_hash", &content_hash.to_le_bytes());
    }
    dst_file.sync_all()?;
    Ok(())
}

pub fn revert_snapshot(version_path: &Path, live_path: &Path) -> Result<()> {
    let src_file = std::fs::File::open(version_path)?;
    let src_fd = src_file.as_raw_fd();
    let size = src_file.metadata()?.len();
    let dst_file = std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(live_path)?;
    let dst_fd = dst_file.as_raw_fd();
    let range = FileCloneRange { s: src_fd as i64, so: 0, l: size, do_: 0 };
    let ret = unsafe { libc::ioctl(dst_fd, FIOCLONERANGE, &range) };
    if ret != 0 {
        warn!("IOCTL FICLONERANGE failed revert. Fallback copy.");
        let mut off_in = 0i64;
        let mut off_out = 0i64;
        let ret = unsafe { libc::copy_file_range(src_fd, &mut off_in, dst_fd, &mut off_out, size as usize, 0) };
        if ret != size as isize { return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Fallback revert failed"))); }
    }
    dst_file.set_len(size)?;
    dst_file.sync_all()?;
    let _ = apply_metadata(version_path, live_path);
    Ok(())
}

pub fn commit_epoch(path: &Path, seq: u64, projid: u32) -> Result<()> {
    let f = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
    let fd = f.as_raw_fd();
    acquire_mandatory_lock(fd)?;
    if set_project_id(fd, projid).is_err() {
        let _ = sidecar::set_metadata(path, "xfs.projid", &projid.to_le_bytes());
    }
    let _ = sidecar::set_metadata(path, "user.foxing_epoch", &seq.to_le_bytes());
    f.sync_all()?;
    sidecar::set_dirty_flag(path, false, "COMMIT");
    Ok(())
}

pub fn sync_xattrs(src: &Path, dst: &Path) {
    if let Ok(list) = xattr::list(src) {
        for name in list.filter(|n| n.to_string_lossy().starts_with("user.")) {
            if let Ok(Some(val)) = xattr::get(src, &name) {
                let _ = sidecar::set_metadata(dst, &name.to_string_lossy(), &val);
            }
        }
    }
}

pub fn apply_metadata(src: &Path, dst: &Path) -> Result<()> {
    let m = std::fs::symlink_metadata(src).map_err(FoxingError::Io)?;
    if !m.is_symlink() {
        std::os::unix::fs::chown(dst, Some(m.uid()), Some(m.gid())).map_err(FoxingError::Io)?;
        std::fs::set_permissions(dst, m.permissions()).map_err(FoxingError::Io)?;
    } else {
         let dst_c = CString::new(dst.to_string_lossy().as_bytes()).unwrap();
         if unsafe { libc::lchown(dst_c.as_ptr(), m.uid(), m.gid()) } < 0 {
             return Err(FoxingError::Io(std::io::Error::last_os_error()));
         }
    }
    let times = [
        libc::timespec { tv_sec: m.atime(), tv_nsec: m.atime_nsec() },
        libc::timespec { tv_sec: m.mtime(), tv_nsec: m.mtime_nsec() },
    ];
    let dst_c = CString::new(dst.to_string_lossy().as_bytes()).unwrap();
    if unsafe { libc::utimensat(libc::AT_FDCWD, dst_c.as_ptr(), times.as_ptr(), libc::AT_SYMLINK_NOFOLLOW) } < 0 {
         return Err(FoxingError::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

pub fn truncate_file(dst: &Path, size: u64) -> Result<()> {
    let f = std::fs::OpenOptions::new().write(true).open(dst)?;
    f.set_len(size)?;
    Ok(())
}

pub fn do_fallocate(dst: &Path, offset: u64, len: u64, mode: i32) -> Result<()> {
    let f = std::fs::OpenOptions::new().write(true).open(dst)?;
    let fd = f.as_raw_fd();
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let flags = FallocateFlags::from_bits_truncate(mode);
    nix::fcntl::fallocate(borrowed.as_raw_fd(), flags, offset as i64, len as i64)?;
    Ok(())
}

pub fn create_symlink(link_target: &str, dst: &Path) -> Result<()> {
    if dst.exists() || std::fs::symlink_metadata(dst).is_ok() {
        let _ = std::fs::remove_file(dst);
    }
    std::os::unix::fs::symlink(link_target, dst).map_err(FoxingError::Io)
}

pub fn create_hard_link(original: &Path, link: &Path) -> Result<()> {
    if link.exists() {
        let _ = std::fs::remove_file(link);
    }
    std::fs::hard_link(original, link).map_err(FoxingError::Io)
}

pub fn create_mknod(dst: &Path, mode: u32, dev: u64) -> Result<()> {
    if dst.exists() {
        let _ = std::fs::remove_file(dst);
    }
    let path_c = CString::new(dst.to_string_lossy().as_bytes()).map_err(|_| FoxingError::Security("Invalid path".into()))?;
    let ret = unsafe { libc::mknod(path_c.as_ptr(), mode, dev as libc::dev_t) };
    if ret < 0 {
        return Err(FoxingError::System(nix::Error::last()));
    }
    Ok(())
}
