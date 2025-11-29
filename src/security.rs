use std::path::{Path};
use crate::error::{FoxingError, Result};
use crate::buffer::AlignedBuffer;
use io_uring::{opcode, types}; 
use std::os::unix::io::{AsRawFd, FromRawFd, BorrowedFd}; 
use std::os::fd::IntoRawFd; // <-- FIX E0599: Added missing trait import for safe FD transfer
use nix::fcntl::{fallocate, FallocateFlags}; 
use libc;
use xattr;
use nix::sys::statvfs::statvfs;
use std::ffi::CString;
use tracing::{warn, debug};
use std::hash::Hasher; 
use fxhash::FxHasher; 
use walkdir::WalkDir;
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::Ordering;
use std::fs::File;
use chrono::Utc; 
use uuid::Uuid; 
use crate::sidecar;
use std::io::{copy, Seek, SeekFrom}; 
use tokio::task::spawn_blocking;

const FS_IOC_FSSETXATTR: u64 = 0x40205820; 
const FS_IOC_SETFLAGS: u64 = 0x40086602;
const FS_COMPR_FL: u32 = 0x00000004;
const F2FS_IOC_SET_PIN_FILE: u64 = 0xF50D; 
const FIOCLONERANGE: u64 = 0x4020940D;

// RWF flags for uncached buffered IO
const RWF_UNCACHED: i32 = 0x00000008; // Linux 5.18+

#[repr(C)] struct FileCloneRange { s: i64, so: u64, l: u64, do_: u64 }
#[repr(C)] #[derive(Default)]
struct FsxAttr { fsx_xflags: u32, fsx_extsize: u32, fsx_nextents: u32, fsx_projid: u32, fsx_cowextsize: u32, fsx_pad: [u8; 8] }

fn is_block_zero(buf: &[u8]) -> bool {
    let (prefix, chunks, suffix) = unsafe { buf.align_to::<u128>() };
    chunks.iter().all(|&x| x == 0) && prefix.iter().all(|&x| x == 0) && suffix.iter().all(|&x| x == 0)
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

pub fn probe_direct_io(target_root: &Path) -> bool {
    let probe_file = target_root.join(".xfs_mirror_probe_dio");
    let mut buf = crate::buffer::AlignedBuffer::new(4096); 
    unsafe { buf.capacity_slice_mut()[0] = 1; }
    let flags = libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_DIRECT;
    let path_c = match CString::new(probe_file.to_string_lossy().as_bytes()) { Ok(c) => c, Err(_) => return false };
    let fd = unsafe { libc::open(path_c.as_ptr(), flags, 0o644) };
    if fd < 0 { return false; }
    let _file = unsafe { File::from_raw_fd(fd) };
    let ret = unsafe { libc::write(fd, buf.as_ptr() as *const _, 4096) };
    let _ = std::fs::remove_file(probe_file);
    ret == 4096
}

pub fn preallocate(fd: i32, size: u64) {
    if size > 0 { 
        // SAFETY: BorrowedFd required by nix 0.27+
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
    let mut hasher = FxHasher::default();
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

pub fn get_dir_integrity_hash(path: &Path) -> u64 {
    if let Some(val) = sidecar::get_metadata(path, "user.foxing_dir_hash") {
         if val.len() == 8 {
            return u64::from_le_bytes(val.clone().try_into().unwrap_or_default());
        }
    }
    0
}

pub fn write_dir_integrity_hash(target_dir: &Path, hash: u64) {
    let hash_bytes = hash.to_le_bytes();
    sidecar::set_metadata(target_dir, "user.foxing_dir_hash", &hash_bytes);
}

pub fn create_version_snapshot(path: &Path, epoch_seq: u64, root_path: &Path, inode: u64) -> Result<()> {
    let version_dir = root_path.join(".mirror").join(".versions");
    std::fs::create_dir_all(&version_dir)?;
    let src_file = std::fs::File::open(path)?;
    let src_fd = src_file.as_raw_fd();
    let size = src_file.metadata()?.len();
    let version_path = version_dir.join(format!("{}_{}_{}", inode, epoch_seq, Utc::now().timestamp()));
    let dst_file = std::fs::OpenOptions::new().write(true).create_new(true).open(&version_path)?;
    let dst_fd = dst_file.as_raw_fd();
    let range = FileCloneRange { s: src_fd as i64, so: 0, l: size, do_: 0 };
    let ret = unsafe { libc::ioctl(dst_fd, FIOCLONERANGE, &range) };
    if ret != 0 {
        warn!("IOCTL FICLONERANGE failed (errno: {} / {:?}). Fallback copy.", ret, std::io::Error::last_os_error());
        let mut off_in = 0i64;
        let mut off_out = 0i64;
        let ret = unsafe { libc::copy_file_range(src_fd, &mut off_in, dst_fd, &mut off_out, size as usize, 0) };
        if ret != size as isize {
             return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Fallback copy failed size mismatch"))));
        }
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
    Ok(())
}

/// Helper function for performing uncached buffered I/O.
fn do_uncached_write(fd: i32, buf: &[u8], offset: u64) -> std::io::Result<isize> {
    // We use pwritev2 for RWF_UNCACHED support on newer kernels
    let iov = libc::iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };
    
    // SAFETY: Calling raw libc function pwritev2 for RWF_UNCACHED flag
    let ret = unsafe {
        libc::syscall(
            libc::SYS_pwritev2,
            fd,
            &iov,
            1,
            offset,
            RWF_UNCACHED,
        )
    };

    if ret < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(ret as isize)
    }
}
 
pub async fn copy_smart(
    src: &Path, 
    dst: &Path, 
    ring: &mut io_uring::IoUring, 
    buf: &mut AlignedBuffer, 
    reflink_ok: &std::sync::atomic::AtomicBool, 
    vdo_opt: bool,
    offset: u64,
    length: u64
) -> Result<u64> {
    
    // NOTE: sf ownership is not required by copy_smart beyond metadata and fds
    let sf = File::open(src)?;
    let src_file_size = sf.metadata()?.len(); 
    let sfd = sf.as_raw_fd();

    // Determine if this is a full atomic file creation/replacement (offset=0, len=file_size)
    let is_full_replace = offset == 0 && length == src_file_size && src_file_size > 0;
    
    // For delta updates, we open the destination file directly.
    // For full replacements, we use a temp file for atomic rename.
    let (target_path, open_flags) = if is_full_replace {
        (dst.with_extension(format!("tmp.{}", Uuid::new_v4())), libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC)
    } else {
        (dst.to_path_buf(), libc::O_RDWR) 
    };

    // Determine IO Mode
    let use_direct_io = reflink_ok.load(Ordering::Relaxed) && is_full_replace && length >= 4096 && (length % 4096 == 0);
    let use_uncached_io = !use_direct_io && length >= 4096; // Use RWF_UNCACHED for larger buffered writes

    let perms = libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH; 

    let final_flags = if use_direct_io { 
        debug!("copy_smart: Using O_DIRECT for full replace.");
        open_flags | libc::O_DIRECT 
    } else { 
        open_flags 
    };

    let path_c = CString::new(target_path.to_string_lossy().as_bytes()).map_err(|_| FoxingError::Security("Invalid path".into()))?;
    
    let dfd = unsafe { libc::open(path_c.as_ptr(), final_flags, perms) };
    if dfd < 0 { 
        let os_err = std::io::Error::last_os_error();
        debug!("copy_smart libc::open FAILED for path {:?} (flags: {:#x}, perms: {:#o}): {:?}", target_path, final_flags, perms, os_err);
        return Err(FoxingError::Io(os_err)); 
    }
    // We intentionally leak the df handle here, relying on dfd being managed below.
    let _df = unsafe { File::from_raw_fd(dfd) }; 
    
    if is_full_replace {
        preallocate(dfd, src_file_size); 
    }

    let mut reflink_success = false;
    let mut data_transfer_succeeded = false;

    // --- REFLINK ATTEMPT (Only for full replacements) ---
    if is_full_replace && reflink_ok.load(Ordering::Relaxed) {
        let mut off_in = 0i64;
        let mut off_out = 0i64;
        
        let ret = unsafe { libc::copy_file_range(sfd, &mut off_in, dfd, &mut off_out, src_file_size as usize, 0) };
        
        if ret > 0 && ret == src_file_size as isize {
             reflink_success = true;
             data_transfer_succeeded = true;
             debug!("copy_smart: Full copy via copy_file_range succeeded (Atomic Mode).");
        } else if ret < 0 {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno == libc::EXDEV { 
                debug!("copy_smart: copy_file_range failed with EXDEV (Cross-device). Falling back to standard copy.");
            } else {
                warn!("copy_file_range failed unexpectedly (errno: {}). Disabling reflink probe flag.", errno);
                reflink_ok.store(false, Ordering::Relaxed);
            }
        }
    }
    // --- END REFLINK ATTEMPT ---

    // --- FALLBACK (io_uring, Uncached, or Synchronous) ---
    if !reflink_success {
        if is_full_replace {
            // Full file copy fallback
            // FIX: Wrap synchronous copy in spawn_blocking to prevent thread pool stall
            debug!("copy_smart: Falling back to full synchronous copy (std::io::copy). Wrapping in spawn_blocking.");

            let src_file_size_clone = src_file_size;
            
            // To pass handles into blocking thread, we must leak them (convert to raw FD)
            // The file descriptor itself is dfd/sfd, which we are managing manually here.
            let sfd_blocking = sf.as_raw_fd(); 
            let dfd_blocking = dfd; 
            
            // Re-create temporary File objects inside the closure for copy's trait requirements
            let bytes_copied_result = spawn_blocking(move || {
                // SAFETY: We manually manage the FDs to ensure they are only dropped by the worker thread.
                let mut sf_blocking = unsafe { File::from_raw_fd(sfd_blocking) };
                let mut df_blocking = unsafe { File::from_raw_fd(dfd_blocking) };
                
                let res = sf_blocking.seek(SeekFrom::Start(0))
                    .and_then(|_| copy(&mut sf_blocking, &mut df_blocking));

                // We must leak the file handles back to prevent the blocking thread from closing the FDs!
                // FIX E0599: Use into_raw_fd() to consume File without closing FD.
                let _ = sf_blocking.into_raw_fd(); 
                let _ = df_blocking.into_raw_fd();

                res
            }).await.unwrap_or_else(|e| {
                Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Blocking copy task failed: {}", e)))
            })?;
            

            if bytes_copied_result != src_file_size_clone {
                 return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Full sync copy failed size mismatch")));
            }
            data_transfer_succeeded = true;
        } else {
            // Delta update path (using existing io_uring logic but adapting for uncached)
            debug!("copy_smart: Performing delta write via io_uring ({} cached).", if use_uncached_io { "RWF_UNCACHED" } else { "standard" });
            let dfd = dfd; 
            let mut current_offset = offset;
            let end_offset = offset + length;
            let max_chunk = buf.capacity() as u64;
            
            while current_offset < end_offset {
                let rlen = std::cmp::min(end_offset - current_offset, max_chunk) as usize;
                
                // 1. Read from Source (always standard read)
                // buf.capacity_slice_mut() creates a temporary mutable reference to the buffer's contents
                let r_op = opcode::ReadFixed::new(types::Fd(sfd), unsafe { buf.capacity_slice_mut() }.as_mut_ptr(), rlen as u32, 0)
                    .offset(current_offset)
                    .build()
                    .user_data(current_offset as u64);
                unsafe { ring.submission().push(&r_op).map_err(|_e| FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "SQ Full")))?; }
                ring.submit_and_wait(1)?; 
                let cqe = ring.completion().next().ok_or(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "No CQE")))?;
                if cqe.result() < 0 { return Err(FoxingError::Io(std::io::Error::from_raw_os_error(-cqe.result()))); }
                
                // 2. Write to Target (Conditional: VDO Optimization or RWF_UNCACHED/Standard Write)
                let data_slice = unsafe { &buf.capacity_slice_mut()[0..rlen] };
                let write_needed = if vdo_opt && is_block_zero(data_slice) {
                    false // Skip zero block write optimization
                } else {
                    true
                };

                if write_needed {
                    if use_uncached_io {
                        // FIX E0499/E0521: Clone the data slice so it can be safely moved into the 'static spawn_blocking closure.
                        let data_to_write: Vec<u8> = data_slice.to_vec(); 
                        let written = spawn_blocking(move || {
                            do_uncached_write(dfd, &data_to_write, current_offset)
                        }).await.unwrap_or(Err(std::io::Error::new(std::io::ErrorKind::Other, "Uncached write task failed")))?;
                        
                        if written as usize != rlen { 
                             return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Uncached write size mismatch: {} != {}", written, rlen))));
                        }
                    } else {
                        // Use standard io_uring write
                        let w_op = opcode::WriteFixed::new(types::Fd(dfd), data_slice.as_ptr(), rlen as u32, 0)
                            .offset(current_offset)
                            .build()
                            .user_data(current_offset as u64 | (1<<63));
                        unsafe { ring.submission().push(&w_op).map_err(|_e| FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "SQ Full")))?; }
                        ring.submit_and_wait(1)?;
                        let cqe_w = ring.completion().next().ok_or(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "No CQE")))?;
                        if cqe_w.result() < 0 { return Err(FoxingError::Io(std::io::Error::from_raw_os_error(-cqe_w.result()))); }
                    }
                } else {
                    debug!("copy_smart: Skipping zero block write at offset {}", current_offset);
                }
                current_offset += rlen as u64;
            }
            data_transfer_succeeded = true;
        }
    }
    
    // Use the raw FD to safely re-open the file handle for final sync, ensuring it is only dropped once.
    let df_reopen = unsafe { File::from_raw_fd(dfd) };
    df_reopen.sync_all()?;
    // The drop(df_reopen) is implicit here, closing the FD
    
    if !data_transfer_succeeded && length > 0 {
        warn!("copy_smart completed without transferring data for {:?}. Check zero-length or optimization flags.", dst);
    }
    
    // Only perform rename if we used a temp file
    if is_full_replace {
        debug!("copy_smart: Attempting atomic rename from {:?} to {:?}", target_path, dst);
        if let Err(e) = std::fs::rename(&target_path, dst) {
            let _ = std::fs::remove_file(&target_path); 
            return Err(FoxingError::Io(e));
        }
    }
    Ok(length)
}

pub fn commit_epoch(path: &Path, seq: u64, projid: u32) -> Result<()> {
    // CRITICAL FIX: Must open with write access to acquire a Write Lock (F_WRLCK).
    // Opening with File::open() is read-only and causes EBADF on lock attempt.
    let f = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
    let fd = f.as_raw_fd();
    acquire_mandatory_lock(fd)?;
    
    if set_project_id(fd, projid).is_err() { 
        sidecar::set_metadata(path, "xfs.projid", &projid.to_le_bytes());
    }
    sidecar::set_metadata(path, "user.foxing_epoch", &seq.to_le_bytes());
    f.sync_all()?; 
    sidecar::set_dirty_flag(path, false);
    Ok(())
}

pub fn sync_xattrs(src: &Path, dst: &Path) {
    if let Ok(list) = xattr::list(src) {
        for name in list.filter(|n| n.to_string_lossy().starts_with("user.")) {
            if let Ok(Some(val)) = xattr::get(src, &name) { 
                sidecar::set_metadata(dst, &name.to_string_lossy(), &val);
            }
        }
    }
}

pub fn apply_metadata(src: &Path, dst: &Path) -> Result<()> {
    let m = std::fs::metadata(src)?;
    let _ = std::os::unix::fs::chown(dst, Some(m.uid()), Some(m.gid()));
    std::fs::set_permissions(dst, m.permissions())?;
    let times = [
        libc::timespec { tv_sec: m.atime(), tv_nsec: m.atime_nsec() },
        libc::timespec { tv_sec: m.mtime(), tv_nsec: m.mtime_nsec() },
    ];
    let dst_c = CString::new(dst.to_string_lossy().as_bytes()).unwrap();
    unsafe { libc::utimensat(libc::AT_FDCWD, dst_c.as_ptr(), times.as_ptr(), 0) };
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
    // SAFETY: nix 0.27 requires BorrowedFd
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let flags = FallocateFlags::from_bits_truncate(mode);
    nix::fcntl::fallocate(borrowed.as_raw_fd(), flags, offset as i64, len as i64)?;
    Ok(())
}
