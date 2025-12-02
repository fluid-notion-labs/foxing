use std::path::{Path, PathBuf};
use std::fs::File;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::fd::IntoRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::{self, Seek, SeekFrom};
use std::ffi::CString;
use tokio::task::spawn_blocking;
use io_uring::{opcode, types, IoUring};
use uuid::Uuid;
use libc;
use nix::sys::statfs;
use std::time::{Duration, Instant};
use tracing::{warn, debug, error};
use std::convert::TryInto;
use crate::buffer::AlignedBuffer;
use crate::error::{FoxingError, Result};
use crate::security;
use crate::metrics;
use std::sync::atomic::fence;
const RWF_UNCACHED: i32 = 0x00000008;
const NFS_SUPER_MAGIC: i64 = 0x6969;
const SMB_SUPER_MAGIC: i64 = 0x517B;
const CIFS_MAGIC_NUMBER: i64 = 0xFF534D42;
#[derive(Debug, Default, Clone, Copy)]
pub struct CopyStats {
    pub bytes_processed: u64,
    pub bytes_zeros: u64,
    pub io_duration: Duration,
}
struct TmpFileGuard {
    path: PathBuf,
    armed: bool,
}
impl TmpFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for TmpFileGuard {
    fn drop(&mut self) {
        if self.armed {
            warn!("IO Transaction Failed: Rolling back temp file {:?}", self.path);
            if let Err(e) = std::fs::remove_file(&self.path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    error!("CRITICAL: Failed to clean up temp file {:?}: {}", self.path, e);
                }
            }
        }
    }
}
pub struct SmartCopier;
impl SmartCopier {
    pub async fn copy(
        src: &Path,
        dst: &Path,
        ring: &mut IoUring,
        buf: &mut AlignedBuffer,
        reflink_ok: &AtomicBool,
        vdo_opt: bool,
        offset: u64,
        length: u64,
        direct_io_ok: bool,
        src_file_size: u64,
    ) -> Result<CopyStats> {
        let sf = File::open(src)?;
        let sfd = sf.as_raw_fd();
        let is_full_replace = offset == 0 && length == src_file_size;
        if is_full_replace && src_file_size > 10 * 1024 * 1024 {
            debug!("SmartCopier: Triggering FULL ATOMIC REPLACE for {:?} (Size: {}).", dst, src_file_size);
        }
        let target_path = if is_full_replace {
            dst.with_extension(format!("tmp.{}", Uuid::new_v4()))
        } else {
            dst.to_path_buf()
        };
        let mut cleanup_guard = if is_full_replace {
            Some(TmpFileGuard::new(target_path.clone()))
        } else {
            None
        };
        let open_flags = if is_full_replace {
            libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC
        } else {
            libc::O_RDWR
        };
        let use_direct_io_for_delta = direct_io_ok && !is_full_replace && length >= 4096 && (length % 4096 == 0);
        let final_flags = if use_direct_io_for_delta {
            open_flags | libc::O_DIRECT
        } else {
            open_flags
        };
        let path_c = CString::new(target_path.to_string_lossy().as_bytes())
            .map_err(|_| FoxingError::Security("Invalid path".into()))?;
        let dfd = unsafe { libc::open(path_c.as_ptr(), final_flags, 0o644) };
        if dfd < 0 {
            return Err(FoxingError::Io(std::io::Error::last_os_error()));
        }
        if is_full_replace {
            security::preallocate(dfd, src_file_size);
        }
        let mut transfer_done = false;
        let mut stats = CopyStats::default();
        if is_full_replace && reflink_ok.load(Ordering::Relaxed) {
            let start = Instant::now();
            let mut total_reflinked = 0usize;
            let size_usize: usize = src_file_size.try_into()
                .map_err(|_| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "File too large")))?;
            let mut success = true;
            if src_file_size > 0 {
                let mut off_in = 0i64;
                let mut off_out = 0i64;
                while total_reflinked < size_usize {
                    let remaining = size_usize - total_reflinked;
                    let chunk = std::cmp::min(remaining, 1024 * 1024 * 1024);
                    let ret = unsafe { libc::copy_file_range(sfd, &mut off_in, dfd, &mut off_out, chunk, 0) };
                    if ret < 0 {
                        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                        if errno == libc::EXDEV {
                        } else {
                            warn!("Reflink failed for {:?}, disabling opt. Errno: {}", dst, errno);
                            reflink_ok.store(false, Ordering::Relaxed);
                        }
                        success = false;
                        break;
                    } else if ret == 0 {
                        break;
                    }
                    total_reflinked += ret as usize;
                }
            }
            if success && total_reflinked == size_usize {
                transfer_done = true;
                stats.bytes_processed = src_file_size;
                stats.io_duration = start.elapsed();
                let is_network_fs = match statfs::statfs(dst) {
                    Ok(s) => {
                        let magic = s.filesystem_type().0 as i64;
                        magic == NFS_SUPER_MAGIC || magic == SMB_SUPER_MAGIC || magic == CIFS_MAGIC_NUMBER
                    },
                    Err(_) => false,
                };
                if is_network_fs { metrics::COPY_METHOD_OFFLOAD.inc(); }
                else { metrics::COPY_METHOD_REFLINK.inc(); }
            } else {
                unsafe {
                    libc::lseek(dfd, 0, libc::SEEK_SET);
                    libc::lseek(sfd, 0, libc::SEEK_SET);
                };
            }
        }
        if !transfer_done {
            if is_full_replace {
                metrics::COPY_METHOD_STANDARD.inc();
                let sfd_raw = sfd;
                let dfd_raw = dfd;
                let (bytes_copied, duration) = spawn_blocking(move || {
                    let mut f_in = unsafe { File::from_raw_fd(sfd_raw) };
                    let mut f_out = unsafe { File::from_raw_fd(dfd_raw) };
                    let start = Instant::now();
                    let res = f_in.seek(SeekFrom::Start(0))
                        .and_then(|_| io::copy(&mut f_in, &mut f_out));
                    let dur = start.elapsed();
                    let _ = f_in.into_raw_fd();
                    let _ = f_out.sync_all();
                    let _ = f_out.into_raw_fd();
                    res.map(|bytes| (bytes, dur))
                }).await.unwrap_or_else(|e| Err(io::Error::new(io::ErrorKind::Other, e.to_string())))?;
                if bytes_copied < src_file_size {
                     let current_len = std::fs::metadata(src)?.len();
                     if bytes_copied != current_len {
                         unsafe { libc::close(dfd) };
                         return Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Short copy")));
                     }
                }
                stats.bytes_processed = bytes_copied;
                stats.io_duration = duration;
            } else {
                metrics::COPY_METHOD_STANDARD.inc();
                let use_uncached = !use_direct_io_for_delta && length >= 4096;
                stats = Self::perform_delta_uring(ring, buf, sfd, dfd, offset, length, vdo_opt, use_uncached).await?;
            }
        }
        
        // IMPROVEMENT: Force fsync on the new file's descriptor and check result before rename
        let sync_res = unsafe { libc::fsync(dfd) };
        if sync_res != 0 {
            let err = io::Error::last_os_error();
            error!("Critical: fsync failed before atomic rename: {}", err);
            unsafe { libc::close(dfd); }
            return Err(FoxingError::Io(err));
        }

        unsafe {
            libc::close(dfd);
        }

        if is_full_replace {
            // Add a memory barrier before rename to ensure previous operations are visible.
            fence(Ordering::SeqCst); 

            if let Err(e) = std::fs::rename(&target_path, dst) {
                error!("Atomic Rename Failed {:?} -> {:?}: {}", target_path, dst, e);
                return Err(FoxingError::Io(e));
            }
            if let Some(g) = &mut cleanup_guard {
                g.disarm();
            }
        }
        Ok(stats)
    }
    async fn perform_delta_uring(
        ring: &mut IoUring,
        buf: &mut AlignedBuffer,
        sfd: i32,
        dfd: i32,
        offset: u64,
        length: u64,
        vdo_opt: bool,
        use_uncached_io: bool,
    ) -> Result<CopyStats> {
        let mut current_offset = offset;
        let end_offset = offset + length;
        let max_chunk = buf.capacity() as u64;
        let mut stats = CopyStats::default();
        let start_time = Instant::now();
        let rw_flags_val: i32 = if use_uncached_io { RWF_UNCACHED } else { 0 };
        let mut uncached_supported = true;
        while current_offset < end_offset {
            let rlen = std::cmp::min(end_offset - current_offset, max_chunk) as usize;
            let current_flags = if uncached_supported { rw_flags_val } else { 0 };
            let mut r_op = opcode::ReadFixed::new(
                types::Fd(sfd),
                unsafe { buf.capacity_slice_mut() }.as_mut_ptr(),
                rlen as u32,
                0,
            )
            .offset(current_offset)
            .rw_flags(current_flags)
            .build()
            .user_data(current_offset);
            loop {
                let pushed = unsafe { ring.submission().push(&r_op).is_ok() };
                if pushed {
                    break;
                }
                let _ = ring.submit_and_wait(1);
            }
            ring.submit_and_wait(1)?;
            let cqe = ring.completion().next()
                .ok_or_else(|| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "No CQE")))?;
            let mut bytes_read = cqe.result();
            if bytes_read < 0 {
                let err = -bytes_read;
                if uncached_supported && (err == libc::EOPNOTSUPP || err == libc::EINVAL) {
                    uncached_supported = false;
                    warn!("RWF_UNCACHED not supported. Downgrading I/O strategy.");
                     r_op = opcode::ReadFixed::new(
                        types::Fd(sfd),
                        unsafe { buf.capacity_slice_mut() }.as_mut_ptr(),
                        rlen as u32,
                        0,
                    )
                    .offset(current_offset)
                    .rw_flags(0)
                    .build()
                    .user_data(current_offset);
                    loop {
                        let pushed = unsafe { ring.submission().push(&r_op).is_ok() };
                        if pushed {
                            break;
                        }
                        let _ = ring.submit_and_wait(1);
                    }
                    ring.submit_and_wait(1)?;
                    let retry_cqe = ring.completion().next().ok_or_else(|| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "No CQE on retry")))?;
                    bytes_read = retry_cqe.result();
                    if bytes_read < 0 {
                         return Err(FoxingError::Io(io::Error::from_raw_os_error(-bytes_read)));
                    }
                } else {
                    return Err(FoxingError::Io(io::Error::from_raw_os_error(err)));
                }
            }
            let bytes_read_usize = bytes_read as usize;
            if bytes_read_usize == 0 {
                break;
            }
            let data_slice = unsafe { &buf.capacity_slice_mut()[0..bytes_read_usize] };
            let is_zero_block = vdo_opt && Self::is_block_zero(data_slice);
            if is_zero_block {
                stats.bytes_zeros += bytes_read_usize as u64;
            } else {
                 let current_flags_w = if uncached_supported { rw_flags_val } else { 0 };
                 let mut w_op = opcode::WriteFixed::new(
                    types::Fd(dfd),
                    data_slice.as_ptr(),
                    bytes_read_usize as u32,
                    0,
                )
                .offset(current_offset)
                .rw_flags(current_flags_w)
                .build()
                .user_data(current_offset | (1<<63));
                loop {
                    let pushed = unsafe { ring.submission().push(&w_op).is_ok() };
                    if pushed {
                        break;
                    }
                    let _ = ring.submit_and_wait(1);
                }
                ring.submit_and_wait(1)?;
                let cqe_w = ring.completion().next()
                     .ok_or_else(|| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "No Write CQE")))?;
                let mut res = cqe_w.result();
                if res < 0 {
                    let err = -res;
                    if uncached_supported && (err == libc::EOPNOTSUPP || err == libc::EINVAL) {
                        uncached_supported = false;
                        w_op = opcode::WriteFixed::new(
                            types::Fd(dfd),
                            data_slice.as_ptr(),
                            bytes_read_usize as u32,
                            0,
                        )
                        .offset(current_offset)
                        .rw_flags(0)
                        .build()
                        .user_data(current_offset | (1<<63));
                         loop {
                            let pushed = unsafe { ring.submission().push(&w_op).is_ok() };
                            if pushed {
                                break;
                            }
                            let _ = ring.submit_and_wait(1);
                        }
                        ring.submit_and_wait(1)?;
                        let retry_cqe = ring.completion().next().ok_or_else(|| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "No CQE on Write retry")))?;
                        res = retry_cqe.result();
                        if res < 0 {
                            return Err(FoxingError::Io(io::Error::from_raw_os_error(-res)));
                        }
                    } else {
                        return Err(FoxingError::Io(io::Error::from_raw_os_error(err)));
                    }
                }
            }
            stats.bytes_processed += bytes_read_usize as u64;
            current_offset += bytes_read_usize as u64;
        }
        stats.io_duration = start_time.elapsed();
        Ok(stats)
    }
    fn is_block_zero(buf: &[u8]) -> bool {
        let (prefix, chunks, suffix) = unsafe { buf.align_to::<u128>() };
        chunks.iter().all(|&x| x == 0) && prefix.iter().all(|&x| x == 0) && suffix.iter().all(|&x| x == 0)
    }
}
