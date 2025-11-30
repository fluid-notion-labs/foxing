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

use crate::buffer::AlignedBuffer;
use crate::error::{FoxingError, Result};
use crate::security; 
use crate::metrics;

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

// RAII Guard to ensure .tmp files are deleted if the operation fails/panics
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
                // It's expected to fail if the file was never created or already moved,
                // but we log just in case.
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
    ) -> Result<CopyStats> {
        let sf = File::open(src)?;
        let src_file_size = sf.metadata()?.len();
        let sfd = sf.as_raw_fd();

        let is_full_replace = offset == 0 && length == src_file_size && src_file_size > 0;

        let (target_path, open_flags) = if is_full_replace {
            (
                dst.with_extension(format!("tmp.{}", Uuid::new_v4())),
                libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC,
            )
        } else {
            (dst.to_path_buf(), libc::O_RDWR)
        };

        // Initialize cleanup guard. If we return Err at any point, this will delete target_path.
        let mut cleanup_guard = if is_full_replace {
            Some(TmpFileGuard::new(target_path.clone()))
        } else {
            None
        };

        let use_direct_io = direct_io_ok && is_full_replace && length >= 4096 && (length % 4096 == 0);
        let use_uncached_io = !use_direct_io && length >= 4096;
        
        let final_flags = if use_direct_io {
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

        // 1. Attempt Reflink / Server-Side Copy
        if is_full_replace && reflink_ok.load(Ordering::Relaxed) {
            let mut off_in = 0i64;
            let mut off_out = 0i64;
            
            // Measure Syscall Duration Only
            let start = Instant::now();
            let ret = unsafe { 
                libc::copy_file_range(sfd, &mut off_in, dfd, &mut off_out, src_file_size as usize, 0) 
            };
            let duration = start.elapsed();

            if ret > 0 && ret == src_file_size as isize {
                transfer_done = true;
                stats.bytes_processed = src_file_size;
                stats.io_duration = duration;
                
                let is_network_fs = match statfs::statfs(&target_path) {
                    Ok(s) => {
                        let magic = s.filesystem_type().0 as i64;
                        magic == NFS_SUPER_MAGIC || magic == SMB_SUPER_MAGIC || magic == CIFS_MAGIC_NUMBER
                    },
                    Err(_) => false,
                };

                if is_network_fs {
                    metrics::COPY_METHOD_OFFLOAD.inc();
                } else {
                    metrics::COPY_METHOD_REFLINK.inc();
                }
                debug!("Reflink success for {:?}", dst);

            } else if ret < 0 {
                let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                if errno != libc::EXDEV {
                    warn!("Reflink failed for {:?}, disabling optimization. Errno: {}", dst, errno);
                    reflink_ok.store(false, Ordering::Relaxed);
                }
            }
        }

        // 2. Fallback: io_uring / Uncached
        if !transfer_done {
            if is_full_replace {
                // Full Sync Copy via Standard IO (Blocking)
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

                if bytes_copied != src_file_size {
                    unsafe { libc::close(dfd) };
                    return Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Short copy")));
                }
                stats.bytes_processed = src_file_size;
                stats.io_duration = duration;
                debug!("Standard copy success for {:?}", dst);
            } else {
                // Delta Update
                metrics::COPY_METHOD_STANDARD.inc();
                stats = Self::perform_delta_uring(ring, buf, sfd, dfd, offset, length, vdo_opt, use_uncached_io).await?;
            }
        }

        // Safe Close
        unsafe { 
            libc::fsync(dfd);
            libc::close(dfd);
        }

        if is_full_replace {
            // Atomic Rename
            debug!("Atomic Commit: Renaming {:?} -> {:?}", target_path, dst);
            if let Err(e) = std::fs::rename(&target_path, dst) {
                error!("Atomic Rename Failed {:?} -> {:?}: {}", target_path, dst, e);
                return Err(FoxingError::Io(e));
            }
            // Success! Disarm the guard so we don't delete the file we just moved.
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

        while current_offset < end_offset {
            let rlen = std::cmp::min(end_offset - current_offset, max_chunk) as usize;

            let r_op = opcode::ReadFixed::new(
                types::Fd(sfd),
                unsafe { buf.capacity_slice_mut() }.as_mut_ptr(),
                rlen as u32,
                0,
            )
            .offset(current_offset)
            .build()
            .user_data(current_offset);

            unsafe {
                ring.submission()
                    .push(&r_op)
                    .map_err(|_| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "SQ Full")))?;
            }
            ring.submit_and_wait(1)?;

            let cqe = ring.completion().next()
                .ok_or_else(|| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "No CQE")))?;
            
            if cqe.result() < 0 {
                return Err(FoxingError::Io(io::Error::from_raw_os_error(-cqe.result())));
            }

            let data_slice = unsafe { &buf.capacity_slice_mut()[0..rlen] };
            
            let is_zero_block = vdo_opt && Self::is_block_zero(data_slice);

            if is_zero_block {
                stats.bytes_zeros += rlen as u64;
            } else {
                if use_uncached_io {
                    let data_vec = data_slice.to_vec();
                    spawn_blocking(move || {
                        Self::do_uncached_write(dfd, &data_vec, current_offset)
                    }).await.unwrap_or_else(|e| Err(io::Error::new(io::ErrorKind::Other, e.to_string())))?;
                } else {
                     let w_op = opcode::WriteFixed::new(
                        types::Fd(dfd),
                        data_slice.as_ptr(),
                        rlen as u32,
                        0,
                    )
                    .offset(current_offset)
                    .build()
                    .user_data(current_offset | (1<<63));

                    unsafe {
                        ring.submission()
                            .push(&w_op)
                            .map_err(|_| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "SQ Full")))?;
                    }
                    ring.submit_and_wait(1)?;
                    
                    let cqe_w = ring.completion().next()
                         .ok_or_else(|| FoxingError::Io(io::Error::new(io::ErrorKind::Other, "No Write CQE")))?;
                    if cqe_w.result() < 0 {
                        return Err(FoxingError::Io(io::Error::from_raw_os_error(-cqe_w.result())));
                    }
                }
            }
            stats.bytes_processed += rlen as u64;
            current_offset += rlen as u64;
        }
        
        stats.io_duration = start_time.elapsed();
        Ok(stats)
    }

    fn is_block_zero(buf: &[u8]) -> bool {
        let (prefix, chunks, suffix) = unsafe { buf.align_to::<u128>() };
        chunks.iter().all(|&x| x == 0) && prefix.iter().all(|&x| x == 0) && suffix.iter().all(|&x| x == 0)
    }

    fn do_uncached_write(fd: i32, buf: &[u8], offset: u64) -> io::Result<isize> {
        let iov = libc::iovec {
            iov_base: buf.as_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        let ret = unsafe {
            libc::syscall(libc::SYS_pwritev2, fd, &iov, 1, offset, RWF_UNCACHED)
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(ret as isize)
        }
    }
}
