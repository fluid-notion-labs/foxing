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
use crate::buffer::{BufferPool};
use crate::error::{FoxingError, Result};
use crate::security;
use crate::metrics;
use std::sync::atomic::fence;
const RWF_UNCACHED: i32 = 0x00000008;
const NFS_SUPER_MAGIC: i64 = 0x6969;
const SMB_SUPER_MAGIC: i64 = 0x517B;
const CIFS_MAGIC_NUMBER: i64 = 0xFF534D42;
// Use 16-bit markers for index + op type. Max buffers is 65536.
const OP_TYPE_MASK: u64 = 0xFFFF0000;
const INDEX_MASK: u64 = 0x0000FFFF;
const READ_OP: u64 = 1 << 16;
const WRITE_OP: u64 = 2 << 16;
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
struct InflightRead {
    offset: u64,
    buf_index: u16, // Changed to u16
    // Removed field `bytes_expected: usize,` as it is never read (Dead Code warning fix)
}
pub struct SmartCopier;
impl SmartCopier {
    pub async fn copy(
        src: &Path,
        dst: &Path,
        ring: &mut IoUring,
        buffer_pool: &mut BufferPool,
        reflink_ok: &AtomicBool,
        vdo_opt: bool,
        offset: u64,
        length: u64,
        direct_io_ok: bool,
        src_file_size: u64,
        // --- START FIX: Dual Capability Flags ---
        src_rwf_uncached_ok: bool, 
        dst_rwf_uncached_ok: bool,
        // --- END FIX ---
    ) -> Result<CopyStats> {
        if buffer_pool.capacity() == 0 {
             return Err(FoxingError::Io(io::Error::new(io::ErrorKind::InvalidInput, "No buffers provided for copy.")));
        }
        let sf = File::open(src)?;
        let sfd = sf.as_raw_fd();
        let is_full_replace = offset == 0 && length == src_file_size;
        debug!("SmartCopier: Triggering FULL ATOMIC REPLACE for {:?} (Size: {}).", dst, src_file_size);
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
                // Rewritten delta copy using io_uring 0.7.x buffer patterns
                metrics::COPY_METHOD_STANDARD.inc();
                stats = Self::perform_delta_uring_pipelined(ring, sfd, dfd, offset, length, vdo_opt, buffer_pool, src_rwf_uncached_ok, dst_rwf_uncached_ok).await?;
            }
        }
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

    // Helper function to submit a read operation
    fn submit_read(ring: &mut IoUring, sfd: i32, offset: u64, len: usize, buf_idx: u16, buf_ptr: *mut u8, rw_flags_val: i32) -> bool {
        let r_op = opcode::ReadFixed::new(
            types::Fd(sfd),
            buf_ptr,
            len as u32,
            buf_idx,
        )
        .offset(offset)
        .rw_flags(rw_flags_val)
        .build()
        .user_data(READ_OP | buf_idx as u64);
        unsafe { ring.submission().push(&r_op) }.is_ok()
    }

    // Helper function to submit a write operation
    fn submit_write(ring: &mut IoUring, dfd: i32, offset: u64, len: usize, buf_idx: u16, buf_ptr: *mut u8, rw_flags_val: i32) -> bool {
        let w_op = opcode::WriteFixed::new(
            types::Fd(dfd),
            buf_ptr,
            len as u32,
            buf_idx,
        )
        .offset(offset)
        .rw_flags(rw_flags_val)
        .build()
        .user_data(WRITE_OP | buf_idx as u64);
        unsafe { ring.submission().push(&w_op) }.is_ok()
    }

    async fn perform_delta_uring_pipelined(
        ring: &mut IoUring,
        sfd: i32,
        dfd: i32,
        mut current_offset: u64,
        length: u64,
        vdo_opt: bool,
        buffer_pool: &mut BufferPool, // Changed from &mut [AlignedBuffer]
        src_rwf_uncached_ok: bool, // Passed probe result
        dst_rwf_uncached_ok: bool, // Passed probe result
    ) -> Result<CopyStats> {
        let max_sqe = ring.submission().capacity();
        let num_buffers = buffer_pool.capacity();
        let max_concurrent_io = num_buffers.min(max_sqe as usize).min(8) as u32; // Limit concurrency
        let chunk_size = buffer_pool.chunk_size() as u64;
        let end_offset = current_offset + length;
        let start_time = Instant::now();
        
        // --- FIX: Use separate flags for read and write ---
        let rw_flags_val_read: i32 = if src_rwf_uncached_ok { RWF_UNCACHED } else { 0 };
        let rw_flags_val_write: i32 = if dst_rwf_uncached_ok { RWF_UNCACHED } else { 0 };
        // ----------------------------------------------------

        let mut inflight_reads: Vec<InflightRead> = Vec::with_capacity(num_buffers);
        let mut stats = CopyStats::default();
        let mut submit_reads_pending = true; 
        
        while current_offset < end_offset || !inflight_reads.is_empty() || buffer_pool.free_count() < num_buffers {
            
            if submit_reads_pending {
                // 1. Submit Reads (Pipelining)
                while current_offset < end_offset && !buffer_pool.is_empty() && inflight_reads.len() < max_concurrent_io as usize {
                    if let Some(buf_idx) = buffer_pool.acquire() {
                        let rlen = (end_offset - current_offset).min(chunk_size) as usize;
                        let buf_ptr = buffer_pool.get_ptr(buf_idx); // Acquire pointer *before* submitting
                        
                        // Use src_rwf_uncached_ok flag for READ operation
                        if Self::submit_read(ring, sfd, current_offset, rlen, buf_idx, buf_ptr, rw_flags_val_read) {
                            inflight_reads.push(InflightRead {
                                offset: current_offset,
                                buf_index: buf_idx,
                            });
                            current_offset += rlen as u64;
                        } else {
                            // Submission failed (e.g., ring full)
                            buffer_pool.release(buf_idx);
                            submit_reads_pending = false; 
                            break;
                        }
                    } else {
                        // No free buffers available
                        submit_reads_pending = false;
                        break;
                    }
                }
            }

            // 2. Wait for Completion and Process Submissions
            let ops_to_submit = ring.submission().len();
            
            // Only wait if there are active operations or work queued in the SQ
            if ops_to_submit > 0 || !inflight_reads.is_empty() {
                let wait_min = if ops_to_submit > 0 || inflight_reads.len() > 0 { 1 } else { 0 };
                
                let num_completed = ring.submit_and_wait(wait_min)?;
                let mut cqes = Vec::new();
                for cqe in ring.completion().take(num_completed as usize) {
                    cqes.push(cqe);
                }

                // 3. Process Completion Events
                for cqe in cqes {
                    let user_data = cqe.user_data();
                    let res = cqe.result();
                    let buf_idx = (user_data & INDEX_MASK) as u16;
                    let op_type = user_data & OP_TYPE_MASK;

                    if res < 0 {
                        let errno = -res;
                        
                        // Since probes are done at startup, this is a fatal I/O error.
                        buffer_pool.release(buf_idx);
                        return Err(FoxingError::Io(io::Error::from_raw_os_error(errno)));
                    }

                    if op_type == WRITE_OP {
                        // Write completed: release buffer
                        buffer_pool.release(buf_idx);
                        submit_reads_pending = true; // A write completed, try submitting reads again
                    } else if op_type == READ_OP {
                        let bytes_read = res as usize;
                        // Find and remove the inflight read operation
                        let index_in_inflight = inflight_reads.iter().position(|r| r.buf_index == buf_idx);
                        let read_offset = if let Some(index) = index_in_inflight {
                             inflight_reads.remove(index).offset
                        } else {
                            warn!("Received read CQE for untracked buffer index {}. Releasing.", buf_idx);
                            buffer_pool.release(buf_idx); // Release untracked buffer
                            continue;
                        };

                        if bytes_read == 0 {
                            // Empty read: release buffer
                            buffer_pool.release(buf_idx);
                            continue;
                        }

                        // Update the buffer's effective length
                        buffer_pool.set_len(buf_idx, bytes_read);
                        stats.bytes_processed += bytes_read as u64;

                        // Check for zero-block optimization
                        let buf_ptr = buffer_pool.get_ptr(buf_idx); // Re-acquire pointer for zero check/write submission

                        let is_zero_block = vdo_opt && Self::is_block_zero(unsafe {
                            std::slice::from_raw_parts(buf_ptr, bytes_read)
                        });

                        if is_zero_block {
                            stats.bytes_zeros += bytes_read as u64;
                            // Punch hole: release buffer after ioctl
                            let ret = unsafe {
                                libc::fallocate(dfd, libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE, read_offset as i64, bytes_read as i64)
                            };
                            if ret != 0 {
                                warn!("fallocate(PUNCH_HOLE) failed. Skipping write.");
                            }
                            buffer_pool.release(buf_idx);
                        } else {
                            // Submit Write: keep buffer acquired
                            // Use dst_rwf_uncached_ok flag for WRITE operation
                            Self::submit_write(ring, dfd, read_offset, bytes_read, buf_idx, buf_ptr, rw_flags_val_write);
                        }
                    }
                }
            }
            let _ = ring.submit(); // Submit any pending writes/reads
            
            // If nothing is moving, wait briefly
            if ops_to_submit == 0 && inflight_reads.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            if current_offset >= end_offset && buffer_pool.free_count() == num_buffers {
                // All data queued, all inflight I/O completed, all buffers released.
                break;
            }
        }
        stats.io_duration = start_time.elapsed();
        Ok(stats)
    }
    fn is_block_zero(buf: &[u8]) -> bool {
        let (prefix, chunks, suffix) = unsafe { buf.align_to::<u128>() };
        chunks.iter().all(|&x| x == 0) && prefix.iter().all(|&x| x == 0) && suffix.iter().all(|&x| x == 0)
    }
}
