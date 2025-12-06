use std::path::{Path, PathBuf};
use std::fs::File;
use std::os::unix::io::{AsRawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::io;
use std::ffi::CString;
use tokio::task::spawn_blocking;
use io_uring::{opcode, types, IoUring};
use uuid::Uuid;
use libc;
use nix::sys::statfs;
use std::time::{Duration, Instant};
use tracing::{warn, debug, error, info, trace};
use std::convert::TryInto;
use crate::buffer::{BufferPool, AlignedBuffer};
use crate::error::{FoxingError, Result};
use crate::security;
use crate::metrics;
use std::sync::atomic::fence;
use std::os::unix::fs::MetadataExt;

const RWF_UNCACHED: i32 = 0x00000008;
const NFS_SUPER_MAGIC: i64 = 0x6969;
const SMB_SUPER_MAGIC: i64 = 0x517B;
const CIFS_MAGIC_NUMBER: i64 = 0xFF534D42;

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
        debug!("TmpFileGuard: Armed for {:?}", path);
        Self { path, armed: true }
    }
    fn disarm(&mut self) {
        debug!("TmpFileGuard: Disarmed for {:?}", self.path);
        self.armed = false;
    }
}

impl Drop for TmpFileGuard {
    fn drop(&mut self) {
        if self.armed {
            warn!("IO Transaction Failed (Guard Dropped): Rolling back temp file {:?}", self.path);
            if let Err(e) = std::fs::remove_file(&self.path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    error!("CRITICAL: Failed to clean up temp file {:?}: {}", self.path, e);
                }
            }
        }
    }
}

pub struct InflightRead {
    pub offset: u64,
    pub buf_index: u16,
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
        src_rwf_uncached_ok: bool,
        dst_rwf_uncached_ok: bool,
        vdo_stall_threshold: u32,
    ) -> Result<CopyStats> {
        if buffer_pool.capacity() == 0 {
             return Err(FoxingError::Io(io::Error::new(io::ErrorKind::InvalidInput, "No buffers provided for copy.")));
        }

        let sf = File::open(src)?;
        let sfd = sf.as_raw_fd();
        
        let is_full_replace = offset == 0 && length == src_file_size;
        let is_compressed = security::is_filesystem_compressed(src);
        
        let is_sparse_source = if !is_compressed && is_full_replace && src_file_size > 1024 * 1024 {
            let metadata = sf.metadata()?;
            let blocks = metadata.blocks();
            let allocated_size = blocks * 512;
            allocated_size < src_file_size
        } else {
            false
        };

        if is_sparse_source {
            info!("SmartCopier: Detected SPARSE source {:?} (Allocated: {} < Size: {}).", src, 0, src_file_size);
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

        let aligned_io = offset % 4096 == 0;
        let use_direct_io_for_delta = !is_sparse_source && direct_io_ok && !is_full_replace && length >= 4096 && (length % 4096 == 0) && aligned_io;
        
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

        if is_full_replace && !is_sparse_source {
            security::preallocate(dfd, src_file_size);
        }

        let mut transfer_done = false;
        let mut stats = CopyStats::default();

        if is_full_replace && reflink_ok.load(Ordering::Relaxed) {
            let start = Instant::now();
            let mut total_reflinked = 0usize;
            let size_usize: usize = src_file_size.try_into().unwrap_or(usize::MAX);
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
                        if errno != libc::EXDEV {
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
            metrics::COPY_METHOD_STANDARD.inc();
            if is_sparse_source {
                let src_path_debug = src.to_path_buf();
                stats = spawn_blocking(move || {
                    Self::perform_sparse_copy_blocking(sfd, dfd, src_file_size, &src_path_debug)
                }).await.unwrap_or_else(|e| Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))))?;
            } else {
                stats = Self::perform_delta_uring_pipelined(
                    ring, sfd, dfd, offset, length, vdo_opt, buffer_pool,
                    target_path.clone(), src_rwf_uncached_ok, dst_rwf_uncached_ok, vdo_stall_threshold
                ).await?;
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
            let final_len = std::fs::metadata(&target_path)?.len();
            if final_len != src_file_size {
                error!("CRITICAL: Copy size mismatch for {:?}. Source: {}, Target: {}. Data: {}, Zeros: {}.", 
                       target_path, src_file_size, final_len, stats.bytes_processed, stats.bytes_zeros);
                return Err(FoxingError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "Copy Size Mismatch")));
            }

            fence(Ordering::SeqCst);
            debug!("Atomic Rename Start: {:?} -> {:?}", target_path, dst);
            if let Err(e) = std::fs::rename(&target_path, dst) {
                error!("Atomic Rename FAILED {:?} -> {:?}: {}", target_path, dst, e);
                return Err(FoxingError::Io(e));
            }
            debug!("Atomic Rename Success: {:?} -> {:?}", target_path, dst);
            if let Some(g) = &mut cleanup_guard {
                g.disarm();
            }
        }

        Ok(stats)
    }

    fn perform_sparse_copy_blocking(sfd: i32, dfd: i32, total_size: u64, path_debug: &Path) -> Result<CopyStats> {
        let start_time = Instant::now();
        let mut bytes_processed = 0u64;
        let mut offset = 0i64;
        let end_offset = total_size as i64;
        
        if unsafe { libc::ftruncate(dfd, total_size as i64) } < 0 {
             return Err(FoxingError::Io(io::Error::last_os_error()));
        }

        let buf = AlignedBuffer::new(1024 * 1024);
        
        while offset < end_offset {
            let data_pos = unsafe { libc::lseek(sfd, offset, libc::SEEK_DATA) };
            if data_pos < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ENXIO) {
                    break;
                }
                
                warn!("Sparse Copy: SEEK_DATA failed for {:?}: {}. Degrading.", path_debug, err);
                if unsafe { libc::lseek(sfd, offset, libc::SEEK_SET) } < 0 { return Err(FoxingError::Io(io::Error::last_os_error())); }
                if unsafe { libc::lseek(dfd, offset, libc::SEEK_SET) } < 0 { return Err(FoxingError::Io(io::Error::last_os_error())); }
                
                let mut remaining = end_offset - offset;
                while remaining > 0 {
                    let to_read = std::cmp::min(remaining as usize, buf.capacity());
                    let read_res = unsafe { libc::read(sfd, buf.ptr() as *mut libc::c_void, to_read) };
                    if read_res <= 0 { break; }
                    let write_res = unsafe { libc::write(dfd, buf.ptr() as *const libc::c_void, read_res as usize) };
                    if write_res < 0 { return Err(FoxingError::Io(io::Error::last_os_error())); }
                    offset += read_res as i64;
                    remaining -= read_res as i64;
                    bytes_processed += read_res as u64;
                }
                break;
            }

            let hole_pos = unsafe { libc::lseek(sfd, data_pos, libc::SEEK_HOLE) };
            if hole_pos < 0 {
                return Err(FoxingError::Io(io::Error::last_os_error()));
            }

            let mut chunk_start = data_pos;
            let chunk_end = if hole_pos > 0 && hole_pos < end_offset { hole_pos } else { end_offset };

            if unsafe { libc::lseek(sfd, chunk_start, libc::SEEK_SET) } < 0 { return Err(FoxingError::Io(io::Error::last_os_error())); }
            if unsafe { libc::lseek(dfd, chunk_start, libc::SEEK_SET) } < 0 { return Err(FoxingError::Io(io::Error::last_os_error())); }

            while chunk_start < chunk_end {
                let to_read = std::cmp::min((chunk_end - chunk_start) as usize, buf.capacity());
                let read_res = unsafe { libc::read(sfd, buf.ptr() as *mut libc::c_void, to_read) };
                if read_res <= 0 { break; }
                
                let write_res = unsafe { libc::write(dfd, buf.ptr() as *const libc::c_void, read_res as usize) };
                if write_res < 0 { return Err(FoxingError::Io(io::Error::last_os_error())); }
                
                chunk_start += read_res as i64;
                bytes_processed += read_res as u64;
            }
            offset = chunk_end;
        }

        let current_size = unsafe { libc::lseek(dfd, 0, libc::SEEK_END) };
        if current_size != total_size as i64 {
             return Err(FoxingError::Io(io::Error::new(io::ErrorKind::Other, "Sparse fallback size mismatch")));
        }

        info!("Sparse Copy Success: {:?} ({} bytes).", path_debug, bytes_processed);
        Ok(CopyStats {
            bytes_processed,
            bytes_zeros: total_size.saturating_sub(bytes_processed),
            io_duration: start_time.elapsed(),
        })
    }

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
        buffer_pool: &mut BufferPool,
        _target_path: PathBuf,
        src_rwf_uncached_ok: bool,
        dst_rwf_uncached_ok: bool,
        vdo_stall_threshold: u32,
    ) -> Result<CopyStats> {
        let max_sqe = ring.submission().capacity();
        let num_buffers = buffer_pool.capacity();
        let max_concurrent_io = num_buffers.min(max_sqe as usize).min(8) as u32;
        let chunk_size = buffer_pool.chunk_size() as u64;
        let end_offset = current_offset + length;
        let start_time = Instant::now();

        let rw_flags_val_read: i32 = if src_rwf_uncached_ok { RWF_UNCACHED } else { 0 };
        let rw_flags_val_write: i32 = if dst_rwf_uncached_ok { RWF_UNCACHED } else { 0 };

        let mut inflight_reads: Vec<InflightRead> = Vec::with_capacity(num_buffers);
        let mut stats = CopyStats::default();
        let mut submit_reads_pending = true;
        
        let mut consecutive_zero_blocks: u32 = 0;
        let mut total_zero_blocks: u64 = 0;
        let mut skip_vdo_opt_temp = false;
        
        let mut loop_iterations = 0u64;

        while current_offset < end_offset || !inflight_reads.is_empty() || buffer_pool.free_count() < num_buffers {
            loop_iterations += 1;
            
            if submit_reads_pending {
                let mut loop_blocked = true;
                while current_offset < end_offset && !buffer_pool.is_empty() && inflight_reads.len() < max_concurrent_io as usize {
                    if ring.submission().is_full() { break; }
                    if let Some(buf_idx) = buffer_pool.acquire() {
                        loop_blocked = false;
                        let rlen = (end_offset - current_offset).min(chunk_size) as usize;
                        let buf_ptr = buffer_pool.get_ptr(buf_idx);
                        if Self::submit_read(ring, sfd, current_offset, rlen, buf_idx, buf_ptr, rw_flags_val_read) {
                            inflight_reads.push(InflightRead { offset: current_offset, buf_index: buf_idx });
                            current_offset += rlen as u64;
                        } else {
                            buffer_pool.release(buf_idx);
                            break;
                        }
                    } else { break; }
                }
                if loop_blocked && inflight_reads.is_empty() { std::thread::sleep(Duration::from_millis(1)); }
            }

            let ops_to_submit = ring.submission().len();
            let buffers_busy = buffer_pool.free_count() < num_buffers;

            if ops_to_submit > 0 || buffers_busy {
                let wait_min = if buffers_busy { 1 } else { 0 };
                let num_completed = ring.submit_and_wait(wait_min)?;
                
                let mut cqes = Vec::new();
                for cqe in ring.completion().take(num_completed as usize) { cqes.push(cqe); }
                for cqe in ring.completion() { cqes.push(cqe); }

                for cqe in cqes {
                    let user_data = cqe.user_data();
                    let res = cqe.result();
                    let buf_idx = (user_data & INDEX_MASK) as u16;
                    let op_type = user_data & OP_TYPE_MASK;

                    if res < 0 {
                        let errno = -res;
                        buffer_pool.release(buf_idx);
                        return Err(FoxingError::Io(io::Error::from_raw_os_error(errno)));
                    }

                    if op_type == WRITE_OP {
                        buffer_pool.release(buf_idx);
                        submit_reads_pending = true;
                        consecutive_zero_blocks = 0;
                        if skip_vdo_opt_temp {
                            skip_vdo_opt_temp = false;
                            trace!("VDO optimization re-enabled after non-zero write.");
                        }
                    } else if op_type == READ_OP {
                        let bytes_read = res as usize;
                        let index_in_inflight = inflight_reads.iter().position(|r| r.buf_index == buf_idx);
                        let read_offset = if let Some(index) = index_in_inflight { inflight_reads.remove(index).offset } else {
                            buffer_pool.release(buf_idx); continue;
                        };

                        if bytes_read == 0 { buffer_pool.release(buf_idx); continue; }

                        buffer_pool.set_len(buf_idx, bytes_read);
                        stats.bytes_processed += bytes_read as u64;
                        let buf_ptr = buffer_pool.get_ptr(buf_idx);
                        
                        let is_zero_block = vdo_opt && Self::is_block_zero(unsafe { std::slice::from_raw_parts(buf_ptr, bytes_read) });

                        let total_blocks_so_far = (stats.bytes_processed + stats.bytes_zeros) / chunk_size;
                        let is_mostly_zeros = if total_blocks_so_far > 0 {
                            (total_zero_blocks as f64 / total_blocks_so_far as f64) > 0.95
                        } else { false };

                        if consecutive_zero_blocks >= vdo_stall_threshold {
                             if !skip_vdo_opt_temp && !is_mostly_zeros {
                                 warn!("VDO stall hit ({} consecutive zeros). Writing zeros to pipeline.", vdo_stall_threshold);
                                 skip_vdo_opt_temp = true;
                             }
                        }

                        if is_zero_block && !skip_vdo_opt_temp {
                            stats.bytes_zeros += bytes_read as u64;
                            consecutive_zero_blocks += 1;
                            total_zero_blocks += 1;
                            
                            let ret = unsafe { libc::fallocate(dfd, libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE, read_offset as i64, bytes_read as i64) };
                            if ret != 0 {
                                skip_vdo_opt_temp = true;
                            }
                            buffer_pool.release(buf_idx);
                            submit_reads_pending = true;
                        } else {
                            if is_zero_block && skip_vdo_opt_temp {
                                consecutive_zero_blocks = 0;
                            }
                            if !Self::submit_write(ring, dfd, read_offset, bytes_read, buf_idx, buf_ptr, rw_flags_val_write) {
                                buffer_pool.release(buf_idx);
                                return Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Ring submission failure")));
                            }
                        }
                    }
                }
            } else {
                if current_offset < end_offset {
                     // Pipeline stalled logic
                }
                let _ = ring.submit();
            }
            if loop_iterations % 1000 == 0 { tokio::task::yield_now().await; }
            if current_offset >= end_offset && buffer_pool.free_count() == num_buffers { break; }
        }
        
        stats.io_duration = start_time.elapsed();
        Ok(stats)
    }

    fn is_block_zero(buf: &[u8]) -> bool {
        let (prefix, chunks, suffix) = unsafe { buf.align_to::<u128>() };
        chunks.iter().all(|&x| x == 0) && prefix.iter().all(|&x| x == 0) && suffix.iter().all(|&x| x == 0)
    }
}
