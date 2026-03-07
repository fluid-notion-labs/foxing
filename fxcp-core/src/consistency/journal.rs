// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/consistency/journal.rs — Event journal for durable operation tracking

//! Durable event journal for recording filesystem operations.

use std::path::{Path};
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::io;
use tracing::{debug, warn};
use libc;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::process::Command;

const XFS_IOC_EXCHANGE_RANGE: u64 = 0xC0385828;
const XFS_EXCHANGE_RANGE_TO_EOF: u64 = 1 << 0;
#[repr(C)]
struct xfs_exchange_range {
    file1_fd: i32,
    pad: i32,
    file1_offset: u64,
    file2_offset: u64,
    length: u64,
    flags: u64,
    pad2: [u64; 2],
}
pub fn atomic_commit(temp_path: &Path, target_path: &Path) -> io::Result<()> {
    if let (Ok(temp_file), Ok(target_file)) = (File::open(temp_path), File::open(target_path)) {
        let temp_fd = temp_file.as_raw_fd();
        let target_fd = target_file.as_raw_fd();
        let args = xfs_exchange_range {
            file1_fd: temp_fd,
            pad: 0,
            file1_offset: 0,
            file2_offset: 0,
            length: 0,
            flags: XFS_EXCHANGE_RANGE_TO_EOF,
            pad2: [0; 2],
        };
        let ret = unsafe {
            libc::ioctl(target_fd, XFS_IOC_EXCHANGE_RANGE, &args)
        };
        if ret == 0 {
            debug!("Atomic Commit: XFS Exchange Range successful for {:?}", target_path);
            let _ = std::fs::remove_file(temp_path);
            return Ok(());
        } else {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EOPNOTSUPP) | Some(libc::ENOTTY) | Some(libc::EINVAL) => {
                    debug!("Atomic Commit: IOCTL failed ({}), falling back to rename.", err);
                },
                _ => {
                    warn!("Atomic Commit: Unexpected IOCTL error: {}. Falling back to rename.", err);
                }
            }
        }
    }
    atomic_rename(temp_path, target_path, None)
}
pub fn atomic_rename(src: &Path, dst: &Path, flags: Option<u32>) -> io::Result<()> {
    if let Some(parent) = dst.parent() {
        if !parent.exists() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
    let flags = flags.unwrap_or(0);
    
    // Check if we need advanced syscalls
    if flags & (libc::RENAME_EXCHANGE | libc::RENAME_NOREPLACE) != 0 {
        let src_c = CString::new(src.as_os_str().as_bytes())?;
        let dst_c = CString::new(dst.as_os_str().as_bytes())?;
        let ret = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                src_c.as_ptr(),
                libc::AT_FDCWD,
                dst_c.as_ptr(),
                flags
            )
        };
        
        if ret == 0 {
            return Ok(());
        } else {
            let err = io::Error::last_os_error();
            let errno = err.raw_os_error().unwrap_or(0);

            // Handle RENAME_EXCHANGE Failure
            if (flags & libc::RENAME_EXCHANGE) != 0 {
                // If supported failed or logical error, attempt fallback
                debug!("Atomic Commit: RENAME_EXCHANGE failed ({}), attempting userspace fallback.", err);

                // Fallback Logic
                let temp_swap_path = dst.with_extension("exchange_bak");
                
                // Try Reflink Snapshot first (Safer)
                let reflink_status = Command::new("cp")
                    .arg("--reflink=always")
                    .arg(dst)
                    .arg(&temp_swap_path)
                    .status();

                let reflink_success = reflink_status.map(|s| s.success()).unwrap_or(false);

                if reflink_success {
                    // Step 3: Reflink succeeded
                    // 1. Rename src -> dst (overwrite dst, we have backup)
                    if let Err(e) = std::fs::rename(src, dst) {
                        // Rollback attempt: remove temp
                        let _ = std::fs::remove_file(&temp_swap_path);
                        return Err(e);
                    }
                    // 2. Rename temp -> src (complete swap)
                    if let Err(e) = std::fs::rename(&temp_swap_path, src) {
                        warn!("Atomic Swap (Reflink): Failed to move temp back to src: {}. State inconsistent.", e);
                        return Err(e);
                    }
                    return Ok(());
                } else {
                    // Step 4: Reflink failed, try Rename Dance
                    warn!("Atomic Swap: Reflink fallback failed, attempting standard rename dance.");
                    // 1. dst -> temp
                    if let Err(e) = std::fs::rename(dst, &temp_swap_path) {
                         if e.kind() == io::ErrorKind::NotFound {
                             // Destination doesn't exist, proceed to simple rename src->dst at end of function
                         } else {
                             return Err(e); 
                         }
                    } else {
                        // 2. src -> dst
                        if let Err(e) = std::fs::rename(src, dst) {
                            // Rollback: temp -> dst
                            let _ = std::fs::rename(&temp_swap_path, dst);
                            return Err(e);
                        }
                        // 3. temp -> src
                        if let Err(e) = std::fs::rename(&temp_swap_path, src) {
                            warn!("Atomic Swap (Dance): Failed to move temp to src: {}.", e);
                            return Err(e);
                        }
                        return Ok(());
                    }
                }
            } else {
                // Handle RENAME_NOREPLACE Failure
                if (flags & libc::RENAME_NOREPLACE) != 0 {
                    // If target exists, NOREPLACE *should* fail. Do not fallback to overwrite.
                    if errno == libc::EEXIST {
                        return Err(err);
                    }
                }
                
                // If failure is due to lack of support, fall through to std::fs::rename.
                if errno == libc::EINVAL || errno == libc::EOPNOTSUPP || errno == libc::ENOSYS {
                    debug!("atomic_rename: Advanced flags ({}) not supported ({}). Falling back to standard rename.", flags, errno);
                    // Fall through to std::fs::rename below
                } else {
                    // Actual IO error (e.g. permission denied), return it.
                    return Err(err);
                }
            }
        }
    }
    
    // Standard POSIX rename (overwrites dst if it exists)
    std::fs::rename(src, dst)
}
