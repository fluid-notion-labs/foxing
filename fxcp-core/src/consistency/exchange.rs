// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/consistency/exchange.rs — Atomic exchange operations for safe file replacement

use std::path::Path;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::io;
use libc;

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

/// Attempts to atomically exchange the contents of `temp` and `target` using XFS_IOC_EXCHANGE_RANGE.
/// This preserves the inode number and hardlinks of `target`.
pub fn atomic_exchange(temp: &Path, target: &Path) -> io::Result<()> {
    let temp_file = File::open(temp)?;
    let target_file = File::open(target)?;
    
    let args = xfs_exchange_range {
        file1_fd: temp_file.as_raw_fd(),
        pad: 0,
        file1_offset: 0,
        file2_offset: 0,
        length: 0, // 0 with TO_EOF implies full file exchange
        flags: XFS_EXCHANGE_RANGE_TO_EOF,
        pad2: [0; 2],
    };

    let ret = unsafe {
        libc::ioctl(target_file.as_raw_fd(), XFS_IOC_EXCHANGE_RANGE, &args)
    };

    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
