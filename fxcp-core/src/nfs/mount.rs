// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/nfs/mount.rs — NFS mount discovery and file handle extraction

//! Parses `/proc/self/mountinfo` to identify NFSv4.2 mounts and extracts
//! opaque NFS file handles via `name_to_handle_at(2)` for use in userspace
//! compound RPCs.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

/// Describes an NFS bypass-capable mount point.
#[derive(Debug, Clone)]
pub struct NfsBypassInfo {
    /// NFS server address (IP:port, default port 2049).
    pub server_addr: SocketAddr,
    /// NFS export path on the server (e.g., "/vol/data").
    pub export_path: String,
    /// Local mount point path.
    pub mount_point: PathBuf,
    /// Whether NFSv4.2 was confirmed in mount options.
    pub v42_confirmed: bool,
}

/// Probe whether `path` resides on an NFSv4.2 mount eligible for compound RPC bypass.
///
/// Parses `/proc/self/mountinfo` to find the NFS mount covering `path`, extracts
/// the server address and export path, and checks mount options for `vers=4.2`.
///
/// Returns `None` if:
/// - Path is not on an NFS mount
/// - NFS version is not 4.2
/// - Kerberos auth is required (`sec=krb5`)
/// - Server address cannot be resolved
pub fn probe_nfs_bypass(path: &Path) -> Option<NfsBypassInfo> {
    let canonical = path.canonicalize().ok()?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;

    let mut best_match: Option<(usize, String, String, String)> = None; // (len, mount_point, source, options)

    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 10 { continue; }

        let mount_point = fields[4];

        // Find the " - " separator
        let sep_pos = match fields.iter().position(|&f| f == "-") {
            Some(p) => p,
            None => continue,
        };
        if sep_pos + 3 > fields.len() { continue; }

        let fs_type = fields[sep_pos + 1];
        let source = fields[sep_pos + 2];
        let super_opts = if sep_pos + 3 < fields.len() { fields[sep_pos + 3] } else { "" };

        // Only NFS mounts
        if fs_type != "nfs" && fs_type != "nfs4" { continue; }

        // Check if this mount covers our path (longest prefix match)
        let mount_path = PathBuf::from(mount_point);
        if canonical.starts_with(&mount_path) {
            let mlen = mount_point.len();
            let is_better = match &best_match {
                Some((best_len, ..)) => mlen > *best_len,
                None => true,
            };
            if is_better {
                best_match = Some((mlen, mount_point.to_string(), source.to_string(), super_opts.to_string()));
            }
        }
    }

    let (_, mount_point, source, super_opts) = best_match?;

    // Check for Kerberos — bypass requires AUTH_SYS
    if super_opts.contains("sec=krb5") || super_opts.contains("sec=krb5i") || super_opts.contains("sec=krb5p") {
        debug!("NFS bypass: Kerberos auth detected on {} — bypass unavailable", mount_point);
        return None;
    }

    // Check NFS version — must be 4.2
    let v42 = super_opts.contains("vers=4.2") || super_opts.contains("nfsvers=4.2")
        || super_opts.contains("vers=4,2") || super_opts.contains("minorversion=2");
    if !v42 {
        debug!("NFS bypass: mount {} is not NFSv4.2 (opts: {})", mount_point, super_opts);
        return None;
    }

    // Parse server:export from source field (format: "server:/export" or "server:/export/path")
    let (server_str, export_path) = match source.split_once(':') {
        Some((s, e)) => (s.to_string(), e.to_string()),
        None => {
            debug!("NFS bypass: cannot parse source '{}' for {}", source, mount_point);
            return None;
        }
    };

    // Check for port override in mount options
    let port: u16 = super_opts.split(',')
        .find_map(|opt| {
            let (k, v) = opt.split_once('=')?;
            if k == "port" { v.parse().ok() } else { None }
        })
        .unwrap_or(2049);

    // Resolve server address
    let addr_str = format!("{}:{}", server_str, port);
    let server_addr = match addr_str.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => {
                warn!("NFS bypass: no addresses resolved for {}", addr_str);
                return None;
            }
        },
        Err(e) => {
            warn!("NFS bypass: cannot resolve {}: {}", addr_str, e);
            return None;
        }
    };

    debug!("NFS bypass: detected v4.2 mount {} → {}:{} export={}",
           mount_point, server_addr, port, export_path);

    Some(NfsBypassInfo {
        server_addr,
        export_path,
        mount_point: PathBuf::from(mount_point),
        v42_confirmed: v42,
    })
}

/// Get the mount ID for a path from `/proc/self/mountinfo`.
///
/// Each mount gets a unique ID. After lazy unmount + remount, the mount ID
/// changes even if the device ID stays the same. This detects NFS remounts
/// that `metadata().dev()` misses.
pub fn get_mount_id(path: &Path) -> Option<u64> {
    let canonical = path.canonicalize().ok()?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut best: Option<(usize, u64)> = None;
    for line in mountinfo.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 5 { continue; }
        let mount_id: u64 = fields[0].parse().ok()?;
        let mount_point = fields[4];
        let mp = PathBuf::from(mount_point);
        if canonical.starts_with(&mp) {
            let len = mount_point.len();
            if best.map_or(true, |(best_len, _)| len > best_len) {
                best = Some((len, mount_id));
            }
        }
    }
    best.map(|(_, id)| id)
}

/// Maximum NFS file handle size (kernel constant).
const MAX_HANDLE_SZ: usize = 128;

/// Resolve a directory path to an opaque NFS file handle using `name_to_handle_at(2)`.
///
/// The kernel NFS client stores the server-side file handle in its inode cache.
/// This syscall extracts it without requiring userspace LOOKUP RPCs.
///
/// The returned bytes are the raw NFSv4 filehandle suitable for PUTFH operations.
pub fn resolve_nfs_handle(dir_path: &Path) -> std::io::Result<Vec<u8>> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path_cstr = CString::new(dir_path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    // struct file_handle layout: u32 handle_bytes, i32 handle_type, u8 f_handle[MAX_HANDLE_SZ]
    #[repr(C)]
    struct FileHandleBuf {
        handle_bytes: u32,
        handle_type: i32,
        f_handle: [u8; MAX_HANDLE_SZ],
    }

    let mut fh = FileHandleBuf {
        handle_bytes: MAX_HANDLE_SZ as u32,
        handle_type: 0,
        f_handle: [0u8; MAX_HANDLE_SZ],
    };
    let mut mount_id: libc::c_int = 0;

    let ret = unsafe {
        libc::syscall(
            libc::SYS_name_to_handle_at,
            libc::AT_FDCWD,
            path_cstr.as_ptr(),
            &mut fh as *mut FileHandleBuf,
            &mut mount_id as *mut libc::c_int,
            0 as libc::c_int, // flags
        )
    };

    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }

    let handle_len = fh.handle_bytes as usize;
    if handle_len > MAX_HANDLE_SZ {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("NFS handle too large: {} > {}", handle_len, MAX_HANDLE_SZ),
        ));
    }

    let raw = &fh.f_handle[..handle_len];

    // For NFS CLIENT mounts (handle_type >= 2), the kernel stores:
    //   [0..4]   fileid (inode, LE u32)
    //   [4..8]   generation (LE u32)
    //   [8..14]  internal metadata (size fields, padding)
    //   [14..14+N] the actual NFS wire protocol filehandle
    //   [14+N..] padding zeros
    //
    // The wire filehandle is what the NFS server gave during mount.
    // We extract it by finding the non-zero payload after the 14-byte header,
    // trimming trailing zeros.
    if fh.handle_type >= 2 && handle_len > 14 {
        let wire_region = &raw[14..];
        // Trim trailing zero padding
        let wire_len = wire_region.iter().rposition(|&b| b != 0)
            .map(|p| p + 1)
            .unwrap_or(0);
        if wire_len > 0 {
            return Ok(wire_region[..wire_len].to_vec());
        }
    }

    // Fallback: return full handle
    Ok(raw.to_vec())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_parse_nfs_source() {
        // Test source parsing
        let source = "server.example.com:/vol/data";
        let (server, export) = source.split_once(':').unwrap();
        assert_eq!(server, "server.example.com");
        assert_eq!(export, "/vol/data");
    }

    #[test]
    fn test_kerberos_detection() {
        let opts = "rw,vers=4.2,sec=krb5p,rsize=1048576";
        assert!(opts.contains("sec=krb5"));
    }

    #[test]
    fn test_v42_detection() {
        assert!("vers=4.2,rsize=1048576".contains("vers=4.2"));
        assert!("nfsvers=4.2".contains("nfsvers=4.2"));
        assert!(!"vers=4.1,rsize=1048576".contains("vers=4.2"));
    }

    #[test]
    fn test_port_extraction() {
        let opts = "rw,vers=4.2,port=2050,rsize=1048576";
        let port: u16 = opts.split(',')
            .find_map(|opt| {
                let (k, v) = opt.split_once('=')?;
                if k == "port" { v.parse().ok() } else { None }
            })
            .unwrap_or(2049);
        assert_eq!(port, 2050);

        let opts_no_port = "rw,vers=4.2,rsize=1048576";
        let port2: u16 = opts_no_port.split(',')
            .find_map(|opt| {
                let (k, v) = opt.split_once('=')?;
                if k == "port" { v.parse().ok() } else { None }
            })
            .unwrap_or(2049);
        assert_eq!(port2, 2049);
    }
}
