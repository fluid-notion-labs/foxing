// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/nfs/mod.rs — NFSv4.2 userspace compound RPC bypass

//! Userspace NFSv4.2 compound RPC client for bypassing VFS per-file overhead.
//!
//! Instead of routing small file writes through the kernel VFS (4-6 NFS round-trips
//! per file), this module constructs raw XDR compound payloads and sends them over
//! TCP directly to the NFS server. One compound = one round-trip per file.
//!
//! Only active for NFSv4.2 targets with AUTH_SYS, files ≤16MB.

pub mod mount;
pub mod xdr;
pub mod rpc;
pub mod client;

pub use mount::NfsBypassInfo;
pub use client::NfsCompoundClient;

/// Maximum file size eligible for NFS compound bypass (16MB).
pub const NFS_BYPASS_MAX_SIZE: u64 = 16 * 1024 * 1024;

/// NFS-specific errors for the compound RPC bypass.
#[derive(Debug, thiserror::Error)]
pub enum NfsError {
    #[error("TCP connection failed: {0}")]
    ConnectionFailed(#[from] std::io::Error),

    #[error("Session establishment failed: {0}")]
    SessionFailed(String),

    #[error("Stale file handle for {path}")]
    StaleHandle { path: String },

    #[error("NFS4 error {code}: {message}")]
    Nfs4Error { code: u32, message: String },

    #[error("XDR decode error: {0}")]
    XdrDecode(String),

    #[error("RPC error: {0}")]
    RpcError(String),

    #[error("Timeout after {0:?}")]
    Timeout(std::time::Duration),

    #[error("File too large for bypass: {size} > {max}")]
    FileTooLarge { size: u64, max: u64 },

    #[error("Kerberos auth required — bypass unavailable")]
    KerberosRequired,
}
