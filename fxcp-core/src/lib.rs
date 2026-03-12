// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/lib.rs — Core copy engine library — re-exports and module declarations

//! Core copy engine shared by fxcp and foxingd. Provides SmartCopier (io_uring),
//! BLAKE3 Merkle hashing, xattr/sidecar metadata, and adaptive I/O strategies.

pub mod error;
pub mod constants;
pub mod metrics;
pub mod buffer;
pub mod hashing;
pub mod sidecar;
pub mod security;
pub mod operations;
pub mod governor;
pub mod versioning;
pub mod consistency;
pub mod filter;
pub mod version_store;
#[cfg(feature = "tui")]
pub mod browser;
pub mod sync;
pub mod tombstone;
#[cfg(feature = "nfs-bypass")]
pub mod nfs;

pub use error::{FxcpError, CopyErrorKind, Result};
