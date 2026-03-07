// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/lib.rs — foxingd library — module declarations and re-exports

//! foxingd eBPF-powered filesystem replication daemon.
//! Provides continuous event-driven mirroring with BPF capture, adaptive tuning,
//! mount monitoring, and MARS versioning.

pub mod error;
pub mod metrics;
pub mod config;
pub mod event;
pub mod bpf;
pub mod mirror;
pub mod ordering;
pub mod identity;
pub mod identity_watch;
pub mod projector;
pub mod worker;
pub mod hydration;
pub mod hydration_worker;
pub mod tuner;
pub mod resilience;
pub mod columnar;
pub mod api;
pub mod tui;

pub use mirror::SharedConfig;
pub use error::{FoxingError, Result};
