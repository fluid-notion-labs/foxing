// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/browser/mod.rs — MC-style dual-pane file/snapshot browser

//! Midnight Commander-inspired TUI for browsing filesystems, snapshots,
//! and .fxar archives. Feature-gated behind `tui`.

mod model;
mod navigator;
mod render;
mod events;

pub use model::{BrowserApp, BrowserMode};
