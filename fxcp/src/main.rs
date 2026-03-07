// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp/src/main.rs — fxcp CLI entry point — thin wrapper over fxcp_core::sync

//! fxcp — Smart filesystem copy CLI (thin wrapper over fxcp_core::sync)
//!
//! This binary can also be obtained by symlinking foxingd as `fxcp`.

fn main() -> anyhow::Result<()> {
    fxcp_core::sync::cli_main()
}
