// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/consistency/mod.rs — Consistency module — WAL, journal, sequencer

//! Crash consistency subsystem providing write-ahead logging,
//! event journaling, and monotonic sequence generation.

pub mod journal;
pub mod serialization;
pub mod wal;
pub mod exchange;
pub mod sequencer;

pub use journal::{atomic_rename, atomic_commit};
pub use serialization::{SerializationEngine, OpKind};
pub use wal::{InMemoryWal, WalOpKind};
pub use sequencer::{GlobalSequencer, SequenceBarrier};
