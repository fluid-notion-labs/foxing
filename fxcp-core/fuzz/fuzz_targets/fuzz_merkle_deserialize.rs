// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/fuzz/fuzz_targets/fuzz_merkle_deserialize.rs — Fuzz target for MerkleSignature deserialization

#![no_main]
use libfuzzer_sys::fuzz_target;
use fxcp_core::hashing::MerkleSignature;

fuzz_target!(|data: &[u8]| {
    // Should never panic, regardless of input
    let _ = bincode::deserialize::<MerkleSignature>(data);
});
