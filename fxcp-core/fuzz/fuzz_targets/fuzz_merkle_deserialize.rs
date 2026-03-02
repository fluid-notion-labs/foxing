#![no_main]
use libfuzzer_sys::fuzz_target;
use fxcp_core::hashing::MerkleSignature;

fuzz_target!(|data: &[u8]| {
    // Should never panic, regardless of input
    let _ = bincode::deserialize::<MerkleSignature>(data);
});
