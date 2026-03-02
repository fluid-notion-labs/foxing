#![no_main]
use libfuzzer_sys::fuzz_target;
use fxcp_core::sidecar::SyncSignature;

fuzz_target!(|data: &[u8]| {
    // Should never panic, regardless of input
    let _ = SyncSignature::deserialize(data);
});
