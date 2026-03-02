#![no_main]
use libfuzzer_sys::fuzz_target;
use std::path::Path;
use fxcp_core::security;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let path = Path::new(s);
        let root = Path::new("/tmp");
        // Should never panic, regardless of input
        let _ = security::path_within_root(path, root);
        let _ = security::canonicalize_safe(path, root);
    }
});
