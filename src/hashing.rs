use blake3::{Hasher, Hash};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::fs::File;
use crate::error::{Result};
use crate::metrics;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// Default 128KB
static LITE_HASH_THRESHOLD_BYTES: AtomicU64 = AtomicU64::new(128 * 1024);
pub const CHUNK_SIZE: usize = 65536; // 64KB chunks for incremental hashing

static HASHING_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn set_hashing_enabled(enabled: bool) {
    HASHING_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn is_hashing_enabled() -> bool {
    HASHING_ENABLED.load(Ordering::Relaxed)
}

pub fn set_lite_threshold_kb(kb: u64) {
    LITE_HASH_THRESHOLD_BYTES.store(kb * 1024, Ordering::Relaxed);
}

pub fn get_lite_threshold_bytes() -> u64 {
    LITE_HASH_THRESHOLD_BYTES.load(Ordering::Relaxed)
}

pub fn hash_file_lite(path: &Path, size: u64) -> Result<Option<Hash>> {
    if !is_hashing_enabled() { return Ok(None); }
    if size < get_lite_threshold_bytes() { return Ok(None); }
    
    let _timer = metrics::HASH_COMPUTATION_DURATION.start_timer();
    let mut file = File::open(path)?;
    let mut hasher = Hasher::new();
    
    // Sample: Head (64KB) + Tail (64KB) + Metadata
    let mut buf = vec![0u8; CHUNK_SIZE];
    hasher.update(&size.to_le_bytes());
    
    let n = file.read(&mut buf)?;
    hasher.update(&buf[..n]);
    
    if size > CHUNK_SIZE as u64 * 2 {
        file.seek(SeekFrom::End(-(CHUNK_SIZE as i64)))?;
        let n = file.read(&mut buf)?;
        hasher.update(&buf[..n]);
    }
    
    Ok(Some(hasher.finalize()))
}

pub fn hash_file_full(path: &Path) -> Result<Option<Hash>> {
    if !is_hashing_enabled() { return Ok(None); }
    
    let _timer = metrics::HASH_COMPUTATION_DURATION.start_timer();
    let mut file = File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buf = vec![0u8; CHUNK_SIZE];
    
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    
    Ok(Some(hasher.finalize()))
}

pub fn verify_incremental(src: &Path, dst: &Path, size: u64) -> Result<bool> {
    if !is_hashing_enabled() { return Ok(true); }
    if size < get_lite_threshold_bytes() { return Ok(true); }
    
    let src_hash = hash_file_lite(src, size)?;
    let dst_hash = hash_file_lite(dst, size)?;
    
    Ok(src_hash == dst_hash)
}
