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

    let src_hash = match hash_file_lite(src, size) {
        Ok(h) => h,
        Err(crate::error::FxcpError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            // Source vanished — file lifecycle ended, skip verification
            return Ok(true);
        },
        Err(e) => return Err(e),
    };
    let dst_hash = match hash_file_lite(dst, size) {
        Ok(h) => h,
        Err(crate::error::FxcpError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            // Target vanished — needs re-copy, not a verification pass
            return Ok(false);
        },
        Err(e) => return Err(e),
    };

    Ok(src_hash == dst_hash)
}

/// Compute a directory hash from sorted child (name, hash) pairs.
/// Uses BLAKE3 over concatenation of sorted `name:hash` entries.
/// Provides a stable, order-independent directory fingerprint.
pub fn compute_dir_hash(children: &mut Vec<(String, [u8; 32])>) -> [u8; 32] {
    children.sort_by(|a, b| a.0.cmp(&b.0));
    let mut hasher = Hasher::new();
    for (name, hash) in children.iter() {
        hasher.update(name.as_bytes());
        hasher.update(b":");
        hasher.update(hash);
    }
    *hasher.finalize().as_bytes()
}

// ---------------------------------------------------------------------------
// Merkle Tree Engine — BLAKE3 chunk-level delta detection
// ---------------------------------------------------------------------------

/// Per-chunk hash with file offset metadata.
pub struct ChunkHash {
    pub offset: u64,
    pub length: u32,
    pub hash: Hash,
}

/// BLAKE3 Merkle tree over fixed-size file chunks.
pub struct MerkleTree {
    pub chunk_size: u64,
    pub file_size: u64,
    pub root: Hash,
    pub leaves: Vec<ChunkHash>,
}

/// A contiguous byte range that differs between source and target.
pub struct DirtyRange {
    pub offset: u64,
    pub length: u64,
}

/// Serializable Merkle signature for xattr/sidecar storage.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MerkleSignature {
    pub root: [u8; 32],
    pub chunk_size: u64,
    pub file_size: u64,
    pub leaf_hashes: Vec<[u8; 32]>,
}

const MAX_MERKLE_XATTR_BYTES: usize = 64 * 1024;

impl MerkleTree {
    /// Build a Merkle tree by hashing a file in fixed-size chunks.
    pub fn from_file(path: &Path, chunk_size: u64) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let mut leaves = Vec::new();
        let mut buf = vec![0u8; chunk_size as usize];
        let mut offset = 0u64;

        loop {
            let mut read_total = 0;
            // Read a full chunk (handling partial reads)
            loop {
                let n = file.read(&mut buf[read_total..])?;
                if n == 0 { break; }
                read_total += n;
                if read_total >= chunk_size as usize { break; }
            }
            if read_total == 0 { break; }

            let hash = blake3::hash(&buf[..read_total]);
            leaves.push(ChunkHash {
                offset,
                length: read_total as u32,
                hash,
            });
            offset += read_total as u64;
        }

        // Compute root hash from all leaf hashes
        let root = Self::compute_root(&leaves);

        Ok(MerkleTree { chunk_size, file_size, root, leaves })
    }

    /// Compute a root hash from leaf hashes by hashing them together.
    fn compute_root(leaves: &[ChunkHash]) -> Hash {
        if leaves.is_empty() {
            return blake3::hash(b"");
        }
        if leaves.len() == 1 {
            return leaves[0].hash;
        }
        let mut hasher = Hasher::new();
        for leaf in leaves {
            hasher.update(leaf.hash.as_bytes());
        }
        hasher.finalize()
    }

    /// Compare two Merkle trees and return ranges that differ.
    /// Adjacent dirty chunks are merged into contiguous ranges.
    pub fn diff(source: &MerkleTree, target: &MerkleTree) -> Vec<DirtyRange> {
        let mut ranges = Vec::new();
        let max_leaves = source.leaves.len().max(target.leaves.len());

        let mut current_dirty: Option<DirtyRange> = None;

        for i in 0..max_leaves {
            let src_chunk = source.leaves.get(i);
            let tgt_chunk = target.leaves.get(i);

            let is_dirty = match (src_chunk, tgt_chunk) {
                (Some(s), Some(t)) => s.hash != t.hash,
                (Some(_), None) => true,  // source has extra chunks (file grew)
                (None, Some(_)) => false, // target has extra chunks (file shrunk — handled by truncate)
                (None, None) => false,
            };

            if is_dirty {
                let s = src_chunk.unwrap();
                match &mut current_dirty {
                    Some(range) => {
                        // Extend existing dirty range
                        range.length = (s.offset + s.length as u64) - range.offset;
                    }
                    None => {
                        // Start new dirty range
                        current_dirty = Some(DirtyRange {
                            offset: s.offset,
                            length: s.length as u64,
                        });
                    }
                }
            } else if let Some(range) = current_dirty.take() {
                ranges.push(range);
            }
        }

        // Flush trailing dirty range
        if let Some(range) = current_dirty {
            ranges.push(range);
        }

        ranges
    }

    /// Convert to a serializable signature.
    pub fn to_signature(&self) -> MerkleSignature {
        MerkleSignature {
            root: *self.root.as_bytes(),
            chunk_size: self.chunk_size,
            file_size: self.file_size,
            leaf_hashes: self.leaves.iter().map(|l| *l.hash.as_bytes()).collect(),
        }
    }

    /// Reconstruct a MerkleTree from a stored signature.
    pub fn from_signature(sig: &MerkleSignature) -> Option<Self> {
        // Bounds check: leaf count must be reasonable
        let expected_max = sig.file_size / sig.chunk_size + 2;
        if sig.leaf_hashes.len() as u64 > expected_max {
            return None;
        }

        let mut leaves = Vec::with_capacity(sig.leaf_hashes.len());
        let mut offset = 0u64;
        for (i, hash_bytes) in sig.leaf_hashes.iter().enumerate() {
            let remaining = sig.file_size.saturating_sub(offset);
            let length = remaining.min(sig.chunk_size) as u32;
            if length == 0 && i > 0 { break; }
            leaves.push(ChunkHash {
                offset,
                length,
                hash: Hash::from_bytes(*hash_bytes),
            });
            offset += length as u64;
        }

        let root = Hash::from_bytes(sig.root);
        Some(MerkleTree {
            chunk_size: sig.chunk_size,
            file_size: sig.file_size,
            root,
            leaves,
        })
    }
}

impl MerkleSignature {
    /// Estimated serialized size for xattr storage bounds checking.
    pub fn serialized_size(&self) -> usize {
        // root(32) + chunk_size(8) + file_size(8) + vec_len(8) + hashes(32 each)
        56 + self.leaf_hashes.len() * 32
    }

    /// Check if this signature fits within xattr limits.
    pub fn fits_in_xattr(&self) -> bool {
        self.serialized_size() <= MAX_MERKLE_XATTR_BYTES
    }
}

/// Compare a source file's Merkle root against a stored signature.
///
/// Builds the source Merkle tree using the stored signature's chunk size,
/// then compares roots. Returns `Ok(true)` if the roots match (file unchanged),
/// `Ok(false)` if they differ (file needs resync).
pub fn verify_with_merkle(src: &Path, stored_sig: &MerkleSignature) -> Result<bool> {
    let src_tree = MerkleTree::from_file(src, stored_sig.chunk_size)?;
    Ok(src_tree.root.as_bytes() == &stored_sig.root)
}
