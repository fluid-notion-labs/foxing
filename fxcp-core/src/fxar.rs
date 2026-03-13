// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/fxar.rs — FXAR v2 content-addressable archive format

//! FXAR v2: variable-size gear-hash chunking + BLAKE3 CAS + binary index.
//! Replaces v1's tar+whole-file-dedup with true chunk-level deduplication.
//!
//! Archive layout: HEADER | MANIFEST | CHUNK INDEX | CHUNK DATA [ | FOOTER ]

use std::collections::HashSet;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::Path;
use serde::{Serialize, Deserialize};
use tracing::{info, warn};

use crate::chunker::GearChunker;

// -----------------------------------------------------------------------
// Magic bytes and constants
// -----------------------------------------------------------------------

const FXAR_MAGIC: &[u8; 4] = b"FXAR";
const FXAR_FOOTER_MAGIC: &[u8; 4] = b"FXAF";
const FXAR_VERSION: u32 = 2;
const HEADER_SIZE: usize = 64;
const CHUNK_INDEX_ENTRY_SIZE: usize = 48;
const FOOTER_SIZE: usize = 32;

// Compression flag values
const FLAG_COMPRESS_NONE: u32 = 0;
const FLAG_COMPRESS_ZSTD: u32 = 1;
const FLAG_COMPRESS_LZ4: u32 = 2;
const FLAG_COMPRESS_GZIP: u32 = 3;
const FLAG_COMPRESS_XZ: u32 = 4;

// -----------------------------------------------------------------------
// Header (64 bytes, little-endian)
// -----------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct FxarHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub flags: u32,
    pub chunk_min: u32,
    pub chunk_max: u32,
    pub chunk_avg: u32,
    pub index_offset: u64,
    pub index_size: u64,
    pub manifest_offset: u64,
    pub manifest_size: u64,
    pub chunk_count: u64,
}
// Header is 64 bytes: 4+4+4+4+4+4 + 8+8+8+8+8 = 64

impl FxarHeader {
    fn new(flags: u32, chunker: &GearChunker) -> Self {
        Self {
            magic: *FXAR_MAGIC,
            version: FXAR_VERSION,
            flags,
            chunk_min: chunker.min as u32,
            chunk_max: chunker.max as u32,
            chunk_avg: chunker.avg as u32,
            index_offset: 0,
            index_size: 0,
            manifest_offset: 0,
            manifest_size: 0,
            chunk_count: 0,
        }
    }

    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.magic)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&self.chunk_min.to_le_bytes())?;
        w.write_all(&self.chunk_max.to_le_bytes())?;
        w.write_all(&self.chunk_avg.to_le_bytes())?;
        w.write_all(&self.index_offset.to_le_bytes())?;
        w.write_all(&self.index_size.to_le_bytes())?;
        w.write_all(&self.manifest_offset.to_le_bytes())?;
        w.write_all(&self.manifest_size.to_le_bytes())?;
        w.write_all(&self.chunk_count.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; HEADER_SIZE];
        r.read_exact(&mut buf)?;

        let magic = [buf[0], buf[1], buf[2], buf[3]];
        if &magic != FXAR_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not an FXAR archive"));
        }

        Ok(Self {
            magic,
            version: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            flags: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            chunk_min: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
            chunk_max: u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]),
            chunk_avg: u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]),
            index_offset: u64::from_le_bytes([buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31]]),
            index_size: u64::from_le_bytes([buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39]]),
            manifest_offset: u64::from_le_bytes([buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47]]),
            manifest_size: u64::from_le_bytes([buf[48], buf[49], buf[50], buf[51], buf[52], buf[53], buf[54], buf[55]]),
            chunk_count: u64::from_le_bytes([buf[56], buf[57], buf[58], buf[59], buf[60], buf[61], buf[62], buf[63]]),
        })
    }
}

// -----------------------------------------------------------------------
// Chunk Index Entry (48 bytes, little-endian)
// -----------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ChunkIndexEntry {
    pub blake3_hash: [u8; 32],
    pub size: u32,
    pub offset: u64,
    pub compressed_size: u32,
}

impl ChunkIndexEntry {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.blake3_hash)?;
        w.write_all(&self.size.to_le_bytes())?;
        w.write_all(&self.offset.to_le_bytes())?;
        w.write_all(&self.compressed_size.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; CHUNK_INDEX_ENTRY_SIZE];
        r.read_exact(&mut buf)?;

        let mut blake3_hash = [0u8; 32];
        blake3_hash.copy_from_slice(&buf[0..32]);

        Ok(Self {
            blake3_hash,
            size: u32::from_le_bytes([buf[32], buf[33], buf[34], buf[35]]),
            offset: u64::from_le_bytes([buf[36], buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43]]),
            compressed_size: u32::from_le_bytes([buf[44], buf[45], buf[46], buf[47]]),
        })
    }
}

// -----------------------------------------------------------------------
// Footer (32 bytes, for non-seekable streams)
// -----------------------------------------------------------------------

#[derive(Debug, Clone)]
struct FxarFooter {
    magic: [u8; 4],
    manifest_offset: u64,
    index_offset: u64,
    chunk_count: u64,
    checksum: u32,
}

impl FxarFooter {
    fn compute_checksum(manifest_offset: u64, index_offset: u64, chunk_count: u64) -> u32 {
        // Simple checksum: XOR of all u32 words
        let mut crc: u32 = 0;
        for b in FXAR_FOOTER_MAGIC {
            crc = crc.wrapping_add(*b as u32);
        }
        for word in [
            manifest_offset as u32, (manifest_offset >> 32) as u32,
            index_offset as u32, (index_offset >> 32) as u32,
            chunk_count as u32, (chunk_count >> 32) as u32,
        ] {
            crc = crc.wrapping_add(word);
        }
        crc
    }

    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.magic)?;
        w.write_all(&self.manifest_offset.to_le_bytes())?;
        w.write_all(&self.index_offset.to_le_bytes())?;
        w.write_all(&self.chunk_count.to_le_bytes())?;
        w.write_all(&self.checksum.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; FOOTER_SIZE];
        r.read_exact(&mut buf)?;

        let magic = [buf[0], buf[1], buf[2], buf[3]];
        if &magic != FXAR_FOOTER_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid FXAR footer"));
        }

        Ok(Self {
            magic,
            manifest_offset: u64::from_le_bytes([buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11]]),
            index_offset: u64::from_le_bytes([buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19]]),
            chunk_count: u64::from_le_bytes([buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27]]),
            checksum: u32::from_le_bytes([buf[28], buf[29], buf[30], buf[31]]),
        })
    }
}

// -----------------------------------------------------------------------
// Manifest types (JSON)
// -----------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FxarManifest {
    pub version: u32,
    pub created: String,
    pub files: Vec<FxarManifestEntry>,
    #[serde(default)]
    pub snapshots: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FxarManifestEntry {
    pub path: String,
    pub size: u64,
    pub mode: u32,
    pub mtime: i64,
    pub uid: u32,
    pub gid: u32,
    pub blake3: String,
    pub chunks: Vec<u64>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub xattr: std::collections::HashMap<String, String>,
}

// -----------------------------------------------------------------------
// Export/Import stats
// -----------------------------------------------------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FxarExportStats {
    pub snapshots_exported: u64,
    pub total_files: u64,
    pub total_apparent_bytes: u64,
    pub chunk_count: u64,
    pub unique_chunks: u64,
    pub dedup_chunks: u64,
    pub archive_bytes: u64,
}

impl FxarExportStats {
    pub fn dedup_ratio(&self) -> f64 {
        if self.total_apparent_bytes == 0 { return 0.0; }
        1.0 - (self.archive_bytes as f64 / self.total_apparent_bytes as f64)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FxarImportStats {
    pub files_restored: u64,
    pub bytes_restored: u64,
    pub chunks_verified: u64,
    pub chunks_failed: u64,
}

// -----------------------------------------------------------------------
// Writer
// -----------------------------------------------------------------------

/// Write an FXAR v2 archive from a VersionStore.
pub fn write_archive<W: Write>(
    store: &crate::version_store::VersionStore,
    mut writer: W,
    compress: &str,
    timestamp_filter: Option<&str>,
) -> crate::Result<FxarExportStats> {
    let chunker = GearChunker::default();
    let flags = compress_flag(compress);
    let mut header = FxarHeader::new(flags, &chunker);
    let mut stats = FxarExportStats::default();

    // Phase 1: Collect all files from snapshots, chunk them, build manifest
    let snap_dirs = store.collect_snap_dirs(timestamp_filter)?;
    let mut manifest = FxarManifest {
        version: FXAR_VERSION,
        created: chrono::Utc::now().to_rfc3339(),
        files: Vec::new(),
        snapshots: snap_dirs.iter()
            .map(|d| d.file_name().unwrap_or_default().to_string_lossy().to_string())
            .collect(),
    };

    // In-memory chunk dedup: hash → index into chunk_entries
    let mut seen_chunks: std::collections::HashMap<[u8; 32], u64> = std::collections::HashMap::new();
    let mut chunk_entries: Vec<ChunkIndexEntry> = Vec::new();
    let mut chunk_data_blobs: Vec<Vec<u8>> = Vec::new();

    for snap_dir in &snap_dirs {
        let tree_dir = snap_dir.join("tree");
        if !tree_dir.exists() { continue; }

        for entry in walkdir::WalkDir::new(&tree_dir).follow_links(false) {
            let entry = match entry { Ok(e) => e, Err(_) => continue };
            if !entry.file_type().is_file() { continue; }

            let rel = match entry.path().strip_prefix(&tree_dir) {
                Ok(r) => r.to_path_buf(),
                Err(_) => continue,
            };
            // Include snapshot name in path for multi-snapshot archives
            let snap_name = snap_dir.file_name().unwrap_or_default().to_string_lossy();
            let archive_path = format!("{}/tree/{}", snap_name, rel.display());

            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };

            stats.total_files += 1;
            stats.total_apparent_bytes += meta.len();

            // Read file and chunk it
            let file_data = match std::fs::read(entry.path()) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Cannot read {}: {}", entry.path().display(), e);
                    continue;
                }
            };

            let file_hash = blake3::hash(&file_data);
            let chunks = chunker.chunk_slice(&file_data);
            let mut chunk_indices = Vec::with_capacity(chunks.len());

            for chunk in &chunks {
                let hash_bytes = *chunk.hash.as_bytes();
                if let Some(&existing_idx) = seen_chunks.get(&hash_bytes) {
                    // Dedup hit
                    chunk_indices.push(existing_idx);
                    stats.dedup_chunks += 1;
                } else {
                    // New unique chunk — compress and store
                    let compressed = compress_chunk(&chunk.data, compress);
                    let idx = chunk_entries.len() as u64;
                    seen_chunks.insert(hash_bytes, idx);
                    chunk_entries.push(ChunkIndexEntry {
                        blake3_hash: hash_bytes,
                        size: chunk.data.len() as u32,
                        offset: 0, // filled in later
                        compressed_size: compressed.len() as u32,
                    });
                    chunk_data_blobs.push(compressed);
                    chunk_indices.push(idx);
                    stats.unique_chunks += 1;
                }
                stats.chunk_count += 1;
            }

            // Read xattrs
            let mut xattr_map = std::collections::HashMap::new();
            if let Ok(attrs) = xattr::list(entry.path()) {
                for attr in attrs {
                    let key = attr.to_string_lossy().to_string();
                    if key.starts_with("user.foxing") {
                        if let Ok(val) = xattr::get(entry.path(), &attr) {
                            if let Some(val) = val {
                                xattr_map.insert(key, hex::encode(&val));
                            }
                        }
                    }
                }
            }

            manifest.files.push(FxarManifestEntry {
                path: archive_path,
                size: meta.len(),
                mode: {
                    use std::os::unix::fs::PermissionsExt;
                    meta.permissions().mode()
                },
                mtime: {
                    use std::os::unix::fs::MetadataExt;
                    meta.mtime()
                },
                uid: {
                    use std::os::unix::fs::MetadataExt;
                    meta.uid()
                },
                gid: {
                    use std::os::unix::fs::MetadataExt;
                    meta.gid()
                },
                blake3: hex::encode(file_hash.as_bytes()),
                chunks: chunk_indices,
                xattr: xattr_map,
            });
        }
        stats.snapshots_exported += 1;
    }

    header.chunk_count = chunk_entries.len() as u64;

    // Phase 2: Write header (placeholder)
    header.write_to(&mut writer)?;
    let mut offset = HEADER_SIZE as u64;

    // Phase 3: Write manifest (JSON, zstd compressed)
    let manifest_json = serde_json::to_vec(&manifest)
        .map_err(|e| crate::error::FxcpError::Config(format!("manifest JSON error: {}", e)))?;
    let manifest_compressed = compress_chunk(&manifest_json, compress);
    header.manifest_offset = offset;
    header.manifest_size = manifest_compressed.len() as u64;
    // Write manifest length prefix (u64 LE) + uncompressed size (u64 LE) + data
    writer.write_all(&manifest_compressed.len().to_le_bytes())?;
    writer.write_all(&manifest_json.len().to_le_bytes())?;
    writer.write_all(&manifest_compressed)?;
    offset += 16 + manifest_compressed.len() as u64;

    // Phase 4: Write chunk index
    header.index_offset = offset;
    header.index_size = (chunk_entries.len() * CHUNK_INDEX_ENTRY_SIZE) as u64;

    // Compute chunk data offsets
    let chunk_data_start = offset + header.index_size;
    let mut data_offset = chunk_data_start;
    for (i, entry) in chunk_entries.iter_mut().enumerate() {
        entry.offset = data_offset;
        data_offset += chunk_data_blobs[i].len() as u64;
    }

    for entry in &chunk_entries {
        entry.write_to(&mut writer)?;
    }
    offset += header.index_size;

    // Phase 5: Write chunk data
    for blob in &chunk_data_blobs {
        writer.write_all(blob)?;
        offset += blob.len() as u64;
    }

    stats.archive_bytes = offset;

    // Phase 6: Append footer (for streaming — always append since we may not be seekable)
    let footer = FxarFooter {
        magic: *FXAR_FOOTER_MAGIC,
        manifest_offset: header.manifest_offset,
        index_offset: header.index_offset,
        chunk_count: header.chunk_count,
        checksum: FxarFooter::compute_checksum(
            header.manifest_offset, header.index_offset, header.chunk_count
        ),
    };
    footer.write_to(&mut writer)?;
    offset += FOOTER_SIZE as u64;

    writer.flush()?;

    info!("FXAR v2 export: {} snapshots, {} files, {} chunks ({} unique, {} dedup), {:.1}% dedup ratio",
          stats.snapshots_exported, stats.total_files, stats.chunk_count,
          stats.unique_chunks, stats.dedup_chunks, stats.dedup_ratio() * 100.0);

    Ok(stats)
}

/// Write FXAR v2 with seekable writer — updates header offsets in-place.
pub fn write_archive_seekable<W: Write + Seek>(
    store: &crate::version_store::VersionStore,
    mut writer: W,
    compress: &str,
    timestamp_filter: Option<&str>,
) -> crate::Result<FxarExportStats> {
    let chunker = GearChunker::default();
    let flags = compress_flag(compress);
    let mut header = FxarHeader::new(flags, &chunker);
    let mut stats = FxarExportStats::default();

    let snap_dirs = store.collect_snap_dirs(timestamp_filter)?;
    let mut manifest = FxarManifest {
        version: FXAR_VERSION,
        created: chrono::Utc::now().to_rfc3339(),
        files: Vec::new(),
        snapshots: snap_dirs.iter()
            .map(|d| d.file_name().unwrap_or_default().to_string_lossy().to_string())
            .collect(),
    };

    let mut seen_chunks: std::collections::HashMap<[u8; 32], u64> = std::collections::HashMap::new();
    let mut chunk_entries: Vec<ChunkIndexEntry> = Vec::new();
    let mut chunk_data_blobs: Vec<Vec<u8>> = Vec::new();

    for snap_dir in &snap_dirs {
        let tree_dir = snap_dir.join("tree");
        if !tree_dir.exists() { continue; }

        for entry in walkdir::WalkDir::new(&tree_dir).follow_links(false) {
            let entry = match entry { Ok(e) => e, Err(_) => continue };
            if !entry.file_type().is_file() { continue; }

            let rel = match entry.path().strip_prefix(&tree_dir) {
                Ok(r) => r.to_path_buf(),
                Err(_) => continue,
            };
            let snap_name = snap_dir.file_name().unwrap_or_default().to_string_lossy();
            let archive_path = format!("{}/tree/{}", snap_name, rel.display());

            let meta = match entry.metadata() { Ok(m) => m, Err(_) => continue };
            stats.total_files += 1;
            stats.total_apparent_bytes += meta.len();

            let file_data = match std::fs::read(entry.path()) {
                Ok(d) => d,
                Err(e) => { warn!("Cannot read {}: {}", entry.path().display(), e); continue; }
            };

            let file_hash = blake3::hash(&file_data);
            let chunks = chunker.chunk_slice(&file_data);
            let mut chunk_indices = Vec::with_capacity(chunks.len());

            for chunk in &chunks {
                let hash_bytes = *chunk.hash.as_bytes();
                if let Some(&existing_idx) = seen_chunks.get(&hash_bytes) {
                    chunk_indices.push(existing_idx);
                    stats.dedup_chunks += 1;
                } else {
                    let compressed = compress_chunk(&chunk.data, compress);
                    let idx = chunk_entries.len() as u64;
                    seen_chunks.insert(hash_bytes, idx);
                    chunk_entries.push(ChunkIndexEntry {
                        blake3_hash: hash_bytes,
                        size: chunk.data.len() as u32,
                        offset: 0,
                        compressed_size: compressed.len() as u32,
                    });
                    chunk_data_blobs.push(compressed);
                    chunk_indices.push(idx);
                    stats.unique_chunks += 1;
                }
                stats.chunk_count += 1;
            }

            let mut xattr_map = std::collections::HashMap::new();
            if let Ok(attrs) = xattr::list(entry.path()) {
                for attr in attrs {
                    let key = attr.to_string_lossy().to_string();
                    if key.starts_with("user.foxing") {
                        if let Ok(Some(val)) = xattr::get(entry.path(), &attr) {
                            xattr_map.insert(key, hex::encode(&val));
                        }
                    }
                }
            }

            manifest.files.push(FxarManifestEntry {
                path: archive_path,
                size: meta.len(),
                mode: { use std::os::unix::fs::PermissionsExt; meta.permissions().mode() },
                mtime: { use std::os::unix::fs::MetadataExt; meta.mtime() },
                uid: { use std::os::unix::fs::MetadataExt; meta.uid() },
                gid: { use std::os::unix::fs::MetadataExt; meta.gid() },
                blake3: hex::encode(file_hash.as_bytes()),
                chunks: chunk_indices,
                xattr: xattr_map,
            });
        }
        stats.snapshots_exported += 1;
    }

    header.chunk_count = chunk_entries.len() as u64;

    // Write placeholder header
    header.write_to(&mut writer)?;
    let mut offset = HEADER_SIZE as u64;

    // Write manifest
    let manifest_json = serde_json::to_vec(&manifest)
        .map_err(|e| crate::error::FxcpError::Config(format!("manifest JSON error: {}", e)))?;
    let manifest_compressed = compress_chunk(&manifest_json, compress);
    header.manifest_offset = offset;
    header.manifest_size = manifest_compressed.len() as u64;
    writer.write_all(&manifest_compressed.len().to_le_bytes())?;
    writer.write_all(&manifest_json.len().to_le_bytes())?;
    writer.write_all(&manifest_compressed)?;
    offset += 16 + manifest_compressed.len() as u64;

    // Write chunk index
    header.index_offset = offset;
    header.index_size = (chunk_entries.len() * CHUNK_INDEX_ENTRY_SIZE) as u64;

    let chunk_data_start = offset + header.index_size;
    let mut data_offset = chunk_data_start;
    for (i, entry) in chunk_entries.iter_mut().enumerate() {
        entry.offset = data_offset;
        data_offset += chunk_data_blobs[i].len() as u64;
    }

    for entry in &chunk_entries {
        entry.write_to(&mut writer)?;
    }
    offset += header.index_size;

    // Write chunk data
    for blob in &chunk_data_blobs {
        writer.write_all(blob)?;
        offset += blob.len() as u64;
    }

    stats.archive_bytes = offset;

    // Seek back and update header with real offsets
    writer.seek(SeekFrom::Start(0))?;
    header.write_to(&mut writer)?;
    writer.seek(SeekFrom::End(0))?;
    writer.flush()?;

    info!("FXAR v2 export (seekable): {} snapshots, {} files, {} unique chunks, {:.1}% dedup",
          stats.snapshots_exported, stats.total_files, stats.unique_chunks,
          stats.dedup_ratio() * 100.0);

    Ok(stats)
}

// -----------------------------------------------------------------------
// Reader (seekable random access)
// -----------------------------------------------------------------------

/// Read an FXAR v2 archive with random access.
pub struct FxarReader<R: Read + Seek> {
    reader: R,
    pub header: FxarHeader,
}

impl<R: Read + Seek> FxarReader<R> {
    pub fn open(mut reader: R) -> io::Result<Self> {
        let header = FxarHeader::read_from(&mut reader)?;
        if header.version != FXAR_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported FXAR version: {}", header.version),
            ));
        }
        Ok(Self { reader, header })
    }

    /// Read and parse the manifest.
    pub fn read_manifest(&mut self) -> io::Result<FxarManifest> {
        self.reader.seek(SeekFrom::Start(self.header.manifest_offset))?;
        let mut size_buf = [0u8; 8];
        self.reader.read_exact(&mut size_buf)?;
        let compressed_len = u64::from_le_bytes(size_buf) as usize;
        self.reader.read_exact(&mut size_buf)?;
        let _uncompressed_len = u64::from_le_bytes(size_buf) as usize;

        let mut compressed = vec![0u8; compressed_len];
        self.reader.read_exact(&mut compressed)?;

        let json_data = decompress_chunk(&compressed, self.header.flags)?;
        serde_json::from_slice(&json_data).map_err(|e|
            io::Error::new(io::ErrorKind::InvalidData, format!("manifest JSON: {}", e))
        )
    }

    /// Read the chunk index.
    pub fn read_chunk_index(&mut self) -> io::Result<Vec<ChunkIndexEntry>> {
        self.reader.seek(SeekFrom::Start(self.header.index_offset))?;
        let count = self.header.chunk_count as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(ChunkIndexEntry::read_from(&mut self.reader)?);
        }
        Ok(entries)
    }

    /// Read and decompress a single chunk by its index entry.
    fn read_chunk_data(&mut self, entry: &ChunkIndexEntry) -> io::Result<Vec<u8>> {
        self.reader.seek(SeekFrom::Start(entry.offset))?;
        let mut compressed = vec![0u8; entry.compressed_size as usize];
        self.reader.read_exact(&mut compressed)?;

        let data = decompress_chunk(&compressed, self.header.flags)?;

        // Verify BLAKE3
        let actual_hash = blake3::hash(&data);
        if actual_hash.as_bytes() != &entry.blake3_hash {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("BLAKE3 mismatch for chunk at offset {}", entry.offset),
            ));
        }

        Ok(data)
    }

    /// Restore a single file by path from the archive.
    pub fn restore_file(&mut self, file_path: &str) -> io::Result<Vec<u8>> {
        let manifest = self.read_manifest()?;
        let index = self.read_chunk_index()?;

        let entry = manifest.files.iter()
            .find(|f| f.path == file_path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found in archive"))?;

        let mut data = Vec::with_capacity(entry.size as usize);
        for &chunk_idx in &entry.chunks {
            let chunk_meta = &index[chunk_idx as usize];
            let chunk_data = self.read_chunk_data(chunk_meta)?;
            data.extend_from_slice(&chunk_data);
        }

        // Verify whole-file BLAKE3
        let actual = blake3::hash(&data);
        let expected = hex::decode(&entry.blake3).unwrap_or_default();
        if actual.as_bytes() != expected.as_slice() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("file BLAKE3 mismatch for {}", file_path),
            ));
        }

        Ok(data)
    }

    /// Restore all files to a target directory.
    pub fn restore_all(&mut self, target: &Path) -> io::Result<FxarImportStats> {
        let manifest = self.read_manifest()?;
        let index = self.read_chunk_index()?;
        let mut stats = FxarImportStats::default();

        for file_entry in &manifest.files {
            let dest = target.join(&file_entry.path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let mut data = Vec::with_capacity(file_entry.size as usize);
            for &chunk_idx in &file_entry.chunks {
                let chunk_meta = &index[chunk_idx as usize];
                let chunk_data = self.read_chunk_data(chunk_meta)?;
                stats.chunks_verified += 1;
                data.extend_from_slice(&chunk_data);
            }

            // Verify whole-file hash
            let actual = blake3::hash(&data);
            let expected = hex::decode(&file_entry.blake3).unwrap_or_default();
            if actual.as_bytes() != expected.as_slice() {
                warn!("BLAKE3 mismatch for {}, skipping", file_entry.path);
                stats.chunks_failed += 1;
                continue;
            }

            std::fs::write(&dest, &data)?;

            // Restore metadata
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(file_entry.mode));

            // Restore mtime
            let mtime = filetime::FileTime::from_unix_time(file_entry.mtime, 0);
            let _ = filetime::set_file_mtime(&dest, mtime);

            stats.files_restored += 1;
            stats.bytes_restored += file_entry.size;
        }

        info!("FXAR v2 import: {} files, {} bytes, {} chunks verified",
              stats.files_restored, stats.bytes_restored, stats.chunks_verified);

        Ok(stats)
    }

    /// List all files in the archive without extracting.
    pub fn list_files(&mut self) -> io::Result<Vec<FxarManifestEntry>> {
        let manifest = self.read_manifest()?;
        Ok(manifest.files)
    }
}

// -----------------------------------------------------------------------
// Stream reader (for piped imports)
// -----------------------------------------------------------------------

/// Read an FXAR v2 archive from a non-seekable stream.
pub fn read_archive_stream<R: Read>(
    mut reader: R,
    target: &Path,
) -> io::Result<FxarImportStats> {
    let header = FxarHeader::read_from(&mut reader)?;
    if header.version != FXAR_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported FXAR version: {}", header.version),
        ));
    }

    // Streaming mode: read manifest, then index, then chunks sequentially
    // Read manifest
    let mut size_buf = [0u8; 8];
    reader.read_exact(&mut size_buf)?;
    let compressed_len = u64::from_le_bytes(size_buf) as usize;
    reader.read_exact(&mut size_buf)?;
    let _uncompressed_len = u64::from_le_bytes(size_buf) as usize;

    let mut manifest_compressed = vec![0u8; compressed_len];
    reader.read_exact(&mut manifest_compressed)?;
    let manifest_json = decompress_chunk(&manifest_compressed, header.flags)?;
    let manifest: FxarManifest = serde_json::from_slice(&manifest_json)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("manifest: {}", e)))?;

    // Read chunk index
    let chunk_count = header.chunk_count as usize;
    let mut index = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        index.push(ChunkIndexEntry::read_from(&mut reader)?);
    }

    // Read all chunk data into memory (streaming mode — can't seek)
    let mut chunk_store: Vec<Vec<u8>> = Vec::with_capacity(chunk_count);
    for entry in &index {
        let mut compressed = vec![0u8; entry.compressed_size as usize];
        reader.read_exact(&mut compressed)?;
        let data = decompress_chunk(&compressed, header.flags)?;

        // Verify BLAKE3
        let actual = blake3::hash(&data);
        if actual.as_bytes() != &entry.blake3_hash {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk BLAKE3 mismatch at index {}", chunk_store.len()),
            ));
        }
        chunk_store.push(data);
    }

    // Reconstruct files
    let mut stats = FxarImportStats::default();
    for file_entry in &manifest.files {
        let dest = target.join(&file_entry.path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut data = Vec::with_capacity(file_entry.size as usize);
        for &chunk_idx in &file_entry.chunks {
            data.extend_from_slice(&chunk_store[chunk_idx as usize]);
            stats.chunks_verified += 1;
        }

        // Verify whole-file hash
        let actual = blake3::hash(&data);
        let expected = hex::decode(&file_entry.blake3).unwrap_or_default();
        if actual.as_bytes() != expected.as_slice() {
            warn!("BLAKE3 mismatch for {}, skipping", file_entry.path);
            stats.chunks_failed += 1;
            continue;
        }

        std::fs::write(&dest, &data)?;

        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(file_entry.mode));
        let mtime = filetime::FileTime::from_unix_time(file_entry.mtime, 0);
        let _ = filetime::set_file_mtime(&dest, mtime);

        stats.files_restored += 1;
        stats.bytes_restored += file_entry.size;
    }

    info!("FXAR v2 stream import: {} files, {} bytes", stats.files_restored, stats.bytes_restored);
    Ok(stats)
}

// -----------------------------------------------------------------------
// Format auto-detection
// -----------------------------------------------------------------------

/// Detect archive format by reading magic bytes.
/// Returns "fxar2" for FXAR v2, "tar" for tar archives, "unknown" otherwise.
pub fn detect_format<R: Read>(reader: &mut R) -> io::Result<(&'static str, [u8; 4])> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;

    if &magic == FXAR_MAGIC {
        return Ok(("fxar2", magic));
    }

    // tar magic is at offset 257 ("ustar"), but first bytes are often
    // a filename or null. Check for common compressed stream headers.
    // zstd: 0x28 0xB5 0x2F 0xFD
    if magic == [0x28, 0xB5, 0x2F, 0xFD] {
        return Ok(("zstd-stream", magic));
    }
    // gzip: 0x1F 0x8B
    if magic[0] == 0x1F && magic[1] == 0x8B {
        return Ok(("gzip-stream", magic));
    }
    // xz: 0xFD 0x37 0x7A 0x58
    if magic == [0xFD, 0x37, 0x7A, 0x58] {
        return Ok(("xz-stream", magic));
    }
    // lz4: 0x04 0x22 0x4D 0x18
    if magic == [0x04, 0x22, 0x4D, 0x18] {
        return Ok(("lz4-stream", magic));
    }

    // Assume tar (raw or old-style without magic at offset 257)
    Ok(("tar", magic))
}

/// Inspect an FXAR v2 archive and return summary info.
pub fn inspect_archive<R: Read + Seek>(mut reader: R) -> io::Result<FxarInspectResult> {
    let mut fxar = FxarReader::open(&mut reader)?;
    let manifest = fxar.read_manifest()?;
    let index = fxar.read_chunk_index()?;

    let total_files = manifest.files.len() as u64;
    let total_apparent: u64 = manifest.files.iter().map(|f| f.size).sum();
    let chunk_count = index.len() as u64;
    let total_chunk_bytes: u64 = index.iter().map(|e| e.size as u64).sum();
    let total_compressed: u64 = index.iter().map(|e| e.compressed_size as u64).sum();

    // Count unique file hashes for file-level dedup stats
    let unique_files: HashSet<&str> = manifest.files.iter().map(|f| f.blake3.as_str()).collect();

    Ok(FxarInspectResult {
        version: fxar.header.version,
        snapshots: manifest.snapshots,
        total_files,
        unique_files: unique_files.len() as u64,
        total_apparent_bytes: total_apparent,
        chunk_count,
        total_chunk_bytes,
        total_compressed_bytes: total_compressed,
        dedup_ratio: if total_apparent > 0 {
            1.0 - (total_compressed as f64 / total_apparent as f64)
        } else { 0.0 },
        files: manifest.files,
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FxarInspectResult {
    pub version: u32,
    pub snapshots: Vec<String>,
    pub total_files: u64,
    pub unique_files: u64,
    pub total_apparent_bytes: u64,
    pub chunk_count: u64,
    pub total_chunk_bytes: u64,
    pub total_compressed_bytes: u64,
    pub dedup_ratio: f64,
    pub files: Vec<FxarManifestEntry>,
}

// -----------------------------------------------------------------------
// Compression helpers
// -----------------------------------------------------------------------

fn compress_flag(compress: &str) -> u32 {
    match compress {
        "none" => FLAG_COMPRESS_NONE,
        s if s.starts_with("zstd") => FLAG_COMPRESS_ZSTD,
        "lz4" => FLAG_COMPRESS_LZ4,
        "gzip" => FLAG_COMPRESS_GZIP,
        s if s.starts_with("xz") => FLAG_COMPRESS_XZ,
        _ => FLAG_COMPRESS_ZSTD,
    }
}

fn compress_chunk(data: &[u8], compress: &str) -> Vec<u8> {
    match compress {
        "none" => data.to_vec(),
        s if s.starts_with("zstd") => {
            let level = s.strip_prefix("zstd:").and_then(|l| l.parse().ok()).unwrap_or(3);
            zstd::bulk::compress(data, level).unwrap_or_else(|_| data.to_vec())
        }
        "lz4" => lz4_flex::compress_prepend_size(data),
        "gzip" => {
            use flate2::write::GzEncoder;
            let mut enc = GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data).ok();
            enc.finish().unwrap_or_else(|_| data.to_vec())
        }
        _ => zstd::bulk::compress(data, 3).unwrap_or_else(|_| data.to_vec()),
    }
}

fn decompress_chunk(data: &[u8], flags: u32) -> io::Result<Vec<u8>> {
    match flags {
        FLAG_COMPRESS_NONE => Ok(data.to_vec()),
        FLAG_COMPRESS_ZSTD => {
            zstd::bulk::decompress(data, 16 * 1024 * 1024) // 16MB max per chunk
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("zstd: {}", e)))
        }
        FLAG_COMPRESS_LZ4 => {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("lz4: {}", e)))
        }
        FLAG_COMPRESS_GZIP => {
            use flate2::read::GzDecoder;
            let mut dec = GzDecoder::new(data);
            let mut out = Vec::new();
            dec.read_to_end(&mut out)?;
            Ok(out)
        }
        FLAG_COMPRESS_XZ => {
            let mut dec = xz2::read::XzDecoder::new(data);
            let mut out = Vec::new();
            dec.read_to_end(&mut out)?;
            Ok(out)
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("unknown compression: {}", flags))),
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let chunker = GearChunker::default();
        let mut header = FxarHeader::new(FLAG_COMPRESS_ZSTD, &chunker);
        header.index_offset = 12345;
        header.manifest_offset = 64;
        header.chunk_count = 42;

        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), HEADER_SIZE);

        let parsed = FxarHeader::read_from(&mut &buf[..]).unwrap();
        assert_eq!(parsed.magic, *FXAR_MAGIC);
        assert_eq!(parsed.version, FXAR_VERSION);
        assert_eq!(parsed.flags, FLAG_COMPRESS_ZSTD);
        assert_eq!(parsed.index_offset, 12345);
        assert_eq!(parsed.manifest_offset, 64);
        assert_eq!(parsed.chunk_count, 42);
        assert_eq!(parsed.chunk_min, chunker.min as u32);
        assert_eq!(parsed.chunk_avg, chunker.avg as u32);
        assert_eq!(parsed.chunk_max, chunker.max as u32);
    }

    #[test]
    fn test_header_bad_magic() {
        let buf = [0u8; HEADER_SIZE];
        let result = FxarHeader::read_from(&mut &buf[..]);
        assert!(result.is_err());
    }

    #[test]
    fn test_chunk_index_entry_roundtrip() {
        let entry = ChunkIndexEntry {
            blake3_hash: [0xAB; 32],
            size: 65536,
            offset: 999999,
            compressed_size: 32768,
        };

        let mut buf = Vec::new();
        entry.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), CHUNK_INDEX_ENTRY_SIZE);

        let parsed = ChunkIndexEntry::read_from(&mut &buf[..]).unwrap();
        assert_eq!(parsed.blake3_hash, [0xAB; 32]);
        assert_eq!(parsed.size, 65536);
        assert_eq!(parsed.offset, 999999);
        assert_eq!(parsed.compressed_size, 32768);
    }

    #[test]
    fn test_footer_roundtrip() {
        let checksum = FxarFooter::compute_checksum(64, 12345, 42);
        let footer = FxarFooter {
            magic: *FXAR_FOOTER_MAGIC,
            manifest_offset: 64,
            index_offset: 12345,
            chunk_count: 42,
            checksum,
        };

        let mut buf = Vec::new();
        footer.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), FOOTER_SIZE);

        let parsed = FxarFooter::read_from(&mut &buf[..]).unwrap();
        assert_eq!(parsed.manifest_offset, 64);
        assert_eq!(parsed.index_offset, 12345);
        assert_eq!(parsed.chunk_count, 42);
        assert_eq!(parsed.checksum, checksum);
    }

    #[test]
    fn test_manifest_json_roundtrip() {
        let manifest = FxarManifest {
            version: 2,
            created: "2026-03-13T00:00:00Z".into(),
            files: vec![FxarManifestEntry {
                path: "snap1/tree/data/test.db".into(),
                size: 104857600,
                mode: 0o100644,
                mtime: 1773484800,
                uid: 1000,
                gid: 1000,
                blake3: "a1b2c3d4".repeat(8),
                chunks: vec![0, 1, 2, 3],
                xattr: std::collections::HashMap::new(),
            }],
            snapshots: vec!["2026-03-12T084500".into()],
        };

        let json = serde_json::to_vec(&manifest).unwrap();
        let parsed: FxarManifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.files.len(), 1);
        assert_eq!(parsed.files[0].chunks, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        let data = b"hello world, this is test data for compression";

        for compress in &["none", "zstd", "zstd:1", "lz4", "gzip"] {
            let compressed = compress_chunk(data, compress);
            let flags = compress_flag(compress);
            let decompressed = decompress_chunk(&compressed, flags).unwrap();
            assert_eq!(decompressed, data, "roundtrip failed for {}", compress);
        }
    }

    #[test]
    fn test_export_stats_dedup_ratio() {
        let mut stats = FxarExportStats::default();
        stats.total_apparent_bytes = 1000;
        stats.archive_bytes = 100;
        assert!((stats.dedup_ratio() - 0.9).abs() < 0.001);

        stats.total_apparent_bytes = 0;
        assert_eq!(stats.dedup_ratio(), 0.0);
    }

    #[test]
    fn test_export_import_roundtrip() {
        use tempfile::TempDir;

        // Create a mock version store with a snapshot
        let source_dir = TempDir::new().unwrap();
        let target_dir = TempDir::new().unwrap();

        // Create "target" with version store structure
        let vs_root = target_dir.path().join(".foxing_versions");
        let snap_dir = vs_root.join("2026-03-12T084500");
        let tree_dir = snap_dir.join("tree");
        std::fs::create_dir_all(&tree_dir).unwrap();

        // Create test files in the snapshot tree
        let test_data = b"hello world, this is test data for FXAR v2 export/import roundtrip";
        std::fs::write(tree_dir.join("file1.txt"), test_data).unwrap();
        std::fs::write(tree_dir.join("file2.txt"), test_data).unwrap(); // duplicate for dedup
        std::fs::write(tree_dir.join("file3.dat"), &vec![0xABu8; 10_000]).unwrap();

        // Write summary.json so scan_snapshots finds it
        let summary = serde_json::json!({
            "timestamp": "2026-03-12T08:45:00Z",
            "status": "success",
            "type": "full",
            "source": "/test",
            "trigger": "test",
            "files": 3,
            "size_bytes": 10066,
            "disk_usage_bytes": 10066,
            "savings_pct": 0.0,
            "elapsed_ms": 1
        });
        std::fs::write(snap_dir.join("summary.json"),
            serde_json::to_string_pretty(&summary).unwrap()).unwrap();

        // Export to FXAR v2
        let store = crate::version_store::VersionStore::open(target_dir.path());
        let archive_path = source_dir.path().join("test.fxar");
        let file = std::fs::File::create(&archive_path).unwrap();
        let stats = write_archive_seekable(&store, file, "zstd", None).unwrap();

        assert_eq!(stats.snapshots_exported, 1);
        assert_eq!(stats.total_files, 3);
        assert!(stats.unique_chunks > 0);
        // file1.txt and file2.txt are identical, so dedup_chunks > 0
        assert!(stats.dedup_chunks > 0, "expected dedup, got {} dedup chunks", stats.dedup_chunks);

        // Verify archive magic
        let archive_data = std::fs::read(&archive_path).unwrap();
        assert_eq!(&archive_data[..4], b"FXAR");

        // Inspect
        let f = std::fs::File::open(&archive_path).unwrap();
        let inspect = inspect_archive(f).unwrap();
        assert_eq!(inspect.version, 2);
        assert_eq!(inspect.total_files, 3);
        assert!(inspect.chunk_count > 0);

        // Import (restore) to a new directory
        let restore_dir = TempDir::new().unwrap();
        let f = std::fs::File::open(&archive_path).unwrap();
        let mut reader = FxarReader::open(f).unwrap();
        let import_stats = reader.restore_all(restore_dir.path()).unwrap();

        assert_eq!(import_stats.files_restored, 3);
        assert_eq!(import_stats.chunks_failed, 0);

        // Verify restored files match originals
        let restored1 = std::fs::read(
            restore_dir.path().join("2026-03-12T084500/tree/file1.txt")
        ).unwrap();
        assert_eq!(restored1, test_data);

        let restored3 = std::fs::read(
            restore_dir.path().join("2026-03-12T084500/tree/file3.dat")
        ).unwrap();
        assert_eq!(restored3, vec![0xABu8; 10_000]);
    }

    #[test]
    fn test_selective_restore() {
        use tempfile::TempDir;

        let target_dir = TempDir::new().unwrap();
        let vs_root = target_dir.path().join(".foxing_versions");
        let snap_dir = vs_root.join("2026-03-12T084500");
        let tree_dir = snap_dir.join("tree");
        std::fs::create_dir_all(tree_dir.join("subdir")).unwrap();

        std::fs::write(tree_dir.join("keep.txt"), b"keep this").unwrap();
        std::fs::write(tree_dir.join("subdir/nested.txt"), b"nested data").unwrap();
        let summary = serde_json::json!({
            "timestamp": "2026-03-12T08:45:00Z", "status": "success", "type": "full",
            "source": "/test", "trigger": "test", "files": 2, "size_bytes": 20,
            "disk_usage_bytes": 20, "savings_pct": 0.0, "elapsed_ms": 1
        });
        std::fs::write(snap_dir.join("summary.json"),
            serde_json::to_string_pretty(&summary).unwrap()).unwrap();

        // Export
        let store = crate::version_store::VersionStore::open(target_dir.path());
        let archive_path = target_dir.path().join("test.fxar");
        let file = std::fs::File::create(&archive_path).unwrap();
        write_archive_seekable(&store, file, "none", None).unwrap();

        // Selective restore — just keep.txt
        let f = std::fs::File::open(&archive_path).unwrap();
        let mut reader = FxarReader::open(f).unwrap();
        let manifest = reader.read_manifest().unwrap();

        let keep_entry = manifest.files.iter().find(|f| f.path.contains("keep.txt")).unwrap();
        let data = reader.restore_file(&keep_entry.path).unwrap();
        assert_eq!(data, b"keep this");
    }

    #[test]
    fn test_stream_roundtrip() {
        use tempfile::TempDir;

        let target_dir = TempDir::new().unwrap();
        let vs_root = target_dir.path().join(".foxing_versions");
        let snap_dir = vs_root.join("2026-03-12T090000");
        let tree_dir = snap_dir.join("tree");
        std::fs::create_dir_all(&tree_dir).unwrap();
        std::fs::write(tree_dir.join("stream_test.bin"), &vec![0x55u8; 5000]).unwrap();
        let summary = serde_json::json!({
            "timestamp": "2026-03-12T09:00:00Z", "status": "success", "type": "full",
            "source": "/test", "trigger": "test", "files": 1, "size_bytes": 5000,
            "disk_usage_bytes": 5000, "savings_pct": 0.0, "elapsed_ms": 1
        });
        std::fs::write(snap_dir.join("summary.json"),
            serde_json::to_string_pretty(&summary).unwrap()).unwrap();

        // Export to in-memory buffer (simulates streaming/pipe)
        let store = crate::version_store::VersionStore::open(target_dir.path());
        let mut buf = Vec::new();
        write_archive(&store, &mut buf, "zstd", None).unwrap();

        // Import from buffer via stream reader
        let restore_dir = TempDir::new().unwrap();
        let stats = read_archive_stream(std::io::Cursor::new(buf), restore_dir.path()).unwrap();

        assert_eq!(stats.files_restored, 1);
        assert_eq!(stats.chunks_failed, 0);

        let restored = std::fs::read(
            restore_dir.path().join("2026-03-12T090000/tree/stream_test.bin")
        ).unwrap();
        assert_eq!(restored, vec![0x55u8; 5000]);
    }

    #[test]
    fn test_dedup_across_snapshots() {
        use tempfile::TempDir;

        let target_dir = TempDir::new().unwrap();
        let vs_root = target_dir.path().join(".foxing_versions");

        // Create 2 snapshots with mostly identical files
        for (snap_ts, change_byte) in &[("2026-03-12T080000", 0xAAu8), ("2026-03-12T090000", 0xBBu8)] {
            let snap_dir = vs_root.join(snap_ts);
            let tree_dir = snap_dir.join("tree");
            std::fs::create_dir_all(&tree_dir).unwrap();

            // Identical file across snapshots
            std::fs::write(tree_dir.join("stable.dat"), &vec![0x42u8; 50_000]).unwrap();
            // File that changes between snapshots
            std::fs::write(tree_dir.join("changing.dat"), &vec![*change_byte; 10_000]).unwrap();

            let summary = serde_json::json!({
                "timestamp": format!("{}Z", snap_ts.replace('T', "T").replace("080000", "08:00:00").replace("090000", "09:00:00")),
                "status": "success", "type": "full",
                "source": "/test", "trigger": "test", "files": 2, "size_bytes": 60000,
                "disk_usage_bytes": 60000, "savings_pct": 0.0, "elapsed_ms": 1
            });
            std::fs::write(snap_dir.join("summary.json"),
                serde_json::to_string_pretty(&summary).unwrap()).unwrap();
        }

        let store = crate::version_store::VersionStore::open(target_dir.path());
        let mut buf = Vec::new();
        let stats = write_archive(&store, &mut buf, "none", None).unwrap();

        assert_eq!(stats.snapshots_exported, 2);
        assert_eq!(stats.total_files, 4);
        // stable.dat is identical across snapshots → its chunks should be deduped
        assert!(stats.dedup_chunks > 0,
            "expected cross-snapshot dedup, got {} dedup chunks", stats.dedup_chunks);
    }
}
