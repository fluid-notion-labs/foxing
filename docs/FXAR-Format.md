# ADR: FXAR v2 Archive Format

**Status:** Accepted (implemented v0.8.1)
**Date:** 2026-03-13
**Author:** Joel Wiramu Pauling

## Decision

FXAR v2 uses gear-hash variable-size chunking + BLAKE3 content-addressable storage + binary index for portable snapshot archives. Replaces v1's tar+whole-file-dedup.

## Format

### Header (64 bytes, little-endian)

| Field | Type | Offset | Description |
|-------|------|--------|-------------|
| magic | [u8; 4] | 0 | `b"FXAR"` |
| version | u32 | 4 | `2` |
| flags | u32 | 8 | Compression (0=none, 1=zstd, 2=lz4, 3=gzip, 4=xz) |
| chunk_min | u32 | 12 | Minimum chunk size (default 2KB) |
| chunk_max | u32 | 16 | Maximum chunk size (default 2MB) |
| chunk_avg | u32 | 20 | Average chunk size (default 64KB) |
| index_offset | u64 | 24 | Byte offset to chunk index |
| index_size | u64 | 32 | Chunk index size in bytes |
| manifest_offset | u64 | 40 | Byte offset to manifest |
| manifest_size | u64 | 48 | Manifest compressed size |
| chunk_count | u64 | 56 | Number of unique chunks |

### Manifest (JSON, optionally zstd-compressed)

Preceded by 16-byte length prefix: `compressed_len: u64` + `uncompressed_len: u64`.

```json
{
  "version": 2,
  "created": "2026-03-13T00:00:00Z",
  "snapshots": ["2026-03-12T084500", "2026-03-13T084500"],
  "files": [{
    "path": "2026-03-12T084500/tree/data/test.db",
    "size": 104857600,
    "mode": 33188,
    "mtime": 1773484800,
    "uid": 1000, "gid": 1000,
    "blake3": "a1b2c3d4...",
    "chunks": [0, 1, 2, 3],
    "xattr": {"user.foxing.sig": "..."}
  }]
}
```

### Chunk Index (binary, 48 bytes per entry)

| Field | Type | Offset | Description |
|-------|------|--------|-------------|
| blake3_hash | [u8; 32] | 0 | BLAKE3 hash of uncompressed chunk data |
| size | u32 | 32 | Uncompressed size |
| offset | u64 | 36 | Byte offset in archive |
| compressed_size | u32 | 44 | Compressed size |

### Footer (32 bytes, streaming mode only)

Appended when the writer can't seek (pipe/stdout). Magic: `b"FXAF"`.

| Field | Type | Description |
|-------|------|-------------|
| magic | [u8; 4] | `b"FXAF"` |
| manifest_offset | u64 | Offset to manifest section |
| index_offset | u64 | Offset to chunk index |
| chunk_count | u64 | Number of unique chunks |
| checksum | u32 | Additive checksum of footer fields |

## Chunking Algorithm

Gear hash rolling chunker (same family as borg/casync):

- Lookup table: 256 pre-computed u64 constants
- Rolling hash: `hash = hash.wrapping_shl(1).wrapping_add(GEAR_TABLE[byte])`
- Boundary: triggered when `hash & (avg - 1) == 0` (after min bytes, or forced at max bytes)
- Content-defined boundaries survive insertions — only nearby chunks change

Parameters match borg defaults: min=2KB, avg=64KB, max=2MB.

## Dedup Performance

| Scenario | FXAR v1 (whole-file) | FXAR v2 (chunk CAS) |
|----------|:---:|:---:|
| 3 snapshots, 10% change | ~50% dedup | **65.6% dedup** |
| 10 snapshots, 1% change | ~63% dedup | **~96% dedup** |

## Implementation

- `fxcp-core/src/chunker.rs` — `GearChunker` struct (~200 lines)
- `fxcp-core/src/fxar.rs` — header/index/manifest types, writer, reader, stream reader (~1400 lines)
- Writer: parallel via rayon (file read + chunk + compress), sequential dedup merge
- Reader: pipelined (background chunk loader + rayon file reconstruction)
- NFS import: `NfsClientPool` (4 sessions) for parallel compound RPCs

## References

- borgbackup internals: variable-size chunking via Buzhash
- casync: index/store separation (`.caibx` + `.castr`)
- Gear hash: Xia et al., "A Comprehensive Study of Data Deduplication"
- BLAKE3: https://github.com/BLAKE3-team/BLAKE3-specs
