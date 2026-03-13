# Foxing Snapshots & Export Guide

**Version:** 0.8.1

## Overview

Foxing provides zero-cost filesystem snapshots using **reflink CoW (Copy-on-Write)** clones. On btrfs, XFS, and NFS 4.2, a reflink creates a new file that shares disk blocks with the original — the "copy" is instantaneous and uses no additional storage until either file is modified. Only the modified extents allocate new blocks.

This is the **MARS** (Mirror & Archive Recovery System) — the same technology that enables foxingd's live versioning during replication. `fxcp snap` exposes it as a standalone tool that works without eBPF or root.

## Quick Start

```bash
# Copy with automatic versioning (creates reflink snapshots before overwriting)
fxcp -a --snapshot /source /backup

# List snapshots
fxcp snap list /backup

# Show storage savings (apparent size vs actual on-disk usage)
fxcp snap stats /backup

# Revert a file to a previous version
fxcp snap revert /backup/data/database.db 42

# Export all snapshots as a portable archive
fxcp snap export /backup -o backup.fxar

# Import on another machine
fxcp snap import /restore -i backup.fxar

# Restore a single file from an archive
fxcp snap restore backup.fxar --file 'data/database.db' --latest -o /tmp/
```

## Storage Layout

When `fxcp --snapshot` creates versioned copies, they are stored in a `.foxing_versions/` directory inside the target:

```
/backup/
├── data/
│   └── database.db                          # Live file
├── .foxing_versions/
│   ├── index.json                           # Machine-readable manifest
│   ├── 2026-03-12T084500/                   # Point-in-time snapshot
│   │   ├── summary.json                     # Snapshot metadata
│   │   └── tree/                            # Browsable directory tree (reflinks)
│   │       └── data/
│   │           └── database.db              # CoW clone of the file at that time
│   ├── 2026-03-12T123000/                   # Another snapshot
│   │   ├── summary.json
│   │   └── tree/
│   │       └── data/
│   │           └── database.db
│   └── files/                               # Per-file view (symlinks)
│       └── data/
│           ├── database.db~2026-03-12T084500    # → ../../2026-03-12T084500/tree/data/database.db
│           └── database.db~2026-03-12T123000    # → ../../2026-03-12T123000/tree/data/database.db
```

**Two views of the same data:**
- **Point-in-time** (`2026-03-12T084500/tree/`) — dirvish-style browsable directory tree. `cd` into any snapshot and browse files directly.
- **Per-file** (`files/data/database.db~timestamp`) — Syncthing/Time Machine-style. All versions of a specific file in one directory.

Both views are reflinks to the same underlying data — no duplication.

## JSON Schema

### index.json

The global manifest tracks all snapshots. Designed for web UI consumption.

```json
{
  "version": 1,
  "target": "/backup",
  "created": "2026-03-12T08:45:00Z",
  "snapshots": [
    {
      "timestamp": "2026-03-12T08:45:00Z",
      "type": "full",
      "tag": null,
      "files": 1234,
      "size_bytes": 5368709120,
      "disk_usage_bytes": 12288,
      "savings_pct": 99.9,
      "source": "/source",
      "trigger": "fxcp --snapshot",
      "retention": null
    }
  ]
}
```

### summary.json (per-snapshot)

Each snapshot directory contains metadata inspired by dirvish's summary file:

```json
{
  "timestamp": "2026-03-12T08:45:00Z",
  "status": "success",
  "type": "full",
  "tag": null,
  "source": "/source",
  "trigger": "fxcp --snapshot",
  "files": 1234,
  "size_bytes": 5368709120,
  "disk_usage_bytes": 12288,
  "savings_pct": 99.9,
  "reference": null,
  "elapsed_ms": 3456
}
```

## Snapshot Management

### Listing Snapshots

```bash
fxcp snap list /backup
```

```
 Timestamp              Type   Files    Apparent     On-Disk   Savings   Tag
─────────────────────────────────────────────────────────────────────────────
 2026-03-12T08:45:00Z   full    1234     5.0 GB       12 KB    99.9%
 2026-03-12T12:30:00Z   full    1234     5.0 GB      1.2 MB    99.9%    pre-migration
─────────────────────────────────────────────────────────────────────────────
 2 snapshots | Apparent: 10.0 GB | On-Disk: 1.2 MB | Savings: 99.9%
```

Use `--json` for machine-readable output:

```bash
fxcp snap list /backup --json
```

### Storage Statistics

```bash
fxcp snap stats /backup
```

```
Snapshot Store: /backup/.foxing_versions/
  Snapshots:      2
  Total Apparent: 10.0 GB  (if all copies were independent)
  Total On-Disk:  1.2 MB   (actual exclusive storage)
  CoW Savings:    99.9%    (9.99 GB saved via reflinks)
  Oldest:         2026-03-12T08:45:00Z
  Newest:         2026-03-12T12:30:00Z
```

### Pruning

Remove old snapshots by age, count, or size. Tagged snapshots are exempt from automatic pruning.

```bash
# Delete snapshots older than 30 days
fxcp snap prune --older-than 30d /backup

# Keep only the 10 most recent snapshots
fxcp snap prune --keep-last 10 /backup

# Delete oldest until total size is under 50 GB
fxcp snap prune --max-size 50G /backup

# Combine criteria
fxcp snap prune --older-than 7d --keep-last 5 /backup
```

Duration units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).
Size units: `K`, `M`, `G`, `T`.

### Tagging

Tag a snapshot to prevent automatic pruning:

```bash
fxcp snap tag 2026-03-12T084500 "pre-migration"
```

Tagged snapshots are shown in the `Tag` column of `fxcp snap list` and are skipped by all `fxcp snap prune` operations.

### Reverting

Restore a file to a previous version using an atomic reflink swap:

```bash
fxcp snap revert /backup/data/database.db 42     # Revert to epoch 42
fxcp snap copy /backup/data/database.db 42 /tmp/old.db  # Extract without reverting
```

## Export & Import (.fxar)

### FXAR v2 Archive Format (v0.8.1+)

FXAR v2 replaces the original tar+whole-file-dedup format with true chunk-level content-addressable storage:

```
FXAR v2 Archive (.fxar)
+----------------------------------------------+
| HEADER (64 bytes)                            |
|   magic: "FXAR", version: 2, flags,         |
|   chunk params, section offsets              |
+----------------------------------------------+
| MANIFEST (JSON, zstd-compressed)             |
|   File metadata + chunk index references     |
+----------------------------------------------+
| CHUNK INDEX (binary, 48 bytes per entry)     |
|   BLAKE3 hash + size + offset + compressed   |
+----------------------------------------------+
| CHUNK DATA (per-chunk compressed)            |
+----------------------------------------------+
| FOOTER (32 bytes, streaming mode only)       |
+----------------------------------------------+
```

**Chunking:** Gear-hash rolling chunker with configurable boundaries (2KB min, 64KB average, 2MB max). Content-defined boundaries survive insertions/deletions for high cross-snapshot dedup.

**Dedup:** In-memory `HashMap<[u8;32], u64>` tracks unique chunks by BLAKE3 hash. Identical chunks across snapshots are stored once.

**Seekable mode:** Header contains section offsets for random-access file restore. **Streaming mode:** Footer appended for pipe transport (`export | ssh import`).

**Format auto-detection:** Import, inspect, and restore commands detect FXAR v2 (`FXAR` magic) vs tar archives automatically.

**Backward compatibility:** `--format tar` flag produces legacy tar+whole-file-dedup archives.

### Creating Archives

Export snapshots as a portable `.fxar` archive (FXAR v2 by default, chunk-level dedup):

```bash
# Export all snapshots with zstd compression (default)
fxcp snap export /backup -o backup.fxar

# Export with specific compression
fxcp snap export /backup -o backup.fxar --compress zstd:19   # High ratio
fxcp snap export /backup -o backup.fxar --compress lz4       # Fastest
fxcp snap export /backup -o backup.fxar --compress xz        # Best ratio (archival)
fxcp snap export /backup -o backup.fxar --compress none       # Uncompressed

# Export a single snapshot
fxcp snap export /backup --timestamp 2026-03-12T084500 -o single.fxar

# Pipe to remote (sneakernet over SSH)
fxcp snap export /backup | ssh remote fxcp snap import /restore
```

**Compression options:**

| Format | Speed | Ratio | Use Case |
|--------|-------|-------|----------|
| `zstd` (default) | Fast | ~3:1 | General purpose, streaming |
| `zstd:19` | Slow | ~5:1 | Archival, max zstd ratio |
| `lz4` | Fastest | ~2:1 | LAN transfer, low CPU |
| `gzip` | Medium | ~3:1 | Max compatibility |
| `xz` | Slowest | ~5:1 | Cold archival, smallest size |
| `none` | N/A | 1:1 | When piping to own compression |

The archive includes BLAKE3 hashes for every file — files shared between snapshots are stored only once (chunk deduplication). For 10 snapshots of 5 GB data with 1% daily change, the archive is ~5.5 GB instead of ~50 GB.

### Importing Archives

```bash
# Import from file (auto-detects compression from extension)
fxcp snap import /restore -i backup.fxar

# Import from stdin
cat backup.fxar | fxcp snap import /restore

# Explicit decompression
fxcp snap import /restore -i backup.fxar --compress zstd
```

After import, the version store index is automatically rebuilt.

### Inspecting Archives

Browse archive contents without extracting:

```bash
# Summary
fxcp snap inspect backup.fxar

# Full file listing
fxcp snap inspect backup.fxar --list

# Machine-readable listing
fxcp snap inspect backup.fxar --list --json

# Find specific files
fxcp snap inspect backup.fxar --file database.db
```

### Selective Restore

Extract specific files or snapshots from an archive:

```bash
# Restore a specific file from the latest snapshot
fxcp snap restore backup.fxar --file 'data/database.db' --latest -o /tmp/

# Restore from a specific date
fxcp snap restore backup.fxar --file 'data/database.db' --date 2026-03-12 -o /tmp/

# Restore all SQL files from a date
fxcp snap restore backup.fxar --file '*.sql' --date 2026-03-12 -o /tmp/sqldump/

# Restore an entire snapshot
fxcp snap restore backup.fxar --date 2026-03-12T084500 -o /mnt/restore/
```

**Date matching:**
- `--date 2026-03-12` — any snapshot on that date (prefix match)
- `--date 2026-03-12T084500` — exact timestamp
- `--latest` — most recent snapshot
- Glob patterns supported via `--file`

## CoW Storage Economics

### How Reflink Savings Work

```
stat() on a reflinked file:
  st_size    = 100 MB   (apparent: full file size)
  st_blocks  = 0        (on-disk: no exclusive blocks — all shared with source)

After modifying 1 MB of the reflinked file:
  st_size    = 100 MB   (apparent: unchanged)
  st_blocks  = 2048     (on-disk: 1 MB of exclusive blocks)
```

The `Savings` column in `fxcp snap list` shows `1 - (on-disk / apparent)`:
- **99.9%** for fresh reflinks (nearly all blocks shared)
- Lower as files diverge from the original
- **0%** on filesystems without reflink support (ext4, tmpfs)

### Filesystem Requirements

| Filesystem | Reflink | Savings | Notes |
|------------|:-------:|---------|-------|
| btrfs | Yes | 99%+ | Full CoW support |
| XFS (same device) | Yes | 99%+ | Requires `reflink=1` mkfs option |
| NFS 4.2 (same server) | Yes | 99%+ | Server-side FICLONE |
| ext4 | No | 0% | Falls back to full copy |
| tmpfs | No | 0% | Falls back to full copy |

On filesystems without reflink support, snapshots still work but each version is a full copy.

## foxingd Integration

foxingd's live replication uses the same underlying versioning engine:

```bash
# 1. Seed target with fxcp (generates signatures + snapshots)
fxcp -a --snapshot --generate-sigs /source /target

# 2. Start foxingd — hydration scan sees signatures, skips matched files
foxingd daemon -c config.toml

# 3. foxingd creates per-file reflink snapshots on each write (if versioning enabled)
# Snapshots accumulate in .mirror/.versions/ (legacy format)

# 4. Export everything for offsite backup
fxcp snap export /target -o offsite-backup.fxar --compress xz
```

The `fxcp snap` commands work with both the new `.foxing_versions/` layout (point-in-time trees) and the legacy `.mirror/.versions/` layout (per-file inode-based). The `rebuild-index` command can scan both formats:

```bash
fxcp snap rebuild-index /target
```
