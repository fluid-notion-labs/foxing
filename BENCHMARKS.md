# Foxing Performance Benchmarks

**Version:** 0.6.0
**Date:** 2026-03-08
**Rust:** nightly (1.96+), edition 2024, release profile (opt-level 3, debuginfo)

## Binary Comparison

| Binary | Size | BPF Deps | Root Required | Notes |
|--------|-----:|:--------:|:-------------:|-------|
| fxcp | 142 MB | No | No | Standalone CLI; also obtainable via `ln -sf foxingd fxcp` |
| foxingd | 290 MB | Yes (libbpf) | Yes (eBPF) | Superset of fxcp (symlink dispatch) |
| rsync | 0.7 MB | No | No | Reference tool |
| cp | 0.1 MB | No | No | Reference tool |

foxingd is a complete superset of fxcp — when symlinked as `fxcp`, it behaves identically to the standalone binary. Users who don't need BPF/TUI/daemon can build fxcp alone (`cargo build -p fxcp`) for a 48% smaller binary.

## Resource Usage

Measured on fox-test VM (Xeon Gold 6130, 16GB RAM, Fedora 43, kernel 6.18.5):

| Metric | fxcp | foxingd sync | rsync | cp |
|--------|-----:|-------------:|------:|---:|
| Peak RSS (1K files) | ~1 MB | ~1 MB | ~7.5 MB | ~2.8 MB |
| Peak RSS (100MB file) | ~1 MB | ~1 MB | ~7.5 MB | ~2.6 MB |
| CPU% (1K files) | 64-75% | 64-75% | 74-79% | 95% |
| Context switches (100MB) | 1 | 1 | 1,896 | 1 |
| Startup time | ~5 ms | ~25 ms | ~5 ms | ~1 ms |

fxcp has the lowest RSS of all tools (~1MB). foxingd sync shares the same copy engine and has identical resource usage. rsync uses 7.5MB RSS due to checksum computation buffers.

## Cold Copy Performance (btrfs-over-LUKS2, local same-device)

**Platform:** AMD Ryzen AI 9 HX 370 (24 threads), 92GB RAM, btrfs on dm-crypt, NVMe
**Method:** Same-device copy — FICLONE reflink available

| Workload | Files | Size | rsync | cp | fxcp | fxcp vs rsync | fxcp Method |
|----------|------:|-----:|------:|---:|-----:|--------------:|:------------|
| small_files | 10,000 | 39 MB | 536ms | 404ms | **587ms** | 0.91x | sendfile (reflink per file) |
| large_files | 10 | 1 GB | 1237ms | 4ms | **26ms** | **47.6x** | FICLONE (instant CoW) |
| mixed | 5,000 | 2.1 GB | 2390ms | 201ms | **535ms** | **4.5x** | FICLONE + sendfile |
| deep_tree | 50 | 200 KB | 57ms | 14ms | **84ms** | 0.68x | sendfile |
| sparse | 10 | 500 MB | 653ms | 3ms | **27ms** | **24.2x** | FICLONE (instant CoW) |

### fxcp vs rsync Ratios (>1.0 = fxcp faster)

| Workload | Ratio | Why |
|----------|------:|:----|
| small_files | 0.91x | Per-file overhead (probe_capabilities + stat) slightly exceeds rsync |
| large_files | **47.6x** | FICLONE reflink vs rsync's full read+write |
| mixed | **4.5x** | Reflink for large files, sendfile for small |
| deep_tree | 0.68x | 50 files too few to amortize fxcp startup |
| sparse | **24.2x** | FICLONE preserves sparsity; rsync reads+writes all data |

### Why cp is Fastest on btrfs/XFS (same-device)

`cp -a` on btrfs/XFS uses `FICLONE` automatically via glibc's `copy_file_range()`. This is a metadata-only operation — 1GB copies in 4ms. fxcp also uses FICLONE but has additional overhead from `probe_capabilities()` (~10-30ms for reflink probe, sysfs reads, container detection). This overhead is fixed regardless of file size.

## Cross-Device Copy Performance (XFS→XFS, NVMe-backed)

**Platform:** fox-test VM (16 vCPU Xeon Gold 6130, 16GB RAM), source=vdb (XFS), target=vdc (XFS)
**Method:** Different block devices — no FICLONE, falls through to sendfile/io_uring

| Workload | Files | Size | cp | rsync | fxcp | foxingd sync | fxcp Method |
|----------|------:|-----:|---:|------:|-----:|-------------:|:------------|
| small_files | 1,000 | 4 MB | 115ms | 156ms | 170ms | 212ms | sendfile |
| large_files | 1 | 10 MB | 17ms | 80ms | 79ms | 85ms | io_uring |
| mixed | 500 | 191 MB | 186ms | 387ms | 507ms | 567ms | sendfile + io_uring |
| single_large | 1 | 100 MB | 67ms | 163ms | 224ms | 311ms | io_uring |
| many_tiny | 5,000 | 5 MB | 491ms | 443ms | 612ms | 668ms | sendfile |

foxingd sync adds ~30-40% overhead over fxcp because it always generates foxingd-compatible signatures (SyncSignature + MerkleSignature + dir_hash xattrs).

## NFS 4.2 Performance (XFS→NFS, NVMe→HDD)

**Platform:** fox-test VM → awa.3d.ae.net.nz NFS 4.2 (HDD-backed, 32TB XFS on Stratis)
**Mount options:** `soft,timeo=50,retrans=3,rsize=1048576,wsize=1048576,lookupcache=none,actimeo=0`

### Baseline NFS Throughput

| Tool | 111 files (60MB) | Throughput |
|------|-----------------|-----------|
| cp | 305ms | **196 MB/s** |
| rsync | 513ms | **116 MB/s** |

### Cross-Network Copy (XFS NVMe → NFS HDD)

With NFS bypass enabled (default), fxcp sends OPEN+WRITE+CLOSE as a single compound RPC for files ≤16MB, reducing per-file NFS round-trips from 4+ to 1.

| Workload | Files | Size | cp | rsync | fxcp | rsync/fxcp | fxcp Method |
|----------|------:|-----:|---:|------:|-----:|-----------:|:------------|
| small_files | 1,000 | 4 MB | 1.9s | 2.7s | 2.5s | **1.10x** | NFS compound RPC |
| large_files | 1 | 100 MB | 211ms | 335ms | 428ms | 0.78x | io_uring |
| mixed | 500 | 128 MB | 1.4s | 2.9s | 6.0s | 0.48x | compound + io_uring |
| many_tiny | 5,000 | ~150 KB | 9.2s | 13.2s | 12.3s | **1.07x** | NFS compound RPC |
| resync (no changes) | 500 | 128 MB | — | 352ms | 460ms | 0.76x | stat + skip |
| NFS→NFS server-side | 1 | 100 MB | — | 286ms | 83ms | **3.44x** | Server-side FICLONE |

**Analysis:** With NFS bypass, fxcp is faster than rsync for small files (1.10x for 1000x4KB, 1.07x for 5000 tiny). Large files (100MB) use io_uring and are competitive with rsync. NFS→NFS same-server copies are 3.4x faster via server-side FICLONE.

### NFS Bypass Performance (before/after)

| Workload | VFS path (no bypass) | NFS bypass | Speedup |
|----------|------:|------:|------:|
| 1,000 x 4KB files | 6731ms | **2430ms** | **2.8x** |
| 5,000 tiny files | 31174ms | **12033ms** | **2.6x** |

The bypass eliminates per-file VFS overhead by packing OPEN+WRITE+CLOSE into one TCP round-trip (~2ms per file vs ~6ms through the kernel NFS client).

### --verify Overhead

| Workload | Without verify | With --verify | Overhead |
|----------|------:|------:|------:|
| 500-file mixed → NFS | 469ms | 1468ms | +999ms |

### NFS Same-Server (server-side copy)

| Test | rsync | fxcp | fxcp vs rsync | Method |
|------|------:|-----:|--------------:|:-------|
| 100MB NFS→NFS | 286ms | **83ms** | **3.44x** | Server-side FICLONE |

For NFS→NFS (same server), fxcp triggers server-side FICLONE — data never traverses the network.

### XFS-to-NFS Overhead vs XFS-to-XFS

| Copy | XFS-to-XFS | XFS-to-NFS | NFS overhead |
|------|------:|------:|------:|
| 100MB fxcp | 235ms | 406ms | **72.7%** |

## Delta Copy Performance (btrfs-over-LUKS2, local)

After mutating 10% of source files (modify, add, delete), re-sync to existing target.

| Workload | rsync | cp | fxcp | fxcp vs rsync |
|----------|------:|---:|-----:|--------------:|
| small_files | 162ms | 433ms FAIL | **357ms** FAIL | 0.45x |
| large_files | 65ms | 171ms FAIL | **18ms** FAIL | **3.6x** |
| mixed | 263ms | 565ms FAIL | **166ms** FAIL | **1.6x** |
| deep_tree | 54ms | 10ms FAIL | **23ms** FAIL | **2.4x** |
| sparse | 58ms | 83ms FAIL | **38ms** FAIL | **1.5x** |

cp and fxcp delta tests show FAIL because neither deletes files removed from source (no `--delete` by default). rsync uses `--delete`.

## foxingd Daemon Performance (XFS→NFS)

### Adversarial Test Results (v0.6.0, 9 phases)

**VM:** fox-test.3d.ae.net.nz (koero, 16 vCPU, 16GB RAM, Fedora 43, kernel 6.18.5)
**Source:** `/mnt/source` (XFS on virtio-blk, NVMe-backed)
**Target:** `/mnt/target-nfs` (NFS 4.2 → awa.3d.ae.net.nz, HDD-backed 32TB)

| Phase | Test | Duration | Result | Key Metric |
|-------|------|----------|--------|------------|
| 0 | Baseline cp/rsync | 2s | **PASS** | cp=196MB/s rsync=116MB/s |
| 1 | Heavy Hydration (5000 files, 2.6GB) | 52s | **PASS** | 5000/5000 converged in ~15s |
| 2 | Live Write Storm (fio randwrite 30s) | 43s | **PASS** | no_coalescing, transient_leak=25 |
| 3 | Rename Chain Storm (100 chains a→e) | 11s | **PASS** | finals=100/100, ghosts=349 (cleaned) |
| 4 | NFS Target Drop + Resync (300 files) | 58s | **FAIL** | 200/300 — intermittent recovery stall |
| 5 | Large File Kill/Resume (100MB) | 27s | **PASS** | SHA-256 match after SIGKILL + restart |
| 6 | Disk Pressure | SKIP | — | NFS share too large (22TB) |
| 7 | BLAKE3 Delta Copy | 44s | **PASS** | 10 deltas, 20MB saved |
| 8 | Directory Merkle Pruning | 45s | **PASS** | 13 dirs pruned |
| 9 | Combined Delta + Pruning | 53s | **PASS** | delta=6, pruned=13, 12MB saved |

**Total:** 336 seconds (5.6 min). NFS batch_stat prescan confirmed working (851 file attributes cached via compound RPCs). Phase 4 intermittent failure is a pre-existing recovery scan issue.

### Initial Hydration Throughput (5000 files, 2.6GB → NFS)

| Tool | Time | Throughput | Files/sec |
|------|-----:|----------:|----------:|
| cp -r | ~14s | **196 MB/s** | 357 |
| rsync -a | ~20s | **116 MB/s** | 250 |
| foxingd hydration | ~15s | **~173 MB/s** | 333 |

foxingd hydration matches cp throughput due to batched small-file processing and copy_file_range usage.

### Incremental Resync (10 of 20 files modified, 1 chunk each)

| Tool | Time | Data transferred | Speedup vs full copy |
|------|-----:|----------------:|:--------------------:|
| cp -r (full) | ~14s | 40MB (all 20 files) | baseline |
| rsync -a | ~8s | ~20MB (changed files) | 1.8x |
| foxingd delta copy | **~3s** | **0.6MB** (10 × 64KB chunks) | **23x** |

foxingd transfers only the modified 64KB chunks via BLAKE3 Merkle diff. 97% data reduction vs full copy.

### Fast Resume (no changes, daemon restart)

| Tool | Time | Work done | Speedup |
|------|-----:|-----------:|:-------:|
| rsync -a --checksum | ~24s | Hash all 5000 files | baseline |
| rsync -a (mtime) | ~3s | Stat all 5000 files | 8x |
| foxingd dir Merkle | **<1s** | 9 dir hashes compared | **>24x** |

foxingd skips entire directory subtrees via 32-byte BLAKE3 dir hashes. O(dirs) not O(files).

### Mount Recovery (NFS drop + resync)

| Metric | Value |
|--------|-------|
| Detection time | <10s (device ID + /proc/mounts + fsync probe, 10s interval) |
| Worker pause | Immediate (events drain to outage journal) |
| Recovery scan | Pruning-disabled full scan with NFS batch_stat prescan |
| NFS prescan cache | 851 file attributes cached via compound RPCs |

### Live Replication (BPF event-driven)

| Metric | Value |
|--------|-------|
| Events dropped | 0 (ENOENT→repair path) |
| Source performance impact | 0% (CQRS decoupling) |
| Throughput adaptation | BBR auto-tuning to target latency |
| Middle-of-file change detection | BLAKE3 Merkle root comparison |
| Delta copy threshold | >256KB files, <50% dirty chunks |
| Small file detection | Size + mtime fallback for <128KB |
| Metrics endpoint | Always responsive (dedicated thread) |

## Mean Time to Consistency (MTTC)

### Phase 1: fxcp One-Shot

How long from a source modification until the target is fully consistent (fxcp -a completes with content verified). Measures include fxcp startup, filesystem detection, copy, and fsync on target.

**Platform:** fox-test VM (16 vCPU Xeon Gold 6130, 16GB RAM, Fedora 43, kernel 6.18.5)
**Source:** XFS on virtio-blk (NVMe-backed), **fxcp:** 0.6.0, **Iterations:** 5 (median)

| Workload | XFS-to-XFS | XFS-to-NFS | NFS-to-NFS | XFS-to-tmpfs | XFS-same |
|----------|------:|------:|------:|------:|------:|
| single 4KB | 79ms | 90ms | 95ms | 55ms | 66ms |
| single 1MB | 81ms | 97ms | 101ms | 56ms | 63ms |
| single 100MB | 232ms | 374ms | 312ms | 228ms | 113ms |
| modify 4KB | 77ms | 91ms | 100ms | 63ms | 67ms |
| modify 1MB | 82ms | 103ms | 118ms | 64ms | 74ms |
| append 4KB | 81ms | 101ms | 108ms | 64ms | 68ms |
| metadata | 77ms | 119ms | 93ms | 65ms | 69ms |
| rename | 78ms | 119ms | 100ms | 58ms | 65ms |
| batch 100x4KB | 115ms | 426ms | 576ms | 80ms | 85ms |
| batch 10x10MB | 269ms | 1031ms | 1445ms | 225ms | 297ms |

**Topologies:**
- **XFS-to-XFS**: Cross-device local copy (vdb→vdc, sendfile/io_uring)
- **XFS-to-NFS**: Network copy to NFS 4.2 HDD-backed target (NFS bypass for small files, io_uring for large)
- **NFS-to-NFS**: Same-server copy (FICLONE server-side for large files, NFS bypass for small)
- **XFS-to-tmpfs**: Memory-backed target (copy_file_range/sendfile, no xattr support)
- **XFS-same**: Same-device copy (FICLONE reflink — instant CoW for large files)

**Key observations:**
- **Fixed overhead ~55-79ms**: fxcp startup + probe_capabilities + io_uring ring creation dominates small-file MTTC across all topologies
- **XFS-same 100MB = 113ms**: FICLONE reflink is metadata-only — 100MB copies in ~40ms after startup overhead
- **NFS single 4KB = 90ms**: NFS bypass compound RPC adds only ~11ms over local XFS (90ms vs 79ms)
- **NFS batch 100x4KB = 426ms**: ~4ms per file via NFS bypass compounds (vs ~6ms without bypass)
- **NFS-to-NFS 100MB = 312ms**: Server-side FICLONE — faster than XFS-to-NFS (374ms) since data stays on server

### Phase 2: foxingd Daemon (BPF Event-Driven)

**Mode:** foxingd daemon with eBPF event capture, target pre-synced with `--generate-sigs`
**Latency:** Source write to target file consistent (polled at 10ms resolution)

| Workload | XFS-to-XFS | XFS-to-NFS | XFS-to-tmpfs |
|----------|------:|------:|------:|
| create 4KB | 16ms | 20ms | 16ms |
| create 8KB | 16ms | 19ms | 16ms |
| create 32KB | 16ms | 17ms | 14ms |
| create 64KB | 16ms | 20ms | 14ms |
| rename 4KB | 16ms | 20ms | TIMEOUT |
| batch 10x4KB | TIMEOUT | TIMEOUT | TIMEOUT |

**Key observations:**
- **Single-file BPF latency = 14-20ms**: BPF event capture → copy → fsync in under 1 polling interval
- **XFS-to-NFS overhead = ~4ms**: NFS compound RPC adds minimal latency (20ms vs 16ms)
- **Batch TIMEOUT**: 10-file batches exceed the polling window; write coalescer timing issue under investigation
- **rename TIMEOUT on tmpfs**: xattr-based dirty tracking not available on tmpfs

## fxcp → foxingd Integration

`fxcp --generate-sigs` writes the same xattr/sidecar signatures that foxingd uses for fast resync:

| Signature | xattr Key | Purpose |
|-----------|-----------|---------|
| SyncSignature | `user.foxing.sig` | Size + mtime + BLAKE3 lite hash + Merkle root |
| MerkleSignature | `user.foxing.merkle` | 64KB chunk leaf hashes for delta copy |
| Dir hash | `user.foxing.dir_hash` | BLAKE3 directory fingerprint for tree pruning |

**Workflow:**
```bash
fxcp -a --generate-sigs /source /target   # Fast initial seed
foxingd daemon -c config.toml              # Hydration scan → 0 files need sync
```

## Auto-Adaptive Copy Strategy

fxcp and foxingd select the optimal copy method automatically:

```
Tier 0.5: NFS compound RPC — userspace OPEN+WRITE+CLOSE in single round-trip (NFSv4.2, ≤16MB)
Tier 1:   FICLONE          — instant CoW clone (btrfs/XFS/NFS 4.2 same-server)
Tier 1.5: copy_file_range  — NFS 4.2 server-side copy / tmpfs fallback
Tier 2:   sendfile          — kernel-optimized for small files (<64KB)
Tier 3:   io_uring          — async pipelined for large/cross-device files
```

Sparse files bypass Tiers 1.5 and 2 (both destroy holes) and go directly to Tier 3 with hole-aware I/O.

| Detected Environment | Strategy |
|---------------------|:---------|
| NFS 4.2 cross-server (≤16MB) | Tier 0.5 (NFS compound RPC) |
| Same btrfs/XFS device | Tier 1 (FICLONE) |
| NFS 4.2 same server | Tier 1 (FICLONE) or Tier 1.5 (copy_file_range) |
| tmpfs / ramfs | Tier 1.5 (copy_file_range) or Tier 2 (sendfile) |
| Small file (<64KB) | Tier 2 (sendfile) |
| Large cross-device | Tier 3 (io_uring) |
| Sparse file any device | Tier 3 (io_uring with SEEK_HOLE/PUNCH_HOLE) |
| Block device destination | Direct pwrite (no O_TRUNC/fallocate) |

## Filesystem Compatibility

| Filesystem | FICLONE | copy_file_range | io_uring | xattr | Status |
|------------|:-------:|:---------------:|:--------:|:-----:|:------:|
| XFS (same device) | ✅ | ✅ | ✅ | ✅ | Full support |
| XFS (cross device) | ❌ (EXDEV) | ✅ | ✅ | ✅ | Full support |
| btrfs | ✅ | ✅ | ✅ | ✅ | Full support |
| ext4 | ❌ | ✅ | ✅ | ✅ | Full support |
| NFS 4.2 | ✅ (same server) | ✅ | ✅ | ✅ | Full support |
| tmpfs | ❌ | ✅ | ✅ (unregistered) | ❌ | Copies work, no sigs |
| F2FS | ❌ | ✅ | ✅ | ✅ | Full support |
| overlayfs | ❌ | ✅ | ✅ | varies | Container support |

## SIMD Zero-Block Detection

Used in stdin pipe mode for sparse output.

| Architecture | Intrinsic | Throughput | Status |
|-------------|-----------|-----------|:------:|
| x86_64 AVX-512 | `_mm512_test_epi64_mask` | 256 B/cycle | Implemented |
| x86_64 AVX2 | `_mm256_testz_si256` | 32 B/cycle | Implemented |
| AArch64 NEON | `vmaxvq_u8` | 64 B/iter | Implemented |
| Generic | `align_to::<u128>` | 16 B/iter | Fallback (all archs) |

## Storage Stack Detection

| Layer | Detection | Adaptation |
|-------|-----------|-----------|
| dm-crypt (LUKS2) | `/sys/block/*/dm/uuid` `CRYPT-LUKS2-` | 4096B sector alignment |
| dm-crypt (LUKS1) | `CRYPT-LUKS1-` prefix | 512B sector alignment |
| kvdo (VDO) | `VDO-` prefix | Override STATX_DIOALIGN to 4096 |
| dm-thin | LVM tpool name | Pool monitoring |
| Stratis | Name contains "stratis" | Combined strategy |
| NFS 4.2 | statfs `NFS_SUPER_MAGIC` (0x6969) | Server-side copy path |
| tmpfs | statfs `TMPFS_MAGIC` (0x01021994) | Skip registered buffers |
| ramfs | statfs `RAMFS_MAGIC` (0x09041934) | Skip registered buffers |
| overlayfs | statfs `OVERLAY_MAGIC` (0x794c7630) | Container-aware |
| Container | `/run/.containerenv` | mountinfo-based device resolution |

## foxingd Prometheus Metrics (port 9100)

Served on a dedicated thread — always responsive even under heavy I/O.

| Metric | Type | Purpose |
|--------|------|---------|
| `foxing_worker_copy_in_flight` | Gauge | Active copy ops per worker |
| `foxing_worker_last_copy_epoch_ms` | Gauge | Last successful copy timestamp |
| `foxing_worker_retry_queue_size` | Gauge | Retry queue depth per worker |
| `foxing_events_repair_queued_total` | Counter | ENOENT → repair job dispatched |
| `foxing_events_repair_completed_total` | Counter | Successful repair completions |
| `foxing_events_repair_failed_total` | Counter | Failed repairs |
| `foxing_events_source_gone_total` | Counter | Source file vanished (skip) |
| `foxing_events_dropped` | Counter | Permanently failed events (0 = healthy) |
| `foxing_copy_timeout_total` | Counter | Copy operations exceeding deadline |
| `foxing_hydration_dir_pruned` | Counter | Directories skipped by Merkle tree |
| `foxing_hydration_nfs_prescan_hits` | Counter | Files skipped via NFS batch_stat prescan |
| `foxing_delta_copy_attempted` | Counter | Delta copy operations |
| `foxing_delta_bytes_saved` | Counter | Bytes avoided by delta copy |
| `foxing_tuner_state` | Gauge | BBR state (0=Steady, 1=Startup, 2=Drain, 3=ProbeBW) |

## Overhead Analysis

| Component | Time | When |
|-----------|-----:|:-----|
| probe_capabilities (per path) | 8-15ms | Once per source/dest pair |
| Reflink probe (FICLONE test) | 5-10ms | Once per filesystem |
| Container detection | 1-2ms | Once per run |
| dm-stack sysfs scan | 1-3ms | Once per run |
| tokio runtime startup | 1-2ms | Once per run |
| io_uring ring creation | <1ms | Once per run |
| BufferPool allocation | <1ms | Once per run |
| foxingd symlink dispatch | <1ms | argv0 check |
| **Total fixed overhead** | **~20-30ms** | — |

For workloads with >100 files or >100MB data, this overhead is negligible. foxingd sync adds ~25ms extra for Tokio runtime vs fxcp's direct execution.

## Running Benchmarks

```bash
# Quick benchmark (1 iteration, small workloads)
make benchmark-quick

# Full benchmark (3 iterations, all workloads, with statistics)
make benchmark

# Generate markdown report with telemetry
make benchmark-report

# Save as baseline for regression detection
make benchmark-baseline

# Compare against baseline
python3 tests/harness.py --benchmark --compare tests/baseline.json
```

The benchmark harness captures per-tool telemetry: Peak RSS, CPU%, user/system time, context switches, and disk I/O via GNU time and `/proc/diskstats`.
