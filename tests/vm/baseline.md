# foxingd VM Test Baseline Report

**Date:** 2026-03-04
**VM:** fox-test (192.168.77.11), Fedora 43, kernel 6.18.5, 8 vCPU, 8GB RAM
**Storage:** qcow2 on NFS 4.2 (32TB, nconnect=4)
**foxingd:** v0.4.0, 4 targets (XFS, ext4, btrfs, f2fs)
**Source workload:** 556 files, 302MB (500x4KB + 50x1MB + 5x50MB + 1 sparse)

## Bugs Found and Fixed

| Bug | Commit | Impact |
|-----|--------|--------|
| Device ID mismatch in hydration WalkDir | `723f328` | **Critical** — hydration rejected ALL files on separate block devices |
| Governor over-throttling in VMs | `a4e9fc3` | PSI_IO score 2.04 → throttled hydration to near-zero |
| Worker error kills entire worker | `a4e9fc3` | Single file copy error killed hydration worker |
| Shared receiver mutex starvation | `1c98ccf` | Workers serialized on Arc<Mutex<Receiver>> |
| Worker select CPU spin | `c3157e8` | 390% CPU idle — std::future::ready() in select! |

## Hydration Performance (556 files x 4 targets = 2224 jobs)

| Run | Config | Time | Files/target | CPU Idle | Notes |
|-----|--------|------|-------------|----------|-------|
| Baseline (pre-fix) | Default PSI 10.0 | 24s scan | 0/556 | N/A | Device ID mismatch |
| After dev ID fix | Default PSI 10.0 | 24s scan | 65/556 | ~14/target | Governor throttling |
| After hypervisor detect | Default PSI (auto 50.0) | 30s | 83/556 | 370% | Queue drain stall |
| After per-worker channels | Default PSI (auto 50.0) | 60s | 108/556 (XFS) | 380% | Uneven distribution |
| After select fix | Default PSI (auto 50.0) | 60s | 108/556 (XFS) | 370% | Timer guards |

## Remaining Issues

### Event Worker CPU Spin (~370% idle)
4 tokio runtime threads at 75-83% CPU with zero events being processed.
Root cause: 8 event workers competing on 8 tokio threads. Timer intervals
(tune: 50ms, flush: 100ms) wake tasks frequently. The `external_stats_rx`
unbounded channel keeps workers from fully idling when hydration sends stats.

**Proposed fix:** Worker hibernation — when no events received for 5+ seconds
and coalescer empty, enter deep sleep mode. Only wake on event_rx.recv().

### Hydration Queue Partial Drain (~20% completion)
With per-worker channels and owned receivers, hydration processes ~100-115
files per XFS target (largest/fastest). ext4/btrfs/f2fs targets lag due to
slower I/O throughput on qcow2-over-NFS.

**Root cause:** qcow2-on-NFS latency (5-50ms per op) limits throughput.
Each large file (50MB) takes 30-60s to copy. Total estimated time for full
556-file sync: ~15 minutes per target at current throughput.

### Tuner Observations
- btrfs targets enter Conservative mode (state 99) — btrfs quotas detected
- XFS/f2fs targets stay in Startup (state 0) — no events to tune against
- ext4 Worker 1 reaches Drain (state 1) intermittently

## Performance Characteristics (NFS-backed qcow2)

| Metric | Value |
|--------|-------|
| foxingd RSS | 1.8 GB |
| Thread count | 36 total (8 tokio + 28 other) |
| Buffer pool allocation | 4x 358MB (hydration) + 8x 16MB (event) = 1.6GB |
| BPF probes attached | 32 (2 failed: vfs_write_iter, flock_lock_inode_wait) |
| Identity scan time | 1ms for 564 items |
| Hydration scan time | ~150ms for 556 files |
| Job submission time | ~40ms for 2224 jobs |

## Next Steps

1. **Move VMs to koero host** with fast workspace tier storage (not NFS-backed qcow2)
   - Real NVMe/SSD performance will enable realistic tuner state transitions
   - Eliminates qcow2 latency overhead confounding measurements

2. **Worker hibernation** for idle event workers to reduce CPU from 370% to <5%

3. **XFS AG-aware worker mapping** for zero-contention parallelism

4. **SIMD columnar scanning** for coalescer acceleration

## koero NVMe Baseline (2026-03-04)

**Host:** koero (RHEL 10.1, 64-core Xeon Gold 6130, 377GB RAM)
**Storage:** NVMe Tier 1 (Stratis/XFS, 1TB)
**VM:** 16 vCPU, 16GB RAM, Fedora 43, kernel 6.18.5
**Network:** br0 bridge, DHCP, DNS: fox-test.3d.ae.net.nz

### Hydration (555 files, 302MB, 4 targets)

| Target | Files (60s) | Data (60s) | Tuner State | Bytes Replicated |
|--------|------------|-----------|-------------|-----------------|
| XFS | 188/555 | 301MB | Startup→? | 315MB |
| ext4 | 136/555 | 301MB | **Drain (1)** | 315MB |
| btrfs | 138/555 | 301MB | Conservative (99) | 315MB |
| f2fs | 123/555 | 297MB | **Drain (1)** | 310MB |

### vs NFS-backed (previous host)

| Metric | NFS-backed qcow2 | NVMe qcow2 | Improvement |
|--------|----------------:|------------:|:-----------:|
| XFS files in 60s | 108 | 188 | **1.7x** |
| ext4 files in 60s | 83 | 136 | **1.6x** |
| Data per target | 151MB | 301MB | **2.0x** |
| Tuner transitions | Stuck at Startup | ext4/f2fs reach Drain | Working |
| Hypervisor detect | kvm (auto 50.0) | kvm (auto 50.0) | Same |
| CPU idle | 370% | 363% | Same (need P0 fix) |

### Key Observations (pre-fix)
- NVMe enables real tuner state transitions (Drain detected on ext4/f2fs)
- All data replicated (300MB/target) but file count stalled (~30% of files)
- Root cause: per-file overhead (spawn_blocking, io_uring ring cycle, atomic rename)
- 2.25 files/sec/worker — 555 × 4KB files took 60s+ when should be <1s

## koero NVMe After Hydration Fix (2026-03-04)

**Commit:** `1c733bc` — small file fast path + inline identity + sync stat

### Hydration (555 files, 302MB, 4 targets)

| Target | Files (30s) | Data (30s) | Complete |
|--------|------------|-----------|:--------:|
| XFS | **555/555** | 302MB | **YES** |
| ext4 | **555/555** | 303MB | **YES** |
| btrfs | **555/555** | 302MB | **YES** |
| f2fs | **555/555** | 303MB | **YES** |

### Before vs After

| Metric | Before Fix | After Fix | Improvement |
|--------|-----------|----------|:-----------:|
| Files replicated (30s) | 153/555 | **555/555** | **100% complete** |
| Time to full sync | >5min (never completed) | **<30s** | **>10x** |
| CPU idle | 363% | **0%** | **Fixed** |
| Files/sec/worker | 2.25 | **~18** | **8x faster** |

### What Changed
1. **Small file fast path** (<=256KB): `std::fs::copy` instead of SmartCopier io_uring
2. **Inline identity resolution**: DashMap lookup directly, no `spawn_blocking` thread
3. **Synchronous stat**: `std::fs::metadata` instead of `tokio::fs::metadata`
4. **Inline metadata for small files**: permissions + timestamps via `utimensat`, no thread

Large files (>256KB) still use SmartCopier for io_uring throughput and sparse handling.
