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
