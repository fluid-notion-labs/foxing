
# Synthetic Failure Scenarios & System Resilience

This document outlines how `foxing` handles common operational failure modes, ranging from hardware disconnects to malicious activity and capacity exhaustion.

## Story 1: The "Digital Nomad" Gap
**Scenario:** A user unmounts the backup target USB drive to take off-site for 2 weeks. The daemon keeps running on the source. The user returns, plugs the drive back in, and expects the mirror to catch up.

### System Response
1.  **Disconnect Phase:**
    * The `Worker` attempts to write to the missing path.
    * **FailureState:** Captures `ENOENT` or `EIO`.
    * **Circuit Breaker:** Trips immediately due to inability to stat filesystem capacity.
    * **Backoff:** The worker enters "Hibernation Mode" (sleeping 300s between retry probes) to stop spamming logs and burning CPU.
2.  **Reconnect Phase:**
    * The `FailureState` probe detects the directory is readable again.
    * **Auto-Hydration:** The Manager triggers a `spawn_hydration_thread`.
    * **Catch-Up:** The walker compares Source `mtime` vs Target `mtime`. Since the source files changed during the 2 weeks, their timestamps are newer.
    * **Sync:** The worker queue fills with update events. The `TargetTuner` likely shifts to `HighLoad` mode to churn through the backlog efficiently.

**Result:** Consistency restored. No daemon restart required.

---

## Story 2: The "Power Cut" (Crash Consistency)
**Scenario:** A power failure occurs while the daemon is replicating a 50GB database file mid-write.

### System Response
1.  **During Crash:**
    * The OS halts. The write operation to the target is interrupted.
2.  **Recovery (On Boot):**
    * **Atomic Write Check:** If the file was a *new* file, it was being written to `.tmp.<uuid>`. The partial file exists but does not overwrite the valid destination. The destination remains missing (correct).
    * **Delta Write Check:** If it was an *update* (In-Place), the target file might have a "torn page" (half old data, half new).
    * **Sidecar Recovery:** The worker marked the sidecar (`.foxing_meta`) as `dirty=true` *before* starting the write.
    * **Startup Scan:** The `initial_sync` hydration sees the `dirty` flag in the sidecar.
    * **Action:** It immediately queues a full re-sync of that file from the source, overwriting the corrupt data with valid source data.

**Result:** No corrupt data persists. The file self-heals on daemon start.

---

## Story 3: The "Metadata Storm" (<code>chown -R</code>)
**Scenario:** An admin runs `chown -R user:group /source/project`, modifying 1,000,000 inodes in seconds.

### System Response
1.  **Ingestion:** The BPF ring buffer fills rapidly with `EVENT_CHOWN`.
2.  **Governor Intervention:**
    * The system Load Average likely spikes due to the metadata I/O.
    * The `Governor` detects `load.one > 4.0`.
    * **Throttling:** It forces the `TargetTuner` into `GovernorThrottled` mode.
    * **Impact:** Batch sizes are reduced to minimize Target I/O contention, allowing the Source system to breathe.
3.  **Buffer Overflow:** If the kernel generates events faster than the throttled worker can consume, the Ring Buffer fills.
    * **Drop Detection:** `foxing_events_dropped` increments.
    * **Safety Valve:** The worker logic detects the drop count increase.
    * **Reaction:** It triggers a background `hydration` scan to catch what was missed, ensuring eventual consistency without crashing the application.

**Result:** Source system stability is prioritized. Consistency is achieved lazily via hydration fallback.

---

## Story 4: The "Ransomware" Simulation
**Scenario:** A malicious script executes `rm -rf /source/*`.

### System Response
1.  **Replication:** The daemon faithfully replicates the `EVENT_UNLINK` events. The target files are deleted. (A mirror mirrors everything, including mistakes).
2.  **The MARS Safety Net:**
    * However, if `enable_versioning = true`, the system has been creating `Reflink` snapshots on every valid `fsync` epoch.
    * While the *Live* copies are deleted, the `.mirror/.versions/` directory retains the history of files as they existed at previous commit points.
3.  **Recovery:**
    * The admin mounts the target.
    * The live data is gone.
    * The admin copies files back from `.mirror/.versions/` to restore the state from 1 hour ago.

**Result:** Data loss mitigated by Versioning history, despite accurate replication of the delete command.

---

## Story 5: The "Matched Capacity" Fill (1TB Source to 1TB Target)
**Scenario:** Unpacking a 1TB disk image onto a 1TB Source NVMe, mirroring to a 1TB Target NVMe. Both use standard XFS on Stratis (Thin Provisioning).

### System Response
1.  **The Metadata Tax:**
    * `foxing` writes a 4KB `.foxing_meta` sidecar for every file.
    * If the archive has 100,000 files, the Target consumes ~400MB more than the Source.
2.  **The Wall:**
    * The Source hits 99.9% capacity but finishes the unpack.
    * The Target hits 100% physical capacity *before* the Source does.
3.  **Failure Handling:**
    * **Detection:** The worker catches `OS Error 28 (ENOSPC)` on a write syscall.
    * **Pruning:** It attempts `versioning::prune_global_history()`, but finds no versions (fresh files).
    * **Circuit Breaker:** Trips to **OPEN**.
    * **Status:** Replication **PAUSES** (Safe Stall) to prevent busy-looping.
4.  **Recovery:**
    * Administrator expands the Stratis pool by 1GB.
    * Daemon wakes from hibernation, detects space, triggers hydration, and syncs the final 1%.

**Result:** Mirror safely stalls without crashing or corrupting existing files. Requires target expansion.

---

## Story 6: The "VDO Magic Bucket" (Matched Capacity with Dedupe)
**Scenario:** Same as Story 5, but the Target uses **VDO (Virtual Data Optimizer)** compression/dedupe layer.

### System Response
1.  **The VDO Advantage:**
    * VDO compresses the incoming data stream (LZ4) and dedupes redundant blocks (like zero-filled regions).
    * **Logical Space:** The XFS filesystem sees 10TB available.
    * **Physical Space:** The 1TB Source data likely compresses to ~700GB Physical on Target (assuming typical compressibility).
2.  **The Outcome:**
    * The Source fills to 100%.
    * The Target is only ~70% physically full.
    * **Result:** The mirror completes successfully with massive headroom for Version History, despite the drives being the same physical size.

**Result:** **SUCCESS.** VDO effectively absorbs the metadata overhead and version history.

**Warning:** If the data is *incompressible* (e.g., encrypted video), VDO adds overhead without gain, causing the target to fail *sooner* than in Story 5. Monitor `foxing_target_capacity_bytes_available`.

---

## Story 7: The "Hydration Race" (BPF Events During Initial Sync)
**Scenario:** foxingd starts with `initial_sync = true` on a source directory containing 5,000 files. The source is actively being written to (CI pipeline, build system) while hydration copies files to the NFS target. BPF captures write events for files that haven't been copied to the target yet.

### System Response
1.  **Hydration Phase:**
    * The hydration scanner identifies 5,000 files and submits them as bulk jobs.
    * Hydration workers begin copying files to the NFS target (4 workers, round-robin).
    * Hydration takes ~30-60 seconds for 5,000 files on a slow NFS target.
2.  **Concurrent BPF Events:**
    * BPF probes capture `WriteRange` events for files being modified on the source.
    * Events are dispatched to workers immediately — no synchronization with hydration.
    * Workers attempt `optimized_copy_range()` on target paths that don't exist yet.
3.  **Error Classification:**
    * **`ErrorClass::TargetNotFound`:** Worker detects `ENOENT` (NotFound) on a data operation (Write, WriteRange, Clone, Truncate, Fallocate). Instead of retrying, it sends an absolute-path repair job via `hydration_tx.send_repair_job()`.
    * **`ErrorClass::SourceNotFound`:** If the source file was deleted (transient lifecycle — temp files, build artifacts), the event is skipped and `foxing_events_source_gone_total` incremented.
    * Repair jobs are deduplicated via `source.active_repairs` (DashSet) to prevent storms when many WriteRange events arrive for the same unhydrated file.
4.  **Repair Execution:**
    * The repair channel consumer in `mirror.rs` matches the absolute source path against the source mount, computes the relative path, and submits a `HydrationJob` to ALL configured targets.
    * The hydration worker performs a full file copy (source → target), creating the file on the NFS target.
    * On completion, `foxing_events_repair_completed_total` increments and the path is removed from `active_repairs`.
5.  **BBR Tuner Resilience:**
    * Even when all copies fail (ENOENT storm), the tuner receives failure latency samples to prevent starvation.
    * If stuck in `Startup` state for `startup_limit * 3` (6 seconds default) without bandwidth data, the tuner forces a transition to `Drain`.

**Result:** Zero data loss. Files created during the hydration window are correctly repaired via full-copy fallback. The `foxing_events_repair_queued_total` metric tracks how many events took this path. The `foxing_events_dropped` counter stays at 0.

**Metrics to monitor:**
- `foxing_events_repair_queued_total` — should be non-zero during initial sync with active writes
- `foxing_events_repair_completed_total` — should match queued count after sync settles
- `foxing_events_source_gone_total` — counts transient files correctly skipped
- `foxing_events_dropped` — should remain 0 (data loss indicator)

---

## Story 8: The "Slow NFS Target" (Source/Target Performance Decoupling)
**Scenario:** A high-performance NVMe source (XFS, 3GB/s) replicates to a remote HDD-backed NFS target (100MB/s). Hundreds of files are written per second on the source while the target can only absorb a fraction of that throughput.

### System Response
1.  **BBR Auto-Tuning:**
    * The `BbrTuner` detects NFS latency (>5ms) and classifies storage as HDD/Network.
    * Transitions through `Startup` → `Drain` → `ProbeBW` based on measured throughput.
    * `current_batch_size` and `current_coalesce_bytes` adapt to NFS round-trip time.
    * Flush interval multiplier set to 8-16x base (vs 1-2x for NVMe).
2.  **Write Coalescing:**
    * The `Coalescer` merges multiple writes to the same inode into a single larger I/O.
    * Under back-pressure (retry queue growing), the coalescer pressure flush kicks in, draining events before they stagnate.
3.  **Governor Failure-Rate Signal:**
    * If >50% of copies fail (NFS timeouts during congestion), `Governor.signal_copy_result(false)` boosts `stress_score` by 0.3.
    * This triggers worker backoff — pacing target writes to avoid overwhelming the NFS connection.
    * Source filesystem performance is unaffected — the Governor only throttles target-bound operations.
4.  **Copy Timeouts:**
    * All copy operations are wrapped in adaptive timeouts (60-300s based on file size).
    * Timed-out copies increment `foxing_copy_timeout_total` and are retried.
    * Prevents indefinite hangs when NFS becomes unresponsive.
5.  **Retry Batch Drain:**
    * `pop_ready_batch(16)` drains up to 16 retry events per tick (vs 1 previously).
    * 1,438 queued events drain in ~5 seconds instead of ~72 seconds.

**Result:** Source write performance remains at native NVMe speed. Target receives a throttled, coalesced stream adapted to its throughput capacity. No event loss — retries and repairs ensure eventual consistency.

**Key design principle:** The CQRS event architecture decouples source capture (BPF ring buffer) from target application (worker select loop). The BBR tuner adapts the worker's output rate to match the target's absorption capacity, while the Governor prevents system-wide stress from target-induced backpressure.
