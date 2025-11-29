# File: foxing/docs/FAILURE_SCENARIOS.md | Index: 21 of 24 | Function: Documentation of synthetic failure modes and system resilience strategies.

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
