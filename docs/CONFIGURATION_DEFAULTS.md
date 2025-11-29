# File: foxing/docs/CONFIGURATION_DEFAULTS.md | Index: 23 of 24 | Function: Detailed breakdown of default limits and safety behaviors.

# Default Configuration & Safety Limits

If `enable_versioning = true` is set without further tuning, the system enforces the following limits to balance data safety with storage efficiency.

## 1. Storage Retention Limits
These limits apply *per file*.

| Setting | Default Value | Description |
| :--- | :--- | :--- |
| `max_versions` | **5** | The maximum number of historical snapshots to keep for a single file. <br> *Behavior:* FIFO. When the 6th snapshot is created, the oldest is deleted. |
| `max_versions_size_mb` | **10,240 MB (10 GB)** | The maximum aggregate logical size of all versions for a file. <br> *Behavior:* Eviction. If keeping 5 versions of a 3GB file would equal 15GB, the system deletes oldest versions until the total is under 10GB. |

**Important:** These limits are enforced *after* a new snapshot is created.

## 2. System Load Protections (The Governor)
The system monitors the host environment to prevent the replication daemon from causing performance degradation.

| Setting | Default Value | Description |
| :--- | :--- | :--- |
| `max_system_load_avg` | **4.0** | If the 1-minute Load Average exceeds this value, the Governor signals "Stress". |
| `worker_buffer_utilization` | **N/A (Hardcoded)** | If internal queues exceed **50%** capacity, the system signals "High Load". |

### Impact of Stress on Versioning
When the system is under stress (High Load or Full Buffers):
1.  **Creation Continues:** New version snapshots are still created because `ioctl_ficlonerange` is near-instantaneous and protects crash consistency.
2.  **Cleanup Paused:** The "Cleanup" phase (enforcing the 5 version / 10GB limit) is **SKIPPED**.
    * *Why?* Deleting files is metadata-heavy and can cause I/O contention.
    * *Result:* You may temporarily exceed your retention limits during load spikes.
    * *Recovery:* Once load normalizes, the next file operation will trigger cleanup and prune the excess versions.

## 3. Crash Consistency Defaults
* **Atomic Writes:** Enabled for all new files. Files are written to `.tmp.<uuid>` and renamed.
* **Delta Writes:** Enabled for existing files. Large files are updated in-place to avoid copy overhead, relying on the previous Version Snapshot as the safety net.

## 4. Tuning Profile Defaults
If `profile = "Auto"` (default):

| Target Storage | Batch Size | Coalesce Window | Flush Strategy |
| :--- | :--- | :--- | :--- |
| **NVMe / SSD** | 16 | 1 MB - 4 MB | Aggressive (Freq: High) |
| **HDD / NFS** | 4 | 512 KB | Conservative (Freq: Low) |

*Note:* These values adjust dynamically in real-time based on latency measurements.
