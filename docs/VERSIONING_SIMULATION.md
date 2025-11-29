# File: foxing/docs/VERSIONING_SIMULATION.md | Index: 22 of 24 | Function: Theoretical simulation of storage efficiency under various workloads.

# MARS Versioning: Storage Efficiency Simulation

This document simulates the storage impact of enabling `enable_versioning = true` on the Target filesystem.

**Core Concept:**
`foxing` uses **Reflinks** (via `ioctl_ficlonerange`). A version snapshot initially consumes **0 bytes** of additional physical storage. It only consumes space when the *Live* file is modified (Copy-on-Write), retaining the original blocks.

---

## Scenario A: The Database (High Churn / Random Overwrite)
**Profile:** An OLTP database file (e.g., `postgres.db`) with heavy random write activity.
* **Source File Size:** 500 GB (Fixed)
* **Change Rate:** 2 GB of random overwrites per hour.
* **Config:** `max_versions = 24` (Retention: 24 hours if fsync/hour).

### Simulation Timeline

| Time | Event | Live Size | Changes | Version Size (Logical) | **Physical Cost on Target** | Efficiency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 00:00 | Initial Sync | 500 GB | - | - | **500 GB** | 1:1 |
| 01:00 | Snapshot V1 | 500 GB | 2 GB Overwritten | 500 GB | **502 GB** | 99.6% Shared |
| 02:00 | Snapshot V2 | 500 GB | 2 GB Overwritten | 500 GB | **504 GB** | 99.2% Shared |
| ... | ... | ... | ... | ... | ... | ... |
| 12:00 | Snapshot V12 | 500 GB | 2 GB * 12 | 500 GB | **524 GB** | 95.4% Shared |
| 24:00 | Snapshot V24 | 500 GB | 2 GB * 24 | 500 GB | **548 GB** | 91.2% Shared |

**Summary:**
* **Source Used:** 500 GB
* **Target Used:** 548 GB (Live + 24 Versions)
* **Logical Backup Size:** 12.5 TB (25 * 500 GB)
* **Actual Storage Overhead:** +9.6%
* **Conclusion:** Extremely efficient for protecting large databases against corruption.

---

## Scenario B: The Developer Workspace (Medium Churn)
**Profile:** A source code repository with compilation artifacts. Mixed writes (editing files) and replaces (compiler output).
* **Source Total Size:** 50 GB
* **Change Rate:** 500 MB new files, 200 MB edits per hour.
* **Config:** `max_versions = 10`

### Simulation Behavior
1.  **Editing Source Code (Small Text):**
    * File: `main.rs` (10KB).
    * Edit: Change 5 lines (1KB).
    * **Cost:** The file system likely CoW's a 4KB block.
    * **Overhead:** Negligible.
2.  **Re-Compiling (Binary Replacement):**
    * File: `app_binary` (50MB).
    * Action: GCC unlinks old binary, creates new one.
    * **Mirror Action:** `EVENT_UNLINK` (Old) -> `EVENT_CREATE` (New).
    * **Versioning:** The *Old* binary is preserved in `.versions/`. The *New* binary takes full space.
    * **Cost:** 100% of file size (No shared blocks between different binaries).

**Summary:**
* **Source Used:** 50 GB
* **Target Used:** ~60 GB (Heavily dependent on binary turnover)
* **Conclusion:** Binary artifacts bloat versioning history. Recommend using `version_excludes = ["*.o", "*.bin", "target/"]` in `config.toml`.

---

## Scenario C: The Log Server (Append-Only)
**Profile:** System logs or Time-Series data. Data is written to the end of the file.
* **Source File Size:** 100 GB (Growing)
* **Change Rate:** 10 GB appended per day.
* **Config:** `max_versions = 7` (Daily snapshots).

### Simulation Timeline

| Day | Event | Live Size | Operation | Version Size (Logical) | **Physical Cost on Target** | Explanation |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Day 1 | Snapshot V1 | 100 GB | Append 10 GB | 100 GB | **110 GB** | V1 shares 100% of its blocks with Live. |
| Day 2 | Snapshot V2 | 110 GB | Append 10 GB | 110 GB | **120 GB** | V2 shares 100% of its blocks with Live. |
| Day 3 | Snapshot V3 | 120 GB | Append 10 GB | 120 GB | **130 GB** | V3 shares 100% of its blocks with Live. |

**Summary:**
* **Source Used:** 130 GB (at Day 3)
* **Target Used:** 130 GB
* **Storage Overhead:** **0 Bytes**
* **Why?** Because the "old" data (0-100GB) is never overwritten, it exists in the Live file *and* the Version file simultaneously using the same blocks. The "new" data (100-110GB) exists only in the Live file.
* **Conclusion:** Versioning is "Free" for append-only workloads.

---

## Worst Case Scenario: The Defrag / Re-Encode
**Profile:** A user runs a defragmenter or video re-encoder that rewrites every bit of a file.
* **Action:** Rewrite 1TB file completely.
* **Result:**
    * Live File: 1TB (New Blocks).
    * Version 1: 1TB (Old Blocks).
    * **Target Usage:** 2TB.
* **Mitigation:** The `max_versions_size_mb` config limit acts as a safety valve. If the versions bloat beyond the limit (e.g., 500GB), the Governor/Cleaner will delete the oldest versions to protect the volume storage.
