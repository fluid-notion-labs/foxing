# Foxing: High-Fidelity Filesystem Replication

![Version](https://img.shields.io/badge/version-0.2.0-blue) ![License](https://img.shields.io/badge/license-GPLv2-green) ![Platform](https://img.shields.io/badge/platform-Linux%205.14%2B-lightgrey) ![Rust](https://img.shields.io/badge/rust-2024-orange)

**Foxing** (formerly `xfs-mirror`) is a high-performance, event-driven filesystem replication daemon designed for sub-millisecond latency. It leverages **eBPF (Extended Berkeley Packet Filter)** to capture filesystem operations directly from the kernel VFS layer and replays them asynchronously using **io_uring** and **Reflinks**.

Unlike `rsync` (scanning) or `inotify` (userspace polling), Foxing captures the *intent* of filesystem operations at the source, preserving causal ordering and atomic semantics.

## 🚀 Key Features

### 🧠 Kernel-Level Event Sourcing
*   **eBPF Capture:** Uses CO-RE (Compile Once, Run Everywhere) kprobes to intercept VFS calls (`vfs_write`, `vfs_rename`, `vfs_fallocate`) with minimal overhead.
*   **Loop Protection:** Kernel-side filtering of the daemon's own PID prevents infinite replication loops.

### ⚡ Advanced I/O Engine
*   **IoUring Pipelining:** Uses asynchronous, registered-buffer I/O (`io_uring`) for maximum throughput on NVMe devices.
*   **Zero-Copy Offload:** Transparently utilizes `copy_file_range` for NFS 4.2 server-side copies and local filesystem Reflinks (CoW), minimizing data movement.
*   **Smart Sparse Handling:** Detects zero-blocks and uses `FALLOC_FL_PUNCH_HOLE` to preserve sparsity, protected by a VDO stall detector to prevent thrashing.

### 🛡️ Converged Consistency
*   **Causal Ordering:** A **ReorderBuffer** ensures events are processed in strict sequence number order, preventing race conditions (e.g., writing to a file before its creation event is processed).
*   **Atomic Writes (Linux 6.13+):** Supports `RWF_ATOMIC` for aligned writes, ensuring database page updates are never torn.
*   **Crash Consistency:** Uses an In-Memory WAL (Write-Ahead Log) synchronized with XATTR intent journaling (`user.foxing.dirty`) to ensure structural operations like Renames are recoverable.

### 🎛️ Adaptive Autotuning (BBR)
*   **TargetTuner:** Implements a control loop inspired by TCP BBR. It measures I/O completion latency and throughput to dynamically adjust batch sizes and coalescing windows using Windowed Filters.
*   **System Governor:** Monitors Linux PSI (Pressure Stall Information) and Load Average. If the system is stressed, Foxing throttles hydration and background tasks to prevent starvation of the primary workload.

### 🕰️ MARS Versioning
*   **Zero-Cost Snapshots:** Implements **M**irror & **A**rchive **R**ecovery **S**ystem. Uses `ioctl_ficlonerange` (Reflink) to create instant, space-efficient file versions on every `fsync` or `close`.
*   **Time Travel:** Instantly revert files to previous epochs via the CLI using the `VersionIndex`.

## 🏗️ Architecture

Foxing employs a **CQRS (Command Query Responsibility Segregation)** architecture:

1.  **Command Stream (Write Side):** The eBPF probe pushes events to a Ring Buffer. These are the immutable "facts" of filesystem mutation.
2.  **Identity Projector (Read Side):** Consumes raw events to update a **Sharded Inode Map**. It resolves stable Inode IDs to volatile filesystem paths, aided by a proactive `inotify` auxiliary index.
3.  **Control Plane (Worker 0):** Handles structural metadata (Rename, Link, Unlink) and enforces global barriers.
4.  **Data Plane (Workers 1-N):** Handles bulk data transfer (Write, Copy) using coalesced batches to minimize syscalls.

## 🛠️ Prerequisites

*   **OS:** Linux Kernel **5.14+** (Required for modern libbpf/io_uring).
    *   *Recommended:* **6.6+** (LTS) or **6.12+** for full feature support.
*   **Filesystem:** Target must support Extended Attributes (`user.*` xattrs).
    *   *Recommended:* **XFS** or **Btrfs** (Required for Reflink/Versioning features).
*   **Hardware:** x86_64 or arm64.

## 📦 Building from Source

Foxing requires a Rust toolchain (2024 Edition support) and BPF development tools.

```bash
# Install dependencies (Fedora/RHEL)
sudo dnf install clang llvm libbpf-devel bpftool cargo

# Install dependencies (Debian/Ubuntu)
sudo apt install clang llvm libbpf-dev linux-tools-generic cargo

# Build Release Binary
cargo build --release
```

*Note: The build process automatically compiles the eBPF object files using `vmlinux.h` generation.*

## ⚙️ Configuration

Foxing is configured via a TOML file. See `config.toml.example` for a complete reference.

```toml
# config.toml
worker_count = 8
queue_max = 2000000
global_buffer_limit = 8192 # MB

[[sources]]
path = "/mnt/nvme_source"

  [[sources.targets]]
  path = "/mnt/nvme_mirror/hot_replica"
  profile = "NVMe"
  autotune_target_latency_ms = 15
  vdo_optimization = true

  [[sources.targets]]
  path = "/mnt/hdd_array/archive"
  profile = "HDD"
  enable_versioning = true
  max_versions = 24
```

### Profiles
*   `NVMe`: Optimized for low latency, small batches, high parallelism.
*   `HDD`: Optimized for throughput, large batches (coalescing), sequential I/O.
*   `Network`/`NFS`: Enables `RWF_UNCACHED` (Linux 6.14+) to prevent page cache pollution during hydration.

## 🖥️ Usage

### Daemon Mode
Start the replication service.
```bash
# Run with TUI (Terminal UI) for real-time monitoring
sudo ./target/release/foxing daemon --config config.toml --tui
```

### Versioning CLI
Manage snapshots on the target.
```bash
# List versions of a file
foxing version list /mnt/target/important.db

# Revert a file to a specific epoch
foxing version revert /mnt/target/important.db 105

# Prune old versions manually
foxing version cleanup /mnt/target
```

## 📊 Observability

Foxing exposes Prometheus metrics by default on port `9100`.

| Metric | Description |
| :--- | :--- |
| `foxing_target_lag_seconds` | Real-time latency between source event and target commit. |
| `foxing_coalesced_writes_total` | Efficiency metric showing writes merged by the Coalescer. |
| `foxing_governor_stressed` | Boolean indicating if the system governor is throttling IO. |
| `foxing_identity_cache_hit_rate` | Effectiveness of the Inode-to-Path projection cache. |

## ⚠️ Stability & Limitations

*   **Alpha Status:** While the architectural foundations are solid, edge cases in `mmap` coherency and complex rename races are still being hardened.
*   **Metadata Tax:** Foxing stores replication state in `user.foxing.*` xattrs. Ensure your target filesystem has sufficient inode space.
*   **Atomic Writes:** Requires Linux 6.13+ and hardware support (NVMe atomic boundary or SCSI atomic command).

## Why the name?

"Foxing" is an archival term referring to the brownish spots and rusty patina that appear on old paper, stamps, and photographs over time. Historically, it was also used to describe the desilvering or "rusting" of antique mirrors.

Since this project is a Mirror written in Rust, the name fit perfectly.

It also nods to the classic pangram, "**The quick brown fox jumps over the lazy dog.**" In our case, this represents the core architectural goal: allowing the "Quick Fox" (your fast NVMe Source drive) to perform at full speed, completely decoupled from and leaping over the latency of the "Lazy Dog" (your slower backup HDD/Network Target).

## License

This project is licensed under the GPLv2 License.
