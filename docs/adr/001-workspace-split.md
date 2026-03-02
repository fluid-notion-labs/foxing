# ADR-001: Splitting Foxing into fxcp + foxingd Workspace

**Status:** Proposed
**Date:** 2026-03-02
**Authors:** Joel Wirāmu Pauling
**Target Platform** RHEL10 (kernel 6.12>= with some backports), Fedora43 (6.19>=) other distros. (Modern kernels)

---

## 1. Architectural Vision

Foxing's README states its core architectural goal through the naming metaphor:

> *"The quick brown fox jumps over the lazy dog."* This represents the core architectural goal: allowing the "Quick Fox" (your fast NVMe Source drive) to perform at full speed, completely decoupled from and leaping over the latency of the "Lazy Dog" (your slower backup HDD/Network Target).

The project implements a **CQRS (Command Query Responsibility Segregation)** architecture with four planes:

1. **Command Stream (Write Side):** eBPF kprobes intercept VFS calls and push immutable event "facts" to a Ring Buffer. This captures *intent* — the semantic meaning of filesystem operations — rather than scanning for after-the-fact differences (as rsync does [rsync.samba.org]).
2. **Identity Projector (Read Side):** Consumes raw events to maintain a Sharded Inode Map, resolving stable kernel inode IDs to volatile filesystem paths.
3. **Control Plane (Worker 0):** Serializes structural metadata operations (Rename, Link, Unlink) and enforces global ordering barriers.
4. **Data Plane (Workers 1-N):** Handles bulk data transfer using coalesced batches, io_uring pipelining, and zero-copy reflink offload.

This architecture already contains a natural boundary between **event processing** (planes 1-3) and **I/O execution** (plane 4). This ADR proposes formalizing that boundary into two distinct projects.

---

## 2. Problem Statement

The monolithic codebase (~13,000 lines, 36 source files) couples two fundamentally different concerns:

### 2.1 A Smart File Copy Engine

The data plane is a general-purpose, filesystem-aware copy engine that:

- Probes filesystem capabilities at runtime — detecting reflink support (`FICLONE` [Btrfs docs: btrfs.readthedocs.io]), XFS atomic exchange (`XFS_IOC_EXCHANGE_RANGE` [Oracle: XFS atomic file content exchange in UEK8]), F2FS atomic writes, `RWF_ATOMIC` (Linux 6.13+), and NFS 4.2 server-side copy (`copy_file_range` [RFC 7862])
- Pipelines I/O through `io_uring` with registered buffers for NVMe-class throughput
- Detects and preserves sparse files using `FIEMAP` and `FALLOC_FL_PUNCH_HOLE`
- Implements crash-consistent atomic writes via temp-file-then-rename with dirty flag journaling in xattr sidecars (`user.foxing.dirty`)
- Provides MARS (Mirror & Archive Recovery System) versioning through zero-cost reflink snapshots (`ioctl_ficlonerange`)
- Verifies copy integrity through BLAKE3 content hashing (lite sampling: head + tail + metadata)

None of this requires eBPF, kernel privilege, or a resident daemon.

### 2.2 An eBPF Replication Daemon

The control plane is a privileged, long-running daemon that:

- Loads CO-RE (Compile Once, Run Everywhere) eBPF programs attaching kprobes to 23 VFS functions [docs.kernel.org/filesystems/vfs.html], capturing operations before they commit to the filesystem — a technique validated by ExtFUSE research [Usenix ATC'19, Bijlani et al.: "Extension Framework for File Systems in User Space"]
- Maintains real-time inode-to-path identity maps via a 64-shard LRU cache augmented by proactive inotify indexing [watchexec.github.io/docs/inotify-limits.html]
- Implements BBR-inspired adaptive tuning — measuring I/O completion latency and throughput to dynamically adjust batch sizes and coalescing windows, analogous to TCP BBR's Bandwidth-Delay Product estimation
- Monitors system pressure via Linux PSI (Pressure Stall Information) and Load Average, throttling background work to preserve primary workload performance
- Handles failure modes: circuit breakers for disconnected targets, poison cabinets for failing inodes, and automatic hydration (catch-up sync) on reconnection

This requires `CAP_BPF`/root privileges, libbpf, kernel BTF support, and continuous execution.

### 2.3 Why the Coupling is Harmful

| Problem | Impact |
|---------|--------|
| **Deployment inflexibility** | Users wanting a smart rsync replacement must compile against libbpf and require kernel BPF support |
| **Privilege escalation** | The copy engine needs no privilege; the eBPF loader requires root. Bundling forces running everything as root |
| **Build dependency weight** | libbpf-rs pulls in C toolchain (clang, bpftool, vmlinux.h generation). Systems without BTF cannot compile foxing at all |
| **Compilation coupling** | Changing `event.rs` rebuilds the 2002-line `operations.rs`. BPF skeleton regeneration blocks all compilation |
| **Reuse barrier** | The smart copy engine could serve backup systems, CI/CD pipelines, and container runtimes, but extraction requires disentangling daemon concerns |
| **Testing friction** | Copy engine tests cannot run without BPF infrastructure in CI |

---

## 3. Decision

Split foxing into a **Cargo workspace** [doc.rust-lang.org/cargo/reference/features.html] with three crates:

```
foxing/
├── Cargo.toml                    # Workspace root
├── fxcp-core/                    # Library: data plane engine
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── operations.rs         # SmartCopier, OptimizedFs, io_uring pipeline
│       ├── buffer.rs             # AlignedBuffer, BufferPool
│       ├── hashing.rs            # BLAKE3 lite/full/incremental verification
│       ├── sidecar.rs            # SyncSignature, xattr+JSON metadata, AsyncSidecar
│       ├── versioning.rs         # MARS snapshots, VersionIndex, FICLONE
│       ├── security.rs           # Metadata sync, xattr, permissions, ACLs
│       ├── consistency/
│       │   ├── mod.rs
│       │   ├── exchange.rs       # XFS atomic exchange ioctl
│       │   ├── journal.rs        # Atomic rename with dirty flags
│       │   ├── serialization.rs  # Per-inode operation serialization
│       │   ├── sequencer.rs      # GlobalSequencer, SequenceBarrier
│       │   └── wal.rs            # In-memory write-ahead log
│       ├── governor.rs           # System load monitoring (PSI + load avg)
│       ├── error.rs              # FxcpError (IO/System/Security variants)
│       ├── constants.rs          # Shared tuning constants
│       └── metrics.rs            # Copy-plane Prometheus metrics
│
├── fxcp/                         # Binary: standalone copy tool
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs               # CLI: "fxcp SOURCE DEST"
│       └── tui/
│           ├── explorer.rs       # Dual-pane file navigator
│           └── setup.rs          # Interactive setup wizard
│
├── foxingd/                      # Binary: eBPF replication daemon
│   ├── Cargo.toml
│   ├── build.rs                  # BPF compilation via libbpf-cargo
│   └── src/
│       ├── main.rs               # Daemon entry point
│       ├── bpf.rs                # eBPF skeleton, ring buffer, kprobes
│       ├── bpf/
│       │   ├── mirror.bpf.c      # Kernel eBPF program (23 VFS hooks)
│       │   └── vmlinux.h
│       ├── event.rs              # 24 EventTypes, sharded EventQueues
│       ├── worker.rs             # Event processing, coalescing, dispatch
│       ├── mirror.rs             # Manager, SourceInfo, orchestration
│       ├── ordering.rs           # ReorderBuffer, sequence tracking, gap detection
│       ├── identity.rs           # ShardedInodeMap (64-shard LRU)
│       ├── identity_watch.rs     # ProactiveIndex (inotify reverse index)
│       ├── projector.rs          # Event projection onto identity maps
│       ├── tuner.rs              # BBR-inspired adaptive tuning
│       ├── resilience.rs         # PoisonCabinet, CircuitBreaker, FailureState
│       ├── columnar.rs           # Columnar event batch storage
│       ├── hydration.rs          # Catch-up sync orchestration
│       ├── hydration_worker.rs   # Background sync workers
│       ├── config.rs             # TOML config (multi-source/target)
│       ├── metrics.rs            # Full Prometheus metrics (100+)
│       ├── api.rs                # HTTP API (/metrics, /status)
│       └── tui.rs                # Dashboard, targets, versions, debug
│
└── docs/
    ├── ADR-Split.md              # This document
    ├── FAILURE_SCENARIOS.md
    ├── CONFIGURATION_DEFAULTS.md
    └── VERSIONING_SIMULATION.md
```

### 3.1 fxcp: The Quick Fox as a Standalone Tool

**fxcp** is a smart rsync replacement that makes the "Quick Fox" I/O engine available without the daemon. It is designed for filesystems that support atomic operations, reflink/CoW, and modern I/O interfaces — with best-effort graceful fallback for those that don't.

**Copy Strategy Cascade:**

```
1. Reflink (FICLONE/FICLONERANGE)     — zero-copy CoW [Btrfs, XFS 4.9+]
2. Server-side copy (copy_file_range)  — NFS 4.2 offload [RFC 7862]
3. io_uring pipelined copy             — registered buffers, batch SQE submission
4. Standard read/write                 — universal fallback
```

**CLI interface:**
```
fxcp [OPTIONS] SOURCE DESTINATION

Options:
  -a, --archive         Preserve permissions, timestamps, xattrs, ownership
  -r, --recursive       Recurse into directories
  -v, --verify          BLAKE3 integrity verification after copy
  --snapshot            Create MARS version snapshot
  --profile <PROFILE>   Storage profile: Auto, NVMe, SSD, HDD, Network
  --dry-run             Show what would be copied
  --sparse              Detect and preserve sparse files (default: auto)
  --atomic              Force atomic writes (temp + rename)
  --tui                 Interactive file selector
```

**Key differentiators vs rsync:**

| Aspect | rsync | fxcp |
|--------|-------|------|
| **Change detection** | Scan-and-compare (O(n) per sync) | Dirty flag sidecars + BLAKE3 signatures |
| **Copy mechanism** | `read()`/`write()` with delta encoding | Reflink → server-side → io_uring pipeline |
| **Atomicity** | None (partial writes on crash) | Temp-file + atomic rename + xattr dirty flags |
| **Sparse files** | `--sparse` flag (detection only) | FIEMAP + PUNCH_HOLE (preserves holes precisely) |
| **Versioning** | None | MARS reflink snapshots (zero-cost on CoW fs) |
| **Parallelism** | Single-threaded by default | Multi-worker with per-inode serialization |

### 3.2 foxingd: The Lazy Dog Wrangler

**foxingd** is the eBPF daemon that captures VFS intent and drives continuous replication. It depends on `fxcp-core` for all actual I/O operations, maintaining the CQRS separation:

- **foxingd owns the Command Stream** — eBPF event capture, identity projection, ordering, coalescing, and adaptive tuning
- **fxcp-core owns the Data Plane** — the actual file copy, atomic rename, reflink, sparse detection, and versioning

This means foxingd never directly calls `write()`, `sendfile()`, or `copy_file_range()`. It resolves *what* needs to happen (inode X was written at offset Y for length Z) and delegates *how* to fxcp-core's `SmartCopier`.

---

## 4. Module Placement Rationale

### 4.1 Design Questions and Answers

**Q1: Should fxcp-core be a separate library, or combined with the fxcp binary?**

**A: Separate.** foxingd must depend on the copy engine as a library. Combining library and binary in one crate forces foxingd to depend on a crate that also produces a binary, complicating feature flags and CI. The fxcp binary adds TUI dependencies (ratatui, crossterm) that the library shouldn't impose on consumers. Third-party tools (backup systems, container runtimes) can depend on `fxcp-core` alone.

**Q2: Separate foxing-common crate for shared types?**

**A: No.** The types crossing the boundary are overwhelmingly data-plane types defined in `operations.rs` (`CopyStats`, `Capabilities`, `OptimizedFs`, `SmartCopier`). The dependency arrow is unidirectional: foxingd → fxcp-core. A common crate adds coordination cost without benefit. If bidirectional type dependency emerges later, it can be extracted then.

**Q3: Where does hydration live?**

**A: foxingd.** `hydration.rs` and `hydration_worker.rs` are deeply coupled to daemon infrastructure: `SourceInfo`, `TunerBoard`, `GLOBAL_TUNER_REGISTRY`, `identity::resolve_and_update_path`, BPF metrics (`ORDERING_BUF_SIZE`). However, the actual copy operations within hydration_worker go through `SmartCopier::copy_with_limit()` — which is in fxcp-core. The fxcp binary implements its own simpler one-shot scanning logic using fxcp-core primitives directly.

**Q4: Where does governor.rs live?**

**A: fxcp-core.** Governor is a general-purpose system load monitor. Its dependencies (`sysinfo`, `parking_lot`, `metrics`, `constants`) are all suitable for fxcp-core. `SmartCopier` already accepts `governor: Option<Arc<Governor>>` for I/O pacing. The fxcp binary benefits from governor-based pacing during bulk copies to avoid overwhelming storage. foxingd uses the same Governor for both copy pacing AND hydration pacing — the clean dependency direction is preserved.

### 4.2 Module Assignment Table

| Module | Lines | Crate | Rationale |
|--------|-------|-------|-----------|
| `operations.rs` | ~2002 | **fxcp-core** | Core: SmartCopier, io_uring pipeline, fs capability detection. Only internal deps: buffer, error, security, metrics, governor |
| `buffer.rs` | ~283 | **fxcp-core** | Core: AlignedBuffer, BufferPool for io_uring registered buffers |
| `hashing.rs` | ~81 | **fxcp-core** | Core: BLAKE3 lite/full/incremental verification |
| `sidecar.rs` | ~318 | **fxcp-core** | Core: SyncSignature, xattr+JSON metadata. Used by both (foxingd sets dirty flags, fxcp-core reads/clears them) |
| `versioning.rs` | ~354 | **fxcp-core** | Core: MARS snapshot management via `FICLONE`. No daemon deps |
| `security.rs` | ~549 | **fxcp-core** | Core: metadata sync, ownership, timestamps, xattr, ACL probes |
| `consistency/*` | ~462 | **fxcp-core** | Core: atomic exchange, journal, serialization barriers, WAL. No daemon deps |
| `constants.rs` | ~100 | **fxcp-core** | Core: shared tuning constants including `ONE_SHOT_MODE` |
| `governor.rs` | ~238 | **fxcp-core** | Core: system load monitoring via PSI + load avg. Used by SmartCopier for I/O pacing |
| `error.rs` | ~45 | **fxcp-core** | Core: `FxcpError` (IO, System, Security, Versioning, IouPush, etc.) |
| `metrics.rs` (subset) | new | **fxcp-core** | Core: copy-plane metrics only (~20 counters) |
| `tui/explorer.rs` | ~279 | **fxcp** | Binary: dual-pane file navigator for source/dest selection |
| `tui/setup.rs` | ~353 | **fxcp** | Binary: interactive setup wizard |
| `bpf.rs` | ~443 | **foxingd** | Daemon: eBPF skeleton, kprobe attachment, ring buffer. Requires libbpf-rs |
| `event.rs` | ~176 | **foxingd** | Daemon: 24 event types from eBPF, sharded event queues |
| `worker.rs` | ~866 | **foxingd** | Daemon: event processing loop. Bridges events → SmartCopier calls |
| `mirror.rs` | ~429 | **foxingd** | Daemon: Manager, SourceInfo, multi-source orchestration |
| `ordering.rs` | ~439 | **foxingd** | Daemon: ReorderBuffer, per-device sequence tracking, gap detection |
| `identity.rs` | ~491 | **foxingd** | Daemon: ShardedInodeMap (64-shard LRU), inode→path resolution |
| `identity_watch.rs` | ~180 | **foxingd** | Daemon: ProactiveIndex, inotify reverse index |
| `projector.rs` | ~219 | **foxingd** | Daemon: event projection onto identity maps |
| `tuner.rs` | ~668 | **foxingd** | Daemon: BBR-inspired adaptive tuning, GLOBAL_TUNER_REGISTRY |
| `resilience.rs` | ~129 | **foxingd** | Daemon: PoisonCabinet, CircuitBreaker, FailureState |
| `columnar.rs` | ~119 | **foxingd** | Daemon: columnar event batch storage for coalescing |
| `hydration.rs` | ~602 | **foxingd** | Daemon: catch-up sync orchestration, HydrationQueue |
| `hydration_worker.rs` | ~997 | **foxingd** | Daemon: background sync workers (consumes fxcp-core SmartCopier) |
| `config.rs` | ~463 | **foxingd** | Daemon: TOML config with multi-source/target. Subset types may be shared |
| `metrics.rs` | ~427 | **foxingd** | Daemon: re-exports fxcp-core metrics + 80 daemon-specific counters |
| `api.rs` | ~65 | **foxingd** | Daemon: SystemStatus, HTTP API via Axum |
| `tui.rs` | ~851 | **foxingd** | Daemon: dashboard, targets, versions, debug pages |
| `bpf/mirror.bpf.c` | ~900 | **foxingd** | Daemon: kernel eBPF program (23 VFS hooks) |

---

## 5. Interface Contract: foxingd → fxcp-core

### 5.1 Primary API Surface

The boundary is the `OptimizedFs` trait and `SmartCopier` struct in `operations.rs`:

```rust
// fxcp-core public API
pub struct SmartCopier { /* io_uring ring, buffer_pool, capabilities, governor */ }

impl SmartCopier {
    pub async fn optimized_copy(src, dst, file_size, label, buf_limit, skip_fsync) -> Result<CopyStats>;
    pub async fn optimized_copy_range(src, dst, offset, length, file_size, ...) -> Result<CopyStats>;
    pub async fn optimized_truncate(dst, size) -> Result<CopyStats>;
    pub async fn optimized_rename(src, dst, flags) -> Result<CopyStats>;
    pub async fn optimized_fallocate(dst, mode, offset, length) -> Result<CopyStats>;
}

pub fn probe_capabilities(path: &Path) -> Arc<Capabilities>;
pub fn probe_reflink_support(target_root: &Path) -> bool;
pub fn determine_copy_strategy(source_caps, target_caps) -> CopyStrategy;
```

### 5.2 Types Crossing the Boundary

| Type | Direction | Purpose |
|------|-----------|---------|
| `CopyStats` | fxcp-core → foxingd | Return value from copy operations (bytes, duration, ops count) |
| `Capabilities` | fxcp-core → foxingd | Filesystem feature detection results |
| `CopyStrategy` | Internal to fxcp-core | Reflink vs StandardCopy decision |
| `BufferPool` | foxingd → fxcp-core | Constructed by foxingd, passed to SmartCopier |
| `SyncSignature` | fxcp-core → foxingd | Dirty flag coordination via xattr sidecars |
| `Governor` | foxingd → fxcp-core | Constructed by foxingd, `Arc<Governor>` passed to SmartCopier |
| `FxcpError` | fxcp-core → foxingd | Error propagation (wrapped by `FoxingError`) |
| `TargetProfile` | Shared | NVMe/SSD/HDD/Network/NFS/SdCard/Auto — drives strategy selection |
| `FileVersion` | fxcp-core → foxingd | Version snapshot metadata |

### 5.3 Coordination Mechanisms

**1. Dirty Flags (Crash Consistency)**

The crash consistency model [UNC crash-consistency.pdf] uses xattr-based intent journaling:

- **foxingd** sets `user.foxing.dirty = true` via `sidecar::set_dirty_blind()` before writing
- **fxcp-core** clears the flag via `sidecar::remove_metadata()` after successful atomic commit
- On crash recovery, `sidecar::is_dirty()` triggers re-sync from source

Both read/write functions live in `fxcp-core/sidecar.rs`. No interface change needed.

**2. Sync Signatures (Change Detection)**

- **fxcp-core** computes BLAKE3 hashes and stores them in xattr sidecars via `set_sync_signature()`
- **foxingd** hydration workers read them via `get_sync_signature()` to skip unchanged files
- Both functions in `fxcp-core/sidecar.rs`

**3. Serialization Barriers (Per-Inode Ordering)**

`consistency/serialization.rs` provides `SerializationEngine` with ticket-based barriers and `WalGuard` RAII. foxingd's `worker.rs` calls `acquire_barrier()` to serialize operations on the same inode. Since the engine lives in fxcp-core, foxingd constructs and uses it naturally.

**4. Governor Feedback (System Load Pacing)**

foxingd constructs `Governor`, passes `Arc<Governor>` to both `SmartCopier` and `Hydrator::pace_hydration()`. Governor lives in fxcp-core; the dependency direction stays clean.

### 5.4 Data Flow

```
                    foxingd                              fxcp-core
    ┌──────────────────────────────────┐    ┌─────────────────────────────┐
    │  eBPF Kprobes (23 VFS hooks)     │    │                             │
    │         │                        │    │                             │
    │         ▼                        │    │                             │
    │  Ring Buffer → event.rs          │    │                             │
    │         │                        │    │                             │
    │         ▼                        │    │                             │
    │  ordering.rs (ReorderBuffer)     │    │                             │
    │         │                        │    │                             │
    │         ▼                        │    │                             │
    │  identity.rs (inode → path)      │    │                             │
    │         │                        │    │                             │
    │         ▼                        │    │                             │
    │  worker.rs (coalesce + dispatch) │    │                             │
    │         │                        │    │                             │
    │         │  SmartCopier::copy()   │    │  ┌───────────────────────┐  │
    │         ╰──────────────────────────────▶ │  operations.rs        │  │
    │                                  │    │  │  (io_uring pipeline)  │  │
    │                                  │    │  │  (reflink / fallback) │  │
    │                                  │    │  │  (atomic rename)      │  │
    │  tuner.rs ◀── CopyStats ─────────────── │  → CopyStats return   │  │
    │                                  │    │  └───────────────────────┘  │
    │                                  │    │                             │
    │  hydration_worker.rs ────────────────▶│  SmartCopier (catch-up)    │
    │                                  │    │                             │
    └──────────────────────────────────┘    └─────────────────────────────┘
```

---

## 6. Dependency Management

### 6.1 fxcp-core (library)

```toml
[dependencies]
tokio = { version = "1.48", features = ["full", "signal", "time", "fs"] }
tokio-util = "0.7"
futures = "0.3"
io-uring = "0.7"                    # Async I/O engine
blake3 = "1.8"                      # Content verification
nix = { version = "0.27", features = ["fs", "mount", "signal", "process", "user", "ioctl"] }
libc = "0.2"                        # Syscall interface
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
bincode = "1.3"                     # Binary metadata serialization
xattr = "1.6"                       # Extended attribute operations
uuid = { version = "1.18", features = ["v4"] }
walkdir = "2.5"                     # Directory traversal
chrono = { version = "0.4", features = ["serde"] }
glob = "0.3"
regex = "1.12"
anyhow = "1.0"
thiserror = "1.0"
tracing = "0.1"
parking_lot = "0.12"
dashmap = { version = "5.5", features = ["rayon"] }
lazy_static = "1.5"
lru = "0.14"
rayon = "1.11"                      # Parallel iteration
crossbeam = "0.8"                   # Concurrency primitives
hex = "0.4"
fxhash = "0.2"
prometheus = { version = "0.13", features = ["process"] }
sysinfo = "0.37"                    # Governor: system load monitoring
rand = "0.9"
```

**Not included:** libbpf-rs, libbpf-sys, notify, axum, tower-http, reqwest, ratatui, crossterm, clap

### 6.2 fxcp (binary) — adds to fxcp-core

```toml
[dependencies]
fxcp-core = { path = "../fxcp-core" }
clap = { version = "4.5", features = ["derive"] }
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
toml = "0.8"
ratatui = "0.26"
crossterm = { version = "0.27", features = ["event-stream"] }
comfy-table = "7.2"
```

### 6.3 foxingd (binary) — adds to fxcp-core

```toml
[dependencies]
fxcp-core = { path = "../fxcp-core" }
libbpf-rs = "0.26.0-beta.1"        # eBPF runtime
libbpf-sys = "1.5"                  # eBPF syscall bindings
notify = "6.1"                      # inotify watcher for ProactiveIndex
axum = "0.7"                        # HTTP API server
tower-http = { version = "0.6", features = ["trace"] }
reqwest = { version = "0.11", features = ["blocking", "json"] }
clap = { version = "4.5", features = ["derive"] }
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
toml = "0.8"
ratatui = "0.26"
crossterm = { version = "0.27", features = ["event-stream"] }
comfy-table = "7.2"

[build-dependencies]
libbpf-cargo = "0.26.0-beta.1"     # BPF skeleton generation
```

### 6.4 Dependency Weight Impact

- **fxcp binary:** ~15 fewer crates than foxingd (no libbpf-rs/sys, notify, axum, tower-http, reqwest)
- **fxcp-core library:** No libbpf, no axum, no ratatui/crossterm. Pure data-plane library consumable by any Rust project
- **foxingd:** Same total weight as current foxing (no regression)

---

## 7. Error Type Split

```rust
// fxcp-core/src/error.rs
#[derive(Error, Debug)]
pub enum FxcpError {
    #[error("IO: {0}")] Io(#[from] std::io::Error),
    #[error("System: {0}")] System(#[from] nix::Error),
    #[error("Security: {0}")] Security(String),
    #[error("Versioning: {0}")] Versioning(String),
    #[error("CString Nul: {0}")] Nul(#[from] std::ffi::NulError),
    #[error("IOUring Push: {0}")] IouPush(String),
    #[error("JSON: {0}")] Json(#[from] serde_json::Error),
    #[error("UTF-8: {0}")] Utf8(#[from] std::string::FromUtf8Error),
    #[error("Join: {0}")] Join(#[from] tokio::task::JoinError),
    #[error("Memory Exhausted: {0}")] MemoryExhausted(String),
    #[error("{message} (correlation: {correlation_id})")]
    Traced { correlation_id: u64, message: String },
}
pub type Result<T> = std::result::Result<T, FxcpError>;

// foxingd/src/error.rs
#[derive(Error, Debug)]
pub enum FoxingError {
    #[error("Copy Engine: {0}")] Core(#[from] fxcp_core::FxcpError),
    #[error("Config: {0}")] Config(String),
    #[error("BPF: {0}")] Bpf(String),
    #[error("Libbpf: {0}")] Libbpf(#[from] libbpf_rs::Error),
}
```

---

## 8. Migration Strategy

### Phase 0: Preparation (Non-Breaking)

Reduce coupling within the monolith without changing workspace structure:

1. Audit `crate::metrics` usage in copy-plane modules — confirm they only reference copy-plane metrics
2. Ensure `Governor` has no daemon-specific dependencies (confirmed: only `sysinfo`, `parking_lot`, `metrics`, `constants`)
3. Extract `SysSpecs` system detection from `config.rs` into a location accessible to both crates
4. Add `#[cfg(feature = "daemon")]` markers to metrics that are daemon-specific (informational, not functional)

### Phase 1: Create Workspace Structure

1. Create workspace `Cargo.toml` at root
2. Create `fxcp-core/` and move identified modules
3. Create `FxcpError` (subset of `FoxingError`)
4. Create copy-plane `metrics.rs` (~20 counters)
5. Update foxingd imports: `use fxcp_core::operations::*;`
6. foxingd `metrics.rs` re-exports fxcp-core metrics + adds daemon-specific ones
7. `build.rs` (BPF compilation) moves to foxingd only
8. All existing tests must pass

### Phase 2: Create fxcp Binary

1. Create `fxcp/` with binary crate
2. Migrate `Commands::Sync` code path from current `main.rs` into `fxcp/src/main.rs`
3. Implement `fxcp SOURCE DEST` CLI with clap
4. Add `--verify`, `--snapshot`, `--archive/-a`, `--recursive/-r` flags
5. Move `tui/explorer.rs` and `tui/setup.rs` to `fxcp/src/tui/`
6. Test independently: `cargo build -p fxcp` must compile without libbpf

### Phase 3: Stabilize and Document

1. Add `fxcp-core` rustdoc with public API documentation
2. Mark internal types as `pub(crate)` where appropriate
3. Add integration tests per crate
4. Update CI to build fxcp independently (validates no BPF dependency leak)
5. Update README.md with workspace structure
6. Consider `fxcp-core` on crates.io (future)

---

## 9. Core Design: BLAKE3 Merkle Tree Delta Engine

### 9.1 Motivation

A core design goal of the fxcp split is to elevate delta/hash functionality from the current basic implementation (linear BLAKE3 hashing with head+tail sampling) into a proper **Merkle tree-based delta detection and transfer engine**. This positions fxcp not merely as a "copy tool" but as a content-addressable, delta-aware replication primitive — analogous to how rsync uses rolling checksums for delta transfer, but built on modern cryptographic foundations.

The current codebase already has the scaffolding:
- `hashing.rs` provides `hash_file_lite()` (head+tail sampling) and `hash_file_full()` (linear hash)
- `SyncSignature` (sidecar.rs) already includes `merkle_root: Option<String>` and `chunk_size: Option<u64>` fields (schema v3)
- The xattr sidecar system (`user.foxing.sig`) persists signatures between runs

What's missing is the actual Merkle tree structure, per-chunk hash computation, and delta-aware transfer that compares trees to identify changed blocks.

### 9.2 BLAKE3 Merkle Tree Design

BLAKE3 [blake3.io] is inherently a Merkle tree internally — it chunks input into 1024-byte segments and builds a binary hash tree. The fxcp Merkle tree design should leverage this at a **file-block granularity** suitable for storage I/O:

```
File (N bytes)
├── Chunk 0 [0..chunk_size)        → BLAKE3 hash₀
├── Chunk 1 [chunk_size..2*chunk_size) → BLAKE3 hash₁
├── Chunk 2 [2*chunk_size..3*chunk_size) → BLAKE3 hash₂
│   ...
└── Chunk K [(K-1)*chunk_size..N)  → BLAKE3 hashₖ

Merkle Tree:
          root_hash
         /         \
    hash(h₀‖h₁)   hash(h₂‖h₃)
    /     \         /     \
   h₀     h₁      h₂     h₃  ...  hₖ
```

**Chunk size selection:** The chunk size should align with filesystem block boundaries for efficient I/O. Default: 64KB (`CHUNK_SIZE` in current `hashing.rs`), configurable per storage profile:

| Profile | Chunk Size | Rationale |
|---------|-----------|-----------|
| NVMe | 64KB | Matches NVMe optimal I/O size, fine-grained delta detection |
| SSD | 128KB | Balance between granularity and tree overhead |
| HDD | 1MB | Minimize seeks; larger sequential reads amortize rotational latency |
| Network/NFS | 256KB | Match NFS `rsize`/`wsize` defaults, reduce RPC round-trips |

### 9.3 Architecture in fxcp-core

The Merkle tree engine belongs in `fxcp-core/src/hashing.rs` (expanded) with supporting types:

```rust
// fxcp-core/src/hashing.rs (expanded)

/// Per-chunk hash leaf in the Merkle tree
pub struct ChunkHash {
    pub offset: u64,
    pub length: u32,
    pub hash: blake3::Hash,
}

/// Complete Merkle tree for a file
pub struct MerkleTree {
    pub chunk_size: u64,
    pub file_size: u64,
    pub root: blake3::Hash,
    pub leaves: Vec<ChunkHash>,     // Leaf hashes (per-chunk)
    pub levels: Vec<Vec<blake3::Hash>>,  // Internal tree nodes (optional, for partial verification)
}

impl MerkleTree {
    /// Build a Merkle tree from a file, hashing each chunk with BLAKE3
    pub fn from_file(path: &Path, chunk_size: u64) -> Result<Self>;

    /// Build from an io_uring read pipeline (zero-copy where possible)
    pub async fn from_file_async(path: &Path, chunk_size: u64, pool: &BufferPool) -> Result<Self>;

    /// Compare two trees, returning the byte ranges that differ
    pub fn diff(source: &MerkleTree, target: &MerkleTree) -> Vec<DirtyRange>;

    /// Serialize tree metadata for xattr/sidecar storage
    pub fn to_signature(&self) -> MerkleSignature;

    /// Reconstruct from stored signature (leaves only, no file re-read)
    pub fn from_signature(sig: &MerkleSignature) -> Self;
}

/// A contiguous range of bytes that differs between source and target
pub struct DirtyRange {
    pub offset: u64,
    pub length: u64,
}

/// Compact serializable form for xattr storage
pub struct MerkleSignature {
    pub root: [u8; 32],
    pub chunk_size: u64,
    pub file_size: u64,
    pub leaf_hashes: Vec<[u8; 32]>,  // Ordered by chunk offset
}
```

### 9.4 Delta Transfer Flow

**fxcp standalone (one-shot sync):**

```
1. Source: Build MerkleTree for source file
2. Target: Read MerkleSignature from xattr sidecar (if exists)
   └── If no signature: fall back to full copy
3. Compare: MerkleTree::diff(source_tree, target_tree)
   └── Returns Vec<DirtyRange> — only the chunks that changed
4. Transfer: For each DirtyRange:
   ├── If reflink-capable: FICLONERANGE for unchanged chunks + copy dirty chunks
   ├── If io_uring: pipelined read+write of dirty ranges only
   └── If fallback: pread/pwrite at each dirty offset
5. Verify: Recompute target chunk hashes for transferred ranges
6. Commit: Atomic rename + store updated MerkleSignature in xattr sidecar
```

**Delta detection cost:** For a 1GB file with 64KB chunks, the Merkle tree has 16,384 leaves. The leaf hashes consume 512KB (16,384 × 32 bytes). The root computation is O(n) single-pass. This is stored in the xattr sidecar via `user.foxing.merkle` (if it fits, typically up to 64KB for xattr) or in the JSON sidecar fallback for larger files.

### 9.5 Integration with foxingd

foxingd leverages the Merkle tree through two mechanisms:

**1. Sidecar-Stored Merkle Signatures**

When foxingd's worker completes a write via `SmartCopier`, it stores the Merkle signature in the target's xattr sidecar:

```
user.foxing.sig     → SyncSignature (size, mtime, lite hash, merkle root, chunk_size)
user.foxing.merkle  → MerkleSignature (serialized leaf hashes)
user.foxing.dirty   → dirty flag (set before write, cleared after)
```

The xattr namespace hierarchy:
- `trusted.foxing.*` — preferred (tamper-resistant, requires CAP_SYS_ADMIN)
- `user.foxing.*` — fallback (any user can read/write)
- `.{filename}.foxing_meta` — JSON sidecar fallback (when xattrs unsupported)

**2. Event-Driven Partial Merkle Updates**

When foxingd receives a `WriteRange` event from eBPF (with offset and length), it can perform a **partial Merkle tree update** instead of rehashing the entire file:

```
eBPF event: Write(inode=42, offset=131072, length=32768)
→ Affected chunks: [chunk 2] (offset 128KB, size 64KB)
→ Rehash only chunk 2
→ Recompute tree path from leaf 2 to root
→ Update stored MerkleSignature with new leaf hash and root
```

This is O(log n) per write event rather than O(n) for full rehash, making it viable for the continuous replication use case. The partial update flow:

```rust
impl MerkleTree {
    /// Update specific chunks after a known write range
    /// Returns the new root hash without re-reading unchanged chunks
    pub fn update_range(
        &mut self,
        path: &Path,
        offset: u64,
        length: u64,
    ) -> Result<blake3::Hash>;
}
```

**3. Hydration with Delta Detection**

When foxingd's hydration worker catches up after a gap (e.g., target was disconnected), it uses Merkle tree comparison instead of full-file copy:

```
1. Read source MerkleTree (compute fresh or from cache)
2. Read target MerkleSignature from xattr sidecar
3. MerkleTree::diff() → only dirty chunks
4. SmartCopier::optimized_copy_range() for each dirty chunk
5. Update target MerkleSignature
```

This reduces catch-up bandwidth from O(file_size) to O(changed_bytes), which is the key efficiency gain over rsync's approach of checksumming during transfer.

### 9.6 Comparison with rsync Delta Transfer

| Aspect | rsync | fxcp Merkle |
|--------|-------|-------------|
| **Hash algorithm** | MD5 (rolling checksum + strong hash) | BLAKE3 (cryptographic, parallelizable) |
| **Granularity** | Variable block size (512-8192 bytes) | Fixed chunk aligned to storage blocks (64KB-1MB) |
| **Tree structure** | Flat list of block checksums | Binary Merkle tree with O(log n) updates |
| **State persistence** | None (recomputed every sync) | Stored in xattr sidecars (amortized cost) |
| **Delta direction** | Receiver computes checksums, sender matches | Both sides store trees, diff is local comparison |
| **Partial update** | Not possible (full checksum list each time) | O(log n) tree update on known write range |
| **Parallelism** | Single-threaded checksum computation | BLAKE3 leverages SIMD (AVX2/AVX-512) + rayon parallel chunking |
| **Integration with CoW** | None | FICLONERANGE for unchanged chunks, copy only dirty ranges |

### 9.7 xattr Sidecar Schema Evolution

The expanded sidecar schema (version 4) for Merkle tree support:

```
user.foxing.sig     → SyncSignature v4:
  {
    "size": u64,
    "mtime_sec": i64,
    "mtime_nsec": i64,
    "hash": Option<String>,           // BLAKE3 lite hash (head+tail)
    "merkle_root": Option<String>,    // BLAKE3 Merkle root
    "chunk_size": Option<u64>,        // Chunk boundary (default 65536)
    "leaf_count": Option<u32>,        // Number of Merkle leaves
    "version": 4                      // Schema version
  }

user.foxing.merkle  → MerkleSignature (bincode-serialized):
  {
    "root": [u8; 32],
    "chunk_size": u64,
    "file_size": u64,
    "leaf_hashes": Vec<[u8; 32]>      // Ordered leaf hashes
  }

user.foxing.dirty   → [0x01] or absent
```

For files too large for xattr storage (xattr value limit is typically 64KB, which holds ~2000 leaf hashes = ~128MB file at 64KB chunks), the Merkle signature falls back to the JSON sidecar file. For files larger than ~128MB, a **sparse Merkle tree** stores only the top N levels in xattr, with full leaf hashes in the sidecar file.

### 9.8 Implementation Priority

This Merkle tree engine is a **Phase 2** deliverable — after the workspace split (Phase 1) establishes the crate boundaries. The implementation order:

1. **Expand `hashing.rs`** with `MerkleTree`, `ChunkHash`, `DirtyRange`, `MerkleSignature` types
2. **Update `SyncSignature`** to v4 with `leaf_count` field
3. **Add `user.foxing.merkle`** xattr key to sidecar.rs
4. **Implement `MerkleTree::diff()`** for delta detection
5. **Wire into `SmartCopier`** — use `DirtyRange` list to drive `optimized_copy_range()` calls instead of full-file copy
6. **Wire into foxingd hydration** — replace full-file catch-up with Merkle-diff catch-up
7. **Implement `MerkleTree::update_range()`** — partial tree updates for foxingd's `WriteRange` events
8. **Add io_uring async hashing** — `from_file_async()` using registered buffers for zero-copy hash computation

---

## 10. SD Card, NAND Flash, and eMMC Considerations

### 10.1 The NAND Problem

NAND flash storage (SD cards, eMMC, USB flash drives, low-end SSDs) presents fundamentally different I/O characteristics from rotating media or enterprise NVMe. fxcp must be NAND-aware because it is a primary deployment target — replicating data *to* portable flash media for backup, archival, or offline transport is a core use case.

**Key NAND constraints:**

| Constraint | Impact on fxcp |
|-----------|----------------|
| **Write amplification** | NAND cannot overwrite in-place. Each logical write triggers an erase-program cycle on a larger erase block (typically 512KB-4MB). Random small writes cause catastrophic amplification (10x-50x) |
| **Erase block alignment** | Writes crossing erase block boundaries trigger two erase cycles instead of one |
| **Wear leveling** | Flash cells have finite P/E (program/erase) cycles (1K-100K depending on MLC/TLC/QLC). Uneven write patterns accelerate wear on hot blocks |
| **Write cliff** | Performance degrades sharply when the FTL (Flash Translation Layer) runs out of pre-erased blocks, triggering synchronous garbage collection |
| **No hardware atomicity** | Consumer flash lacks the power-loss protection of enterprise NVMe. Interrupted writes corrupt data at the page level |
| **TRIM/DISCARD** | Informing the FTL about freed blocks enables proactive garbage collection and reduces write amplification |

### 10.2 fxcp NAND-Aware I/O Strategy

**Write Coalescing:** The current `SdCard` profile in `constants.rs` already sets conservative batch limits (`HYDRATION_BATCH_LIMITS_SDCARD: (128, 1024)`). fxcp-core should extend this with:

```
fxcp-core NAND strategy:
1. Coalesce small writes into large sequential bursts (≥ erase block size)
2. Align write offsets to 4KB boundaries (FTL page size)
3. Issue TRIM/DISCARD after file deletion (fadvise + fallocate PUNCH_HOLE)
4. Prefer sequential write patterns over random (sort copy queue by offset)
5. Use F2FS atomic writes where available (existing support in operations.rs)
6. Limit concurrent writers to 1-2 (reduce FTL contention / GC pressure)
7. Detect write cliff via latency spike monitoring (tuner.rs StorageClass)
```

**F2FS Integration (existing):** The codebase already supports F2FS atomic writes via `F2FS_IOC_START_ATOMIC_WRITE` / `F2FS_IOC_COMMIT_ATOMIC_WRITE` (operations.rs lines 46-50, 685-842). F2FS is the optimal filesystem for NAND targets because it:
- Implements log-structured writing (sequential only, no in-place overwrites)
- Maintains hot/warm/cold data separation (reduces GC overhead)
- Supports multi-stream writes (aligns to NAND allocation units)
- Provides native TRIM support via `discard` mount option

**TRIM/DISCARD Support (new for fxcp-core):** When fxcp deletes files on a target or punches holes in sparse files, it should issue `FITRIM` or `fallocate(FALLOC_FL_PUNCH_HOLE)` to inform the FTL:

```rust
// fxcp-core/src/operations.rs (proposed addition)
pub fn issue_trim(path: &Path) -> Result<()> {
    // After file deletion or hole punch, issue FITRIM to inform FTL
    // Only on filesystems that support it (F2FS, ext4, XFS on flash)
}
```

**Merkle Tree Impact:** On NAND targets, chunk size should be large (≥128KB, matching or exceeding FTL page groups) to reduce write amplification during delta transfers. The per-profile chunk size table in Section 9.2 should be respected: `SdCard → 128KB` minimum.

### 10.3 eMMC-Specific Considerations

eMMC (embedded MultiMediaCard) is used in single-board computers (Raspberry Pi, ODROID, etc.) and IoT devices — another core fxcp deployment target. eMMC adds:

- **Command queuing depth = 1** (unlike NVMe's 64K queues). fxcp must serialize I/O and avoid io_uring queue depth > 1
- **Partition switching latency** (hardware partitions, not filesystem partitions). Avoid cross-partition copies
- **Boot partition write protection** — fxcp should detect and skip write-protected eMMC boot partitions
- **RPMB (Replay Protected Memory Block)** — authenticated secure storage partition, not a copy target

### 10.4 foxingd Integration

When foxingd targets a NAND device:
- The tuner should detect `StorageClass::SdCard` or `StorageClass::NandFlash` (proposed extension to the enum)
- Governor throttling should be more aggressive (NAND GC stalls affect the entire device, not just one partition)
- Write coalescing windows should be wider (accumulate more changes before flushing)
- Hydration should use single-threaded sequential mode (`HydrationMode::PrioritizeStructure`) to minimize random writes

---

## 11. The Storage Layer Cake: Stratis, LVM, and Device-Mapper Stacks

### 11.1 The Layer Cake Problem

Modern Linux storage is built on composable device-mapper (dm) targets stacked into pipelines. A single target path may traverse multiple layers, each adding overhead, alignment constraints, and behavioral characteristics that fxcp must understand:

```
Application I/O
    │
    ▼
┌─────────────────────────┐
│  Filesystem (XFS/Btrfs)  │  ← fxcp operates here
├─────────────────────────┤
│  dm-integrity            │  ← per-sector checksums (4KB → 4KB+tag overhead)
├─────────────────────────┤
│  dm-crypt (LUKS)         │  ← encryption (AES-XTS, sector-aligned)
├─────────────────────────┤
│  dm-cache / bcache       │  ← SSD cache tier fronting HDD
├─────────────────────────┤
│  LVM (dm-linear/striped) │  ← logical volume management
├─────────────────────────┤
│  dm-thin                 │  ← thin provisioning + snapshots
├─────────────────────────┤
│  kvdo                    │  ← deduplication + compression
├─────────────────────────┤
│  Physical Block Device   │  ← NVMe / SSD / HDD / NAND
└─────────────────────────┘
```

**Stratis** [docs.redhat.com: Stratis file systems] automates this stack, composing a managed pool from:
- XFS filesystem (mandatory, top layer)
- dm-thin (thin provisioning, snapshots)
- dm-cache (optional SSD cache tier)
- dm-integrity (optional data integrity)
- dm-crypt (optional encryption)

Each layer imposes I/O transformation that affects fxcp's copy strategy:

### 11.2 Layer Detection in fxcp-core

fxcp-core should probe the dm stack beneath a target path to adapt its I/O strategy. Detection methods:

```rust
// fxcp-core/src/operations.rs (proposed additions to probe_capabilities)

/// Detect device-mapper layers beneath a filesystem mount
pub struct DmStackInfo {
    pub has_integrity: bool,     // dm-integrity present
    pub has_crypt: bool,         // dm-crypt/LUKS present
    pub has_cache: bool,         // dm-cache/bcache present
    pub has_thin: bool,          // dm-thin (thin provisioning)
    pub has_vdo: bool,           // kvdo (dedup+compression)
    pub has_stratis: bool,       // Managed by Stratis
    pub integrity_tag_size: u32, // Bytes per sector for integrity tags
    pub crypt_sector_size: u32,  // Crypto sector size (512 or 4096)
    pub thin_pool_usage: f64,    // Thin pool utilization percentage
    pub cache_hit_rate: f64,     // dm-cache hit rate (if available)
    pub stack_depth: u8,         // Number of dm layers
}

impl DmStackInfo {
    /// Probe by reading /sys/block/*/dm/uuid and walking the dm table
    pub fn probe(mount_point: &Path) -> Result<Self>;
}
```

**Detection approaches:**
1. **`/sys/block/<dev>/dm/uuid`** — dm UUID prefix indicates type: `CRYPT-`, `INTEGRITY-`, `VDO-`, `LVM-`, `mpath-`
2. **`dmsetup table <dev>`** — Shows the dm target type and parameters for each layer
3. **`/sys/block/<dev>/dm/name`** — Stratis naming convention: `stratis-1-private-<pool>-<vol>`
4. **`/proc/mounts`** — Mount options reveal `discard`, `data=ordered`, etc.
5. **`statfs()` magic** — Already used for XFS/Btrfs/F2FS detection; extend for layer awareness
6. **`/sys/block/<dev>/queue/`** — Optimal I/O size, minimum I/O size, physical block size

### 11.3 dm-integrity

**What it does:** Adds per-sector integrity tags (checksums or MACs) stored out-of-band. Every write includes a tag; every read verifies it. Protects against silent data corruption (bit rot).

**Impact on fxcp:**
- **Write amplification:** Each 4KB data write also writes a 4-80 byte tag. For small random writes, this is negligible. For bulk sequential writes, the tag overhead is amortized
- **Alignment:** Writes must be aligned to the integrity sector size (typically 4KB). Unaligned writes trigger read-modify-write cycles
- **Journal mode:** dm-integrity can journal writes for crash consistency. This doubles the write amplification but ensures tag+data atomicity
- **Read verification:** fxcp can rely on dm-integrity for block-level checksums and potentially skip its own BLAKE3 verification for bit-rot detection (though BLAKE3 still provides file-level semantic verification)

**fxcp adaptation:**
```
When dm-integrity detected:
1. Align all writes to integrity_tag_size boundaries (min 4KB)
2. Prefer large sequential writes (amortize tag overhead)
3. Consider disabling fxcp-level block checksums (dm-integrity already provides them)
4. Merkle tree chunk_size must be ≥ integrity sector size
5. Account for ~1-3% capacity overhead in space calculations
```

### 11.4 dm-verity

**What it does:** Read-only integrity verification using a Merkle tree of block hashes. Used for verified boot (Android dm-verity), immutable container images, and read-only system partitions.

**Impact on fxcp:**
- dm-verity targets are **read-only by design**. fxcp cannot write to a dm-verity device
- fxcp can *read from* a dm-verity source with confidence — the kernel guarantees block-level integrity
- When the *source* is dm-verity protected, fxcp can skip its own BLAKE3 verification (kernel already guarantees integrity)

**fxcp adaptation:**
```
When dm-verity detected on target:
  → Error: "Target is dm-verity protected (read-only). Cannot write."
When dm-verity detected on source:
  → Skip BLAKE3 verification on read (kernel provides block integrity)
  → Log: "Source is dm-verity protected. Block integrity guaranteed by kernel."
```

### 11.5 dm-cache and bcache

**What it does:** Places a fast SSD/NVMe cache tier in front of a slow HDD pool. Frequently accessed blocks are promoted to the cache; cold blocks are demoted to the backing store.

Common configurations:
- **dm-cache** (kernel, managed by LVM or Stratis) — writeback or writethrough modes
- **bcache** (kernel, standalone) — similar but different device model
- **lvmcache** (LVM integration with dm-cache)

**Impact on fxcp:**
- **Writeback mode:** Writes hit fast cache first, then lazily migrate to backing HDD. fxcp sees NVMe-class latency initially but may hit cache eviction storms during large bulk copies
- **Writethrough mode:** Writes go to both cache and backing store. Latency is bounded by the slower device
- **Cache thrashing:** Bulk sequential writes from fxcp may evict valuable cached data. This is the "cache pollution" problem

**fxcp adaptation:**
```
When dm-cache detected:
1. If writethrough: tune for HDD latency (backing store is the bottleneck)
2. If writeback: start with aggressive tuning but watch for latency spikes
   (indicates cache pressure / eviction storms)
3. Use RWF_UNCACHED (Linux 6.14+) or O_DIRECT to bypass page cache
   and reduce cache pollution during bulk hydration
4. Limit concurrent I/O to avoid saturating cache → HDD migration bandwidth
5. Tuner should detect cache_hit_rate drops and throttle proactively
```

### 11.6 LVM Thin Provisioning

**What it does:** dm-thin provides logical volumes that start at zero allocation and grow on demand from a shared pool. Supports instant snapshots via copy-on-write at the block level.

**Impact on fxcp:**
- **Overprovisioning:** The thin pool can be larger than physical storage. fxcp must check *pool* free space, not *volume* free space
- **Snapshot interaction:** Writing to a thin volume with snapshots triggers CoW at the block layer (in addition to any filesystem-level CoW like Btrfs reflink). Double CoW amplification
- **Zeroed blocks:** Newly allocated thin blocks are zero-filled by default. fxcp's sparse file detection (`FIEMAP` + `FALLOC_FL_PUNCH_HOLE`) interacts with thin provisioning — punching holes returns blocks to the pool
- **Metadata exhaustion:** Thin pool metadata can run out before data space. fxcp should monitor both data and metadata usage

**fxcp adaptation:**
```
When dm-thin detected:
1. Check thin pool data AND metadata usage (lvs --noheadings -o data_percent,metadata_percent)
2. TRIM/DISCARD support is critical — issue discard on deleted files to return blocks to pool
3. Prefer large sequential writes to minimize thin block allocation overhead
4. Warn if thin pool is >85% full (risk of pool exhaustion → I/O errors)
5. If target has thin snapshots, account for CoW amplification in space estimates
```

### 11.7 Stratis-Managed Volumes

**Stratis** [fedoramagazine.org: Getting started with Stratis] composes the above layers into a managed storage pool. fxcp should detect Stratis-managed volumes (via `/sys/block/*/dm/uuid` prefix `stratis-`) and apply the combined strategies:

```
Stratis stack = XFS + dm-thin + [dm-cache] + [dm-integrity] + [dm-crypt]

fxcp Stratis strategy:
1. Use XFS-specific optimizations (reflink, atomic exchange, FICLONE)
2. Apply thin provisioning awareness (pool space monitoring, DISCARD)
3. If dm-cache present: RWF_UNCACHED for bulk writes, respect cache tier
4. If dm-integrity present: align to integrity sector boundaries
5. If dm-crypt present: align to crypto sector boundaries (see Section 13)
6. Report combined stack depth and layer overhead in metrics
```

### 11.8 foxingd Integration with Layer Cake

foxingd should probe the dm stack at startup (via `DmStackInfo::probe()`) and propagate the information into:
- **TunerBoard** — adjust batch sizes and flush intervals based on stack depth and layer types
- **Governor** — tighter throttling when dm-cache or thin provisioning is near capacity
- **Metrics** — export `foxing_target_dm_stack_depth`, `foxing_target_thin_pool_usage`, `foxing_target_cache_hit_rate`
- **Hydration** — select `HydrationMode` based on cache presence (sequential for uncached HDD, parallel for cached)

---

## 12. kvdo: Kernel Virtual Data Optimizer

### 12.1 What kvdo Does

kvdo (kernel VDO) [docs.redhat.com: VDO] is a device-mapper target providing inline **deduplication** and **compression** at the block layer. It operates on 4KB blocks:

```
Logical Write (any size)
    │
    ▼
┌──────────────────────────┐
│  VDO Deduplication       │  ← BLAKE2 hash of each 4KB block
│  ├── Dedupe Index (UDS)  │  ← Universal Deduplication Service
│  └── If duplicate:       │  ← Point to existing physical block
│       skip physical write │
├──────────────────────────┤
│  VDO Compression (LZ4)   │  ← Compress non-duplicate blocks
│  └── Pack compressed     │  ← Multiple logical blocks → one physical
│       blocks together     │
├──────────────────────────┤
│  VDO Slab Allocator      │  ← Manages physical block allocation
└──────────────────────────┘
```

### 12.2 Existing VDO Support

The codebase already has VDO zero-block optimization in `operations.rs`:
- `is_block_zero()` — Detects zero-filled buffers using AVX512/AVX2 SIMD (lines 1912-1980)
- `vdo_optimization` config flag (config.rs) with `vdo_stall_threshold` tuning
- When a buffer is all-zeros, VDO deduplicates it to a single shared zero block at near-zero cost

### 12.3 Enhanced kvdo Awareness for fxcp-core

Beyond zero-block detection, fxcp should be kvdo-aware in several ways:

**4KB Block Alignment:** kvdo operates on fixed 4KB blocks. All writes should be aligned to 4KB boundaries. Unaligned writes cause read-modify-write at the VDO layer:

```
fxcp kvdo strategy:
1. Align all write offsets to 4KB boundaries
2. Pad final write to 4KB boundary (VDO will compress the padding)
3. Use 4KB as the minimum Merkle tree chunk granularity
4. Prefer large sequential writes (reduces UDS index lookup pressure)
```

**Deduplication-Aware Delta Transfer:** When transferring to a VDO target, fxcp's Merkle tree delta detection and VDO's deduplication are complementary:
- **fxcp Merkle diff** identifies which *file-level chunks* changed (64KB-1MB granularity)
- **VDO deduplication** identifies which *block-level 4KB blocks* are duplicates across the entire pool
- fxcp should write all dirty Merkle chunks and let VDO deduplicate at the block level — don't try to out-smart VDO's index

**Compression Awareness:** VDO uses LZ4 compression. fxcp should:
- **Not** pre-compress data before writing to a VDO target (double compression wastes CPU and may expand data)
- Detect VDO targets and disable any application-level compression in the copy pipeline
- Avoid `O_DIRECT` on VDO targets when possible (VDO benefits from page cache batching of small writes)

**Space Reporting:** VDO logical size can be much larger than physical size (overprovisioned). fxcp must check VDO *physical* free space:
```
vdostats --human-readable <vdo-device>
  → Used: 30% (physical), Savings: 65% (dedup + compression ratio)
```

### 12.4 VDO Stall Detection

The existing `vdo_stall_threshold` handles the case where VDO's UDS index lookup becomes a bottleneck. When write latency spikes above the threshold, fxcp should:
1. Reduce concurrent I/O depth (less UDS index pressure)
2. Increase write coalescing (fewer, larger writes = fewer index lookups)
3. Report `foxing_vdo_stall_detected` metric

### 12.5 Impact on Merkle Tree

For VDO targets:
- Merkle chunk size should be a multiple of 4KB (already satisfied by default 64KB)
- Zero-block chunks can be skipped entirely in delta transfer (VDO already has them)
- Leaf hashes in the Merkle signature help VDO by providing a pre-computed content fingerprint — though VDO uses BLAKE2 internally, not BLAKE3, so the hashes are not directly reusable

---

## 13. dm-crypt and LUKS

### 13.1 What dm-crypt Does

dm-crypt [gitlab.com/cryptsetup] provides transparent block-level encryption, most commonly configured via LUKS (Linux Unified Key Setup). Every block written passes through:

```
Plaintext I/O (from filesystem)
    │
    ▼
┌──────────────────────────┐
│  dm-crypt                │
│  ├── Cipher: AES-256-XTS │  ← Most common (hardware-accelerated via AES-NI)
│  ├── Sector size: 4096   │  ← LUKS2 default (512 for LUKS1)
│  ├── IV mode: plain64    │  ← Initialization vector per-sector
│  └── Key derivation:     │  ← Argon2id (LUKS2) or PBKDF2 (LUKS1)
│       LUKS header         │
└──────────────────────────┘
    │
    ▼
Ciphertext (to physical device)
```

### 13.2 Impact on fxcp I/O

**Crypto sector alignment:** dm-crypt encrypts in fixed sectors. LUKS2 defaults to 4096-byte sectors. Writes that are not aligned to the crypto sector size trigger a read-modify-encrypt-write cycle:

```
Unaligned write (e.g., 3000 bytes at offset 1000):
1. Read existing 4KB sector from disk
2. Decrypt sector
3. Modify bytes 1000-3999
4. Re-encrypt entire 4KB sector
5. Write back to disk
→ 1 read + 1 write amplification for every unaligned write
```

**fxcp adaptation:**
```
When dm-crypt detected:
1. Align all writes to crypt_sector_size (4096 for LUKS2, 512 for LUKS1)
2. Prefer large sequential writes (amortize crypto overhead)
3. io_uring submission should batch writes to maximize AES-NI pipeline utilization
4. Merkle tree chunk_size must be ≥ crypt_sector_size
5. Do NOT use O_DIRECT if filesystem is on dm-crypt with misaligned sector size
   (dm-crypt handles alignment internally, but O_DIRECT bypasses this)
```

### 13.3 Hardware Crypto Acceleration

Modern CPUs provide AES-NI (x86_64) or ARM CE (AArch64) for hardware-accelerated AES. When crypto acceleration is available, dm-crypt overhead is typically <5% for sequential I/O. Without hardware acceleration, overhead can reach 30-50%.

fxcp should detect crypto acceleration availability:
```rust
// fxcp-core: detect hardware crypto support
fn has_aes_acceleration() -> bool {
    #[cfg(target_arch = "x86_64")]
    { is_x86_feature_detected!("aes") }
    #[cfg(target_arch = "aarch64")]
    { /* check /proc/cpuinfo for "aes" in Features */ }
}
```

When no hardware acceleration is detected and the target is dm-crypt:
- Throttle I/O to avoid CPU saturation from software encryption
- Reduce concurrent io_uring queue depth (crypto is CPU-bound, not I/O-bound)
- Governor should monitor CPU pressure via PSI (`/proc/pressure/cpu`)

### 13.4 LUKS Header Awareness

The LUKS header occupies the first 2-16MB of the device (LUKS2 default: 16MB). fxcp should:
- Never attempt to copy the LUKS header as file data (it's below the filesystem)
- When doing block-level operations (e.g., cloning a LUKS partition), be aware of the header offset
- Report LUKS version in diagnostics for troubleshooting

### 13.5 Interaction with Other dm Layers

dm-crypt is frequently stacked with other layers:

| Stack | Behavior | fxcp Impact |
|-------|----------|-------------|
| **dm-crypt + dm-integrity** | Authenticated encryption (AEAD: AES-GCM or AEGIS). Integrity tags stored alongside ciphertext | Writes must satisfy both alignment constraints (crypto sector + integrity tag). Use `max(crypt_sector_size, integrity_sector_size)` for alignment |
| **dm-crypt + dm-thin** | Encrypted thin volumes. Common in Stratis | DISCARD passthrough must be enabled (`allow_discards` in crypttab). Otherwise TRIM from fxcp is silently dropped, preventing thin pool space reclamation |
| **dm-crypt + kvdo** | Encrypted deduplicated storage | VDO deduplication happens on *ciphertext*. Since identical plaintext produces different ciphertext (per-sector IV), deduplication is ineffective. fxcp should warn about this combination |
| **dm-crypt + dm-cache** | Encrypted cached storage | Cache operates on ciphertext. No special fxcp handling needed beyond standard dm-cache awareness |

### 13.6 Merkle Tree and Encrypted Targets

The BLAKE3 Merkle tree operates at the filesystem/plaintext level, above dm-crypt. This means:
- Merkle hashes are computed on plaintext (correct — fxcp never sees ciphertext)
- Merkle signatures stored in xattr sidecars are themselves encrypted at rest (by dm-crypt)
- No special Merkle tree adaptation needed for dm-crypt targets
- However, crypto CPU overhead should be factored into the Merkle tree computation budget — on slow CPUs without AES-NI, both BLAKE3 hashing and AES encryption compete for CPU cycles

---

## 14. Adversarial Analysis and Security Considerations

*This section incorporates findings from an independent adversarial review of the proposed architecture.*

### 14.1 Cryptographic Trust and State Tampering (Merkle Engine)

The BLAKE3 Merkle tree stored in `user.foxing.merkle` xattrs (Section 9) introduces a **trust boundary** at the target filesystem. Both `fxcp` and `foxingd` will rely on these xattrs to skip full-file content reads during delta transfers.

**Risk: Target-Side "Lying Receipts"**

A malicious actor with write access to the target filesystem could modify a file's contents and synchronously update the `user.foxing.merkle` xattr to match the original source tree. The replication engine would falsely trust the receipt, resulting in **silent data divergence**.

**Trust Model:** The `user.foxing.merkle` xattr relies entirely on target-side OS permissions (SELinux, ACLs, DAC) for integrity. The architecture explicitly does **not** treat the target as a trusted store — it treats xattr signatures as a performance optimization, not a security guarantee.

**Mitigations:**

| Mitigation | Implementation |
|-----------|----------------|
| **`--enforce-strict-hash` flag** | Bypasses xattr receipts entirely. Forces full read + recomputation of target Merkle tree on every sync. Guarantees bit-for-bit verification at the cost of O(file_size) reads |
| **`trusted.foxing.*` namespace** | Prefer `trusted.*` xattrs (require `CAP_SYS_ADMIN`) over `user.*` where available. Unprivileged users cannot tamper with `trusted.*` attributes |
| **HMAC-signed signatures** | Optional mode where Merkle signatures are HMAC'd with a key derived from the source. Target-side tampering is detectable because the attacker lacks the signing key. Key stored in foxingd's config, never written to target |
| **Periodic full verification** | foxingd can schedule periodic full-tree verification sweeps (e.g., daily) that ignore xattr receipts and recompute from disk. Catches any silent divergence |

**Risk: Merkle Tree Parsing Vulnerabilities**

A maliciously crafted or corrupted `user.foxing.merkle` xattr could trigger OOM or panics during deserialization. The `leaf_count` field controls memory allocation — a value of `u32::MAX` would attempt to allocate ~128GB.

**Mitigations:**

- `MerkleSignature::deserialize()` must enforce strict bounds: `leaf_count ≤ file_size / chunk_size + 1`
- Maximum xattr payload size: reject any `user.foxing.merkle` value exceeding 64KB (xattr system limit on most filesystems)
- Maximum sidecar file size: reject `.foxing_meta` JSON files exceeding 16MB
- Use `bincode::Options::with_limit()` to cap deserialization buffer at a configured maximum
- All deserialization returns `Option<T>` (never panics on malformed input — existing pattern in `sidecar.rs`)

### 14.2 Unprivileged Execution Abuse (fxcp Standalone)

A primary benefit of the split is allowing `fxcp` to run without `CAP_BPF` or root. However, `fxcp` exposes high-performance kernel features to standard users.

**Risk: Resource Exhaustion via High-Performance APIs**

A malicious local user could use `fxcp` to saturate the I/O queue via `io_uring`, exhaust file descriptors via `FICLONE` storms, or monopolize block device bandwidth via `copy_file_range` — causing localized Denial of Service for other users on a multi-tenant system.

**Mitigations:**

| Mitigation | Implementation |
|-----------|----------------|
| **Honor cgroups** | fxcp-core must respect cgroup v2 I/O limits (`io.max`, `io.latency`). The `Governor` already monitors PSI which reflects cgroup pressure |
| **Honor ulimits** | Respect `RLIMIT_FSIZE`, `RLIMIT_NOFILE`, `RLIMIT_AS`. BufferPool allocation already checks global memory limits |
| **Governor in standalone mode** | The `Governor` (moved to fxcp-core) must be active by default in the fxcp binary, not just foxingd. Configurable via `--max-load` CLI flag |
| **io_uring queue depth cap** | fxcp-core should cap io_uring SQE depth to a configurable maximum (default: 64 for fxcp standalone vs 256+ for foxingd). Prevents queue monopolization |
| **Rate limiting** | fxcp standalone should default to a conservative `--bandwidth-limit` that can be raised explicitly, similar to `rsync --bwlimit` |

**Risk: Symlink Traversal**

`fxcp` performing recursive copies could follow malicious symlinks outside the intended target directory (symlink-to-`/etc/shadow` attack).

**Mitigation:** `fxcp-core` must implement **path canonicalization with jail enforcement** — resolve all symlinks and verify the canonical path remains under the specified source/target root. The existing `security.rs` module should include a `path_within_root(path, root) -> bool` check called before every file operation. `O_NOFOLLOW` should be used for opens where symlink following is not explicitly intended.

### 14.3 Asymmetric State Recovery (Crash Consistency)

The crash consistency model uses `user.foxing.dirty` flags. foxingd sets the flag before writing and clears it after atomic commit. foxingd has continuous background workers that sweep and repair on restart.

**Risk: Orphaned Temporary Files and Stale Flags**

When using standalone `fxcp` (no daemon), a hard crash (`SIGKILL`, power loss, OOM kill) mid-transfer leaves `.tmp.<uuid>` files and `dirty` xattrs scattered across the target. Without foxingd's automatic sweep, the target accumulates garbage and locked files indefinitely.

**Mitigations:**

| Mitigation | Implementation |
|-----------|----------------|
| **`fxcp --cleanup` command** | Dedicated subcommand that scans a target directory for orphaned `.tmp.*` files and stale `user.foxing.dirty` xattrs, removing them with user confirmation |
| **`CleanupGuard` RAII pattern** | Similar to existing `WalGuard` — registers `.tmp` file paths at creation, removes them on drop. Catches `SIGINT`/`SIGTERM` via signal handler. Cannot catch `SIGKILL` or power loss, hence the `--cleanup` command |
| **Tmp file age limit** | `fxcp --cleanup` should only remove `.tmp.*` files older than a configurable threshold (default: 1 hour) to avoid removing files from a concurrent fxcp process |
| **Startup sweep option** | `fxcp --repair-before-copy SOURCE DEST` scans the target for stale state before beginning a new sync |

### 14.4 Adversarial Pacing and Governor Manipulation

foxingd passes `Arc<Governor>` to fxcp-core for I/O pacing based on system load (PSI, Load Average).

**Risk: Artificial Replication Stalling**

An attacker or noisy neighbor could intentionally generate high CPU/IO pressure to manipulate the `Governor` into throttling fxcp-core. In foxingd context, sustained throttling causes the `ReorderBuffer` or `HydrationQueue` to overflow, forcing event drops and loss of real-time sync.

**Mitigations:**

| Mitigation | Implementation |
|-----------|----------------|
| **QoS Override / Minimum Guaranteed Throughput** | The `SmartCopier` constructor should accept an optional `min_throughput_bytes_sec: u64` parameter. When set, the Governor cannot throttle below this floor — ensuring critical replication paths (WAL, database journals) maintain forward progress |
| **Governor bypass for control plane** | foxingd's Worker 0 (structural metadata: renames, creates, deletes) should bypass Governor throttling entirely. Structural operations are small, latency-sensitive, and must not be delayed |
| **Backpressure signaling** | When the Governor throttles for >30 seconds continuously, foxingd should emit a `foxing_governor_sustained_throttle` metric and log a warning. This makes artificial stalling visible in monitoring |
| **Bounded queue with drop policy** | The `ReorderBuffer` should have a configurable maximum size with an explicit drop policy (drop oldest events when full, trigger gap-based hydration to recover). This prevents unbounded memory growth from sustained throttling |
| **PSI source validation** | Governor should cross-reference PSI pressure against its own I/O contribution (via `/proc/self/io`). If the system pressure is not caused by foxing's own I/O, throttling should be less aggressive |

### 14.5 Supply Chain and Library Attack Surface

Extracting `fxcp-core` into a standalone library crate makes it available to third-party consumers. Any vulnerability in fxcp-core is now exploitable by every application that imports it.

**Risk: Broader Exploit Applicability**

Memory safety bugs, path traversal vulnerabilities, or integer overflows in `fxcp-core/operations.rs` become exploitable vectors not just for foxingd but for any application importing the crate.

**Mandatory Security Practices:**

| Practice | Scope |
|----------|-------|
| **Continuous fuzz testing** | `cargo-fuzz` targets for: `SmartCopier` path resolution, `MerkleSignature::deserialize()`, `SyncSignature::deserialize()`, io_uring SQE construction, xattr value parsing |
| **Unsafe encapsulation** | All `unsafe` blocks handling io_uring, native `ioctl`, and SIMD intrinsics must be encapsulated in minimal-surface `unsafe fn` wrappers with documented safety invariants. No raw pointer arithmetic in public API surface |
| **Path sanitization** | Every path received from external input (CLI args, config, xattr values) must pass through `canonicalize()` + root jail check before use in any filesystem operation |
| **Integer overflow protection** | All size calculations (`chunk_count = file_size / chunk_size`, buffer allocation sizes, offset arithmetic) must use checked arithmetic (`checked_mul`, `checked_add`) or `saturating_*` variants |
| **Dependency auditing** | `cargo-audit` in CI. Minimize transitive dependency count for fxcp-core. Pin security-critical deps (blake3, io-uring, libc) |
| **Symlink policy** | Default to `O_NOFOLLOW` for all opens. Symlink following requires explicit opt-in via `--follow-symlinks` flag |
| **RUSTSEC advisory compliance** | Subscribe to RUSTSEC advisories for all fxcp-core dependencies. Automated PR generation for security updates |

---

## 15. Consequences

### 15.1 Benefits

| Benefit | Description |
|---------|-------------|
| **Independent deployability** | `fxcp` runs on any Linux with io_uring (kernel 5.6+). No BPF, no root for basic copies |
| **Faster compilation** | Changing `event.rs` no longer rebuilds 2002-line `operations.rs`. BPF skeleton generation isolated |
| **Reduced attack surface** | `fxcp` runs unprivileged. eBPF privilege surface isolated to foxingd. Aligns with memory-safe language guidance [White House ONCD 2024] |
| **Library reusability** | `fxcp-core` consumable by backup tools, container runtimes, CI/CD pipelines |
| **Clear CQRS boundary** | Control plane (foxingd) and data plane (fxcp-core) have explicit, documented interfaces |
| **Targeted testing** | Copy engine tests run without BPF infrastructure. Daemon tests can mock fxcp-core |
| **Quick Fox realized** | fxcp embodies the "Quick Fox" as an independent tool — fast, nimble, no daemon baggage |

### 15.2 Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Metrics registry fragmentation | Prometheus metrics from two crates may conflict | Use default Prometheus registry. fxcp-core metrics register on first use. foxingd re-exports |
| Circular dependency introduction | Future changes create foxingd types needed by fxcp-core | Strict rule: fxcp-core NEVER depends on foxingd. Shared types always go to fxcp-core |
| Config type duplication | TargetConfig (foxingd) needs CopyConfig subset (fxcp-core) | `From<TargetConfig> for CopyConfig` conversion in foxingd |
| Migration breaks `foxing sync` users | Users must switch to `fxcp` | foxingd can retain `sync` subcommand as thin wrapper during deprecation period |
| `ONE_SHOT_MODE` global state sharing | `AtomicBool` in constants used by both planes | Lives in fxcp-core. Both binaries set it at startup |

### 15.3 Trade-offs

1. **Workspace complexity vs modularity** — Three crates require coordinating versions and CI. This is the standard Rust workspace pattern, well-supported by Cargo.
2. **Two metrics modules** — fxcp-core (~20 metrics) + foxingd (~80 additional). Acceptable given clean separation.
3. **Config type proliferation** — `CopyConfig` (fxcp-core) and `TargetConfig` (foxingd) vs one monolithic type. Better for documentation, type safety, and API clarity.

---

## 16. Validation Criteria

The split is successful when:

1. `cargo build -p fxcp` compiles on a system **without** BPF support (no libbpf, no vmlinux.h, no `/sys/kernel/btf/`)
2. `cargo build -p foxingd` compiles with identical functionality to current `foxing`
3. `fxcp /src/dir /dst/dir` produces bit-identical results to `foxing sync /src/dir /dst/dir`
4. All existing tests in `tests/` pass against foxingd
5. foxingd Prometheus metrics endpoint includes all current metrics (no regressions)
6. `fxcp` binary size is measurably smaller than current `foxing` binary
7. No circular dependencies exist between crates (`cargo tree -p fxcp-core` shows no foxingd)

---

## 17. References

### Linux Kernel & VFS
- Linux VFS Documentation: https://docs.kernel.org/filesystems/vfs.html
- Linux Kernel Source: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git
- Kernel Documentation Index: https://docs.kernel.org/
- System Call man-pages: https://www.kernel.org/doc/man-pages/
- `statx(2)`: https://man7.org/linux/man-pages/man2/statx.2.html
- `fsync(2)`: https://man7.org/linux/man-pages/man2/fsync.2.html
- `getdents64(2)`: https://man.archlinux.org/man/getdents64.2.en

### Filesystems
- **XFS Atomic Exchange:** https://blogs.oracle.com/linux/xfs-atomic-file-content-exchange-in-uek8
- **XFS Block Atomic Writes:** https://blogs.oracle.com/linux/xfs-block-atomic-writes-in-uek8
- XFS Filesystem Structure: http://ftp.ntu.edu.tw/linux/utils/fs/xfs/docs/xfs_filesystem_structure.pdf
- XFS Administration (RHEL): https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/7/html/storage_administration_guide/ch-xfs
- XFS Delayed Logging Design: https://www.kernel.org/doc/html/v6.1/filesystems/xfs-delayed-logging-design.html
- **Btrfs Design:** https://btrfs.readthedocs.io/en/latest/dev/dev-btrfs-design.html
- Btrfs Introduction: https://btrfs.readthedocs.io/en/latest/Introduction.html
- Btrfs (Arch Wiki): https://wiki.archlinux.org/title/Btrfs
- VFS Tour: https://www.tldp.org/LDP/khg/HyperNews/get/fs/vfstour.html
- VFS Lab: https://linux-kernel-labs.github.io/refs/heads/master/labs/filesystems_part2.html
- Filesystem Journaling (ext4/XFS/Btrfs): https://medium.com/@springmusk/filesystem-journaling-explained-ext4-xfs-btrfs-4ac9a0961638
- **Crash Consistency:** http://www.cs.unc.edu/~porter/courses/comp530/f24/slides/crash-consistency.pdf

### NFS & Network Filesystems
- NFSv4.1 Specification: http://www.rfc-editor.org/info/rfc5661
- **NFSv4.2 Specification** (server-side copy): http://www.rfc-editor.org/info/rfc7862

### eBPF & Kernel Extensions
- **ExtFUSE** (eBPF + FUSE): https://www.usenix.org/system/files/atc19-bijlani.pdf
- eBPF + FUSE Performance: https://events19.linuxfoundation.org/wp-content/uploads/2017/11/When-eBPF-Meets-FUSE-Improving-Performance-of-User-File-Systems-Ashish-Bijlani-Georgia-Tech.pdf
- RFUSE: https://www.usenix.org/system/files/fast24-cho.pdf
- KFuse (Verified Programs): https://people.cs.vt.edu/djwillia/papers/eurosys22-kfuse.pdf
- eBPF Research Survey: https://eunomia.dev/blogs/ebpf-papers/
- eBPF Ten Years: https://eunomia.dev/blogs/ten-years/
- BCC (BPF Compiler Collection): https://github.com/iovisor/bcc

### File Transfer & Synchronization
- **rsync**: http://rsync.samba.org/
- LuminS (rsync alternative): https://github.com/wchang22/LuminS
- MARS (Storage Replication): https://github.com/schoebel/mars
- Dirvish (snapshot-based backup): https://dirvish.org/

### Directory Traversal Optimization
- ripgrep (parallel dir walking): https://github.com/BurntSushi/ripgrep
- ripgrep internals: https://blog.mbrt.dev/posts/ripgrep/
- fd (fast find alternative): https://github.com/sharkdp/fd
- fast-dirscan: https://github.com/krosenvold/fast-dirscan
- inotify limits: https://watchexec.github.io/docs/inotify-limits.html
- getdents64 tracing: https://aquasecurity.github.io/tracee/v0.17/docs/events/builtin/syscalls/getdents64/

### Distributed Systems & Consistency
- **CAP Theorem:** https://en.wikipedia.org/wiki/CAP_theorem
- Strong vs Eventual Consistency: https://www.geeksforgeeks.org/system-design/strong-vs-eventual-consistency-in-system-design/
- Event-Driven Architecture: https://www.confluent.io/learn/event-driven-architecture/

### Storage Technologies & Device-Mapper
- LVM on Software RAID: https://wiki.archlinux.org/title/LVM_on_software_RAID
- **Stratis (Fedora):** https://fedoramagazine.org/getting-started-with-stratis-up-and-running/
- **Stratis (RHEL):** https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/8/html/managing_file_systems/setting-up-stratis-file-systems_managing-file-systems
- **RHEL Storage Administration:** https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/8/html/system_design_guide/managing_storage_devices
- Thin Provisioned Volumes (RHEL): https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/6/html/logical_volume_manager_administration/thinprovisioned_volumes
- RAID vs LVM vs mdadm: https://recoverhdd.com/blog/comparison-and-difference-between-raid-lvm-and-mdadm.html
- RAID vs LVM: https://www.linuxtoday.com/blog/raid-vs-lvm/
- dm-integrity (LWN): https://lwn.net/Articles/755454/
- RAID doesn't protect against bit rot: https://securitypitfalls.wordpress.com/2018/05/08/raid-doesnt-work
- Persistent Memory (PMDK): https://github.com/pmem/pmdk
- PMem Overview: https://pmem.io/
- Intel Optane PMem I/O: https://www.intel.com/content/www/us/en/developer/articles/technical/speeding-up-io-workloads-with-intel-optane-dc-persistent-memory-modules.html
- PMem Programming (USENIX): https://www.usenix.org/system/files/login/articles/login_summer17_07_rudoff.pdf

### Encryption & dm-crypt
- **LUKS / cryptsetup:** https://gitlab.com/cryptsetup/cryptsetup
- Noise Protocol Framework: http://noiseprotocol.org/
- Noise Explorer: https://noiseexplorer.com/
- WireGuard Whitepaper: https://www.wireguard.com/papers/wireguard.pdf
- Signal Double Ratchet: https://signal.org/docs/specifications/doubleratchet/

### NAND Flash & F2FS
- Filesystem comparison (ext4/Btrfs/XFS/ZFS): https://eagleeyet.net/blog/operating-systems/linux/file-systems/ext4-vs-btrfs-vs-xfs-vs-zfs-a-linux-file-system-comparison-for-beginners/

### Rust Language & Tooling
- The Rust Book: https://doc.rust-lang.org/book/
- Rust 2024 Edition Guide: https://doc.rust-lang.org/edition-guide/rust-2024/index.html
- **Cargo Workspace Features:** https://doc.rust-lang.org/cargo/reference/features.html
- Cargo Build Scripts: https://doc.rust-lang.org/cargo/reference/build-scripts.html
- Idiomatic Rust: https://github.com/mre/idiomatic-rust

### Security & Memory Safety
- **White House ONCD Report on Memory Safety (2024):** https://www.whitehouse.gov/oncd/briefing-room/2024/02/26/press-release-technical-report/
- Magic Numbers in Programming: https://en.wikipedia.org/wiki/Magic_number_(programming)
- LUKS (dm-crypt): https://gitlab.com/cryptsetup/cryptsetup

---

*This ADR supersedes the previous monolithic architecture. Implementation should follow the phased migration strategy in Section 8.*
