# ADR-001: Splitting Foxing into fxcp + foxingd Workspace

**Status:** Proposed
**Date:** 2026-03-02
**Authors:** Project maintainers

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

## 10. Consequences

### 10.1 Benefits

| Benefit | Description |
|---------|-------------|
| **Independent deployability** | `fxcp` runs on any Linux with io_uring (kernel 5.6+). No BPF, no root for basic copies |
| **Faster compilation** | Changing `event.rs` no longer rebuilds 2002-line `operations.rs`. BPF skeleton generation isolated |
| **Reduced attack surface** | `fxcp` runs unprivileged. eBPF privilege surface isolated to foxingd. Aligns with memory-safe language guidance [White House ONCD 2024] |
| **Library reusability** | `fxcp-core` consumable by backup tools, container runtimes, CI/CD pipelines |
| **Clear CQRS boundary** | Control plane (foxingd) and data plane (fxcp-core) have explicit, documented interfaces |
| **Targeted testing** | Copy engine tests run without BPF infrastructure. Daemon tests can mock fxcp-core |
| **Quick Fox realized** | fxcp embodies the "Quick Fox" as an independent tool — fast, nimble, no daemon baggage |

### 10.2 Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Metrics registry fragmentation | Prometheus metrics from two crates may conflict | Use default Prometheus registry. fxcp-core metrics register on first use. foxingd re-exports |
| Circular dependency introduction | Future changes create foxingd types needed by fxcp-core | Strict rule: fxcp-core NEVER depends on foxingd. Shared types always go to fxcp-core |
| Config type duplication | TargetConfig (foxingd) needs CopyConfig subset (fxcp-core) | `From<TargetConfig> for CopyConfig` conversion in foxingd |
| Migration breaks `foxing sync` users | Users must switch to `fxcp` | foxingd can retain `sync` subcommand as thin wrapper during deprecation period |
| `ONE_SHOT_MODE` global state sharing | `AtomicBool` in constants used by both planes | Lives in fxcp-core. Both binaries set it at startup |

### 10.3 Trade-offs

1. **Workspace complexity vs modularity** — Three crates require coordinating versions and CI. This is the standard Rust workspace pattern, well-supported by Cargo.
2. **Two metrics modules** — fxcp-core (~20 metrics) + foxingd (~80 additional). Acceptable given clean separation.
3. **Config type proliferation** — `CopyConfig` (fxcp-core) and `TargetConfig` (foxingd) vs one monolithic type. Better for documentation, type safety, and API clarity.

---

## 11. Validation Criteria

The split is successful when:

1. `cargo build -p fxcp` compiles on a system **without** BPF support (no libbpf, no vmlinux.h, no `/sys/kernel/btf/`)
2. `cargo build -p foxingd` compiles with identical functionality to current `foxing`
3. `fxcp /src/dir /dst/dir` produces bit-identical results to `foxing sync /src/dir /dst/dir`
4. All existing tests in `tests/` pass against foxingd
5. foxingd Prometheus metrics endpoint includes all current metrics (no regressions)
6. `fxcp` binary size is measurably smaller than current `foxing` binary
7. No circular dependencies exist between crates (`cargo tree -p fxcp-core` shows no foxingd)

---

## 12. References

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

### Storage Technologies
- LVM on Software RAID: https://wiki.archlinux.org/title/LVM_on_software_RAID
- Stratis (Fedora): https://fedoramagazine.org/getting-started-with-stratis-up-and-running/
- Thin Provisioned Volumes (RHEL): https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/6/html/logical_volume_manager_administration/thinprovisioned_volumes
- Persistent Memory (PMDK): https://github.com/pmem/pmdk
- PMem Overview: https://pmem.io/
- Intel Optane PMem I/O: https://www.intel.com/content/www/us/en/developer/articles/technical/speeding-up-io-workloads-with-intel-optane-dc-persistent-memory-modules.html
- PMem Programming (USENIX): https://www.usenix.org/system/files/login/articles/login_summer17_07_rudoff.pdf

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
