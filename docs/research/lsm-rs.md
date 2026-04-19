# lsm-rs — Filesystem Observation Layer

**Status:** design analysis, pre-code.
**Target kernel:** Linux 6.12+ baseline, 7.0 preferred.
**Language:** Rust (userspace), C (minimal eBPF probes).
**Scope:** observe → log → snapshot → mirror, **without** writing a new filesystem.

---

## 1. Problem statement

Build a fast, zero-copy layer that sits **over** existing Linux filesystems
(ext4, XFS, btrfs, bcachefs, F2FS) and delivers three things:

1. **Logging** — a durable, ordered stream of filesystem mutations (create,
   write, rename, unlink, chmod, truncate, xattr, …) with enough metadata
   and content to reconstruct state.
2. **Snapshotting** — point-in-time views. Use the FS's native reflink/CoW
   primitives where available; fall back to sidecar WAL + delta journaling
   where not.
3. **Mirroring** — near-real-time replication to a target (local directory,
   USB, NFS, SSH) with strong eventual consistency and self-healing.

Explicit non-goals (for now):

- Not writing a new on-disk filesystem format.
- Not replacing the VFS or writing a kernel-resident filesystem driver.
- Not a versioning FS like ize/NILFS2 (we can *use* their semantics, not
  duplicate them).
- Not a FUSE passthrough — FUSE is the failure mode we're routing around.

---

## 2. The prior art (what we're standing on)

### 2.1 foxing — the nearest neighbour (and it's already good)

[`aenertia/foxing`](https://codeberg.org/aenertia/foxing) is a Rust,
eBPF-powered replication engine that does essentially the brief above. It
has been in development since late 2025 and is at v0.8.1 as of March 2026.

Key pieces of its architecture worth internalising:

- **fxcp-core** — shared I/O engine: io_uring, FICLONE reflink, BLAKE3
  Merkle delta, storage-stack awareness (dm-crypt, dm-thin, containers),
  PSI governor.
- **fxcp** — standalone CLI, no BPF/root required (10–54× faster than
  rsync on most workloads per their benchmarks).
- **foxingd** — the daemon: BPF probes (`vfs_write_iter`,
  `security_inode_create`, `vfs_rename`, `notify_change`) → 33 MB ring
  buffer → ReorderBuffer (BTreeMap, per-device seqnum ordering) →
  TransientFilter (suppress create→unlink temp-file churn) →
  IdentityProjector (inode↔path) → 4-tier CAKE-style priority queue →
  Control-plane worker (W0, serialised renames/creates) + data-plane
  workers (W1..N, hash-distributed writes) → SmartCopier → target.
- Auto-adaptive copy tier: NFS compound RPC → FICLONE → copy_file_range
  → sendfile → io_uring, fallback ladder per device/filesystem.
- MARS versioning layer: reflink snapshots on fsync, per-file history.
- FXAR v2 archive format: gear-hash chunking + BLAKE3 CAS (96% dedup on
  slowly-changing data).
- Single-file create→land latency of **15–21 ms** on XFS→NFS.
- GPL-2.0-or-later.

**This is 80%+ of what we want, already built, shipping on Fedora COPR.**
The honest question is whether we build parallel to it, fork it, or
contribute upstream. Treated explicitly in §7.

### 2.2 Kernel primitives available to us

| Primitive | Since | What it gives us |
|---|---|---|
| `inotify` | 2.6.13 | Path-scoped events, no content, racy |
| `fanotify` | 2.6.37 | Mount/FS-scoped events, fd in event (→ content) |
| `FAN_REPORT_FID` | 5.1 | File-handle IDs, resolves rename-cookie mess |
| `FAN_CLASS_PRE_CONTENT` | 5.17 | Block open until userspace populates |
| `FAN_PRE_ACCESS / FAN_PRE_MODIFY` | 6.14 | **HSM-grade** pre-content hooks with range info (Meta engineers upstreamed for tiered storage) |
| `FAN_REPORT_MNT` | 6.14 | Mount identity in event metadata |
| eBPF kprobes (`vfs_*`) | 4.x | Arbitrary hook, unstable ABI |
| eBPF tracepoints (`syscalls:sys_enter_*`) | 4.x | Stable syscall surface, coarse |
| eBPF LSM hooks (`lsm:file_open`, etc.) | 5.7 | Stable semantic API, permission-grade |
| `FICLONE` / `FICLONERANGE` | 4.5 | Reflink on btrfs/XFS/bcachefs/ZFS 2.2+ |
| `copy_file_range(2)` | 4.5 / improved 5.3 | Server-side copy, NFS 4.2 bypass, reflink fallback |
| `splice` / `sendfile` | ancient | Kernel-space pipe, true zero-copy for small files |
| `io_uring` + registered buffers | 5.1+, huge wins 6.x | Async batched I/O, fixed buffers avoid page pinning per-op |
| btrfs `BTRFS_IOC_SEND` | 3.6 | Snapshot→stream, the reference design for fs replication |
| bcachefs subvolume snapshots | 6.7 (since removed in 6.18 — now DKMS only) | Key-versioned snapshots, writable, ~30 ms at thousands of snapshots |
| NILFS2 continuous checkpoints | 2.6.30 | Every few seconds, mountable read-only concurrently |
| Rust-in-kernel | **stable as of 7.0 (Apr 2026)** | Kernel-module option unlocked |

### 2.3 eBPF in Rust — the toolchain landscape

- **aya** (pure Rust, both kernel and userspace) — ergonomic, still
  maturing on CO-RE, preferred for greenfield projects where the
  in-kernel C compiler dependency is a pain point.
- **libbpf-rs** — safe Rust wrapper over libbpf; kernel program still in C.
  Production-grade, CO-RE mature, what most shipping projects use (Cilium,
  Tracee, foxing).
- **redbpf** — older, less active.
- **oxidebpf** — Red Canary, BSD-licensed, pinned to their use case.

For lsm-rs: **libbpf-rs with C probes** is the conservative default.
Aya becomes attractive if we want a single-toolchain Rust build story and
can absorb the CO-RE edge cases.

### 2.4 The reference design: btrfs send/receive

The `BTRFS_IOC_SEND` stream format is the canonical design for
"filesystem diff between snapshots, streamable." It's the quiet
benchmark for what mirroring should look like when the source FS
cooperates. The stream is a sequence of TLV-encoded commands (CREATE,
MKFILE, RENAME, WRITE, CLONE, SETXATTR, CHMOD, UTIMES, END) with CRC32C
per command. Protocol v2 (Linux 6.0+) encodes extents more efficiently
and supports ENCODED_WRITE for direct compressed-data transfer.

Relevance: on btrfs source + btrfs target, we should pipe through
`btrfs send | btrfs receive` and stop trying to be clever. Our value-add
is cross-FS and heterogeneous-target scenarios.

---

## 3. The design space — four axes

Not one axis; four, and they combine.

### Axis A: Event source

How do we notice filesystem mutations?

| Source | Latency | Content | Loss | Root? | Stability |
|---|---|---|---|---|---|
| `inotify` | ~ms | ❌ | queue overflow | no | stable |
| `fanotify` (post-content) | ~ms | ✅ via fd | queue overflow | yes | stable |
| `fanotify` PRE_CONTENT (6.14+) | blocking | ✅ | cannot drop | yes | **new** |
| eBPF kprobe `vfs_*` | ~µs | via ring buf | drop if slow | yes | **unstable ABI** |
| eBPF LSM hooks | ~µs | via ring buf | drop if slow | yes | stable (5.7+) |
| Native FS (btrfs send / nilfs cp) | batch | ✅ | none | yes | stable per-FS |
| FUSE passthrough | ~10–100 µs/op overhead | ✅ | none | no | stable but slow |

**Verdict:** tier it.
- **Tier 1 (default):** fanotify with FAN_REPORT_FID + FAN_REPORT_MNT,
  kernel ≥ 6.14 → use FAN_PRE_MODIFY for write-capture without racing
  the writer. This is now a first-class API; Meta ships it in prod.
- **Tier 2 (optional):** eBPF LSM probes for sub-µs latency and
  per-process attribution (pid/tgid filtering, loop prevention).
- **Tier 3 (fast-path):** on btrfs/bcachefs source, bypass both and use
  native snapshot diff (`btrfs send`, bcachefs subvolume diff).

### Axis B: Content capture

Given an event, how do we get the bytes (if we need them)?

1. **fd in event** (fanotify) — open fd, read. Cache-warm, zero kernel-to-user
   copy via `splice()` to a pipe or `copy_file_range()` direct to target.
2. **Read-by-file-handle** (`open_by_handle_at`) — for fanotify_fid events,
   resolve later from handle. Critical for post-rename events.
3. **Content-in-event** (eBPF ring buffer) — copy write data at probe site.
   Simple, but bounded by ring buffer size; large writes need fd fallback.
4. **Reflink snapshot** (FICLONE) — if source FS supports it, reflink the
   file into a staging dir atomically, read at leisure. This is the
   cleanest zero-copy path: no data moves at all until CoW triggers.

**Zero-copy path for large writes (target on same FS):**
`FICLONERANGE` source→target. Literally zero bytes moved.

**Zero-copy path for same-server NFS 4.2:**
`copy_file_range()` → server-side copy, no data over the wire.

**Zero-copy path for cross-FS large:**
`io_uring` with registered buffers + `IORING_OP_SPLICE`. Data stays in
kernel page cache; no userspace copy.

**Small files:** `sendfile(2)` beats everything; tier accordingly.

### Axis C: Log/snapshot format

Two legitimate designs:

**Design C1: Operation log (foxing-style).**
Append-only log of fs ops with content blobs referenced by content hash.
Replay = apply ops in order. Crash-safe via WAL. Snapshots = "all ops up
to timestamp T."

Pros: filesystem-agnostic, streamable, diffs are natural.
Cons: reconstruction cost is O(ops since snapshot); needs periodic
compaction.

**Design C2: Snapshot delta (btrfs-send-style).**
Take reflink snapshots on the source at interval T. Diff snapshot_N
against snapshot_N-1 to produce a stream. Send stream to target.

Pros: minimal code (the FS does the work), proven, efficient.
Cons: requires reflink-capable source FS, large-grain snapshot interval.

**Hybrid (recommended): C1 for live, C2 for durable checkpoints.**
- Live replication: operation log, ~ms latency, best-effort.
- Durable checkpoints: every N minutes, reflink snapshot + diff stream,
  guaranteed consistent.
- Recovery from crash: replay op log from last checkpoint.

This is essentially what foxing does (MARS versioning is C2-in-disguise;
the event pipeline is C1). We should not pretend otherwise — it's
convergent design from first principles.

### Axis D: Replay target

Where does the mirror land, and how do we stay consistent?

- **Same-FS, same-host:** reflink, instant. No daemon on the target side.
- **Same-FS, different host:** `btrfs send | ssh host btrfs receive` is
  unbeatable on btrfs; similar story for bcachefs (send format is in
  development as of 2026).
- **Cross-FS:** replay op log using `SmartCopier`-style tier selection.
- **NFS 4.2:** compound RPC + server-side copy.
- **S3/object store:** op log → chunked objects, no POSIX guarantees.

---

## 4. Zero-copy analysis — where the data actually moves

A hard look, because "zero-copy" gets sloppy.

| Path | Source bytes | Target bytes | Userspace copies |
|---|---|---|---|
| **FICLONE local btrfs→btrfs** | 0 | 0 | 0 — metadata only |
| **`copy_file_range` NFS 4.2** | 0 (server-side) | 0 (server-side) | 0 |
| **`sendfile` cross-FS** | kernel page cache | kernel page cache | 0 |
| **`splice` pipe-mediated** | kernel page cache | kernel page cache | 0 |
| **`io_uring` fixed buffers** | registered buf | registered buf | 0 (after registration) |
| **fanotify fd + `read`+`write`** | page cache → user | user → page cache | 2 |
| **eBPF ring buffer** | kernel → user ring | user → pagecache | 1–2 |
| **FUSE passthrough** | kernel → fuse → user → fuse → kernel | — | 2× per op, plus ctx switches |

For the "logging" axis, we *do* need to land bytes in our log, so a copy
is unavoidable unless we CAS-store via reflink (the right move when
source = btrfs/XFS/bcachefs).

For the "mirroring" axis, we can and should stay zero-copy in the
common case via reflink + `copy_file_range`.

**Design rule:** the op log records *metadata + a content reference*
(hash + reflink handle OR inline bytes for small writes). Never eagerly
copy the payload into the log if reflink is available.

---

## 5. Concrete architecture — proposed

```
┌────────────────────────────────────────────────────────────────────┐
│                          lsm-rs-core                               │
│                                                                    │
│   fanotify (tier 1) ───┐                                           │
│   eBPF LSM   (tier 2) ─┼──► EventStream ──► Reorder ──► Filter    │
│   btrfs send (tier 3) ─┘   (seq per dev)    (BTreeMap)  (transient)│
│                                                  │                 │
│                                                  ▼                 │
│                                        IdentityProjector           │
│                                        (inode↔path↔handle)         │
│                                                  │                 │
│                                   ┌──────────────┼──────────┐      │
│                                   ▼              ▼          ▼      │
│                              OpLog sink    SnapDriver   MirrorDriver│
│                              (CAS store)   (reflink)    (SmartCopy)│
│                                   │              │          │      │
└───────────────────────────────────┼──────────────┼──────────┼──────┘
                                    ▼              ▼          ▼
                               .lsm/log/     .lsm/snap/    target FS
                               (per-dev      (reflink      (any)
                                append-log)   tree)
```

**Crate layout:**

```
lsm-rs/
├── lsm-core/          # event ingest, ordering, projection
│   ├── src/
│   │   ├── event.rs           # unified Event type
│   │   ├── source/
│   │   │   ├── fanotify.rs    # tier 1
│   │   │   ├── bpf.rs         # tier 2 (libbpf-rs)
│   │   │   └── btrfs_send.rs  # tier 3
│   │   ├── reorder.rs         # per-device seqnum sort
│   │   ├── filter.rs          # transient, pid/tgid, path
│   │   └── projector.rs       # inode→path, handle→path
├── lsm-copy/          # SmartCopier — reflink/splice/sendfile/io_uring ladder
│   └── src/
│       ├── tier.rs
│       ├── reflink.rs
│       ├── uring.rs
│       └── nfs.rs
├── lsm-log/           # OpLog — CAS content store + metadata log
│   └── src/
│       ├── cas.rs             # BLAKE3-keyed content store
│       ├── log.rs             # append-only metadata WAL
│       └── replay.rs
├── lsm-snap/          # snapshot driver (reflink-backed)
│   └── src/
├── lsm-mirror/        # mirror driver (op replay against target)
│   └── src/
├── lsm-bpf/           # BPF kernel programs (C)
│   └── src/*.bpf.c
├── lsmd/              # daemon binary
└── lsmctl/            # CLI: mount, status, snapshot, restore, replay
```

**Explicitly not** a workspace member: anything that implies a new
on-disk format. The log/snap/mirror outputs live inside existing FSes
as ordinary files/dirs.

---

## 6. Feasibility & timeline

Calibrated against Nick's delivery pace (hf-tui, narwhal, tarotality, the
resident/MCP work) and existing Rust expertise.

### Phase 0 — Research spike (1 week)

- Read foxing source top-to-bottom (it's 23 MiB, ~70% Rust). Specifically:
  - `fxcp-core/src/smart_copy.rs` — the tier ladder.
  - `foxingd/src/bpf/` — BPF probes and their CO-RE attachment.
  - `foxingd/src/reorder.rs` + `identity.rs` — the event pipeline.
- Decide: fork foxing, or parallel build? (§7).
- Spike: fanotify FAN_PRE_MODIFY minimal Rust prototype — ~200 LOC,
  confirms kernel version and API shape.

**Exit criterion:** a 50-line Rust program that prints every write to
`/tmp/watch` with offset, length, and pid, using FAN_PRE_MODIFY.

### Phase 1 — MVP logger (2–3 weeks)

- `lsm-core` with fanotify source only (no BPF yet).
- Per-mount seqnum + BTreeMap reorder buffer.
- OpLog = append-only JSON-lines for metadata, BLAKE3-keyed CAS for
  payloads in `.lsm/log/`. Use reflink into CAS when possible.
- `lsmctl tail` — prints events live.
- `lsmctl replay <log> <dir>` — reconstructs dir from log.

**Exit criterion:** recursively build kernel tree under watch, replay
produces byte-identical tree (verify with BLAKE3 of tarball).

### Phase 2 — Zero-copy mirror (3–4 weeks)

- `lsm-copy` crate with the full tier ladder.
- `lsm-mirror` daemon mode — live-apply events to target dir.
- Handle rename chains (foxing's WAL storm problem) — study their fix
  verbatim, it took them weeks of adversarial testing.
- NFS 4.2 tier (compound RPC) — defer if no NFS target needed near-term.

**Exit criterion:** `foxingd sync`-equivalent latency (~20 ms
single-file create to target) on btrfs→btrfs local.

### Phase 3 — Snapshots (2 weeks)

- `lsm-snap` — reflink snapshot tree under `.lsm/snap/<epoch>/`.
- `lsmctl snap list/create/restore/prune`.
- On FSes without reflink (ext4): fall back to sidecar WAL — document
  the asymmetry honestly.

**Exit criterion:** point-in-time restore of a file to a past version.

### Phase 4 — eBPF fast path (3–4 weeks, optional)

- libbpf-rs + C probes for `vfs_write_iter`, `security_inode_create`,
  `vfs_rename`, `notify_change`.
- 33 MB ring buffer per-CPU.
- CAKE-style dispatcher (4 priority queues).
- PID/TGID self-exclusion to prevent feedback loops.

**Exit criterion:** sub-ms event latency under load; matches foxingd
numbers on equivalent hardware.

### Phase 5 — Hardening (ongoing)

- Adversarial test suite (foxing has 11 phases — worth cribbing wholesale).
- Mount monitoring (NFS lazy unmount, USB unplug).
- Hydration cleanup / ghost-file reaping.
- ENOSPC safe-stall.
- prom metrics / TUI.

**Realistic total to production-grade:** 3–4 months single-developer
equivalent. **To a working demo for the reef-break-sim use case: 4–6
weeks.**

### Hardware & environment

- Nick's Ryzen 7 / 64 GB box runs Ubuntu → check kernel: needs 6.14+ for
  FAN_PRE_MODIFY, or 6.12 for the prerequisite pieces. Ubuntu 26.04 LTS
  (April 23, 2026) ships kernel 7.0 → aligns with project start.
- Rust 1.85+ (2024 edition) for async Rust maturity.
- libbpf 1.4+, clang 17+ for BPF CO-RE.
- btrfs or XFS on the dev box for reflink testing (btrfs default on a
  scratch partition is easiest).

---

## 7. The uncomfortable question — do we build this at all?

foxing exists. It does 85% of what's described. It's GPL-2.0, actively
maintained, already benchmarked to 15–21 ms single-file replication
latency, has a TUI, has packaging for Fedora, has adversarial tests.

**Three honest paths:**

### Path A — Use foxing, contribute upstream

- Time to value: days.
- What we give up: design freedom on log format, no justification for
  learning the internals from the ground up.
- What we gain: immediate working system; if we find bugs or want
  features, upstream them.
- **Recommended if:** the goal is to *have* this capability, not to
  *build* it.

### Path B — Fork foxing, specialise

- Time to value: 1–2 weeks for a diverged build.
- Rationale if: we want different semantics (e.g. content-addressed
  op log for the simulation-data replication use case, or integration
  with Seaview/Manifold's mesh-sequence workflow, or a Pijul-style
  mathematical change model on top).
- Risk: merge drift; GPL-2.0 copyleft commitment.

### Path C — Parallel build

- Time to value: months.
- Rationale only if: the design materially diverges (different license,
  different language subset, non-overlapping platforms, or this is as
  much a learning project as a deliverable).
- Risk: reinventing an event-pipeline architecture that Meta and foxing
  already iterated through three generations of bugs to get right.

### A framing that might actually fit

Given the adjacent projects — Seaview (mesh sequences), Manifold
(pipeline), the simulation data volumes that killed the Corsair PSU —
**the real need might be one layer up from foxing**:

- A content-addressed snapshot store for simulation output
- That uses foxing (or similar) as the *transport*
- And adds simulation-aware semantics (mesh epochs, frame ranges,
  seekable time-series access) on top.

This is closer to the `.fxar` archive format foxing already has, but
specialised for Manifold's output shape. That's a well-bounded project:
2–4 weeks, sits clearly above foxing rather than duplicating it.

**My honest recommendation:** start with Path A (install foxing, use it,
hit its limits), and only escalate to B or a specialised layer once the
limits are concrete. The risk of Path C is spending two months to rebuild
what's already running.

---

## 8. Open questions for Nick

1. What's driving the pivot away from ize? If it was FUSE overhead, see
   §2.2 tier list — fanotify + BPF is 10–100× cheaper than FUSE. If it
   was "versioning-as-FS is the wrong abstraction," lsm-rs agrees.
2. What's the target use case — coastal sim data replication, general
   workstation backup, or something else? That decides whether we need
   NFS/SSH transport tiers or just local mirror.
3. Are we okay with GPL-2.0 (foxing's license) for a fork path? MIT-only
   means parallel build.
4. Kernel version floor — committed to 6.14+ (FAN_PRE_MODIFY), or do we
   need to support older (which forces BPF-first)?
5. Is there a specific win condition that foxing *doesn't* hit? If yes,
   that's the spec for lsm-rs. If no, Path A is the answer.

---

## 9. References

**Kernel & APIs**
- fanotify(7): https://man7.org/linux/man-pages/man7/fanotify.7.html
- FAN_PRE_ACCESS / FAN_PRE_MODIFY patch series: https://lwn.net/Articles/985013/
- Phoronix 6.14 pre-content fanotify: https://www.phoronix.com/news/Linux-6.14-precontent-fanotify
- btrfs send stream format: https://btrfs.readthedocs.io/en/latest/dev/dev-send-stream.html
- NILFS2 continuous snapshotting: https://docs.kernel.org/filesystems/nilfs2.html
- bcachefs snapshots: https://bcachefs.org/Snapshots/
- FICLONE / FICLONERANGE: https://man7.org/linux/man-pages/man2/ioctl_ficlonerange.2.html

**Prior art**
- foxing: https://codeberg.org/aenertia/foxing
- ize (predecessor): https://github.com/fluid-notion-labs/ize

**eBPF tooling**
- aya: https://github.com/aya-rs/aya
- libbpf-rs: part of https://github.com/libbpf/libbpf
- oxidebpf: https://github.com/redcanaryco/oxidebpf
- filesystem-watcher-with-ebpf writeup: https://amandeepsp.github.io/blog/fs-watcher/

**Reading list (priority order)**
1. foxing `ARCHITECTURE.md` + event-pipeline.svg (the diagram)
2. LWN pre-content fanotify series (#985013)
3. btrfs-send(8) + send-stream format
4. aya book — ch. on LSM hooks
