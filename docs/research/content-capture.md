# Content Capture — Kernel Options for Full Write Payloads

**Branch:** `timetraveler`
**Status:** research — maps the design space, doesn't commit to one option
**Problem:** foxing captures write *ranges* (inode + offset + length) and
pulls content from the source file later. This is fast, cheap, and
enough for "eventually consistent mirror." It is **not** enough for
true time-travel where every intermediate write state matters.

This document enumerates every plausible way to capture the actual
bytes of every write, with latency, overhead, fidelity, kernel-
invasiveness, and feasibility for each. Plus what an
"async-cache-with-notifications-and-filters" subsystem actually looks
like if we build one.

---

## 1. Requirements

What "full write payload capture" actually needs:

- **R1 — Byte fidelity.** Every write() / pwrite() / writev() /
  mmap-dirty operation's actual bytes, including intermediate states
  before fsync or coalescing.
- **R2 — Non-blocking for writer.** The observed process must not
  stall on userspace doing anything slow. Blocking is acceptable only
  as briefly as a kernel→kernel memcpy.
- **R3 — Async retrieval.** The capture path enqueues; a consumer
  reads asynchronously. If the consumer is slow, buffers drain
  oldest-first (lossy at a configurable watermark). No backpressure
  into the writer.
- **R4 — Event-based notification.** Consumer is woken when data is
  available (epoll / poll / io_uring_wait).
- **R5 — Filtering.** Per-mount, per-inode, per-pid, per-size.
  Filtering done in-kernel so irrelevant events never cross the
  user/kernel boundary.
- **R6 — Eventually overwritten.** Buffer is bounded. Old entries
  drop when full. This is correct behaviour, not a bug — a snapshot
  is the durable fallback.

---

## 2. Option space (sorted by kernel-invasiveness, low to high)

### Option A — eBPF kprobe/tracepoint with payload copy into ringbuf

**What it is:** Extend foxing's existing `mirror.bpf.c` to, on
`vfs_write_iter`, walk the `iov_iter`, copy bytes from the user
buffer into the BPF ring buffer, submit alongside the existing
metadata event.

**How the bytes travel:** `vfs_write_iter` gets a `struct iov_iter *`
as second arg. In BPF context we can read its `iov_base` (user
pointer) and iterate — but only with strict verifier limits.

**Verifier constraints (the hard bit):**
- BPF stack is 512 bytes. Large buffers must go through
  ring-buffer reservation.
- No unbounded loops. `bpf_loop()` helper exists but is capped
  (default 1M iterations, practically limited to small fixed counts).
- `bpf_probe_read_user()` copies each chunk — not cheap.

**Recent improvement: `bpf_dynptr` (Linux ≥ 5.19):**
- `bpf_ringbuf_reserve_dynptr(&rb, size, 0, &ptr)` reserves a
  **runtime-variable-sized** slot in the ring buffer.
- `bpf_dynptr_write()` / `bpf_probe_read_user_dynptr()` fill it.
- Verifier checks bounds *at runtime*, not compile time — this is
  what unlocks variable write sizes.

**Practical cap:** a few KB to tens of KB per event. For larger
writes, truncate and emit a "payload-too-large + source-file-offset"
event, have userspace pull the remainder from the live file. Same
strategy eCapture uses for TLS plaintext (16 KB cap).

**Pros:**
- Zero kernel patching. Deployable now on Linux 5.19+.
- Ring buffer is shared across CPUs (since 5.8), ordering preserved.
- Filtering in-kernel (check pid / dev_id / inode / size before
  reserving buffer space). Meets R5 natively.
- Adaptive wakeup (consumer caught up → notify; still processing →
  don't). Meets R4.
- When consumer is slow, ring wraps silently. Meets R6.

**Cons:**
- Per-write copy cost. Not zero-cost; writers see microsecond latency
  added per hooked write.
- BPF verifier ABI creep across kernels. Production projects pin
  kernel versions.
- Requires CAP_BPF / root.
- Not 100% fidelity on mmap writes — page-dirty path doesn't hit
  `vfs_write_iter`. (Separate hook needed: `folio_mark_dirty` or
  similar, with higher overhead.)

**Verdict:** the pragmatic pick. Matches all requirements R1–R6 for
read/write syscalls; degrades gracefully on unsupported paths
(mmap). This is what I'd prototype first.

---

### Option B — fanotify `FAN_PRE_MODIFY` (kernel 6.14+)

**What it is:** User registers a `FAN_CLASS_PRE_CONTENT` group with
`FAN_PRE_MODIFY`. Kernel fires the event **before** the write
completes, blocking the writer until userspace responds `FAN_ALLOW`.
The event carries a file descriptor and range (offset + length).
Userspace can pread the old bytes (undo log) or let the write
proceed and pread the new bytes (redo log).

**How bytes travel:** not directly — the event is a metadata
notification. Userspace does a `pread(fd, buf, len, offset)` against
the same fd to get the actual content. Cache-warm read, cheap.

**Pros:**
- No kernel patching. UAPI-stable (Meta ships this in production for
  HSM tiered storage).
- Full byte fidelity including mmap — pre-content hook fires on
  page fault too (bcachefs / gfs2 / xfs already hooked upstream).
- Pre-image capture possible (read before letting write proceed).
- Kernel-internal permission-event mechanism; high reliability.

**Cons:**
- **Writer blocks** on userspace response. This violates R2. Meta's
  benchmark showed ~17s overhead on a kernel tree build — 3% penalty
  — acceptable but not free. For high-throughput workloads this is
  the wrong hammer.
- 5-second default timeout; kernel will `FAN_ALLOW` automatically
  past that, so worst case isn't catastrophic.
- Requires kernel 6.14 — Nick's Ryzen 7 box needs verification.
  Ubuntu 26.04 LTS (April 23) ships 7.0 so this is imminent-default.
- Requires `CAP_SYS_ADMIN`.

**Verdict:** best fidelity, highest latency. Right pick for
**workloads where correctness > throughput**: scientific computing,
source code editing, databases with user-accepted fsync-latency
tradeoff. Wrong pick for: build systems, general workstation,
simulation data pipelines where throughput is the point.

---

### Option C — `iov_iter` walk via a userspace-backed FUSE or overlay

**What it is:** Interpose a thin FUSE/overlay layer that captures
every write before passing through to the real FS.

**Pros:**
- Complete fidelity, complete control, user-space-only.
- No kernel patching. Works on any kernel.

**Cons:**
- **FUSE is what we're routing around.** Even FUSE-passthrough adds
  10–100 µs per op plus two context switches. This is the old ize
  architecture. Foxing's whole existence is because this was too
  slow.
- Same general problem with OverlayFS+userspace-capture variants.

**Verdict:** rejected. Back to the ize problem. Only worth
reconsidering if Options A+B+D all fail.

---

### Option D — Custom kernel module (LSM hooks + pagewalk)

**What it is:** Write a kernel module (Rust-in-kernel now stable as of
7.0) that registers LSM hooks or overrides `address_space_operations`
on the VFS layer. Module allocates a bounded kernel buffer,
intercepts writes, copies payloads, exposes via
`/dev/timetraveler` char device or a netlink socket to userspace.

**Pros:**
- Full fidelity, including mmap. LSM `file_permission` hook fires on
  every access; can hook `write_end` for page-cache pre-writeback
  content.
- Can use any kernel primitive: per-CPU pools, RCU, lockless queues.
- Rust-in-kernel is *stable*, not experimental, since April 12, 2026.
- Can mmap the buffer to userspace directly for zero-copy reads
  from the consumer side.
- No BPF verifier constraints — arbitrary code within the module.

**Cons:**
- **Kernel module distribution pain.** DKMS or per-distro packaging.
  Users hate this. bcachefs just got removed from in-tree in 6.18
  and users are already complaining.
- **Upstream acceptability is questionable.** The use case is
  niche ("I want every write's bytes captured to userspace"); the
  kernel mailing list will ask why not use existing APIs. Answer
  has to be a benchmark showing Options A/B fundamentally inadequate.
- **Security surface.** Anything that captures every write is one
  bug away from an information disclosure vulnerability.
- **Maintenance burden.** VFS internals churn. Rust-in-kernel API
  is stable, but the kernel types we bind to (`struct iov_iter`,
  `struct file`, `struct folio`) are not.
- Hard to test. `virtme-ng` or equivalent required.

**Verdict:** a year-long research project, not a branch deliverable.
Only makes sense if:
1. We prove Options A and B are inadequate for a specific,
   important workload.
2. We commit to upstreaming, with a legitimate use case that
   survives LKML scrutiny (HSM / security / forensics).
3. We have resources for maintenance across kernel versions.

Revisit if/when there's a concrete customer for whom A+B aren't
enough.

---

### Option E — Page-cache dirty tracking via `userfaultfd` WP-async

**What it is:** `userfaultfd`'s write-protect-async mode (since ~6.7)
lets userspace mark pages write-protected, receive faults on write
attempts, track dirtiness via `PAGEMAP_SCAN` ioctl. Originally for
CRIU / live migration / userspace garbage collectors. Could be
repurposed for write observation.

**Pros:**
- No BPF, no module. UAPI-stable.
- Works on mmap — in fact it's *only* for mmap-backed memory.

**Cons:**
- Only covers mmap'd regions, not regular read/write syscalls.
  Complement to Option A, not a replacement.
- Per-page granularity (4 KB or huge pages); can't get byte-range
  within a page without additional tracking.
- Requires userspace to have set up the mapping with WP mode —
  can't retrofit onto arbitrary processes.

**Verdict:** useful as a *secondary* capture path for mmap workloads,
combined with A. Not a primary channel.

---

### Option F — Native filesystem journals / send streams

**What it is:** Don't hook the VFS at all. Use the filesystem's own
mechanisms:
- **btrfs send** between periodic snapshots — byte-exact extent diffs.
- **bcachefs subvolume diff** — similar, key-versioned.
- **ZFS send/receive** (ZoL) — same family.
- **NILFS2 continuous checkpoints** — every few seconds, read
  checkpoint N-1 vs N.

**Pros:**
- The filesystem *already logs this*. It's the most faithful write
  capture Linux supports, because the FS built it to be.
- No VFS hook overhead. Zero on the write path.
- Proven, supported, stable.

**Cons:**
- Requires a supporting filesystem. ext4 out. XFS has reflink but not
  send.
- Granularity is "between snapshots." True live capture needs snapshot
  cadence tuned high (every few seconds) — then you're essentially at
  NILFS2.
- Send stream is a format we'd need to parse/transform for our event
  model — moderate engineering.

**Verdict:** the *cleanest* answer for users who can commit to btrfs
or bcachefs. Timetraveler should support this as a first-class
option: "if source is btrfs, use send stream instead of BPF." Falls
through to A/B for other FSes.

---

## 3. Async-cache-with-notifications — what this looks like built out

The question "what can be added to the kernel itself" suggests a
purpose-built subsystem. Here's what it would actually look like:

### Subsystem name (working title): `fwcache` — filesystem write cache

```
┌─────────────────────────────────────────────────────────────────────┐
│ Userspace (timetraveler daemon)                                     │
│                                                                     │
│   epoll wait ──► read batches from /dev/fwcache/<uuid>              │
│                   │                                                 │
│                   └─► {event_hdr, payload_slab_offset, len}         │
│                                                                     │
│   mmap(fd, ...) ─► direct read of payload slab (zero-copy)          │
└──────────────▲──────────────────────────────────────────────────────┘
               │ (epoll / poll / uring_wait notification)
               │
┌──────────────┼──────────────────────────────────────────────────────┐
│ Kernel       │                                                      │
│              │                                                      │
│  VFS write path                                                     │
│  └─► LSM hook: fwcache_on_write(file, iov, offset, len, pid)        │
│      │                                                              │
│      ├─► filter chain (per-mount, per-inode, per-pid, per-size)     │
│      │   (check filter bitmap in percpu map)                        │
│      │                                                              │
│      ├─► slab allocator: reserve len bytes in ring (lockless CAS)   │
│      │   (wraps around on full — dropped_count++)                   │
│      │                                                              │
│      ├─► memcpy iov → slab  (kernel→kernel, no user space)          │
│      │                                                              │
│      └─► enqueue event header (seq, inode, offset, len,             │
│                                 slab_offset, timestamp_ns)          │
│                                                                     │
│  wakeup() on watermark crossing                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Characteristics

- **Separate data and metadata rings.** Metadata ring is small,
  high-churn events. Data slab is large (GB-scale, configurable),
  append-only with ring wrap, addressed by offset. Same trick NIC
  DMA rings use.
- **mmap the data slab read-only to userspace.** Consumer reads
  payload via direct memory access, no syscalls.
- **Watermark-driven wakeup.** Don't wake userspace per event;
  wake when N% full or T ms elapsed since last wake, whichever
  comes first. (CAKE-style — foxing already uses this pattern.)
- **Per-group tenancy.** Multiple independent consumers, each with
  their own filter + slab pair.
- **Drop policy configurable per-group.** Options:
  - `DROP_OLDEST` — FIFO overwrite (default; what R6 asks for).
  - `DROP_NEWEST` — preserve history prefix, lose tail.
  - `APPLY_BACKPRESSURE` — block writer (violates R2; only for
    HSM-style durability guarantees).
- **In-kernel filtering.** Compiled eBPF-style predicate or a
  simple match-list per group. Filtering happens before slab
  reservation — irrelevant writes cost nothing.

### Implementation paths

1. **eBPF-only (Option A).** Build this *entirely* in userspace with
   a ring buffer. Simplest, shipping today. Limitations: kernel→
   userspace copy cost, verifier payload caps (dynptr helps).
2. **Kernel module (Option D).** Ship as a DKMS module. Native
   performance. Distribution pain.
3. **In-tree patch.** Full upstream path. Year-long effort, requires
   justification. Realistic only if we have a customer and a story.

### What actually exists in-tree that's close

- **fanotify + `FAN_PRE_MODIFY` + userspace slab.** The kernel does the
  "notify + provide fd" part. Userspace does the "read + cache"
  part. Not automatic caching in-kernel but structurally identical
  once you wire it up.
- **perf event array + ring buffer.** The mature precursor to the
  design above. Used for tracing, not filesystem I/O, but the
  mechanical shape is the same.
- **audit subsystem.** Has the "kernel-buffered events with userspace
  drain" model, including policy-based filtering. Not extensible to
  carry payloads, though — events are records, not blobs.

### If I were designing this today

I'd build **fwcache as a userspace library first**, using Option A
(BPF with dynptr for variable-size payloads) as the backend.
Expose a clean Rust API:

```rust
let cache = Fwcache::builder()
    .mount("/data")
    .filter(FilterSpec::exclude_paths(&["/data/tmp/**"]))
    .filter(FilterSpec::min_size(64))
    .slab_size(2 * GB)
    .drop_policy(DropPolicy::DropOldest)
    .build()?;

while let Some(event) = cache.recv().await {
    // event.payload is a zero-copy slice into the slab
    process(event);
}
```

Then benchmark it against realistic workloads. If it's fast enough,
stop there — no kernel work needed. If it isn't, we know exactly
what's slow and have a concrete case for either a module (D) or
kernel patches.

---

## 4. Fidelity matrix

| Capture | Reg. write | pwritev | mmap | Pre-image | Writer block | Loss mode | Kernel |
|---|---|---|---|---|---|---|---|
| A (BPF + dynptr ringbuf) | ✅ | ✅ | ⚠️ needs `folio_mark_dirty` hook | ❌ (post-only) | none | silent drop | 5.19+ |
| B (`FAN_PRE_MODIFY`) | ✅ | ✅ | ✅ (page fault hook) | ✅ (read before allow) | 5s timeout | cannot drop, blocks | 6.14+ |
| C (FUSE overlay) | ✅ | ✅ | ⚠️ mmap via FUSE is slow | ✅ | per-op | 2 ctx switches | any |
| D (kernel module) | ✅ | ✅ | ✅ | ✅ if hooked pre-`write_end` | configurable | configurable | any (module) |
| E (`userfaultfd` WP) | ❌ | ❌ | ✅ | ✅ (WP trap) | per-page | blocks faulting thread briefly | 6.7+ |
| F (native FS diff) | ✅ | ✅ | ✅ | ❌ (snapshot delta) | none | none | btrfs/bcachefs/ZFS |

Best coverage = B+F combination. Best throughput = A. Best fidelity
with least blocking = F where supported, A elsewhere, E as mmap
supplement.

---

## 5. Concrete proposal for timetraveler

Extend the `CaptureFidelity` enum (from the previous design doc):

```rust
enum CaptureFidelity {
    /// Foxing's current model. Snapshot-at-trigger.
    CoalescedSnapshot,

    /// Option A. eBPF with dynptr. All regular writes captured with
    /// byte-fidelity; mmap falls back to CoalescedSnapshot.
    BpfPayloadCapture {
        max_payload_bytes: usize,   // per event; >this → metadata-only
        slab_bytes: usize,          // total ring size
        drop_policy: DropPolicy,
    },

    /// Option B. fanotify pre-content. Full fidelity including mmap,
    /// at cost of writer blocking.
    PreContentBlocking {
        capture_preimage: bool,
        max_latency_ms: u32,        // give up after this, FAN_ALLOW
    },

    /// Option F. Native FS diff. Requires btrfs / bcachefs source.
    NativeSnapshotDiff,

    /// Compose multiple — first one that succeeds per event wins.
    /// E.g., [NativeSnapshotDiff, BpfPayloadCapture { .. }] means
    /// "use btrfs send if available, fall back to BPF".
    Tiered(Vec<CaptureFidelity>),
}
```

Default: `CoalescedSnapshot` (current foxing). Users opt into higher
fidelity for specific workloads. Sim/database profiles get sensible
presets.

The `fwcache` library I sketched in §3 would be the implementation
for `BpfPayloadCapture`. It's a one-week spike to have it working,
two more weeks to have it production-shaped.

---

## 6. What I'm *not* proposing

Things that came up while researching but which I'm parking:

- **Full in-kernel write log.** Too invasive, too niche. Build it in
  userspace on top of BPF; upstream if and only if benchmarks force
  the issue.
- **Replace foxing's metadata-only BPF with payload BPF.** Wrong
  default. Most users don't need byte-level fidelity and the
  copy overhead isn't free. Keep foxing's metadata pipeline as the
  default; offer payload capture as an opt-in.
- **Hook every write twice (pre + post).** Tempting for exact
  before/after pairs, doubles overhead, rarely needed. Use B for
  workloads that require pre-images.
- **Custom ring buffer design.** BPF ringbuf with dynptr is already
  most of what §3's `fwcache` needs. Reinventing the ring is
  premature optimisation.

---

## 7. Open questions

1. Nick's kernel version on the Ryzen 7 box? (Decides A vs B vs both.)
2. Concrete use case for per-write fidelity — is there one? Mesh
   sequences are written-once, databases fsync on commit, source
   code gets fsynced by editors. What workload actually needs
   intermediate write states preserved?
3. Is upstreaming (either to foxing or to the kernel) an explicit
   goal, or is this a downstream-only branch? Affects whether we
   write throwaway prototype vs merge-ready code.
4. Build a prototype `fwcache` in Rust this week as a spike?
   ~300 LOC userspace + a BPF C program; answers a lot of these
   questions empirically.

---

## 8. References

**BPF mechanics**
- BPF ring buffer — https://nakryiko.com/posts/bpf-ringbuf/
- Dynptrs — https://docs.ebpf.io/linux/concepts/dynptrs/
- `bpf_dynptr` ringbuf patch — torvalds/linux@bc34dee
- Map type `BPF_MAP_TYPE_RINGBUF` — https://docs.ebpf.io/linux/map-type/BPF_MAP_TYPE_RINGBUF/

**Kernel APIs**
- fanotify pre-content patch series — https://lwn.net/Articles/985013/
- Phoronix 6.14 pre-content fanotify — https://www.phoronix.com/news/Linux-6.14-precontent-fanotify
- Page cache / writeback — Linux kernel `Documentation/filesystems/`
- `userfaultfd` WP-async — kernel `Documentation/admin-guide/mm/pagemap.rst`

**Prior art**
- eCapture (TLS plaintext capture via BPF + perf buffer, 16 KB cap per event) —
  https://ecapture.cc/en/2-architecture/2.9-ebpf-maps-and-data-structures.html
- foxing mirror pipeline — `docs/ARCHITECTURE.md` in this repo
- lsm-rs initial analysis — `docs/research/lsm-rs.md` in this repo
