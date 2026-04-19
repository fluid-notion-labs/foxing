# Timetraveler — Snapshot & Time-Travel FS on foxing

**Branch:** `timetraveler` off `main` @ v0.8.1
**Status:** design — pre-code
**Owner:** fluid-notion-labs

---

## 0. What this branch is

Foxing's stated identity is **replication** — source → target mirror. Time-travel
capability exists (MARS) but is *implicit*: it falls out of the mirror's
reflink strategy when you enable `enable_versioning = true`.

This branch inverts the emphasis: **time-travel as the primary product**,
replication as an infrastructure detail. The output is still a foxing daemon
and a CLI, but the user-facing mental model and the CLI surface shift.

We're not starting from scratch. Concretely, ~80% of the machinery is
already here — we're reframing, extending, and filling gaps.

---

## 1. What foxing already does (inventory)

Reading the code, the existing pieces that matter:

### MARS (Mirror & Archive Recovery System) — `fxcp-core/src/versioning.rs`

- `FileVersion { inode, epoch_seq, timestamp, path, size, mtime, content_hash }`
- `VersionIndex { inode_index: DashMap, hash_index: DashMap, ... }` — two-way
  lookup: by inode (history of a file) and by content hash (dedup).
- Public API: `list_versions`, `cleanup_versions`, `prune_global_history`,
  `copy_version_to_path`, `revert_file`, `force_version_cli`.
- 368 lines; accompanying `version_store.rs` is 1075 lines.

### Storage layout — already dual-indexed

```
<target>/.foxing_versions/
├── index.json                        # Machine-readable manifest
├── <timestamp>/                      # Point-in-time snapshot (dirvish view)
│   ├── summary.json
│   └── tree/                         # Reflinks to file state at that moment
└── files/                            # Per-file view (Time Machine style)
    └── path/to/file~<timestamp>      # Symlink into tree/
```

Two views of one data set:
- **Temporal:** `cd` into `2026-03-12T084500/tree/` and browse as if it were then.
- **Per-file:** `ls files/data/db.sqlite~*` to see every version of one file.

This is the right dual-mount. It's what ize was reaching for via FUSE and
Pijul; foxing got there via reflinks + manifest, which is simpler and faster.

### Snapshot trigger points

- On `fsync` in mirror mode (live replication creates versions automatically).
- Via `fxcp -a --snapshot /source /target` (manual, from copy).
- Via `fxcp snap force <path> --tag <name>` (explicit tagged snapshot).

### CLI surface (existing)

```
fxcp snap list <path>              # List versions
fxcp snap stats <path>             # CoW savings report
fxcp snap revert <path> <epoch>    # Atomic rollback
fxcp snap copy <path> <epoch> <dest>  # Extract to file
fxcp snap prune --older-than 30d   # Cleanup
fxcp snap export <path> -o a.fxar  # Portable archive
fxcp snap import <path> -i a.fxar
fxcp snap restore <archive> --file 'pattern' --latest -o <dest>
```

### Archive format — FXAR v2

Content-addressable, BLAKE3-keyed, gear-hash chunked (2KB–2MB, 64KB avg),
96% dedup on slowly-changing data. Streamable over pipes / SSH. Seekable
(binary chunk index, O(chunks) random-access restore).

**All of the above exists and works.** We're not reinventing it.

---

## 2. What timetraveler adds

The gaps between "snapshotting that happens during mirroring" and
"time-travel filesystem as a product":

### 2.1 Triggering model — beyond fsync-only

Currently MARS snaps on fsync. Real workloads need richer policies:

| Trigger | Status | Needed? |
|---|---|---|
| On fsync | ✅ implemented | yes |
| On interval (every N min) | ⚠️ partial (via foxingd config) | extend |
| On idle (no activity for T) | ❌ | yes — natural "commit" point |
| On file close (last fd released) | ❌ | yes — catches writers that don't fsync |
| On tagged event (CLI or API) | ✅ `snap force` | yes |
| On external signal (webhook, file watcher) | ❌ | later |
| On content churn threshold (N MB changed) | ❌ | later |

**Action:** add a `TriggerPolicy` abstraction to `fxcp-core::versioning`,
composable (OR/AND of triggers), configurable per-mount.

### 2.2 Mountable past

Foxing lets you `cd` into a snapshot tree, but it's a live directory of
reflinks — editing it modifies the snapshot. For a true time-travel model
we want:

- **Read-only bind mount** of a past state: `fxcp snap mount <target> <epoch> <mountpoint>`
  → essentially `mount --bind -o ro <target>/.foxing_versions/<ts>/tree <mountpoint>`.
  Trivial, but worth CLI-blessing.
- **Overlay mount** for branch/experiment: `fxcp snap branch <epoch> <workdir>`
  → OverlayFS with snapshot as lowerdir and a scratch upperdir. Now you can
  "check out" a past state, modify it without affecting history, and merge
  back as a new branch.

This is the piece that makes "time-travel FS" feel real.

### 2.3 Cross-snapshot diff

Missing from the current CLI surface:

```
fxcp snap diff <target> <epoch-A> <epoch-B>        # list of changed files
fxcp snap diff <target> <epoch-A> <epoch-B> --content  # per-file diffs
fxcp snap log <target> [--since <time>]            # like git log for files
fxcp snap blame <path>                             # which snapshot introduced this content
```

The data is there (VersionIndex + content hashes). It's a CLI layer.

### 2.4 Branching (the hard part)

Foxing is linear: snapshots form a timeline. Time-travel implies:

- **Read-only branches** (easy) — "mount snapshot X as a subvolume, take
  further snapshots against it." Just another epoch chain rooted at X.
- **Writable branches** (hard) — "fork history at epoch X, make changes,
  merge or discard." Requires either OverlayFS (lightweight, but conflict
  resolution is user's problem) or a proper change-algebra like Pijul's
  (heavyweight, but mathematically clean).

**Proposal:** start with OverlayFS branches. Pijul-style is ize's old
ambition; foxing's reflink model is at a lower layer. If users want true
merging, they compose with git/jj on top of a mounted branch.

### 2.5 Retention policies — time-travel appropriate

Current MARS retention is count-based (`max_versions`) or size-based
(`max_versions_size_mb`). For time-travel we want **logarithmic retention**
(à la ZFS zsnap / btrbk):

- Keep all snapshots from last hour.
- Keep hourly for last day.
- Keep daily for last month.
- Keep monthly for last year.
- Keep yearly forever.

**Action:** `RetentionPolicy::Logarithmic { ... }` as a config variant.
Compose with existing count/size caps for ENOSPC safety.

### 2.6 Simulation data-shape awareness (stretch goal)

Specific to FNL's use case: mesh sequences from FluidX3D have a shape:

- Frames are large (10s of MB to GB), numbered sequentially.
- Files are mostly-static once written (not edited in place).
- The important granularity is "simulation run" not individual frame.
- Per-run metadata (bathymetry, parameters, solver config) is small and
  critical.

For sim workloads, `fxcp snap` could optionally understand:

- A `.simrun` manifest file marks a boundary — snapshot triggers align to
  it (not fsync of individual frames).
- Frame files go straight to FXAR CAS with no intermediate live copy
  (they're immutable once written).
- Parameter/config small files get fsync-tracked normally.

This is a thin layer — probably a `TriggerPolicy::ManifestBoundary` and
a few lines in the archiver. Don't put this in the core; ship it as an
opt-in profile.

---

## 3. Architectural fit — where timetraveler lives

### 3.1 Layer diagram (current vs proposed)

```
Current (foxing v0.8.1):
┌─────────────────────────────────────────────────┐
│   fxcp / foxingd CLI                            │
├─────────────────────────────────────────────────┤
│   foxingd replication pipeline                  │
│   (BPF → Reorder → Filter → Dispatcher → Worker)│
├─────────────────────────────────────────────────┤
│   fxcp-core (SmartCopier, MARS versioning,      │
│              FXAR archive, NFS bypass, reflink) │
├─────────────────────────────────────────────────┤
│   Kernel (VFS, io_uring, FICLONE)               │
└─────────────────────────────────────────────────┘

timetraveler addition:
┌─────────────────────────────────────────────────┐
│   fxtt (new CLI) / or fxcp tt <subcmd>          │
├─────────────────────────────────────────────────┤
│   fxcp-tt — time-travel semantics layer         │  ← NEW
│   · TriggerPolicy   · RetentionPolicy           │
│   · MountAdapter    · DiffEngine                │
│   · BranchManager                               │
├─────────────────────────────────────────────────┤
│   (re-exports MARS + FXAR + VersionIndex)       │
├─────────────────────────────────────────────────┤
│   existing foxing layers                        │
└─────────────────────────────────────────────────┘
```

Aim: minimise changes to existing crates. `fxcp-tt` is a new workspace
member that depends on `fxcp-core` and adds semantics without touching the
replication pipeline.

### 3.2 Workspace delta

```toml
# Cargo.toml
[workspace]
members = ["fxcp-core", "foxingd", "fxcp", "xtask", "fxcp-tt", "fxtt"]
#                                                  ^^^^^^^^^  ^^^^^
```

- `fxcp-tt`   — library crate: trigger, retention, mount, diff, branch.
- `fxtt`      — thin CLI binary, or (alternative) new subcommand tree
                under existing `fxcp snap` namespace. Prefer the latter to
                avoid binary sprawl; see §5.

---

## 4. Concrete gap-to-delivery

What exists → what needs writing, by chapter:

| Area | Exists | Gap | Effort |
|---|---|---|---|
| Reflink snapshot | ✅ MARS | — | 0 |
| Index + manifest | ✅ VersionIndex + index.json | — | 0 |
| Dual view (PIT + per-file) | ✅ | — | 0 |
| fsync trigger | ✅ | — | 0 |
| CLI (list/revert/prune/export) | ✅ | — | 0 |
| FXAR archive | ✅ v2 | — | 0 |
| Interval trigger | ⚠️ partial in foxingd | extract to shared policy | 2d |
| Idle trigger | ❌ | new | 3d |
| Close-on-last-fd trigger | ❌ | needs fanotify FAN_CLOSE_WRITE hook | 3d |
| TriggerPolicy composition | ❌ | new trait + combinators | 2d |
| Logarithmic retention | ❌ | algorithm + config | 2d |
| `snap mount` (read-only) | ❌ | bind-mount wrapper | 1d |
| `snap branch` (overlayfs) | ❌ | overlay setup + lifecycle | 4d |
| `snap diff` | ❌ | walk two trees, diff via hashes | 3d |
| `snap log` | ❌ | VersionIndex traversal + formatting | 2d |
| `snap blame` | ❌ | reverse content-hash lookup | 2d |
| Sim manifest boundary trigger | ❌ | opt-in TriggerPolicy variant | 2d |
| Docs + tests | — | adjacent to each feature | ongoing |

**Sum: ~5 weeks of focused work for full scope. A useful MVP (triggers + mount + diff + log) in ~2 weeks.**

---

## 5. CLI: subcommand vs new binary

Two options:

**Option A — extend `fxcp snap`:**
```
fxcp snap list/revert/prune/export        (existing)
fxcp snap mount <target> <epoch> <mnt>    (new)
fxcp snap branch <target> <epoch> <mnt>   (new)
fxcp snap diff <target> <a> <b>           (new)
fxcp snap log <target> [-f <file>]        (new)
fxcp snap blame <file>                    (new)
fxcp snap tt policy set <path> <spec>     (new — trigger/retention)
```

**Option B — new `fxtt` binary:**
Separate tool, inherits fxcp-core, focuses on time-travel UX. Makes the
product story cleaner if this is pitched independently of foxing.

**Recommendation: A for now, B later if this grows its own identity.**
Upstreaming to aenertia is much easier if it's `snap` subcommands.

---

## 6. Upstreaming vs carrying

Four categories in the diff:

1. **General improvements** (better triggers, log retention, diff/log/blame CLI)
   → upstream to aenertia. Useful to all foxing users.
2. **Mount/branch features** → candidate for upstream but more invasive;
   propose as a feature flag (`--features timetravel`).
3. **Sim-specific trigger policies** → keep downstream, ship as an opt-in
   profile or a separate tiny crate consumers plug in.
4. **FNL integration (Manifold/Seaview hooks)** → keep downstream, not
   generally useful.

Strategy: every commit that could upstream, write commit-clean. Tag
`upstream-candidate/*` branches. When aenertia does a v0.9 cycle, open PRs.

---

## 7. Branch protocol on this fork

```
main            ← track aenertia codeberg main verbatim; never push here
  └─ upstream/v0.8.1  (tag)
timetraveler    ← this work; rebase onto main periodically
  ├─ tt/triggers
  ├─ tt/mount
  ├─ tt/diff
  └─ tt/sim-profile   (keeps sim-specific stuff separate)
```

Keep `main` clean and upstream-aligned. Do work in feature branches off
`timetraveler`. Rebase `timetraveler` onto `main` when upstream moves.

---

## 8. Immediate next steps

Ordered:

1. **Build and run foxing locally.** Verify the existing versioning story
   on Nick's btrfs scratch partition. Confirm CoW numbers match
   VERSIONING_SIMULATION.md. ~½ day.
2. **Read `versioning.rs` + `version_store.rs` end-to-end.** Map the API
   surface precisely before adding to it. ~1 day.
3. **Spike: `fxcp snap log`.** Smallest useful new feature, pure read-path,
   exercises VersionIndex. Delivers value immediately. ~2 days.
4. **Spike: `fxcp snap mount`.** Bind-mount wrapper. 1 day; mostly plumbing.
5. Then: triggers refactor → diff → branch → retention.

---

## 9. Questions outstanding

1. Target filesystem on Nick's box — btrfs or XFS? (Both support reflink;
   decides which kernel path is default test.)
2. Do we care about non-reflink targets (ext4)? Foxing has a sidecar
   fallback but it's more expensive. If answer is "no, btrfs/XFS only," we
   can simplify a lot of the retention logic.
3. Sim-specific triggers: actual use case now, or speculative? Affects
   whether to build it in phase 1 or leave for later.
4. Is mountable-past worth the OverlayFS complexity, or is `cd`-into-tree
   enough? (Read-only bind is trivial; writable branches are where cost
   lives.)
5. GPL-2.0 copyleft sits fine with all planned work — confirm before
   merging anything into Manifold/Seaview that isn't already GPL-compatible.

---

## 10. References

**Foxing internal**
- `docs/ARCHITECTURE.md` — pipeline, error handling, mount monitoring
- `docs/SNAPSHOTS.md` — MARS storage layout, JSON schema, FXAR format
- `docs/VERSIONING_SIMULATION.md` — disk space analysis per workload type
- `fxcp-core/src/versioning.rs` — FileVersion, VersionIndex
- `fxcp-core/src/version_store.rs` — on-disk store
- `fxcp-core/src/fxar.rs` — archive format

**External**
- `lsm-rs/docs/lsm-rs.md` — prior analysis; foxing positioning in Linux fs observability space
- fanotify FAN_CLOSE_WRITE / FAN_MODIFY for close-triggered snapshots
- btrbk / zsnap / sanoid — reference retention policies
- OverlayFS man page — for writable branch implementation
