# Timetraveler — Kernel-Level FS Time Machine for AI-Assisted Development

**Branch:** `timetraveler` on `fluid-notion-labs/foxing`
**Status:** design — this is the one that commits to a thesis
**Platform:** Linux only (kernel ≥ 6.14 baseline, 7.0 preferred)
**Language:** Rust (userspace), C (eBPF programs)
**License:** GPL-2.0-or-later (foxing fork)

Supersedes the jj-native sketch. That was correct about the product
surface, wrong about the level of ambition at the capture layer.

---

## 0. The thesis

Every existing solution for "time-travel AI edits" is implemented at
the wrong layer:

- **Claude Code `/rewind`** hooks its own Write/Edit tool. Misses Bash
  `sed`. Misses anything outside Claude's tool call surface. Per-tool,
  per-session.
- **Cursor "Undo AI"** same, per-editor.
- **Aider `/undo`** same, per-CLI.
- **`claude-code-rewind`** (3rd party) hooks Claude's events. Same class.
- **jj auto-snapshot** hooks the VCS layer. Coarse (Watchman-debounced).
  Per-repo. No agent awareness.
- **git + commit discipline** the manual version of jj. Worse.
- **Filesystem snapshots (btrfs, NILFS2, ZFS)** coarse, whole-volume,
  no agent attribution.

All of them are right about the *shape* of the product. All of them
are at the wrong level of the stack.

**The right level is the kernel.** One persistent system-level capture
of every filesystem write on the workstation, with process provenance
attached at the moment of the write, sourced from the VFS itself.
Then everything else — sessions, agent attribution, jj integration,
MCP, curation — becomes derived views over a single authoritative
event stream.

Nobody has built this because it's genuinely hard. That's the moat.

---

## 1. What "kernel-level" actually means here

Not "we wrote a kernel module." Specifically:

1. **Event capture in the kernel** via eBPF, attached to either LSM
   hooks (stable ABI) or VFS kprobes (flexible, less stable). Events
   land in a shared ring buffer the instant the kernel sees the
   write, before any userspace debouncing.

2. **Process provenance attached at capture time.** `bpf_get_current_pid_tgid()`
   runs in the probe; the pid is captured synchronously with the
   write. No race, no "which pid was active when Watchman noticed."

3. **Content capture in the kernel** for the write payload itself.
   Either via `FAN_PRE_MODIFY` (Linux 6.14+, blocks writer briefly)
   for full fidelity, or via BPF `bpf_dynptr` copy for non-blocking
   with per-event size caps, or via reflink-snapshot when the FS
   supports it. Tiered by workload.

4. **Persistent content-addressed store.** Userspace daemon consumes
   the ring buffer, writes to a BLAKE3-keyed CAS on local disk. This
   is the durable ground truth. Reflinks used where the backing FS
   supports them, so a 1 GB file that gets modified costs ~4 KB of
   new storage.

5. **Query as first-class.** The event log is indexed (sqlite or
   similar): by path, time, pid, tgid, comm, session. "What changed
   between 14:00 and 14:30" and "what did pid 12345 touch" and "show
   me every version of this file in the last month" are all O(log N)
   queries.

6. **Views on top.** The jj integration, the MCP server, the git
   export, the TUI — all read-only consumers of the event log + CAS.
   They don't capture anything themselves.

The critical architectural property: **capture is one thing, views
are another.** The same kernel capture drives the jj view, the MCP
query, the "what did Claude just do" TUI. You can add views forever
without touching the capture layer.

---

## 2. Why this can't be a jj plugin / Claude Code plugin / editor plugin

Because any of those plugins is missing the writes the other two
made. You cannot get "true ground truth of what happened on my disk"
from any single application's hook.

Concretely, all of these happen in a normal dev day and need to be
captured in one log:

- Claude Code `Write` tool edits `src/lib.rs`.
- Claude's `Bash` tool runs `sed -i 's/foo/bar/g' src/*.rs`.
- You edit `src/lib.rs` in Zed.
- `cargo build` writes to `target/`.
- `rust-analyzer` creates `.cache/` files.
- Claude Code runs `cargo fmt`, which rewrites every `.rs` file.
- You `touch .env` to trigger a dev-server reload.
- A sync daemon pulls something into `~/projects/foo/`.

The only place you see all of these in order, with attribution, is at
the VFS / LSM layer. Every layer above loses information.

---

## 3. Concrete design

### 3.1 Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ KERNEL                                                               │
│                                                                      │
│   Any process ──► VFS write path                                     │
│                       │                                              │
│                       ▼                                              │
│                   ┌────────────────────────────────────┐             │
│                   │  fwcache BPF programs              │             │
│                   │                                    │             │
│                   │  1. lsm/file_permission (mask)     │ fast filter │
│                   │     OR kprobe:vfs_write_iter       │             │
│                   │                                    │             │
│                   │  2. match against path filter map  │             │
│                   │     (mount-id / inode / cgroup)    │             │
│                   │                                    │             │
│                   │  3. capture event:                 │             │
│                   │     { seq, ts_ns, pid, tgid,       │             │
│                   │       mnt_id, inode, path*, op,    │             │
│                   │       offset, len, flags }         │             │
│                   │                                    │             │
│                   │  4. optional payload capture:      │             │
│                   │     dynptr reserve → copy iov →    │             │
│                   │     submit (cap at N KB; larger    │             │
│                   │     writes record metadata only)   │             │
│                   │                                    │             │
│                   │  5. bpf_ringbuf_submit             │             │
│                   └────────────────────┬───────────────┘             │
│                                        │                             │
│                                        ▼                             │
│                         ┌──────────────────────────┐                 │
│                         │   metadata ringbuf        │                │
│                         │   (256 MB shared, MPSC)   │                │
│                         └──────────┬───────────────┘                 │
│                                    │                                 │
│                         ┌──────────▼───────────────┐                 │
│                         │   content ringbuf        │                 │
│                         │   (1 GB shared, MPSC)    │                 │
│                         └──────────┬───────────────┘                 │
└────────────────────────────────────┼────────────────────────────────┘
                                     │ epoll / io_uring wait
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│ USERSPACE DAEMON — ttd                                               │
│                                                                      │
│   ┌──────────────┐                                                   │
│   │ ingest loop  │ ← drain ringbufs, never block the kernel         │
│   └──────┬───────┘                                                   │
│          │                                                           │
│          ▼                                                           │
│   ┌──────────────┐     ┌──────────────┐                              │
│   │ enrichment   │ ──► │ pid tracker  │ ← /proc scan + procfs watch  │
│   │              │     │ (ancestors,  │                              │
│   │              │     │  cmdline,    │                              │
│   │              │     │  cgroup,     │                              │
│   │              │     │  session)    │                              │
│   └──────┬───────┘     └──────────────┘                              │
│          │                                                           │
│          ▼                                                           │
│   ┌──────────────┐     ┌──────────────┐                              │
│   │ CAS writer   │ ──► │ BLAKE3 store │ ← reflinks where possible    │
│   │              │     │ ~/.tt/cas/   │                              │
│   └──────┬───────┘     └──────────────┘                              │
│          │                                                           │
│          ▼                                                           │
│   ┌──────────────┐     ┌──────────────┐                              │
│   │ indexer      │ ──► │ event DB     │ ← sqlite / fjall / rocksdb   │
│   │              │     │ (seq, paths, │                              │
│   │              │     │  pids, hash) │                              │
│   └──────────────┘     └──────────────┘                              │
└──────────────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│ VIEW LAYER                                                           │
│                                                                      │
│   tt CLI     tt TUI     tt-mcp (MCP server for agents)              │
│   jj bridge  git export                                              │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Capture tiers

Three capture modes, selectable per-workload:

| Tier | Mechanism | Fidelity | Overhead | Kernel |
|---|---|---|---|---|
| **M** metadata-only | BPF kprobe `vfs_write_iter` | offset+len only, pull content later | ~µs/write | 5.8+ |
| **P** payload via BPF dynptr | kprobe + `bpf_dynptr_write` | full bytes up to cap (~16 KB typical) | ~µs + memcpy | 5.19+ |
| **F** full fidelity via fanotify | `FAN_CLASS_PRE_CONTENT` + `FAN_PRE_MODIFY` | pre-image + post-image, including mmap | blocks writer briefly | 6.14+ |

**Default policy:**
- tier M for everything (always on)
- tier P for paths matching agent-written files (`~/projects/**`, not `~/target/**`)
- tier F opt-in per-mount for correctness-critical workloads

Tier M + post-hoc pull from source works 99% of the time. Tier P fills
the gap for "the source file was rewritten before we could pull."
Tier F is the nuclear option for e.g. a database directory where you
cannot lose any intermediate state.

### 3.3 Content-addressed store

```
~/.tt/
├── cas/                        # BLAKE3 content store
│   ├── ab/cd/abcd1234...ef    # sharded by first 2 bytes
│   └── ...
├── events/                     # append-only event log
│   ├── 2026-04/               # sharded by month
│   │   └── 01.log             # and day
│   └── current.log            # current WAL
├── index.db                    # sqlite: events by path/pid/time/hash
├── sessions.db                 # derived: agent sessions
├── procfs/                     # snapshot of pid tree per epoch
│   └── 2026-04-19T12:00:00/
└── config.toml
```

**Why CAS:**
- Deduplication is free. If Claude rewrites the same content across
  sessions (common for format-only churn), one physical copy.
- Reflink integration: on btrfs/xfs/bcachefs, the CAS entries can be
  reflinks of the real file. Zero-copy storage of "the file as it was
  at this moment." This is what foxing's MARS already does; we reuse
  the code.
- On ext4 (no reflink), fall back to actual copies. Worse but works.
- Enables `tt show <hash>` and content queries.

**Content limits:**
- Files > N MB (configurable, default 100 MB) stored as "pointer +
  reflink OR source-path + inode-generation" rather than a snapshot.
  You can reconstruct IF the source file is still accessible. If not,
  you have the metadata event but not the content.
- Binary files (target/, *.o, node_modules/) filtered at the BPF
  layer via path prefix — never even enter the capture pipeline.

### 3.4 Session model

```rust
struct Session {
    id: Uuid,
    agent: Agent,
    root_pid: u32,              // root of process tree
    root_cgroup: Option<String>,
    started_at: DateTime,
    ended_at: Option<DateTime>,
    working_dirs: Vec<PathBuf>, // distinct cwds observed
    jsonl_paths: Vec<PathBuf>,  // Claude Code JSONL files correlated
    env_markers: HashMap<String, String>, // captured env vars
}

enum Agent {
    User,
    ClaudeCode { session_id: String },
    Cursor,
    Aider { model: Option<String> },
    Zed,
    OpenCode,
    ShellScript { path: PathBuf },
    BuildTool { name: String },  // cargo, npm, make, ...
    Unknown,
}
```

**Attribution rules, composed:**

1. **Direct pid match.** Is this pid in the known agent table?
2. **Ancestor match.** Walk `/proc/<pid>/status` ppid chain up to
   init or a known session root.
3. **Cgroup match.** Some agents run under distinct cgroups (systemd
   user services, container-run tools).
4. **Env marker match.** `CLAUDE_SESSION_ID=...`, `CURSOR_SESSION=...`
   etc. captured from `/proc/<pid>/environ`.
5. **JSONL time-window join.** Claude Code writes
   `~/.claude/projects/*/*.jsonl`. The tool call containing
   `{ "name": "Write", "input": { "file_path": "/x" } }` at timestamp
   T joins to fs events on path `/x` within a small window around T.
   This attaches *tool call IDs* to fs events — unique in the whole
   product.
6. **Explicit MCP registration** (future). Agent calls
   `tt-mcp.register_session({ name, pid, root })` on startup.

Rules 1–5 work without agent cooperation. Rule 6 is the ideal case.

### 3.5 Query layer

Every question the user or an agent can ask maps to indexed queries:

```sql
-- "what did Claude do in the last hour?"
SELECT * FROM events
  WHERE session_id IN (SELECT id FROM sessions WHERE agent_type = 'claude_code')
  AND ts > now() - interval '1 hour'
  ORDER BY seq;

-- "show every version of foo.rs this session"
SELECT e.*, c.hash FROM events e JOIN content c ON e.content_id = c.id
  WHERE e.path = '/home/nick/p/foo.rs'
  AND e.session_id = ?
  ORDER BY e.seq;

-- "what pid is currently editing files in ~/projects/ize?"
SELECT pid, comm, COUNT(*) FROM events
  WHERE path LIKE '/home/nick/projects/ize/%'
  AND ts > now() - interval '5 seconds'
  GROUP BY pid, comm;

-- "blame a line: what write introduced the content at path X, offset Y?"
WITH versions AS (SELECT * FROM events WHERE path = ? ORDER BY seq)
  SELECT * FROM versions WHERE content_hash = ? LIMIT 1;
```

### 3.6 jj / git export

jj becomes a **view**, not a capture. The bridge:

1. User runs `tt curate <session-id>` or `tt curate --since 1h`.
2. ttd reads events in scope, groups them heuristically:
   - by tool call (if Claude Code JSONL joined)
   - by file cluster + time gap (if no JSONL)
3. Each group becomes a proposed jj change, with a commit message
   either generated from the tool call's prompt, or template'd.
4. User reviews in a TUI, edits messages, splits/merges groups.
5. ttd writes the groups as real jj changes in the target repo (jj
   workspace must exist).
6. User runs `jj git push` as normal.

For the "no jj" case, `tt export-git` does the same thing direct to
git via `git fast-import` stream.

---

## 4. The moat — why this can't be cloned in a weekend

Eight things have to be right together, and each is non-trivial:

1. **BPF verifier dance** for variable-size write capture with
   dynptr. Needs care to not get rejected. Not hard if you've done
   it before; painful the first time.
2. **Process provenance under load.** pids recycle, parents die, the
   `/proc` scanner races the BPF events. Real systems need bounded
   staleness and fallback heuristics.
3. **CAS storage with reflinks** that works identically on btrfs,
   XFS, and ext4. Foxing has this; we reuse.
4. **Query indexing** that keeps up with fast churn. 100k events/sec
   from a cold `cargo build` should not DoS the indexer.
5. **Filter design** to exclude noise (build artifacts, caches) at
   the BPF layer without user friction. Misconfigured filter =
   either miss events or drown in them.
6. **JSONL time-window join** to Claude Code's log. Needs to be
   careful about clock skew and tool-call retries.
7. **jj integration** that produces commits users will actually
   accept without editing. This is a UX problem, not a technical one,
   and it's where most similar tools die.
8. **MCP ergonomics.** The agent-facing API has to be *obvious* so
   agents use it. Too many tools = agent confusion; too few = doesn't
   help.

Any single piece: a weekend. All eight, integrated, with good UX: six
months. That's the moat.

---

## 5. Implementation phases

Each phase delivers something independently valuable.

### Phase 0 — Kernel reconnaissance (1 week)

- Verify Nick's kernel version (need 6.14+ for FAN_PRE_MODIFY;
  5.19+ for BPF dynptr). Ubuntu 26.04 ships 7.0.
- Build foxing locally, confirm its BPF pipeline runs.
- Spike: minimal BPF program (C + aya-rs OR libbpf-rs) attached to
  `lsm/file_permission`, printing pid+path+size for every write. 1 day.
- Spike: same program using `bpf_dynptr_write` to capture payloads
  into the ring buffer. 1–2 days.
- Spike: fanotify `FAN_PRE_MODIFY` listener that reads pre-image bytes
  before allowing the write. 1–2 days.
- Spike: procfs pid-tree walker with cgroup + environ enrichment.
  1 day.

**Exit:** empirical data on event rates, payload caps, and latency
overhead under real workloads (a kernel build, a `cargo test`, a
Claude Code session).

### Phase 1 — The capture daemon (3 weeks)

- `ttd` daemon with tier M (metadata-only) capture, all mounts.
- CAS store using foxing's `fxcp-core::versioning` + reflink machinery.
- SQLite index with path/pid/time queries.
- Path filter config (exclude `target/`, `node_modules/`, `.git/`,
  `.jj/` by default; includable list).
- Graceful degrade on older kernels (kprobe fallback if no LSM).

**Exit:** `ttd` runs as a user systemd service, captures every write
to any file under `~/` (except filter list), and persists to
`~/.tt/`. CAS shows reflink savings; index answers queries. No agent
awareness yet.

### Phase 2 — Tier P + session attribution (2 weeks)

- Tier P: payload capture via dynptr for writes ≤ 16 KB.
- Fallback to pulling from source for larger writes (still in Phase 1
  behaviour).
- Session detection: pid-tree + cgroup + env marker attribution.
- Basic `tt sessions`, `tt log`, `tt diff` CLI.

**Exit:** you can ask "what did pid 12345 do" and get the answer. The
CLI renders sessions distinctly.

### Phase 3 — Claude Code integration (1 week)

- JSONL parser for `~/.claude/projects/*/*.jsonl`.
- Time-window join to attach tool-call-ids to fs events.
- `tt show <tool-call-id>` and session detail views.

**Exit:** Claude Code sessions show up with full tool-call causality.
"Show me the diff for Claude's third tool call today" works.

### Phase 4 — Curation + jj/git export (2–3 weeks)

- `tt curate` TUI.
- Heuristic grouping by tool-call / time-gap / file-cluster.
- jj change writer.
- `tt export-git` direct writer.

**Exit:** the full flow "work for a day → `tt curate` → commits
landed in git" is smooth.

### Phase 5 — MCP server (1 week)

- `tt-mcp` stdio server with `list_sessions`, `get_session`,
  `current_session`, `my_edits_this_session`, `rewind`, `diff`,
  `what_changed_file`.
- Docs for Claude Code config.

**Exit:** Claude can query its own edits. Pair with a Claude Code
config that spawns `tt-mcp` as an MCP subprocess.

### Phase 6 — Tier F for correctness-critical workloads (2 weeks, optional)

- `FAN_PRE_MODIFY` listener for opt-in paths.
- Pre-image capture to CAS before allowing the write.
- Configurable timeout policy (kernel auto-ALLOWs after 5s default).

**Exit:** databases, `.git/` dirs, anywhere the user enables it, get
byte-exact pre-image history.

### Phase 7 — Hardening + polish (ongoing)

- Retention / pruning policies. Time-decay, size-bounded, pin-by-
  session.
- Multi-user (systemd --user per human).
- TUI timeline view (ratatui, with scroll + filter).
- Import from Claude Code's existing `file-history/` (migrate old
  data into the unified store).
- Cursor / Aider / Zed attribution heuristics.
- Explicit MCP session registration.

**Total to end-to-end useful MVP (Phases 0–5): ~10 weeks.**
**Phases 6–7 ongoing.**

---

## 6. Honest risk assessment

### Things that could kill the project

1. **BPF verifier edge cases.** Variable-size payload capture is at
   the frontier. If a particular kernel version rejects our program
   we have to either pin kernel versions or downgrade to metadata-
   only. Mitigation: start with metadata-only (known to work, foxing
   proves it), add payload as opt-in.

2. **Event rate explosion.** A `cargo build` can write 100k files in
   minutes. Indexer has to keep up. Mitigation: path filters at BPF
   level, async indexing, circuit-breaker to metadata-only mode if
   event rate spikes.

3. **Disk usage runaway.** Even with CAS and reflinks, a badly
   configured install could fill a disk. Mitigation: size-bounded
   retention by default (e.g. 10 GB total), clear pruning tools.

4. **Claude Code JSONL format changes.** Anthropic could change the
   format any time. Mitigation: vendor the parser, version-sniff,
   graceful degradation to pid-only attribution.

5. **Distribution friction.** BPF programs need recent clang, kernel
   headers, root / CAP_BPF. Mitigation: CO-RE + libbpf + `bpftool
   gen skeleton` → one static binary, no runtime build deps.

6. **jj adoption risk.** If jj doesn't continue to gain traction,
   our curation UX loses its best substrate. Mitigation: `tt
   export-git` works without jj; jj is a preferred path, not required.

7. **We ship something Anthropic includes in Claude Code directly.**
   Realistic — Anthropic could extend `/rewind` to be cross-tool.
   But: they won't do the kernel work, and they won't cover non-
   Claude tools. We win on scope.

### Things that are NOT risks, contrary to what your instinct might say

- **"eBPF is scary."** It's well-documented, Rust has good bindings
  (aya, libbpf-rs), production projects use it constantly. Foxing
  already does this in our codebase.
- **"Kernel module distribution."** We're not writing a module.
  Pure BPF + userspace daemon. Installs like any other binary.
- **"Performance overhead."** Foxing benchmarks at 15-21 ms/write
  for full mirror. Our capture is *less* work than that (no mirror),
  should be sub-ms per write.
- **"Will this work on other FSes?"** Ext4, XFS, btrfs, bcachefs,
  tmpfs all have the same VFS hooks. BPF doesn't care about the FS
  underneath.

---

## 7. Why now, specifically

Three things converged:

1. **`FAN_PRE_MODIFY` landed in Linux 6.14 (early 2025).** First time
   full pre-content fidelity is available via stable UAPI without
   kernel patching. Meta ships it in production.
2. **`bpf_dynptr` is stable (Linux 5.19+).** Variable-size payload
   capture in BPF is no longer a research project.
3. **AI-assisted dev is now the default.** Every serious developer
   uses Claude / Cursor / Aider daily. The "fear of letting AI touch
   my repo" is a real, scaled problem.

Two years ago, this was impossible. Two years from now, someone will
have built it. The window is now.

---

## 8. Relation to foxing

We live on the foxing fork because:

- Foxing's fxcp-core has CAS, reflink, BLAKE3 Merkle, governor, NFS
  bypass — all directly reusable.
- Foxing's BPF infrastructure (ring buffer, event pipeline, reorder
  buffer, transient filter) is battle-tested. We extend it with
  payload capture; we don't rewrite.
- Foxing's MARS versioning is the spiritual ancestor of our CAS.
- The mirror-target machinery is inert here — we set no mirror. But
  keeping the fork means we inherit 80% of the capture infrastructure
  for free.
- Upstream path to aenertia exists when/if we want it.

Concretely, the new code lives in a new workspace crate:

```
foxing/
├── fxcp-core/       (existing — we use: versioning, sidecar, security)
├── foxingd/         (existing — we use: bpf/, ringbuf, reorder)
├── fxcp/            (existing — untouched)
├── tt-core/         NEW: CAS extensions, query layer, session model
├── tt-capture/      NEW: BPF programs + ttd daemon integration
├── ttd/             NEW: daemon binary
├── tt/              NEW: CLI binary
├── tt-mcp/          NEW: MCP server binary
└── xtask/           (existing)
```

If this ever spins off into its own project, the split is clean:
copy tt-* plus the necessary fxcp-core modules, reset the git history,
credit foxing as the parent.

---

## 9. What I'd commit to vs push back on

**Commit:**
- Linux only. No Windows, no macOS. Scope discipline.
- Rust for userspace, C for BPF.
- Kernel ≥ 6.14 baseline (Ubuntu 26.04 LTS default).
- BPF-based capture, not a kernel module.
- CAS with reflinks; BLAKE3.
- jj as primary curation model, git as default export, pijul deferred.
- MCP server from phase 5.
- Process-tree attribution as primary, JSONL join for Claude Code,
  explicit registration later.

**Push back on:**
- **Full kernel module.** Every reason to avoid: distribution,
  upstream, maintenance. BPF gets us 95% with 5% of the pain.
- **Byte-level write ordering fidelity as a *primary* goal.** Tier M
  + post-hoc pull covers 99% of workloads. Tier F exists for the 1%
  that actually needs it. Don't pay for fidelity no real use case
  requires.
- **Claude Code fork / patching.** Join to JSONL externally. Never
  modify Claude's own code.
- **"Replace git."** We export to git. We don't replace it.
- **Shipping in one go.** 10 weeks of tightly scoped phased work
  beats 6 months of everything-at-once.

---

## 10. First code

To make this concrete, Phase 0 Day 1 looks like:

```sh
# In /home/nick/projects, colocated with foxing fork
cd ~/p/foxing
git checkout timetraveler

# New crate scaffold
mkdir -p tt-capture/src tt-capture/bpf
cat > tt-capture/Cargo.toml <<'EOF'
[package]
name = "tt-capture"
version = "0.1.0"
edition = "2021"
[dependencies]
aya = "0.13"          # or libbpf-rs, TBD after spike
anyhow = "1"
tokio = { version = "1", features = ["full"] }
EOF

# Minimal BPF program: trace every vfs_write_iter, print pid+path+size
# target: "cargo run -p tt-capture" prints a line per fs write
# target exit: first reliable ground-truth stream of "every fs write on this system"
```

If this prints clean output for 30 minutes of real dev work, the
project is viable. If it doesn't, we learn the exact failure mode in
days, not months.

---

## 11. References

**Critical kernel docs**
- BPF LSM: https://docs.kernel.org/bpf/prog_lsm.html
- fanotify: https://www.man7.org/linux/man-pages/man7/fanotify.7.html
- `FAN_PRE_MODIFY` patch v5: https://www.mail-archive.com/linux-bcachefs@vger.kernel.org/msg02631.html
- `bpf_dynptr`: https://docs.ebpf.io/linux/concepts/dynptrs/
- LWN pre-content fanotify: https://lwn.net/Articles/985013/

**Adjacent tools**
- foxing (parent project): https://codeberg.org/aenertia/foxing
- eCapture (BPF TLS plaintext capture, 16 KB payloads): https://ecapture.cc/
- Claude Code /rewind: https://code.claude.com/docs/en/how-claude-code-works
- claude-code-rewind (3rd party): https://github.com/holasoymalva/claude-code-rewind

**Prior analyses in this repo**
- `docs/research/lsm-rs.md` — FS observation landscape overview
- `docs/research/content-capture.md` — kernel write capture options
- `docs/archive/TIMETRAVELER-v1-snapshot-focused.md` — v1 sketch
- `docs/archive/TIMETRAVELER-v2-jj-native.md` — v2 sketch (userspace-only,
  now superseded by this)
