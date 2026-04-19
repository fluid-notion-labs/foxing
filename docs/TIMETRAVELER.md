# Timetraveler — Transparent FS Versioning for AI-Assisted Development

**Branch:** `timetraveler` on `fluid-notion-labs/foxing`
**Status:** design — supersedes previous snapshot-focused sketch (archived
at `docs/archive/TIMETRAVELER-v1-snapshot-focused.md`)
**Scope:** ize's original vision, re-targeted. This is the primary design.

---

## 0. Elevator pitch

Transparent, automatic filesystem versioning for workstations where AI
agents and humans both edit files. Every fs mutation is captured with
provenance (who, when, from which process). A jj-native curation
workflow lets you reshape the raw history into proper commits for
export to git. An MCP server lets agents introspect their own edits.

Think: **ize, without FUSE. jj, aware of agents. Claude Code's `/rewind`
but cross-tool, cross-session, and curatable into real commits.**

---

## 1. The world as of April 2026 — honest landscape

### What exists

**AI-side, single-tool:**
- **Claude Code `/rewind`** — per-session file snapshots in
  `~/.claude/file-history/{sessionId}/`, path-hashed + versioned.
  Only tracks Write/Edit tool ops (misses Bash `sed -i`).
  Session-local, not cross-tool.
- **Claude Code JSONL history** — `~/.claude/projects/{project}/*.jsonl`,
  every tool call logged with inputs. Already the causal chain. People
  grep this for crashed-session recovery.
- **`claude-code-rewind`** (holasoymalva) — third-party, SQLite +
  file snapshots, `export --format=commit` for git. Python.
- **Cursor** "Undo AI changes", **Aider** `/undo`. Session-scoped.

**VCS-side, no AI awareness:**
- **jj** — working-copy-as-commit, auto-snapshot on every jj command.
  Watchman integration (`fsmonitor.watchman.register-snapshot-trigger = true`)
  auto-snaps on fs change.
- **jj operation log** — `jj --at-op=<id>` rewinds to any past repo state.
- **jj evolog** — per-change evolution including former working-copy revisions.

**What no one does:**
- Cross-tool fs observation (Claude + Cursor + you + cargo in one view).
- Process-tree pid attribution ("this was Claude session X").
- jj-as-curation-model for AI edits.
- MCP server exposing edit history to agents.
- Cross-repo/cross-directory coverage (Claude editing `~/.config/`
  and your project simultaneously).

**So:** this isn't greenfield, and it isn't reinventing. It's a
**composition layer** that joins fs-event capture + jj curation + agent
session metadata + MCP access.

### What this means for scope

We can and should lean heavily on existing components:

| Component | Source | What we wrap / consume |
|---|---|---|
| FS event capture | foxing's BPF pipeline | Listen, don't rebuild |
| Snapshot store | foxing MARS | Maybe; or go straight to jj |
| Working copy → commit | jj | Use directly |
| Auto-snapshot-on-change | jj + Watchman | Configure, don't rebuild |
| Operation log | jj | Use directly |
| Export to git | jj git push | Use directly |
| Claude causal chain | `~/.claude/projects/*.jsonl` | Join on pid+timestamp |

Our actual new work is:

1. **Attribution layer.** Tag each fs mutation with process ancestry.
2. **Session concept.** Correlate a batch of mutations with an agent
   session (derived from pid ancestry + optional JSONL join).
3. **jj integration.** Either auto-split jj working-copy commits by
   session/agent, or annotate them post-hoc.
4. **MCP server.** Expose the edit history to agents.
5. **Curation UX.** Make the common flows (name this session's
   changes, split by agent, commit) trivial.

---

## 2. The big architectural fork

Before design details, one decision shapes everything:

**Fork A: foxing-based.** Run foxingd in observing mode (no mirror
target). Every fs event → foxing's pipeline → our event log. Then a
separate component reads the log and reshapes jj working-copy commits
to match.

**Fork B: jj-native.** Configure jj with Watchman. Write a jj
post-snapshot hook (or fsmonitor-trigger extension) that annotates
each snapshot with pid/session metadata. Skip foxing entirely for this
use case.

**Fork C: hybrid.** Use jj's snapshotting for the common case (edits
inside a repo). Use foxing's BPF pipeline for cross-repo/out-of-repo
visibility (Claude editing `~/.config/` or whatever). Bridge the two.

### Analysis

Fork A feels right given we're on the foxing fork, but it's probably
overkill. Foxing's strength is real-time replication to a target —
we're not replicating, just observing. And jj already auto-snapshots
on every command; adding Watchman gets us auto-snapshot on every save.
The foxing BPF pipeline buys little here unless we need cross-repo
visibility.

Fork B is the cleanest for the 90% case. Everything a dev cares about
is inside a repo that jj can be init'd in. The gap is:

- Claude editing files outside the repo (e.g. `~/.config/foo/bar.toml`).
- Coverage of non-repo working directories (e.g. experimental scratch dirs).

Fork C splits the difference but adds complexity.

**My recommendation: start with Fork B. Prove it works for the common
case. Add Fork A (foxing BPF) as an optional second-channel capture
when coverage becomes the blocker.** This also gives us a natural
reason to live on the foxing fork — we're not using foxing's pipeline
*yet*, but the integration path is clean when we need it.

---

## 3. Concrete architecture (Fork B, with Fork A growth path)

```
┌────────────────────────────────────────────────────────────────────┐
│ Working tree (colocated jj + git repo)                             │
│                                                                    │
│   user editor ───┐                                                 │
│   Claude Code ───┼─► file writes ─► Watchman ─► jj snapshot       │
│   Cursor      ───┤                                  │              │
│   cargo build ───┘                                  ▼              │
│                                          jj working-copy commit    │
│                                                     │              │
│                                                     ▼              │
│                                          jj operation log (@-op)   │
└──────────────────────────────────────────┬─────────────────────────┘
                                           │
              ┌────────────────────────────┼────────────────────────┐
              │                            │                        │
              ▼                            ▼                        ▼
   ┌──────────────────┐       ┌──────────────────────┐    ┌──────────────────┐
   │ ttd (daemon)     │       │ tt (CLI)             │    │ tt-mcp (server)  │
   │                  │       │                      │    │                  │
   │ • pid tracker    │       │ • tt sessions        │    │ • list sessions  │
   │ • session detect │       │ • tt curate <sess>   │    │ • get edits <s>  │
   │ • annotate jj    │       │ • tt split-by-agent  │    │ • rewind         │
   │   snapshots      │       │ • tt export-git      │    │ • diff           │
   └──────────┬───────┘       └──────────┬───────────┘    └──────────────────┘
              │                          │
              └──────────┬───────────────┘
                         │
                         ▼
          ┌──────────────────────────────┐
          │ .jj/timetraveler/            │
          │   sessions.db (sqlite)       │
          │   events.log (append-only)   │
          │   pid-map/                   │
          └──────────────────────────────┘
```

### Components

- **`ttd`** — lightweight daemon. Watches `/proc` for new processes
  and builds a pid ancestry table (ppid → pid → cmdline). On every
  jj snapshot, reads jj's "changed files" and correlates to which pids
  last wrote them (via fanotify on the repo mount, cheap). Emits
  session metadata per snapshot.
- **`tt`** — CLI for humans. Thin wrapper around jj with added
  `sessions`, `agent`, `split-by-agent`, `curate` subcommands.
- **`tt-mcp`** — MCP server exposing the same queries as tt but to
  agents. Stdio MCP; agent spawns it as a subprocess.

### Data model

```rust
struct Session {
    id: Uuid,
    agent: Agent,
    pid_root: u32,
    started_at: DateTime,
    ended_at: Option<DateTime>,
    jsonl_path: Option<PathBuf>,
}

struct Event {
    seq: u64,
    session_id: Uuid,
    timestamp: DateTime,
    pid: u32,
    comm: String,
    path: PathBuf,
    op: Op,
    jj_op_id: Option<String>,
    jj_change_id: Option<String>,
    tool_call_id: Option<String>,
}

enum Agent {
    User,
    ClaudeCode { session_id: String },
    Cursor,
    Aider,
    Zed,
    Other(String),
}
```

### Attribution — how `ttd` knows who did what

Three signals, composed:

1. **Process tree.** Walk `/proc/<pid>/stat` ppid chain up to a known
   agent process (`claude`, `cursor`, `aider`, user shell, etc.).
   Cheap, accurate for modern agents that spawn tool calls as direct
   children.

2. **Agent-specific markers.** Claude Code writes JSONL per session.
   Check `~/.claude/projects/*/<uuid>.jsonl` mtime/recent tool calls
   for correlation by (timestamp window + file path match). Cursor
   writes its own session data — can be consumed similarly. For
   agents that expose nothing, pid tree is it.

3. **Explicit registration** (future). MCP handshake: agent calls
   `tt-mcp register-session { name, pid }` on startup. Explicit tag,
   no guessing. Requires agent cooperation.

For v1, just (1) + (2). Registration later.

### Storage

- **`sessions.db`** — sqlite, one row per session, index on pid and
  time range. Query: "what session was pid P doing at time T?"
- **`events.log`** — append-only, text or binary, one event per line.
  Cheap. For real-time tail.
- **`pid-map/`** — pid ancestry snapshots, one per jj operation.
  Lets us go back and answer "which pid wrote file X in jj op Y".

`.jj/timetraveler/` is under jj's own directory but not tracked by jj
(jj-ignored). Metadata about jj, stored beside jj.

### How jj snapshots get annotated

jj's fsmonitor+Watchman trigger fires → jj takes a working-copy
snapshot → new jj op recorded. `ttd` hooks via:

- **Option 1: jj hook scripts** — if jj gains hooks (there's an issue
  for this). Cleanest. Not available now.
- **Option 2: jj op log poll** — `ttd` polls `jj op log --limit 1`
  every N seconds, picks up new ops. Crude but works today.
- **Option 3: inotify on `.jj/op_heads/`** — watch the op log
  directly, filesystem-level. Faster, still works today.

Go with Option 3 for v1. File Option 1 as an upstream feature request
with the jj folks; Martin (jj's creator) is responsive.

---

## 4. CLI sketch

```
tt sessions                     # list recent sessions
tt sessions --agent claude      # filter
tt session <id>                 # detail view

tt log                          # like jj log, but annotated with session/agent
tt log --agent claude           # only Claude's changes

tt curate <session>             # open interactive curation UI
  # groups session's changes into proposed commits by:
  # - time gaps
  # - file clusters
  # - tool call grouping (from JSONL if Claude)
  # offers squash / split / describe per group
  # writes as jj changes

tt split-by-agent               # take current jj working copy,
                                # split into separate jj changes
                                # per agent

tt diff <session>               # aggregate diff of a session
tt show <tool-call-id>          # specific tool call's diff (Claude)

tt export-git [<rev>]           # jj git push, with optional curation
                                # pass — adds Claude session info to
                                # commit messages if absent

tt undo <session>               # revert an entire session's changes
                                # (as a new change on top, jj-style)

tt branch-from <session>        # start a new jj change from the state
                                # before <session> ran — "what if I'd
                                # stopped Claude there"
```

### Relationship to jj

`tt` never hides jj. Everything `tt` does corresponds to jj operations,
and `jj` continues to work directly. `tt curate` produces named jj
changes; `jj log` shows them normally. Users can drop to jj at any
time and back.

Think of `tt` as "jj with agent glasses on."

---

## 5. MCP server — what agents can do with their own history

```
tt-mcp — stdio MCP server

tools:
  list_sessions(agent?, since?, limit?)
  get_session(id) -> { events, tool_calls, changed_files, diff }
  diff(session_id, file?) -> unified diff
  rewind(session_id) -> restore files to state before session
  current_session() -> UUID of the calling agent's own session
  my_edits_this_session() -> list of edits with paths + diffs
  what_changed_file(path, since?) -> session list for that file
```

This lets an agent ask itself "what have I changed in this session?"
or "what did I do to `auth.rs` today?" without needing to track it
manually. Pairs naturally with Claude Code's existing JSONL log — the
MCP is a higher-level query interface over the combined (fs events +
tool call log) data.

Biggest potential value: an agent can **check its own work against
what actually landed**, catching mismatches between "what I thought I
edited" and "what actually changed on disk" (e.g. a `sed` in Bash that
didn't match any line, a failed Write that got rolled back).

---

## 6. Implementation phases

### Phase 0 — Spikes (1 week)

- Set up jj + Watchman + snapshot-trigger on Nick's dev box. Verify
  auto-snap works on every save and what the op log looks like. ½ day.
- Spike: Rust program that watches `.jj/op_heads/` via inotify and
  prints new op IDs. Proves we can react to jj snapshots in real time.
  1 day.
- Spike: Rust program that reads `/proc/<pid>/stat` and emits the
  pid→ppid→cmdline tree. Proves attribution viability. 1 day.
- Spike: parse `~/.claude/projects/*/*.jsonl` and correlate tool calls
  to (pid, timestamp, files). Sanity check the correlation hypothesis.
  1–2 days.

**Exit:** confidence that each leg of the design is buildable in Rust
without exotic dependencies.

### Phase 1 — `ttd` + read-only `tt` (2 weeks)

- `ttd` daemon: op-head watcher + pid tracker + session detector.
- `.jj/timetraveler/` store (sqlite + events.log).
- `tt sessions` / `tt log` / `tt session <id>` / `tt diff <session>`.
- No curation yet — just observation.

**Exit:** `tt sessions` lists Claude sessions accurately across a day
of real dev work. `tt diff <session-id>` shows exactly what Claude did.

### Phase 2 — Curation (2 weeks)

- `tt curate <session>` interactive TUI (or scm-diff-editor-style
  external tool).
- `tt split-by-agent` — common fast path.
- `tt export-git` with session-aware commit messages.
- `tt undo <session>`, `tt branch-from <session>`.

**Exit:** workflow "work for an afternoon with Claude → `tt curate
today` → commits landed in git" is smooth.

### Phase 3 — MCP (1 week)

- `tt-mcp` stdio server exposing the read API.
- Claude Code MCP config instructions.
- Test end-to-end: Claude asks itself "what did I edit?" and gets an
  accurate answer.

**Exit:** MCP server published, docs written, one tested agent
integration.

### Phase 4 — Foxing integration (2–3 weeks, conditional)

Only if Phase 1–3 show coverage gaps:

- `ttd` can optionally subscribe to foxingd events for paths outside
  the jj repo.
- Cross-repo session tracking.
- The "Claude edited my ~/.config/ too" story.

**Exit:** cross-repo / out-of-repo fs events flow into sessions
correctly.

### Phase 5 — Polish (ongoing)

- Other agents (Cursor, Aider, Zed) — session detection.
- Explicit MCP-based session registration.
- Retention / pruning policies for old sessions.
- Multi-workspace handling.
- Pijul export adapter (deferred).

**Total to useful MVP: 4–5 weeks.** Substantially less than the
previous design because jj does so much of the heavy lifting.

---

## 7. Open questions & risks

1. **jj snapshot granularity.** Watchman debounces fs changes. Claude
   might do 5 Write calls in 200ms; Watchman collapses them. Do we
   lose per-call granularity? Mitigation: join with JSONL per-tool-call
   log; jj snapshot is coarse but Claude's log is fine.

2. **Non-Claude tools with no native log.** Cursor, Aider, Zed.
   Session detection via pid tree works; per-tool-call detail
   requires per-tool integration. Acceptable: "Claude Code has rich
   sessions; others get coarse session attribution."

3. **Multiple concurrent agents.** Claude session A edits `foo.rs`
   while Cursor session B edits `bar.rs`. `ttd` needs to correlate
   each write to the right session via pid tree. Should work; test
   carefully.

4. **jj repo required.** This won't work in directories that aren't
   jj-initialized. Acceptable: tell the user "run `jj git init
   --colocate` first." Most dev workflows already have git repos.

5. **Performance.** inotify on `.jj/op_heads/` + `/proc` polling —
   both cheap. Session DB writes — cheap. MCP query latency —
   sqlite queries, cheap. Should be fine.

6. **Privacy / security.** `~/.claude/projects/*.jsonl` contains
   prompts and tool outputs — potentially sensitive. `ttd` needs to
   respect that (read-only, never export).

7. **Upstream opportunity with jj.** Some of this is generally useful.
   Martin (jj creator) might want to upstream session/agent metadata
   as a first-class jj concept. Worth an early conversation.

8. **Existing tools in space.** `claude-code-rewind` and Claude Code's
   native `/rewind` both cover the single-tool case. `tt` needs to
   articulate why cross-tool + jj + MCP makes it worth using over
   just `/rewind`. Positioning matters.

---

## 8. Why this and why now

- AI-assisted dev is now mainstream. Session-scoped undo has emerged
  as a pattern (Cursor, Claude Code, Aider all ship it) but every
  tool reinvents it locally and none compose.
- jj went from "interesting" to "mainstream-adjacent" in 2025–2026.
  Chris Krycho, Steve Klabnik, Martin's Git Merge 2024 talk. It's the
  right substrate for a curation-heavy workflow.
- FNL's own agent usage (Claude Code, opencode, Zed agents) generates
  the workload this tool is for.
- Upstreaming path is real: jj session metadata, foxing observer mode,
  MCP reference server.

## 9. Relationship to earlier docs

- **[docs/archive/TIMETRAVELER-v1-snapshot-focused.md](archive/TIMETRAVELER-v1-snapshot-focused.md)**
  — earlier sketch focused on snapshot/trigger policies. Those
  features still relevant (they become `tt export-git` policy knobs
  and session-boundary triggers), but the main architecture has
  shifted.
- **[research/content-capture.md](research/content-capture.md)** — the
  six-option kernel write capture analysis. Mostly not needed for this
  scope. We use jj's post-state snapshots; we don't need pre-image
  capture.
- **[research/lsm-rs.md](research/lsm-rs.md)** — the broader FS
  observation landscape review. Still the best overall orientation to
  the space.

## 10. References

**jj**
- Working copy: https://docs.jj-vcs.dev/latest/working-copy/
- Operation log & undo: https://github.com/jj-vcs/jj
- Watchman fsmonitor: https://docs.jj-vcs.dev/latest/config/

**AI tool state today**
- Claude Code how it works: https://code.claude.com/docs/en/how-claude-code-works
- Claude Code JSONL format: https://towardsai.net/p/machine-learning/time-travel-debugging-with-claude-codes-conversation-history
- Claude Code storage design: https://milvus.io/blog/why-claude-code-feels-so-stable
- claude-code-rewind (3rd party): https://github.com/holasoymalva/claude-code-rewind

**Prior art / adjacent**
- Undo.io MCP integration: https://undo.io/resources/time-travel-ai-code-assistant/
- Claude Code session-undo feature request: https://github.com/anthropics/claude-code/issues/21645

**MCP**
- MCP spec: https://spec.modelcontextprotocol.io/
