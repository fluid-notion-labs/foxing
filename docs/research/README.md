# Research — timetraveler branch

Research materials carried forward from prior projects, plus analysis
specific to this branch. Organised by provenance.

## This branch

- **[lsm-rs.md](lsm-rs.md)** — Analysis of the Linux filesystem
  observation landscape (fanotify, eBPF, kernel primitives, zero-copy
  paths, foxing positioning). Produced while evaluating whether to
  build from scratch, fork foxing, or layer on top. Outcome: fork →
  this branch.

## Legacy (from ize)

Ize was the FUSE + Pijul versioned-filesystem predecessor. Most of its
research is tightly coupled to FUSE and libpijul and doesn't apply to
the foxing context, but a few documents carry over cleanly:

- **[legacy-ize/filesystem_interception_alternatives.md](legacy-ize/filesystem_interception_alternatives.md)**
  — Comparison matrix of filesystem event interception approaches
  (eBPF, fanotify, inotify, audit, LD_PRELOAD, ptrace, kernel modules,
  FUSE). Still the clearest single summary of the tradeoff space.
  Foxing uses eBPF; this document explains why the alternatives weren't
  picked and what they'd buy if adopted later.

- **[legacy-ize/3d_diff_visualization.md](legacy-ize/3d_diff_visualization.md)**
  — Vision for rendering version control diffs as textures on 3D
  geometry (specifically a sphere). Format-agnostic; could become a
  `fxcp snap diff --visual` exported to Seaview, or a standalone
  timeline-browser using the same mesh-sequence primitives. Not a
  near-term deliverable, but worth not losing.

- **[legacy-ize/opcode-design.md](legacy-ize/opcode-design.md)** —
  Opcode schema design for filesystem mutations. Foxing's `EventType`
  enum in `foxingd/src/event.rs` is a more concrete equivalent;
  keeping this as a reference point for anyone designing extensions to
  foxing's event type system.

- **[legacy-ize/izev.md](legacy-ize/izev.md)** — TUI design for ize.
  Foxing has its own TUI already; this is relevant if we add timeline-
  browsing views to `fxcp snap` (e.g. a `snap tui` for walking
  snapshots, branch trees, diffs).

## Left behind (ize-specific, not portable)

For the record, these documents stayed with ize and weren't copied:

- `pijul/*` — libpijul integration details; timetraveler uses reflinks,
  not change-algebra.
- `filesystem-layering.md` — FUSE composition patterns.
- `interception/fd_handling.md` — FUSE-specific fd passthrough.
- `opcode_queue_design.md` — foxing's dispatcher/coalescer pipeline
  supersedes this.
- `end-to-end-testing.md` — FUSE test harness.
- `pijul-backend-opcode-recording-backend-rework.md` — Pijul internals.
- `opcodes/opcode-recorder-design.md` — FsObserver trait for FUSE.
- `old/*` — already archived in the source.
