# Phase 0 Cross-Boundary Import Map

**Generated:** 2026-03-02
**Purpose:** Reference for Phase 1 workspace split (fxcp-core + foxingd)

## Module Classification

### fxcp-core — Copy/IO plane (no BPF, no daemon lifecycle)

```
FOUNDATIONAL (no crate:: deps):
  error.rs           — FoxingError, Result
  constants.rs       — tuning constants, ONE_SHOT_MODE
  metrics.rs (split) — copy-plane metrics only (~22 of ~57)

UTILITY:
  buffer.rs          → metrics, error, constants
  hashing.rs         → error, metrics
  sidecar.rs         → hashing

OPERATIONS (core copy engine):
  operations.rs      → buffer, error, security, metrics, governor(⚠ trait needed)
  security.rs        → error, operations(⚠ circular), buffer, sidecar

CONSISTENCY:
  consistency/mod.rs
  consistency/exchange.rs
  consistency/journal.rs
  consistency/serialization.rs → consistency::sequencer
  consistency/sequencer.rs
  consistency/wal.rs

VERSIONING:
  versioning.rs      → error, sidecar, security
```

### foxingd — Daemon/BPF plane

```
EVENT PIPELINE:
  event.rs           → metrics
  columnar.rs        → event, metrics
  ordering.rs        → event, metrics, columnar, constants

IDENTITY:
  identity.rs        → error, event, metrics, constants
  identity_watch.rs  — (no crate:: deps)
  projector.rs       → identity, event

SYSTEM:
  governor.rs        → metrics, constants
  config.rs          → error, security, constants (SysSpecs block → fxcp-core)
  tuner.rs           → config, constants
  resilience.rs      → security, constants

DAEMON CORE:
  mirror.rs          → event, config, error, tuner, hydration, hydration_worker,
                       identity, worker, governor, security, versioning,
                       identity_watch, projector, operations
  worker.rs          → error, mirror, config, operations, tuner, governor,
                       identity, buffer, event, ordering, consistency,
                       resilience, sidecar, constants, security, metrics
  hydration.rs       → error, mirror, config, tuner, governor, constants, operations
  hydration_worker.rs → config, mirror, governor, tuner, security, error,
                        identity, consistency, sidecar, event, operations,
                        buffer, constants, metrics, hashing

BPF:
  bpf.rs             → event, error, metrics, ordering, mirror, constants

PRESENTATION:
  api.rs             → tuner
  tui.rs             → api, tuner, versioning, config
  tui/setup.rs       → config
  tui/explorer.rs    → config (→ fxcp in Phase 2)

ENTRY:
  main.rs            → config, mirror, tuner, metrics, tui, versioning,
                       constants, hydration_worker
```

## Phase 1 Blockers

| Blocker | Location | Issue | Fix |
|---------|----------|-------|-----|
| Governor dep | operations.rs:21, :625, :1540 | `Option<Arc<Governor>>` passed through copy pipeline | Extract trait: `trait SystemStress { fn memory_usage_pct(&self) -> f64; }` |
| Circular dep | security.rs ↔ operations.rs | security imports `Capabilities` from operations; operations imports security | Move `Capabilities` struct to shared types module |
| SysSpecs location | config.rs:14-55 | SysSpecs/SystemClass needed by both crates | Move to fxcp-core (no crate:: deps, only sysinfo) |
| metrics split | metrics.rs | Single file with mixed metrics | Split into fxcp-core/metrics.rs + foxingd re-exports |

## Dependency Counts

| Module | Inbound (imported by N) | Outbound (imports N) | Category |
|--------|------------------------|---------------------|----------|
| error.rs | ~20 | 0 | Foundation |
| constants.rs | ~10 | 0 | Foundation |
| metrics.rs | ~15 | 0 | Foundation |
| operations.rs | ~6 | 5 | Core |
| buffer.rs | ~4 | 3 | Utility |
| security.rs | ~5 | 5 | Utility |
| governor.rs | ~5 | 2 | System |
| config.rs | ~6 | 3 | Config |
| mirror.rs | ~3 | 15 | Orchestrator |
| worker.rs | ~1 | 16 | Execution |
| bpf.rs | ~1 | 6 | BPF |
