# Foxing: High-Fidelity Filesystem Replication

![Version](https://img.shields.io/badge/version-0.4.0-blue) ![License](https://img.shields.io/badge/license-GPLv2-green) ![Platform](https://img.shields.io/badge/platform-Linux%206.12%2B-lightgrey) ![Rust](https://img.shields.io/badge/rust-2024-orange)

**Foxing** is a high-performance filesystem replication system with two components:

- **fxcp** — Standalone smart copy tool. Drop-in replacement for `rsync`/`cp` with auto-adaptive CoW/reflink, io_uring, and BLAKE3 Merkle delta detection. No BPF or root required.
- **foxingd** — eBPF-powered replication daemon for continuous, event-driven mirroring with sub-millisecond latency.

## Performance (fxcp vs rsync vs cp)

Measured on btrfs-over-LUKS2 (NVMe), Fedora 43, kernel 6.17:

| Workload | rsync | cp | fxcp | fxcp vs rsync |
|----------|------:|---:|-----:|--------------:|
| 10K small files (4KB each) | 607ms | 424ms | **607ms** | parity |
| 10 large files (100MB each) | 1236ms | 4ms | **23ms** | **54x faster** |
| Mixed (5K files, 2.1GB) | 3998ms | 239ms | **383ms** | **10x faster** |
| Sparse files (10x50MB) | 764ms | 3ms | **21ms** | **36x faster** |

fxcp auto-selects the optimal strategy: reflink (instant CoW) for same-device copies, sendfile for small files, io_uring for large cross-device transfers.

## Quick Start: fxcp (No Root, No BPF)

```bash
# Build (no BPF toolchain needed)
cargo build --release -p fxcp

# Basic copy (like cp -a)
fxcp -a /source /destination

# With delete (like rsync --delete)
fxcp -a --delete /source /destination

# Dry run
fxcp -a -n /source /destination
```

fxcp detects the storage stack (dm-crypt, btrfs, XFS, containers) and adapts automatically.

## Quick Start: foxingd (eBPF Daemon)

```bash
# Install BPF dependencies (Fedora/RHEL)
sudo dnf install clang llvm libbpf-devel bpftool

# Build
cargo build --release -p foxingd

# Run with TUI
sudo ./target/release/foxingd daemon --config config.toml --tui

# One-shot sync (like rsync)
./target/release/foxingd sync -a /source /destination
```

## Building

```bash
# fxcp only (no BPF deps needed)
cargo build --release -p fxcp

# foxingd only (requires BPF toolchain)
cargo build --release -p foxingd

# Full workspace
cargo build --release --workspace
```

## Workspace Structure

```
foxing/
├── fxcp-core/     Smart copy engine library (io_uring, reflink, Merkle, sidecar)
├── fxcp/          Standalone CLI binary (118MB, no BPF)
├── foxingd/       eBPF replication daemon (230MB, requires libbpf)
├── tests/         Regression harness (rsync/cp/fxcp/foxingd comparison)
└── docs/          Architecture decisions and documentation
```

## Architecture

fxcp-core provides the I/O engine shared by both binaries:

- **SmartCopier**: io_uring async copy with registered buffers
- **Reflink/CoW**: FICLONE ioctl for instant copies on btrfs/XFS
- **BLAKE3 Merkle**: Chunk-level delta detection for incremental sync
- **Storage awareness**: dm-crypt, dm-thin, kvdo, container detection
- **Governor**: PSI-based system stress management with QoS floor

foxingd adds eBPF event capture, CQRS event ordering, adaptive BBR tuning, and MARS versioning on top.

## Configuration

foxingd uses TOML configuration. See the built-in help:

```bash
foxingd --help
foxingd explain  # Prints configuration cheatsheet
```

fxcp requires no configuration — it auto-detects everything.

## Testing

```bash
make test-quick    # Fast regression gate (~15s)
make test          # Full suite with human output
make test-json     # JSON output for CI
make test-compare  # Compare against saved baseline
```

## Documentation

- [ADR-001: Workspace Split](docs/adr/001-workspace-split.md) — Architecture decision record
- [Implementation Plan](docs/adr/001-implementation-plan.md) — Phase-by-phase execution plan
- [Configuration Defaults](docs/CONFIGURATION_DEFAULTS.md)
- [Failure Scenarios](docs/FAILURE_SCENARIOS.md)

## Why the name?

"Foxing" is an archival term for the brownish spots that appear on old paper and antique mirrors. Since this project is a Mirror written in Rust, the name fit perfectly.

It also nods to "The quick brown fox jumps over the lazy dog" — the fast NVMe source drive leaping over the latency of the slower backup target.

## License

GPLv2
