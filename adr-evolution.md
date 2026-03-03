# ADR-002: Unified High-Performance Replication, Block Awareness, and Resiliency

**Status:** Mostly Implemented

**Date:** 2026-03-03

**Authors:** Joel Wirāmu Pauling

**Target Platform:** Linux 6.18+ (mainline), XFS, NFS 4.2+, io_uring, block-mq, FUSE, CSI, MinIO, NPU (Teflon), GPU (Vulkan), IBM Z (s390x), IBM Power (ppc64le)

## 1. Context & Implementation Philosophy

Foxing provides zero-throttling replication across asymmetric storage topologies. This document serves as the high-fidelity source of truth for the replication engine's evolution, ensuring that high-performance kernel bypasses and resilient fallbacks coexist without conflict across the full spectrum of modern hardware.

### 1.1 The "Immutable-to-Sparse" Extraction Mandate

A core requirement of Foxing is the efficient extraction of immutable, compressed filesystem images (**EROFS**, **SquashFS**). Standard VFS extraction pathologically expands these images into "dense" files on the target. Foxing acts as a **Dynamic Re-Sparsifier**, reclaiming empty space in real-time during the extraction pipeline by utilizing architecture-specific vector units (AVX, NEON, RVV, VSX, Z-Vector).

### 1.2 Zero-Regression Verification Protocol

1. **Pre-Flight Baseline:** `python3 tests/harness.py --save-baseline`

2. **Implementation:** Apply architecture-specific modifications.

3. **Compilation Check:** `cargo check --workspace`

4. **Comprehensive Verification:** `python3 tests/harness.py --compare tests/baseline.json --human`.

## 2. Decision Log (Ordered by Implementation Complexity)

### 2.1 Batch A: Foundation (Tier 1 - Low Complexity)

* **Block Device Awareness (S_ISBLK):** **\[IMPLEMENTED\]** Detect raw block devices via `FileTypeExt::is_block_device()`. If detected, skip `O_TRUNC`, `O_CREAT`, `fallocate`, and `ftruncate`. *(Implemented in `fxcp/src/main.rs:191-206` with separate open path for block devices).*

* **Sidecar Metadata Fallback:** **\[PARTIAL\]** If `setxattr(2)` is unsupported (FAT32, SMB), store sync state in an atomic, hidden `.foxing.json` sidecar. *(JSON fallback is implemented in `sidecar.rs:122-198`. However, the BLAKE3 hash-matching for volatile `vfs_rename` tracking is not yet implemented).*

* **Transparent Decompression:** **\[IMPLEMENTED\]** Internalized optimized Rust native engines (`zstd`, `lz4-flex`, `flate2`). *(Dependencies in `fxcp/Cargo.toml:24-26`. Auto-detection via magic bytes in `fxcp/src/main.rs:132-175` with `detect_compression()` and `wrap_decompressor()`. Supports zstd, gzip, lz4, xz detection).*

* **Encryption Hierarchy:** **\[PLANNED\]** Support password-protected 7z/RAR containers via `FXCP_PASSWORD` (Env) > `--password-file` > TTY Prompt.

### 2.2 Batch B: Performance (Tier 2 - Medium Complexity)

* **Zero-Copy Streaming:** **\[PARTIAL\]** `--zero-copy` CLI flag exists (`fxcp/src/main.rs:49`) and enforces mutual exclusivity with sparse detection (`use_sparse = !cli.zero_copy` at line 222). However, actual `io_uring_prep_splice` kernel-bypass is not yet implemented — the flag currently only disables zero-block scanning.

* **Deep EROFS/SquashFS Re-Sparsification Engine:** **\[IMPLEMENTED\]** \* **The Pipeline:** `Kernel VFS (Decompression) -> io_uring Buffer -> SIMD/Vector Zero-Scan -> fallocate(FALLOC_FL_PUNCH_HOLE) -> Target FS`. *(Sparse `fallocate` pipeline is fully wired in `operations.rs:1490-1598,2083-2106`).*

  * **SIMD/Vector Logic:** \* **x86_64:** AVX-512 (`_mm512_test_epi64_mask`) / AVX2 (`_mm256_testz_si256`). **\[IMPLEMENTED\]** *(operations.rs:2231-2283)*

    * **AArch64:** ARM NEON `vmaxvq_u8`. **\[IMPLEMENTED\]** *(operations.rs:2191-2228, `is_block_zero_neon()` with 64B/iter vectorized scanning)*

    * **RISC-V:** RVV VLA scanning. **\[PLANNED\]** *(Uses generic u128 fallback)*

    * **ppc64le (IBM Power):** VSX 128-bit parallel zero-detection. **\[PLANNED\]** *(Uses generic u128 fallback)*

    * **s390x (IBM Z):** Vector Facility `vceq`. **\[PLANNED\]** *(Uses generic u128 fallback)*

* **Cloud & CSI Compatibility:**

  * **Object Buffer Mode:** **\[PLANNED\]** Align writes to 8MB for S3/MinIO.

  * **CSI Chill Mode:** **\[PLANNED\]** Exponential backoff on `EAGAIN` or 1000ms+ latency.

* **Multi-part Archive Reassembly:** **\[PLANNED\]** Implement a `MultiPartReader` to chain split sequences (`.001`, `.part1.rar`).

### 2.3 Batch C: Resiliency & Kernel Integration (Tier 3 - High Complexity)

* **eBPF Hardening:** **\[IMPLEMENTED\]** Use eBPF Ring Buffers with atomic sequence counters (`__sync_fetch_and_add` in `mirror.bpf.c:189-194`) to detect drops.

* **Gap Injection:** **\[IMPLEMENTED\]** On overflow, drop the event but advance the sequence. Userspace detects the gap and triggers Frontier-based Surgical Hydration. *(events_dropped counter at `mirror.bpf.c:240-241,345-346`. `EVENT_SEQUENCE_GAP` handled in `ordering.rs`).*

* **kretprobe Verification:** **\[IMPLEMENTED\]** Use `kretprobe/vfs_rename` (via `trace_rename_exit` at `mirror.bpf.c:512-587`) to emit events only if `ret == 0`. Also applied to `vfs_unlink` (lines 614-621) and `vfs_rmdir` (lines 623-630).

* **Live Stream Checkpointing:** **\[IMPLEMENTED\]** Periodic `FICLONE` snapshots of `stdin` ingestion for 0-byte cost "Time-Travel" recording. *(`--checkpoint-interval` and `--checkpoint-keep` flags at `fxcp/src/main.rs:45-47`. Checkpoint logic at lines 303-327. Rolling pruning via `prune_stream_checkpoints()` at lines 351-373).*

* **Secure Extraction:** **\[IMPLEMENTED\]** Safe canonicalization (`security::canonicalize_safe` at `security.rs:567-582`) AND `openat2(2)` with `RESOLVE_BENEATH` for hardened archive extraction (`security::open_beneath` at `security.rs:584-658`). Falls back to `canonicalize_safe` on kernels without `openat2` (ENOSYS).

### 2.4 Batch D: Hardware Accelerators (Tier 4 - Expert Complexity)

* **Kernel 6.18+ Alignment:** **\[IMPLEMENTED\]** Use `statx(2)` with `STATX_DIOALIGN` for dynamic Direct I/O discovery. *(`probe_capabilities` parses `stx_dio_mem_align` and `stx_dio_offset_align` at `operations.rs:452-476`. `STATX_DIOALIGN` const at line 310. Capabilities fields at lines 251-252).*

* **Offloading Backends:**

  * **Async Copy:** **\[NOT FEASIBLE\]** `IORING_OP_COPY_FILE_RANGE` does not exist in the Linux kernel io_uring opcode table (confirmed via kernel 6.17 headers and io-uring crate 0.7.11 source). Synchronous `libc::copy_file_range` via blocking thread is the correct approach.

  * **NPU/GPU Offloading:** **\[PLANNED\]** Offload scanning and Merkle hashing via Mesa Teflon (NPU) or Vulkan Compute (GPU).

  * **s390x (IBM Z) Compression Offload:** **\[PLANNED\]** Utilize `dfltcc` hardware acceleration.

  * **ppc64le (Power) Cache Optimization:** **\[PLANNED\]** Use `dcbz` for ultra-fast buffer initialization.

* **NUMA-Aware Allocation:** **\[STUBBED\]** `AlignedBuffer::try_new_numa()` exists at `buffer.rs:26-32` but falls back to standard allocation. Real `mbind(2)` pinning requires NUMA hardware for testing.

## 3. Mutually Exclusive Constraints & Overrides

| Feature A | Feature B | Resolution / Constraint |
 | ----- | ----- | ----- |
| **`kvdo` (VDO)** | **`STATX_DIOALIGN`** | **\[IMPLEMENTED\]** Clamp all SQEs to 4096 (VDO fixed block). *(operations.rs:467-473)* |
| **Zero-Copy Splice** | **Sparse Detection** | **\[IMPLEMENTED\]** `--zero-copy` disables sparse detection (`use_sparse = !cli.zero_copy` at main.rs:222). Actual splice kernel-bypass not yet wired. |
| **`RWF_ATOMIC`** | **Unaligned I/O** | **\[IMPLEMENTED\]** Fallback to buffered writes if alignment is not met. *(operations.rs:1461-1484, `check_atomic_invariants()`)* |
| **s390x (Big Endian)** | **Merkle Hashing** | **\[PLANNED\]** Merkle tree implementation must enforce little-endian byte-order. |

## 4. Implementation Matrix & Hardware Constraints

| Constraint | Resolution | Status |
 | ----- | ----- | ----- |
| **`kvdo` (VDO)** | Clamp to 4K alignment; ignore `statx` suggestions. | **\[IMPLEMENTED\]** |
| **x86_64 SIMD** | Use `_mm256_testz_si256` / `_mm512_test_epi64_mask`. | **\[IMPLEMENTED\]** |
| **AArch64 (NEON)** | Use `vmaxvq_u8` for zero detection. | **\[IMPLEMENTED\]** |
| **RISC-V (RVV)** | Use `vle8.v` and vector reduction for scanning. | **\[PLANNED\]** *(generic u128 fallback active)* |
| **ppc64le (VSX)** | Use `xvtestzdp` for 128-bit zero testing. | **\[PLANNED\]** *(generic u128 fallback active)* |
| **s390x (Vector)** | Use `vceq` (Vector Compare Equal) logic. | **\[PLANNED\]** *(generic u128 fallback active)* |
| **s390x (Compression)** | Leverage `dfltcc` hardware acceleration. | **\[PLANNED\]** |

## 5. Consequences

* **Benefits:** Unified deployment tool; hardware-optimal extraction across x86, ARM, RISC-V, Power, and Z; absolute decoupling of source/target speed.

* **Risks:** \* **Endianness Parity:** Big-endian (s390x) vs Little-endian (others) requires strict Merkle tree serialization standards.

  * **CPU Saturation:** Real-time re-sparsification (SIMD scanning) increases CPU load; mitigated by hardware offloading.

## 6. Verification Protocol

1. **Sparsity Test:** Extract a SquashFS image containing 50% packed zeros. Verify target disk usage. *(Passes locally on x86_64).*

2. **Endianness Test:** Generate Merkle hashes on x86_64 and s390x for the same data; ensure bit-parity.

3. **NPU/GPU/Z-Compression Test:** Use Teflon/Vulkan/dfltcc delegates to verify bit-parity across all compute backends.

## 7. Implementation Summary (as of 2026-03-03)

| Category | Implemented | Partial | Planned | Not Feasible |
|----------|:-----------:|:-------:|:-------:|:------------:|
| Batch A (Foundation) | 2 | 1 | 1 | 0 |
| Batch B (Performance) | 3 | 1 | 4 | 0 |
| Batch C (Resiliency) | 5 | 0 | 0 | 0 |
| Batch D (Hardware) | 2 | 0 | 4 | 1 |
| Constraints | 3 | 0 | 1 | 0 |
| **Total** | **15** | **2** | **10** | **1** |
