// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/metrics.rs — Prometheus metrics for foxingd daemon

//! Prometheus metric definitions for the foxingd replication daemon.
//! Includes copy stats, worker health, tuner state, and hydration progress.

// Re-export all fxcp-core metrics (latency, data movement, IO methods,
// versioning core, memory/buffer pool, reliability copy-plane, integrity,
// BatchedCounter, BatchedAtomicCounter, initialize_metrics)
pub use fxcp_core::metrics::*;

use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec,
    Counter, CounterVec, Gauge, GaugeVec,
};
use std::sync::atomic::AtomicBool;

lazy_static! {
    // --- [foxingd] Core Event Metrics ---
    pub static ref EVENTS_TOTAL: CounterVec = register_counter_vec!(
        "foxing_events_total",
        "Total events received by type",
        &["type"]
    ).unwrap();
    pub static ref EVENTS_DROPPED: Counter = register_counter!(
        "foxing_events_dropped",
        "Events dropped due to queue/buffer overflow or retry exhaustion"
    ).unwrap();
    pub static ref EVENTS_FILTERED: CounterVec = register_counter_vec!(
        "foxing_events_filtered_total",
        "Events filtered by configuration rules",
        &["path"]
    ).unwrap();
    pub static ref EVENTS_MALFORMED: Counter = register_counter!(
        "foxing_events_malformed",
        "Events received from BPF that could not be parsed"
    ).unwrap();
    pub static ref EVENTS_UNWATCHED: Counter = register_counter!(
        "foxing_events_unwatched",
        "Events received for devices not currently watched"
    ).unwrap();
    pub static ref LATE_EVENTS: Counter = register_counter!(
        "foxing_late_events_total",
        "Events that arrived after their window passed (Reorder Buffer)"
    ).unwrap();

    // --- [foxingd] BPF & Ring Diagnostics ---
    pub static ref BPF_PANIC_CAUGHT: Counter = register_counter!(
        "foxing_bpf_panic_caught_total",
        "Number of panics caught within the BPF ring buffer callback"
    ).unwrap();
    pub static ref BPF_RENAME_INCOMPLETE_DATA: Counter = register_counter!(
        "foxing_bpf_rename_incomplete_data",
        "Rename events delivered by BPF lacking new_parent_inode or new_name."
    ).unwrap();
    pub static ref SEQUENCE_GAPS: CounterVec = register_counter_vec!(
        "foxing_sequence_gaps_total",
        "Number of detected sequence gaps in BPF stream",
        &["device"]
    ).unwrap();
    pub static ref LARGE_SEQUENCE_GAPS: Counter = register_counter!(
        "foxing_large_sequence_gaps_total",
        "Number of gaps > 1000 events triggering Panic Mode"
    ).unwrap();
}

lazy_static! {
    // --- [foxingd] Versioning (target-scoped) ---
    pub static ref TARGET_DYNAMIC_VERSION_LIMIT_COUNT: GaugeVec = register_gauge_vec!(
        "foxing_target_dynamic_version_limit_count",
        "Current adaptive limit for version count",
        &["target"]
    ).unwrap();
    pub static ref TARGET_DYNAMIC_VERSION_LIMIT_BYTES: GaugeVec = register_gauge_vec!(
        "foxing_target_dynamic_version_limit_bytes",
        "Current adaptive limit for version storage (MB)",
        &["target"]
    ).unwrap();
    pub static ref TARGET_FORCED_VERSIONING_ACTIVE: GaugeVec = register_gauge_vec!(
        "foxing_target_forced_versioning_active",
        "1 if force retention policies are currently active",
        &["target"]
    ).unwrap();

    // Governor metrics come from fxcp_core::metrics re-export
}

lazy_static! {
    // --- [foxingd] BBR Tuner Metrics ---
    pub static ref TARGET_BATCH_SIZE: GaugeVec = register_gauge_vec!(
        "foxing_target_batch_size",
        "Current dynamic batch size calculated by BBR tuner",
        &["target", "worker"]
    ).unwrap();
    pub static ref TARGET_COALESCE_BYTES: GaugeVec = register_gauge_vec!(
        "foxing_target_coalesce_bytes",
        "Current dynamic coalesce window size in bytes",
        &["target", "worker"]
    ).unwrap();
    pub static ref TARGET_FLUSH_INTERVAL_MS: GaugeVec = register_gauge_vec!(
        "foxing_target_flush_interval_ms",
        "Current adaptive flush interval derived from storage RTT",
        &["target", "worker"]
    ).unwrap();
    pub static ref TARGET_STORAGE_CLASS: GaugeVec = register_gauge_vec!(
        "foxing_target_storage_class",
        "Inferred storage class (0=Unknown, 1=HDD, 2=SATA, 3=NVMe, 4=Throttled)",
        &["target", "worker"]
    ).unwrap();
    pub static ref TUNER_STATE: GaugeVec = register_gauge_vec!(
        "foxing_tuner_state",
        "Current state of the adaptive tuner (0=Startup, 1=Drain, 2=ProbeBW, 3=Muted, 8=Steady, 99=Conservative)",
        &["target", "worker"]
    ).unwrap();

    // --- [foxingd] Worker Internals ---
    pub static ref WORKER_BUFFER_UTILIZATION: GaugeVec = register_gauge_vec!(
        "foxing_worker_buffer_utilization",
        "Ratio of pending events to max queue depth (0.0 - 1.0)",
        &["target", "worker"]
    ).unwrap();
    pub static ref WORKER_RETRY_QUEUE_SIZE: GaugeVec = register_gauge_vec!(
        "foxing_worker_retry_queue_size",
        "Number of events currently pending retry",
        &["target", "worker"]
    ).unwrap();

    // Fixed: Only one definition of ORDERING_BUF_SIZE with correct cardinality
    pub static ref ORDERING_BUF_SIZE: GaugeVec = register_gauge_vec!(
        "foxing_ordering_buffer_size",
        "Ordering buffer pending count",
        &["type"] // type=ingress|structural|worker
    ).unwrap();
}

lazy_static! {
    // --- [foxingd] Stall Detection ---
    pub static ref WORKER_COPY_IN_FLIGHT: GaugeVec = register_gauge_vec!(
        "foxing_worker_copy_in_flight",
        "Number of copy operations currently executing per worker",
        &["target", "worker"]
    ).unwrap();
    pub static ref WORKER_LAST_COPY_EPOCH_MS: GaugeVec = register_gauge_vec!(
        "foxing_worker_last_copy_epoch_ms",
        "Epoch milliseconds of last successful copy operation (stale = stall)",
        &["target", "worker"]
    ).unwrap();
    pub static ref HYDRATION_WORKER_BLOCKED_MS: CounterVec = register_counter_vec!(
        "foxing_hydration_worker_blocked_ms_total",
        "Cumulative milliseconds hydration workers spent in blocking I/O",
        &["worker"]
    ).unwrap();
    pub static ref COPY_TIMEOUT_TOTAL: CounterVec = register_counter_vec!(
        "foxing_copy_timeout_total",
        "Copy operations that exceeded timeout",
        &["target"]
    ).unwrap();

    // --- [foxingd] Stall Detection & Recovery Metrics ---
    pub static ref POSTCOPY_TIMEOUT_TOTAL: CounterVec = register_counter_vec!(
        "foxing_postcopy_timeout_total",
        "Post-copy metadata operations that timed out",
        &["target"]
    ).unwrap();
    pub static ref SEGMENT_TIMEOUT_TOTAL: CounterVec = register_counter_vec!(
        "foxing_segment_timeout_total",
        "Data segment operations that timed out",
        &["target"]
    ).unwrap();
    pub static ref SEGMENT_STALL_TOTAL: CounterVec = register_counter_vec!(
        "foxing_segment_stall_total",
        "Data segment stalls detected (no progress)",
        &["target"]
    ).unwrap();
    pub static ref WORKER_STALL_DETECTED: CounterVec = register_counter_vec!(
        "foxing_worker_stall_detected_total",
        "Workers detected as stalled by watchdog",
        &["worker"]
    ).unwrap();
    pub static ref HYDRATION_QUEUE_STALL_CRITICAL: Gauge = register_gauge!(
        "foxing_hydration_queue_stall_critical",
        "1 if hydration queue is critically stalled (not draining)"
    ).unwrap();
    pub static ref RETRY_QUEUE_FORCED_DRAIN_TOTAL: CounterVec = register_counter_vec!(
        "foxing_retry_queue_forced_drain_total",
        "Forced retry queue drains triggered",
        &["target", "worker"]
    ).unwrap();
}

lazy_static! {
    // --- [foxingd] Adaptive Timeout Metrics ---
    pub static ref ADAPTIVE_TIMEOUT_SEGMENT_STALL_SECS: GaugeVec = register_gauge_vec!(
        "foxing_adaptive_timeout_segment_stall_secs",
        "Current adaptive segment stall timeout in seconds",
        &["target"]
    ).unwrap();
    pub static ref ADAPTIVE_TIMEOUT_SEGMENT_OVERALL_SECS: GaugeVec = register_gauge_vec!(
        "foxing_adaptive_timeout_segment_overall_secs",
        "Current adaptive segment overall timeout in seconds",
        &["target"]
    ).unwrap();
    pub static ref ADAPTIVE_TIMEOUT_POSTCOPY_SECS: GaugeVec = register_gauge_vec!(
        "foxing_adaptive_timeout_postcopy_secs",
        "Current adaptive post-copy timeout in seconds",
        &["target"]
    ).unwrap();
}

lazy_static! {
    // --- [foxingd] Repair & Data Loss Tracking ---
    pub static ref EVENTS_REPAIR_QUEUED: Counter = register_counter!(
        "foxing_events_repair_queued_total",
        "Events routed to repair (full copy) due to target ENOENT"
    ).unwrap();
    pub static ref EVENTS_REPAIR_COMPLETED: Counter = register_counter!(
        "foxing_events_repair_completed_total",
        "Repair jobs completed successfully"
    ).unwrap();
    pub static ref EVENTS_REPAIR_FAILED: Counter = register_counter!(
        "foxing_events_repair_failed_total",
        "Repair jobs that failed"
    ).unwrap();
    pub static ref EVENTS_SOURCE_GONE: Counter = register_counter!(
        "foxing_events_source_gone_total",
        "Events skipped because source file no longer exists"
    ).unwrap();
}

lazy_static! {
    // --- [foxingd] Reliability (daemon-plane) ---
    pub static ref POISON_CABINET_ACTIVE: Gauge = register_gauge!(
        "foxing_poison_cabinet_active_inodes",
        "Number of inodes currently in backoff state due to repeated failures"
    ).unwrap();
    pub static ref WAL_COHERENCE_FAILURES: CounterVec = register_counter_vec!(
        "foxing_wal_coherence_failures_total",
        "Detected mismatches between WAL state and file state",
        &["target"]
    ).unwrap();
    pub static ref WORKER_SHUTDOWN_TIMEOUTS: Counter = register_counter!(
        "foxing_worker_shutdown_timeouts",
        "Workers that failed to drain gracefully"
    ).unwrap();

    // --- [foxingd] Identity & Hydration ---
    pub static ref IDENTITY_CACHE_HIT_RATE: Gauge = register_gauge!(
        "foxing_identity_cache_hit_rate",
        "Ratio of InodeMap hits vs misses"
    ).unwrap();
    pub static ref GENERATION_MISMATCHES: Counter = register_counter!(
        "foxing_generation_mismatches",
        "Inode generation mismatches detected"
    ).unwrap();
    pub static ref SYNTHETIC_IDENTITY_FILES: Counter = register_counter!(
        "foxing_synthetic_identity_files",
        "Identity files created for tracking"
    ).unwrap();
    pub static ref SYNTHETIC_MARKER_RECREATIONS: Counter = register_counter!(
        "foxing_synthetic_marker_recreations_total",
        "Number of times synthetic markers were recreated"
    ).unwrap();
    pub static ref LIVE_ADDITIONS: Counter = register_counter!(
        "foxing_live_additions_total",
        "New files detected by BPF/Inotify during run"
    ).unwrap();
    pub static ref TOTAL_ITEMS_DISCOVERED: Gauge = register_gauge!(
        "foxing_total_items_discovered",
        "Total items found during initial scan"
    ).unwrap();
    pub static ref HYDRATION_ACTIVE: GaugeVec = register_gauge_vec!(
        "foxing_hydration_active",
        "1 if hydration thread is active",
        &["device"]
    ).unwrap();
    pub static ref HYDRATION_HASH_SKIPPED: Counter = register_counter!(
        "foxing_hydration_hash_skipped",
        "Files skipped during hydration because size/mtime matched"
    ).unwrap();
    pub static ref HYDRATION_DIR_PRUNED: Counter = register_counter!(
        "foxing_hydration_dir_pruned_total",
        "Directories skipped by tree-level Merkle hash pruning"
    ).unwrap();
    pub static ref HYDRATION_GATE_REDIRECTED: Counter = register_counter!(
        "foxing_hydration_gate_redirected_total",
        "Events proactively rerouted to repair by hydration gate"
    ).unwrap();

    // --- [foxingd] Capacity ---
    pub static ref TARGET_CAPACITY_BYTES_TOTAL: GaugeVec = register_gauge_vec!(
        "foxing_target_capacity_bytes_total",
        "Total capacity of target filesystem",
        &["target"]
    ).unwrap();
    pub static ref TARGET_CAPACITY_BYTES_AVAILABLE: GaugeVec = register_gauge_vec!(
        "foxing_target_capacity_bytes_available",
        "Available capacity of target filesystem",
        &["target"]
    ).unwrap();
}

lazy_static! {
    // --- [foxingd] Delta Copy (Merkle diff) ---
    pub static ref DELTA_COPY_ATTEMPTED: Counter = register_counter!(
        "foxing_delta_copy_attempted_total",
        "Delta copy attempts using Merkle diff"
    ).unwrap();
    pub static ref DELTA_COPY_BYTES_SAVED: Counter = register_counter!(
        "foxing_delta_copy_bytes_saved_total",
        "Bytes avoided by delta copy vs full copy"
    ).unwrap();
    pub static ref DELTA_COPY_FELL_THROUGH: Counter = register_counter!(
        "foxing_delta_copy_fell_through_total",
        "Delta attempts that fell back to full copy"
    ).unwrap();
}

// --- [foxingd] Debug ---
#[cfg(feature = "debug_metrics")]
lazy_static! {
    pub static ref DEBUG_EVENTS_BY_DEV: CounterVec = register_counter_vec!(
        "foxing_debug_events_by_dev",
        "Total events received by type and device (Debug)",
        &["device", "type"]
    ).unwrap();
}

pub static DISCOVERY_COMPLETE: AtomicBool = AtomicBool::new(false);
