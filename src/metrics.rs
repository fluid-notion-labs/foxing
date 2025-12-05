use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    register_histogram, Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Histogram, Registry,
};
use std::sync::atomic::{AtomicBool, AtomicU64};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // --- Core Event Metrics ---
    pub static ref EVENTS_TOTAL: CounterVec = register_counter_vec!(
        "foxing_events_total",
        "Total events received by type",
        &["device", "type"]
    ).unwrap();

    pub static ref EVENTS_DROPPED: Counter = register_counter!(
        "foxing_events_dropped",
        "Events dropped due to queue/buffer overflow"
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

    pub static ref BPF_RENAME_INCOMPLETE_DATA: Counter = register_counter!(
        "foxing_bpf_rename_incomplete_data",
        "Rename events delivered by BPF lacking new_parent_inode or new_name."
    ).unwrap();

    pub static ref SEQUENCE_GAPS: CounterVec = register_counter_vec!(
        "foxing_sequence_gaps_total",
        "Number of detected sequence gaps in BPF stream",
        &["device"]
    ).unwrap();

    // --- Latency & Performance ---
    pub static ref REPLICATION_LATENCY: HistogramVec = register_histogram_vec!(
        "foxing_replication_latency_seconds",
        "End-to-end latency from source event to target write",
        &["target"]
    ).unwrap();

    pub static ref INODE_LOOKUP_DURATION: Histogram = register_histogram!(
        "foxing_inode_lookup_duration_seconds",
        "Time spent resolving inode to path",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
    ).unwrap();

    // --- Tuner & Buffer ---
    pub static ref TARGET_BATCH_SIZE: GaugeVec = register_gauge_vec!(
        "foxing_target_batch_size",
        "Current dynamic batch size calculated by BBR tuner",
        &["target"]
    ).unwrap();

    pub static ref TARGET_COALESCE_BYTES: GaugeVec = register_gauge_vec!(
        "foxing_target_coalesce_bytes",
        "Current dynamic coalesce window size in bytes",
        &["target"]
    ).unwrap();

    pub static ref TUNER_STATE: GaugeVec = register_gauge_vec!(
        "foxing_tuner_state",
        "Current state of the adaptive tuner (0=Startup, 1=Steady, 2=HighLoad, 3=Muted, 4=Drain, 5=Critical)",
        &["target"]
    ).unwrap();

    pub static ref WORKER_BUFFER_UTILIZATION: GaugeVec = register_gauge_vec!(
        "foxing_worker_buffer_utilization",
        "Ratio of pending events to max queue depth (0.0 - 1.0)",
        &["target"]
    ).unwrap();

    // --- Throughput & Operations ---
    pub static ref BYTES_REPLICATED: CounterVec = register_counter_vec!(
        "foxing_bytes_replicated_total",
        "Bytes successfully written to target",
        &["target"]
    ).unwrap();

    pub static ref RENAME_EVENTS: Counter = register_counter!(
        "foxing_rename_events_total",
        "Total rename operations processed"
    ).unwrap();

    pub static ref COALESCED_WRITES: Counter = register_counter!(
        "foxing_coalesced_writes_total",
        "Number of write events merged into larger chunks"
    ).unwrap();

    pub static ref COPY_METHOD_REFLINK: Counter = register_counter!(
        "foxing_copy_method_reflink_total",
        "Writes handled via CoW Reflink"
    ).unwrap();

    pub static ref COPY_METHOD_OFFLOAD: Counter = register_counter!(
        "foxing_copy_method_offload_total",
        "Writes handled via hardware/network offload"
    ).unwrap();

    pub static ref COPY_METHOD_STANDARD: Counter = register_counter!(
        "foxing_copy_method_standard_total",
        "Writes handled via standard read/write"
    ).unwrap();

    pub static ref JOURNAL_RECOVERIES: Counter = register_counter!(
        "foxing_journal_recoveries_total",
        "Number of atomic rename operations recovered from intent journal"
    ).unwrap();

    // --- Stability & Errors ---
    pub static ref POISON_CABINET_ACTIVE: Gauge = register_gauge!(
        "foxing_poison_cabinet_active_inodes",
        "Number of inodes currently in backoff state due to repeated failures"
    ).unwrap();

    pub static ref SIDECAR_FILES_CREATED: Counter = register_counter!(
        "foxing_sidecar_files_created_total",
        "Number of .foxing_meta files created (fallback metadata)"
    ).unwrap();

    pub static ref SYNTHETIC_MARKER_RECREATIONS: Counter = register_counter!(
        "foxing_synthetic_marker_recreations_total",
        "Number of times synthetic markers were recreated"
    ).unwrap();

    pub static ref IDENTITY_CACHE_HIT_RATE: Gauge = register_gauge!(
        "foxing_identity_cache_hit_rate",
        "Ratio of InodeMap hits vs misses"
    ).unwrap();

    pub static ref WAL_COHERENCE_FAILURES: CounterVec = register_counter_vec!(
        "foxing_wal_coherence_failures_total",
        "Detected mismatches between WAL state and file state",
        &["target"]
    ).unwrap();

    // --- System Governor ---
    pub static ref GOVERNOR_STRESSED: Gauge = register_gauge!(
        "foxing_governor_stressed",
        "Current system stress state (1=stressed, 0=normal)"
    ).unwrap();

    pub static ref GOVERNOR_THROTTLED_EVENTS: Counter = register_counter!(
        "foxing_governor_throttled_events_total",
        "Events delayed due to governor pressure"
    ).unwrap();

    pub static ref GOVERNOR_LOAD_AVERAGE: GaugeVec = register_gauge_vec!(
        "foxing_governor_load_average",
        "System load average (1m, 5m, 15m)",
        &["period"]
    ).unwrap();

    pub static ref GOVERNOR_PACING_DURATION_MS: Counter = register_counter!(
        "foxing_governor_pacing_duration_milliseconds_total",
        "Total time spent sleeping due to governor pacing"
    ).unwrap();

    // --- Discovery & Hydration ---
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

    // --- Capacity & Versioning ---
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

    pub static ref ORDERING_BUF_SIZE: GaugeVec = register_gauge_vec!(
        "foxing_ordering_buffer_size",
        "Ordering buffer pending count",
        &["target"]
    ).unwrap();

    pub static ref LATE_EVENTS: Counter = register_counter!(
        "foxing_late_events_total",
        "Events that arrived after their window passed"
    ).unwrap();

    pub static ref WORKER_SHUTDOWN_TIMEOUTS: Counter = register_counter!(
        "foxing_worker_shutdown_timeouts",
        "Workers that failed to drain gracefully"
    ).unwrap();

    pub static ref GENERATION_MISMATCHES: Counter = register_counter!(
        "foxing_generation_mismatches",
        "Inode generation mismatches detected"
    ).unwrap();

    pub static ref SYNTHETIC_IDENTITY_FILES: Counter = register_counter!(
        "foxing_synthetic_identity_files",
        "Identity files created for tracking"
    ).unwrap();

    pub static ref GLOBAL_BUFFER_LIMIT: Gauge = register_gauge!(
        "foxing_global_buffer_limit",
        "Maximum buffered events across all workers"
    ).unwrap();

    pub static ref GLOBAL_BUFFER_COUNT: AtomicU64 = AtomicU64::new(0);
}

pub static DISCOVERY_COMPLETE: AtomicBool = AtomicBool::new(false);

pub fn initialize_metrics(global_limit: u64) {
    GLOBAL_BUFFER_LIMIT.set((global_limit as i64) as f64);
}
