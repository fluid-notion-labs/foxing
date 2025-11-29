use prometheus::{
    Registry, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, HistogramVec, GaugeVec,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry, 
    register_histogram_vec_with_registry, register_int_counter_with_registry,
    register_gauge_vec_with_registry
};
use lazy_static::lazy_static;
use std::sync::atomic::AtomicU64;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    
    pub static ref EVENTS_TOTAL: IntCounterVec = register_int_counter_vec_with_registry!("foxing_events_total", "Total events received by type", &["device", "type"], REGISTRY).unwrap();
    pub static ref EVENTS_DROPPED: IntCounter = prometheus::register_int_counter_with_registry!("foxing_events_dropped", "Events dropped due to queue/buffer overflow", REGISTRY).unwrap();
    pub static ref EVENTS_MALFORMED: IntCounter = prometheus::register_int_counter_with_registry!("foxing_events_malformed", "Malformed BPF events", REGISTRY).unwrap();
    pub static ref EVENTS_FILTERED: IntCounterVec = register_int_counter_vec_with_registry!("foxing_events_filtered", "Events filtered by target include/exclude rules", &["target"], REGISTRY).unwrap();
    pub static ref RENAME_EVENTS: IntCounter = prometheus::register_int_counter_with_registry!("foxing_rename_events_total", "Successful rename events processed", REGISTRY).unwrap();
    pub static ref COALESCED_WRITES: IntCounter = prometheus::register_int_counter_with_registry!("foxing_coalesced_writes_total", "Write events consumed by coalescing", REGISTRY).unwrap();
    pub static ref LATE_EVENTS: IntCounter = prometheus::register_int_counter_with_registry!("foxing_late_events_total", "Late events (out of order, ignored)", REGISTRY).unwrap();
    
    pub static ref SEQUENCE_GAPS: IntCounterVec = register_int_counter_vec_with_registry!("foxing_bpf_sequence_gaps_total", "Sequence gaps detected in BPF stream", &["device"], REGISTRY).unwrap();
    pub static ref EVENTS_UNWATCHED: IntCounter = prometheus::register_int_counter_with_registry!("foxing_bpf_events_unwatched_device", "Events from devices not being watched", REGISTRY).unwrap();
    pub static ref ORDERING_BUF_SIZE: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_ordering_buffer_size", "Ordering buffer pending count", &["target"], REGISTRY).unwrap();
    
    pub static ref BYTES_REPLICATED: IntCounterVec = register_int_counter_vec_with_registry!("foxing_bytes_replicated_total", "Bytes successfully written to target", &["target"], REGISTRY).unwrap();
    pub static ref REPLICATION_LATENCY: HistogramVec = register_histogram_vec_with_registry!("foxing_replication_latency_seconds", "End-to-end replication latency (event to disk sync)", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_LAG: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_target_lag_seconds", "Time difference between source event and worker processing", &["target"], REGISTRY).unwrap();
    pub static ref REFLINK_OPS: IntCounterVec = register_int_counter_vec_with_registry!("foxing_reflink_operations_total", "Reflink attempts by result", &["target", "result"], REGISTRY).unwrap();
    
    pub static ref TARGET_CAPACITY_BYTES_TOTAL: GaugeVec = register_gauge_vec_with_registry!("foxing_target_capacity_bytes_total", "Total capacity of target filesystem", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_CAPACITY_BYTES_AVAILABLE: GaugeVec = register_gauge_vec_with_registry!("foxing_target_capacity_bytes_available", "Available capacity of target filesystem", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_CAPACITY_INODES_TOTAL: GaugeVec = register_gauge_vec_with_registry!("foxing_target_capacity_inodes_total", "Total inodes of target filesystem", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_CAPACITY_INODES_AVAILABLE: GaugeVec = register_gauge_vec_with_registry!("foxing_target_capacity_inodes_available", "Available inodes of target filesystem", &["target"], REGISTRY).unwrap();

    pub static ref HYDRATION_ACTIVE: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_hydration_active", "1 if hydration thread is active", &["device"], REGISTRY).unwrap();
    pub static ref HYDRATION_SCANNED: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_hydration_scanned_total", "Files scanned during hydration", &["device"], REGISTRY).unwrap();
    pub static ref HYDRATION_SYNCED: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_hydration_synced_total", "Files synced during hydration", &["device"], REGISTRY).unwrap();
    pub static ref HYDRATION_HASH_SKIPPED: IntCounter = register_int_counter_with_registry!("foxing_hydration_hash_skipped", "Directories skipped via integrity hash check", REGISTRY).unwrap();

    pub static ref SYNTHETIC_IDENTITY_FILES: IntGauge = prometheus::register_int_gauge_with_registry!("foxing_synthetic_identity_files", "Active synthetic files (unlinked/generation mismatched)", REGISTRY).unwrap();
    pub static ref GENERATION_MISMATCHES: IntCounter = register_int_counter_with_registry!("foxing_generation_mismatches_total", "Generation mismatches detected", REGISTRY).unwrap();
    pub static ref WORKER_SHUTDOWN_TIMEOUTS: IntCounter = prometheus::register_int_counter_with_registry!("foxing_worker_shutdown_timeouts_total", "Worker shutdown failures", REGISTRY).unwrap();
    pub static ref SIDECAR_FILES_CREATED: IntCounter = prometheus::register_int_counter_with_registry!("foxing_sidecar_files_created_total", "Sidecar files created/updated", REGISTRY).unwrap();

    pub static ref GOVERNOR_LOAD_AVERAGE: GaugeVec = register_gauge_vec_with_registry!("foxing_governor_load_average", "System load average (1m, 5m, 15m)", &["period"], REGISTRY).unwrap();
    pub static ref GOVERNOR_STRESSED: IntGauge = prometheus::register_int_gauge_with_registry!("foxing_governor_stressed", "Current system stress state (1=stressed, 0=normal)", REGISTRY).unwrap();
    pub static ref GOVERNOR_THROTTLED_EVENTS: IntCounter = prometheus::register_int_counter_with_registry!("foxing_governor_throttled_events_total", "Total times operations were throttled by the governor", REGISTRY).unwrap();
    pub static ref GOVERNOR_PACING_DURATION_MS: IntCounter = prometheus::register_int_counter_with_registry!("foxing_governor_pacing_duration_milliseconds_total", "Total time spent sleeping due to governor pacing", REGISTRY).unwrap();

    pub static ref TARGET_BATCH_SIZE: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_target_tuning_batch_size", "Current adaptive io_uring batch size", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_COALESCE_BYTES: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_target_tuning_coalesce_bytes", "Current adaptive coalesce window size", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_FLUSH_MULTIPLIER: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_target_tuning_flush_multiplier", "Current adaptive flush interval multiplier", &["target"], REGISTRY).unwrap();
    pub static ref TUNER_STATE: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_tuner_state", "Current state of the auto-tuner logic (enum)", &["target"], REGISTRY).unwrap();
    pub static ref WORKER_BUFFER_UTILIZATION: GaugeVec = register_gauge_vec_with_registry!("foxing_worker_buffer_utilization", "Ratio of ordering buffer usage (0.0 - 1.0)", &["target"], REGISTRY).unwrap();

    pub static ref TARGET_VERSION_COUNT: GaugeVec = register_gauge_vec_with_registry!("foxing_target_version_count", "Number of archived file versions", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_VERSION_BYTES: GaugeVec = register_gauge_vec_with_registry!("foxing_target_version_bytes", "Total size of archived file versions", &["target"], REGISTRY).unwrap();
    
    pub static ref TARGET_DYNAMIC_VERSION_LIMIT_COUNT: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_target_dynamic_version_limit_count", "Current adaptive limit for version count", &["target"], REGISTRY).unwrap();
    pub static ref TARGET_DYNAMIC_VERSION_LIMIT_BYTES: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_target_dynamic_version_limit_bytes", "Current adaptive limit for version size (MB)", &["target"], REGISTRY).unwrap();
    
    pub static ref TARGET_FORCED_VERSIONING_ACTIVE: IntGaugeVec = register_int_gauge_vec_with_registry!("foxing_target_forced_versioning_active", "1 if Forced Versioning is overriding safety checks", &["target"], REGISTRY).unwrap();

    pub static ref GLOBAL_BUFFER_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ref GLOBAL_BUFFER_LIMIT: IntGauge = prometheus::register_int_gauge_with_registry!("foxing_global_buffer_limit", "Maximum buffered events across all workers", REGISTRY).unwrap();
    pub static ref METRICS_ENABLED: IntGauge = prometheus::register_int_gauge_with_registry!("foxing_metrics_enabled", "Status of metrics subsystem (1=enabled)", REGISTRY).unwrap();
}

pub fn initialize_metrics(global_limit: u64) {
    GLOBAL_BUFFER_LIMIT.set(global_limit as i64);
}
