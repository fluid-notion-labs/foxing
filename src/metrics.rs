use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    register_histogram, Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Histogram, Registry,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // --- [foxingd] Core Event Metrics (BPF/inotify pipeline) ---
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
    // --- [fxcp-core] Latency & Performance ---
    pub static ref REPLICATION_LATENCY: HistogramVec = register_histogram_vec!(
        "foxing_replication_latency_seconds",
        "End-to-end latency from source event to target write",
        &["target"]
    ).unwrap();
    pub static ref INODE_LOOKUP_DURATION: Histogram = register_histogram!(
        "foxing_inode_lookup_duration_seconds",
        "Time spent resolving inode to path",
        vec![0.001, 0.01, 0.1, 1.0]
    ).unwrap();
    
    // --- [fxcp-core] Data Movement ---
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
    
    // --- [fxcp-core] IO Methods ---
    pub static ref COPY_METHOD_REFLINK: CounterVec = register_counter_vec!(
        "foxing_copy_method_reflink_total",
        "Writes handled via CoW Reflink",
        &["target"]
    ).unwrap();
    pub static ref COPY_METHOD_OFFLOAD: CounterVec = register_counter_vec!(
        "foxing_copy_method_offload_total",
        "Writes handled via hardware/network offload",
        &["target"]
    ).unwrap();
    pub static ref COPY_METHOD_STANDARD: CounterVec = register_counter_vec!(
        "foxing_copy_method_standard_total",
        "Writes handled via standard read/write",
        &["target"]
    ).unwrap();
    pub static ref ATOMIC_WRITE_FALLBACKS: Counter = register_counter!(
        "foxing_atomic_write_fallbacks_total",
        "Number of atomic writes that failed and fell back to buffered/standard I/O"
    ).unwrap();
}

lazy_static! {
    // --- [fxcp-core] Versioning (core) ---
    pub static ref VERSIONING_FAILURES: Counter = register_counter!(
        "foxing_versioning_failures_total",
        "Failed attempts to create version snapshots"
    ).unwrap();
    pub static ref VERSIONING_SUCCESS: Counter = register_counter!(
        "foxing_versioning_success_total",
        "Successfully created version snapshots"
    ).unwrap();
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

    // --- [foxingd] Governor & System Health ---
    pub static ref GOVERNOR_STRESSED: Gauge = register_gauge!(
        "foxing_governor_stressed",
        "Current system stress state (1=stressed, 0=normal)"
    ).unwrap();
    pub static ref GOVERNOR_STRESS_SCORE: Gauge = register_gauge!(
        "foxing_governor_stress_score",
        "Current system stress score (0.0-1.0=OK, >1.0=Critical)"
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
    
    // --- [fxcp-core] Memory & Buffer Pool ---
    pub static ref GLOBAL_BUFFER_LIMIT: Gauge = register_gauge!(
        "foxing_global_buffer_limit_bytes",
        "Maximum configured memory for buffers (Bytes)"
    ).unwrap();
    pub static ref GLOBAL_BUFFER_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ref GLOBAL_MEMORY_USAGE_BYTES: Gauge = register_gauge!(
        "foxing_global_memory_usage_bytes",
        "Current total memory allocated for IO buffers across all workers"
    ).unwrap();

    pub static ref BUFFER_POOL_CAPACITY: Gauge = register_gauge!(
        "foxing_buffer_pool_capacity_buffers",
        "Number of buffers in the worker's buffer pool"
    ).unwrap();
    pub static ref BUFFER_POOL_CHUNK_SIZE: Gauge = register_gauge!(
        "foxing_buffer_pool_chunk_size_bytes",
        "Size of each buffer chunk in bytes"
    ).unwrap();
    pub static ref BUFFER_POOL_TOTAL_BYTES: Gauge = register_gauge!(
        "foxing_buffer_pool_total_bytes",
        "Total memory allocated to buffer pool"
    ).unwrap();
}

lazy_static! {
    // --- [fxcp-core] Reliability (copy-plane) ---
    pub static ref JOURNAL_RECOVERIES: Counter = register_counter!(
        "foxing_journal_recoveries_total",
        "Number of atomic rename operations recovered from intent journal"
    ).unwrap();
    // --- [foxingd] Reliability (daemon-plane) ---
    pub static ref POISON_CABINET_ACTIVE: Gauge = register_gauge!(
        "foxing_poison_cabinet_active_inodes",
        "Number of inodes currently in backoff state due to repeated failures"
    ).unwrap();
    pub static ref SIDECAR_FILES_CREATED: Counter = register_counter!(
        "foxing_sidecar_files_created_total",
        "Number of .foxing_meta files created (fallback metadata)"
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
    
    // --- [fxcp-core] Integrity ---
    pub static ref HASH_VERIFICATIONS_TOTAL: Counter = register_counter!(
        "foxing_hash_verifications_total",
        "Number of BLAKE3 hash verifications performed"
    ).unwrap();
    pub static ref HASH_CACHE_HITS: Counter = register_counter!(
        "foxing_hash_cache_hits_total",
        "Files skipped due to matching signature cache"
    ).unwrap();
    pub static ref HASH_COMPUTATION_DURATION: Histogram = register_histogram!(
        "foxing_hash_computation_seconds",
        "Time spent computing BLAKE3 hashes"
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

// --- [foxingd] Debug ---
#[cfg(feature = "debug_metrics")]
lazy_static! {
    pub static ref DEBUG_EVENTS_BY_DEV: CounterVec = register_counter_vec!(
        "foxing_debug_events_by_dev",
        "Total events received by type and device (Debug)",
        &["device", "type"]
    ).unwrap();
}

// [foxingd]
pub static DISCOVERY_COMPLETE: AtomicBool = AtomicBool::new(false);

// [fxcp-core]
pub fn initialize_metrics(global_limit_mb: u64) {
    GLOBAL_BUFFER_LIMIT.set((global_limit_mb * 1024 * 1024) as f64);
}

// [fxcp-core] BatchedCounter and BatchedAtomicCounter — shared helpers
pub struct BatchedCounter {
    counter: Counter,
    local_count: u64,
    batch_size: u64,
}

impl BatchedCounter {
    pub fn new(counter: Counter, batch_size: u64) -> Self {
        Self {
            counter,
            local_count: 0,
            batch_size,
        }
    }

    #[inline]
    pub fn inc(&mut self) {
        self.local_count += 1;
        if self.local_count >= self.batch_size {
            self.flush();
        }
    }

    #[inline]
    pub fn inc_by(&mut self, val: u64) {
        self.local_count += val;
        if self.local_count >= self.batch_size {
            self.flush();
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        if self.local_count > 0 {
            self.counter.inc_by(self.local_count as f64);
            self.local_count = 0;
        }
    }
}

impl Drop for BatchedCounter {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct BatchedAtomicCounter {
    global: &'static AtomicU64,
    local_count: i64,
    batch_size: i64,
}

impl BatchedAtomicCounter {
    pub fn new(global: &'static AtomicU64, batch_size: i64) -> Self {
        Self {
            global,
            local_count: 0,
            batch_size,
        }
    }

    #[inline]
    pub fn inc(&mut self) {
        self.local_count += 1;
        if self.local_count >= self.batch_size {
            self.flush();
        }
    }

    #[inline]
    pub fn dec(&mut self) {
        self.local_count -= 1;
        if self.local_count <= -self.batch_size {
            self.flush();
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        if self.local_count != 0 {
            if self.local_count > 0 {
                self.global.fetch_add(self.local_count as u64, Ordering::Relaxed);
            } else {
                self.global.fetch_sub((-self.local_count) as u64, Ordering::Relaxed);
            }
            self.local_count = 0;
        }
    }
}

impl Drop for BatchedAtomicCounter {
    fn drop(&mut self) {
        self.flush();
    }
}
