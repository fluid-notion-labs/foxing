use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
    register_histogram, Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Histogram, Registry,
};
use std::sync::atomic::{AtomicU64, Ordering};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

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

    // --- Governor & System Health (used by governor.rs) ---
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

    // --- Storage Layer Detection ---
    pub static ref DM_STACK_DEPTH: Gauge = register_gauge!(
        "foxing_dm_stack_depth",
        "Number of device-mapper layers beneath the filesystem"
    ).unwrap();
    pub static ref DM_CRYPT_DETECTED: Gauge = register_gauge!(
        "foxing_dm_crypt_detected",
        "1 if dm-crypt (LUKS) detected in storage stack"
    ).unwrap();
    pub static ref STORAGE_PHYSICAL_BLOCK_SIZE: Gauge = register_gauge!(
        "foxing_storage_physical_block_size",
        "Physical block size of base storage device (bytes)"
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
    pub static ref SIDECAR_FILES_CREATED: Counter = register_counter!(
        "foxing_sidecar_files_created_total",
        "Number of .foxing_meta files created (fallback metadata)"
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
}

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
