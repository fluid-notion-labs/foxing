// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/constants.rs — Shared constants for tuning, timeouts, thresholds

//! Compile-time and runtime constants for foxing subsystems.
//! Includes worker tuning, retry limits, buffer sizes, and feature flags.

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT_HASH: &str = {
    match option_env!("GIT_HEAD_REF") {
        Some(s) => s,
        None => "unknown",
    }
};
pub const MAX_WORKER_CORES: usize = 64;
pub const DEFAULT_QUEUE_MAX: usize = 500_000;
pub const ERROR_LIMITER_SECS: u64 = 5;
pub const MAX_RETRY_BACKOFF_SECS: u64 = 60;
pub const CAPACITY_THRESHOLD_MB: u64 = 512;
pub const DEFAULT_IO_BUFFER_SIZE_MIB: u64 = 1;
pub const DEFAULT_HIBERNATION_SECS: u64 = 600;
pub const BARRIER_CHECK_INTERVAL: u64 = 50;
pub const MINIMUM_ALIGNMENT_BYTES: usize = 4096;
pub const DEFAULT_IDENTITY_MAP_OVERHEAD_BYTES: u64 = 4096;
pub const EVENT_QUEUE_OVERHEAD_BYTES: u64 = 256;
pub const AVG_EVENT_OVERHEAD_BYTES: u64 = EVENT_QUEUE_OVERHEAD_BYTES + 128;
pub const BPF_EVENT_VERSION: u8 = 5;
pub const ESTIMATED_RING_DEPTH: u64 = 4096;
pub const HYDRATION_QUEUE_CAPACITY: usize = 10_000;
pub const HYDRATION_OOM_RETRY_DELAY_SECS: u64 = 5;
pub const HYDRATION_COPY_MAX_ATTEMPTS: u32 = 10;
pub const HYDRATION_COPY_BACKOFF_BASE_MS: u64 = 100;
pub const HYDRATION_COPY_BACKOFF_MAX_MS: u64 = 5000;
pub const HYDRATION_COPY_JITTER_MAX_MS: u64 = 100;
pub const HYDRATION_PATH_RESOLVE_TIMEOUT_SECS: u64 = 2;
pub const HYDRATION_PATH_RESOLVE_POLL_MS: u64 = 100;
pub const HYDRATION_HEARTBEAT_INTERVAL_SECS: u64 = 30;
pub const HYDRATION_JOB_RECV_TIMEOUT_SECS: u64 = 15;
pub const HYDRATION_ALLOCATION_WARN_INTERVAL_SECS: u64 = 5;
pub const HYDRATION_HOT_DIR_THRESHOLD_SECS: u64 = 24 * 3600;
pub const HYDRATION_BATCH_LIMITS_NVME: (usize, usize) = (4096, 65536);
pub const HYDRATION_BATCH_LIMITS_SSD: (usize, usize) = (2048, 32768);
pub const HYDRATION_BATCH_LIMITS_NETWORK: (usize, usize) = (1024, 16384);
pub const HYDRATION_BATCH_LIMITS_HDD: (usize, usize) = (1024, 8192);
pub const HYDRATION_BATCH_LIMITS_SDCARD: (usize, usize) = (128, 1024);
pub const WORKER_BUFFER_POOL_ALLOC_ATTEMPTS: usize = 5;
pub const WORKER_CLEANUP_INTERVAL_SECS: u64 = 60;
pub const WORKER_REORDER_BUFFER_MIN_BYTES: usize = 16 * 1024;
pub const WORKER_RETRY_QUEUE_MAX_RETRIES: u32 = 10;
pub const WORKER_RETRY_QUEUE_BASE_BACKOFF_MS: u64 = 50;
pub const WORKER_STARTUP_CHECK_DELAY_MS: u64 = 100;
pub const WORKER_IO_URING_DEPTH_MIN: u32 = 4;

// CHANGED: MS -> US (50ms -> 50,000us)
pub const WORKER_TUNE_INTERVAL_US: u64 = 50_000;
pub const WORKER_TUNE_EVENT_THRESHOLD: usize = 10;
pub const WORKER_INITIAL_IO_LATENCY_SECS: u64 = 10;

// CHANGED: MS -> US (50ms -> 50,000us)
pub const TUNER_HYDRATION_DEBOUNCE_US: u64 = 50_000;

pub const TUNER_WINDOW_SECS: u64 = 3; 
pub const TUNER_EWMA_ALPHA: f64 = 0.25; 

// CHANGED: MS -> US (500ms -> 500,000us)
pub const TUNER_STATE_TRANSITION_DELAY_US: u64 = 500_000;

pub const TUNER_IDLE_TIMEOUT_SECS: u64 = 30;
pub const TUNER_STARTUP_DURATION_SECS: u64 = 2; 
pub const TUNER_DRAIN_TIMEOUT_SECS: u64 = 6; 

pub const TUNER_MAX_FLUSH_US: u64 = 5_000_000;
pub const TUNER_MIN_FLUSH_US: u64 = 50;
pub const TUNER_BDP_PACING_GAIN_STARTUP: f64 = 2.0;
pub const TUNER_BDP_PACING_GAIN_DRAIN: f64 = 0.5;
pub const TUNER_BDP_PACING_GAIN_PROBE: f64 = 1.25;
pub const TUNER_BDP_PACING_GAIN_MUTED: f64 = 0.75;
pub const TUNER_MEMORY_PRESSURE_THRESHOLD: f64 = 0.90;
pub const PROFILE_HDD_VALUES: (u64, u64, usize, usize) = (2 * 1024 * 1024, 64 * 1024 * 1024, 4, 32);
pub const PROFILE_NETWORK_VALUES: (u64, u64, usize, usize) = (1 * 1024 * 1024, 32 * 1024 * 1024, 8, 128);
pub const PROFILE_NVME_VALUES: (u64, u64, usize, usize) = (256 * 1024, 16 * 1024 * 1024, 16, 1024);
pub const PROFILE_SSD_VALUES: (u64, u64, usize, usize) = (128 * 1024, 8 * 1024 * 1024, 8, 64);
pub const PROFILE_SDCARD_VALUES: (u64, u64, usize, usize) = (512 * 1024, 16 * 1024 * 1024, 1, 8);
pub const GOVERNOR_HYSTERESIS_SECS: u64 = 1;
pub const GOVERNOR_CHECK_INTERVAL_MS: u64 = 500;
pub const GOVERNOR_PACING_INITIAL_MS: u64 = 10;
pub const GOVERNOR_PACING_MAX_MS: u64 = 1000;
pub const GOVERNOR_MEMORY_HIGH_WATERMARK_PCT: f64 = 0.90;
pub const IDENTITY_EVICTION_TTL_SECS: u64 = 12 * 3600;
pub const CACHE_TTL_EVENTS: u64 = 100_000;
pub const GC_INTERVAL_SECS: u64 = 30;
pub const GC_STARTUP_JITTER_MAX_MS: u64 = 5000;
pub const FULL_SCAN_DEBOUNCE_SECS: u64 = 5;
pub const MIN_INODES_THRESHOLD: u64 = 1000;
pub const BUFFER_POOL_MIN_CHUNK_SIZE: usize = 4096;
pub const GOVERNOR_FAILURE_RATE_THRESHOLD: f64 = 0.5;
pub const GOVERNOR_FAILURE_WINDOW_SECS: u64 = 30;
pub const COPY_TIMEOUT_SMALL_SECS: u64 = 120;
pub const COPY_TIMEOUT_LARGE_SECS: u64 = 600;
#[deprecated(note = "Use adaptive timeout from TunerOutput::postcopy_timeout_secs")]
pub const POSTCOPY_TIMEOUT_SECS: u64 = 300;
#[deprecated(note = "Use adaptive timeout from TunerOutput::segment_overall_timeout_secs")]
pub const PROCESS_SEGMENT_TIMEOUT_SECS: u64 = 600;
#[deprecated(note = "Use adaptive timeout from TunerOutput::segment_stall_timeout_secs")]
pub const PROCESS_SEGMENT_STALL_SECS: u64 = 60;
pub const IOURING_COMPLETION_TIMEOUT_STREAK: usize = 600;
pub const HYDRATION_BACKPRESSURE_TIMEOUT_ATTEMPTS: usize = 120;
pub const STALL_WATCHDOG_INTERVAL_SECS: u64 = 30;
pub const STALL_WATCHDOG_THRESHOLD_SECS: u64 = 120;
pub const RETRY_QUEUE_STALL_THRESHOLD_ITERATIONS: usize = 20;
pub const GOVERNOR_PSI_IO_THRESHOLD_NORMAL: f64 = 10.0;
pub const GOVERNOR_PSI_CPU_THRESHOLD_NORMAL: f64 = 10.0;
pub const GOVERNOR_PSI_IO_THRESHOLD_RELAXED: f64 = 50.0;
pub const GOVERNOR_PSI_CPU_THRESHOLD_RELAXED: f64 = 50.0;
pub static ONE_SHOT_MODE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub const REORDER_SOFT_TIMEOUT_MIN_MS: u64 = 50;
pub const REORDER_SOFT_TIMEOUT_MAX_MS: u64 = 500;
pub const REORDER_BUFFER_CAPACITY: usize = 100_000;
pub const REORDER_ZOMBIE_TIMEOUT_MS: u64 = 30_000;
pub const REORDER_PRESSURE_PANIC_PCT: f64 = 0.8;
pub const REORDER_PRESSURE_WARN_PCT: f64 = 0.5;
pub const REORDER_BUFFER_BYTES: u64 = 64 * 1024 * 1024;

// Mount monitoring & outage journal
pub const MOUNT_PROBE_FSYNC_TIMEOUT_SECS: u64 = 5;
pub const OUTAGE_JOURNAL_MAX_ENTRIES: usize = 100_000;
pub const OUTAGE_FULL_SCAN_THRESHOLD_SECS: u64 = 86400;
