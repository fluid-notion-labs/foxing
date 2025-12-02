use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::tuner::TunerState;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SystemStatus {
    pub load_avg_1m: f64,
    pub governor_stressed: bool,
    pub global_events_dropped: u64,
    pub live_additions: u64,
    pub targets: HashMap<String, TargetStatus>,
    pub debug: DebugStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TargetStatus {
    pub tuner_state: TunerState,
    pub throughput_mb: f64,
    pub latency_ms: f64,
    pub buffer_utilization: f64,
    pub ops_reflink: u64,
    pub ops_offload: u64,
    pub ops_standard: u64,
    pub pending_events: u64,
    pub wal_coherence_failures: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DebugStatus {
    pub bpf_sequence_gaps: u64,
    pub bpf_events_malformed: u64,
    pub bpf_events_unwatched: u64,
    pub bpf_device_stats: HashMap<String, BpfDeviceStat>,
    pub worker_shutdown_timeouts: u64,
    pub sidecars_created: u64,
    pub generation_mismatches: u64,
    pub memory_usage_mb: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BpfDeviceStat {
    pub dev_id_raw: u32,
    pub last_sequence: u64,
    pub total_events: u64,
}

impl Default for SystemStatus {
    fn default() -> Self {
        Self {
            load_avg_1m: 0.0,
            governor_stressed: false,
            global_events_dropped: 0,
            live_additions: 0,
            targets: HashMap::new(),
            debug: DebugStatus::default(),
        }
    }
}
