// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/api.rs — HTTP API handlers for metrics and status endpoints

//! Axum HTTP handlers for the Prometheus metrics and JSON status API.

use std::collections::HashMap;
use crate::tuner::TunerState;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SystemStatus {
    pub load_avg_1m: f64,
    pub governor_stressed: bool,
    pub global_events_dropped: u64,
    pub live_additions: u64,
    pub targets: HashMap<String, TargetStatus>,
    pub debug: DebugStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TargetStatus {
    pub latency_ms: f64,
    pub pending_events: usize,
    pub tuner_state: TunerState,
    pub batch_size: usize,
    pub coalesce_window_kb: u64,
    pub buffer_utilization: f64,
    pub ops_reflink: u64,
    pub ops_offload: u64,
    pub ops_standard: u64,
    // Task 1: Bandwidth/Latency History (Bandwidth MB/s, Latency ms)
    #[serde(default)]
    pub history: Vec<(f64, f64)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DebugStatus {
    pub bpf_events_malformed: u64,
    pub bpf_events_unwatched: u64,
    pub worker_shutdown_timeouts: u64,
    pub sidecars_created: u64,
    pub generation_mismatches: u64,
    pub bpf_device_stats: HashMap<String, BpfDeviceStat>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BpfDeviceStat {
    pub sequence: u64,
    pub event_count: u64,
}

impl Default for SystemStatus {
    fn default() -> Self {
        Self {
            load_avg_1m: 0.0,
            governor_stressed: false,
            global_events_dropped: 0,
            live_additions: 0,
            targets: HashMap::new(),
            debug: DebugStatus {
                bpf_events_malformed: 0,
                bpf_events_unwatched: 0,
                worker_shutdown_timeouts: 0,
                sidecars_created: 0,
                generation_mismatches: 0,
                bpf_device_stats: HashMap::new(),
            }
        }
    }
}
