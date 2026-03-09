// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/config.rs — Configuration — TargetConfig, SourceConfig, profiles

//! TOML configuration loading and validation for foxingd.
//! Defines target profiles (NVMe/SSD/HDD/NFS), worker settings, and feature flags.

use serde::{Deserialize, Serialize};
use std::path::{PathBuf, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use regex::Regex;
use crate::error::{Result, FoxingError};
use tracing::{info, warn, debug, error};
use fxcp_core::security;
use sysinfo::{System, RefreshKind, CpuRefreshKind, MemoryRefreshKind};
use lazy_static::lazy_static;
use fxcp_core::constants;
use clap::ValueEnum;

#[derive(Debug)]
pub struct SysSpecs {
    pub logical_cores: usize,
    pub total_memory_mb: u64,
    pub available_memory_mb: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemClass {
    Constrained,
    Standard,
    Server,
}

impl SysSpecs {
    fn detect() -> Self {
        let mut s = System::new_with_specifics(
            RefreshKind::nothing()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything())
        );
        s.refresh_memory();
        Self {
            logical_cores: s.cpus().len().max(1),
            total_memory_mb: s.total_memory() / 1024 / 1024,
            available_memory_mb: s.available_memory() / 1024 / 1024,
        }
    }
    pub fn system_class(&self) -> SystemClass {
        if self.total_memory_mb < 2048 {
            SystemClass::Constrained
        } else if self.total_memory_mb < 16384 {
            SystemClass::Standard
        } else {
            SystemClass::Server
        }
    }
}

lazy_static! {
    pub static ref SYS: SysSpecs = SysSpecs::detect();
}

fn default_worker_count() -> usize {
    SYS.logical_cores.max(4).min(constants::MAX_WORKER_CORES)
}
fn default_queue_max() -> usize {
    constants::DEFAULT_QUEUE_MAX
}
fn default_global_buffer_limit() -> u64 {
    let total_mb = SYS.total_memory_mb;
    let available_mb = SYS.available_memory_mb;
    if total_mb < 2048 {
        warn!("Running in Constrained Memory Mode (Total RAM: {} MB). Adjusting buffer limits aggressively.", total_mb);
    }
    let heuristic = (available_mb as f64 * 0.5) as u64;
    heuristic.max(128)
}
fn default_metrics_port() -> u16 { 9100 }
fn default_max_load_avg() -> f64 { (SYS.logical_cores as f64 * 2.5).max(4.0) }
fn default_hydration_delay() -> u64 { 10 }
fn default_psi_io() -> f64 { 10.0 }
fn default_psi_cpu() -> f64 { 10.0 }
fn default_capacity_check_interval() -> u64 { 5000 }
fn default_capacity_threshold() -> u64 { constants::CAPACITY_THRESHOLD_MB }
fn default_io_buffer_size() -> u64 { constants::DEFAULT_IO_BUFFER_SIZE_MIB }
fn default_enable_hashing() -> bool { true }
fn default_hash_threshold() -> u64 { 128 }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
pub enum TargetProfile {
    NVMe,
    SSD,
    HDD,
    Network,
    NFS,
    SdCard,
    Auto,
}

impl TargetProfile {
    /// Lower = faster = primary candidate for tiered cloning.
    /// When multiple targets exist, the fastest target computes Merkle/signatures
    /// first and caches them for reuse by slower targets.
    pub fn tier_priority(&self) -> u8 {
        match self {
            Self::NVMe => 0,
            Self::SSD => 1,
            Self::HDD => 2,
            Self::SdCard => 3,
            Self::Network => 4,
            Self::NFS => 5,
            Self::Auto => 2,
        }
    }
}

pub fn get_flush_multiplier_bounds(profile: &TargetProfile) -> (u32, u32) {
    match profile {
        TargetProfile::NVMe => (1, 2),
        TargetProfile::SSD => (2, 4),
        TargetProfile::HDD => (4, 8),
        TargetProfile::Network | TargetProfile::NFS => (8, 16),
        TargetProfile::SdCard => (5, 10),
        TargetProfile::Auto => (2, 8),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_worker_count")]
    pub worker_count: usize,
    #[serde(default = "default_queue_max")]
    pub queue_max: usize,
    #[serde(default = "default_global_buffer_limit")]
    pub global_buffer_limit: u64,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    #[serde(default = "default_max_load_avg")]
    pub max_system_load_avg: f64,
    #[serde(default = "default_hydration_delay")]
    pub hydration_delay_ms: u64,
    #[serde(default = "default_psi_io")]
    pub governor_psi_io_threshold: f64,
    #[serde(default = "default_psi_cpu")]
    pub governor_psi_cpu_threshold: f64,
    #[serde(default = "default_capacity_check_interval")]
    pub worker_capacity_check_interval_ms: u64,
    #[serde(default = "default_capacity_threshold")]
    pub capacity_threshold_mb: u64,
    #[serde(default = "default_io_buffer_size")]
    pub io_buffer_size_mib: u64,
    #[serde(default = "default_enable_hashing")]
    pub enable_content_hashing: bool,
    #[serde(default = "default_hash_threshold")]
    pub hash_lite_threshold_kb: u64,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            worker_count: default_worker_count(),
            queue_max: default_queue_max(),
            global_buffer_limit: default_global_buffer_limit(),
            metrics_port: default_metrics_port(),
            max_system_load_avg: default_max_load_avg(),
            hydration_delay_ms: default_hydration_delay(),
            governor_psi_io_threshold: default_psi_io(),
            governor_psi_cpu_threshold: default_psi_cpu(),
            worker_capacity_check_interval_ms: default_capacity_check_interval(),
            capacity_threshold_mb: default_capacity_threshold(),
            io_buffer_size_mib: default_io_buffer_size(),
            enable_content_hashing: default_enable_hashing(),
            hash_lite_threshold_kb: default_hash_threshold(),
            sources: Vec::new(),
        }
    }
}

impl Config {
    fn validate(&mut self) -> Result<()> {
        let global_limit = self.global_buffer_limit;
        let total_workers = self.worker_count;
        let io_buffer_mib = self.io_buffer_size_mib;
        
        let required_mem_mib = (total_workers as u64) * io_buffer_mib;
        if required_mem_mib > global_limit {
            let recommended_mib = global_limit / total_workers as u64;
            self.io_buffer_size_mib = recommended_mib.max(1);
            warn!(
                "Config Validation Warning: Total IO Buffer size ({} MB) required by {} workers ({} MB each) exceeds global_buffer_limit ({} MB).",
                required_mem_mib, total_workers, io_buffer_mib, global_limit
            );
            warn!("Automatically reducing io_buffer_size_mib to {} MB.", self.io_buffer_size_mib);
        }

        let max_events_by_mem = (global_limit * 1024) / constants::EVENT_QUEUE_OVERHEAD_BYTES;
        if (self.queue_max as u64) > max_events_by_mem {
            self.queue_max = max_events_by_mem.min(constants::DEFAULT_QUEUE_MAX as u64) as usize;
            warn!(
                "Config Validation Warning: Configured queue_max is too high for global_buffer_limit. Clamping to {} events based on memory budget.",
                self.queue_max
            );
        }

        let system_available_mb = SYS.available_memory_mb;
        let minimum_required_available = (global_limit as f64 * 1.2) as u64;
        
        if system_available_mb < minimum_required_available {
            error!(
                "Config Validation Failure: System resource warning! Available physical memory ({} MB) is less than the required safe buffer target ({} MB, 1.2x global limit).", 
                system_available_mb, minimum_required_available
            );
            return Err(FoxingError::Config(format!(
                "System OOM risk detected: Available RAM ({} MB) < Safe Buffer Target ({} MB). Reduce global_buffer_limit.", 
                system_available_mb, minimum_required_available
            )));
        }

        Ok(())
    }

    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(FoxingError::Io)?;
        let mut config: Config = toml::from_str(&content)
            .map_err(|e| FoxingError::Config(format!("Failed to parse TOML: {}", e)))?;
        
        config.validate()?;

        let physical_ram_mb = SYS.total_memory_mb;
        let absolute_max_mb = (physical_ram_mb as f64 * 0.7) as u64;
        
        if config.global_buffer_limit > absolute_max_mb {
            warn!(
                "Config Warning: global_buffer_limit ({} MB) exceeds recommended safe limit (70% RAM: {} MB). \
                Daemon will attempt startup but may encounter memory pressure. Auto-clamping to this limit.", 
                config.global_buffer_limit, absolute_max_mb
            );
            config.global_buffer_limit = absolute_max_mb;
            warn!("Auto-clamped global_buffer_limit to {} MB.", config.global_buffer_limit);
        }

        if config.capacity_threshold_mb != constants::CAPACITY_THRESHOLD_MB {
            debug!("Using custom capacity_threshold_mb: {} (Default: {} MB)", config.capacity_threshold_mb, constants::CAPACITY_THRESHOLD_MB);
        }

        fxcp_core::hashing::set_hashing_enabled(config.enable_content_hashing);
        fxcp_core::hashing::set_lite_threshold_kb(config.hash_lite_threshold_kb);

        if !config.enable_content_hashing {
            warn!("Content hashing DISABLED. Falling back to mtime-based verification. Risk of silent data corruption on timestamp clamping filesystems.");
        } else {
            info!("Content hashing ENABLED. Lite hash threshold: {} KB", config.hash_lite_threshold_kb);
        }

        for sc in config.sources.iter_mut() {
            sc.rwf_uncached_ok.store(security::probe_rwf_uncached(&sc.path), Ordering::Relaxed);
            for tc in sc.targets.iter_mut() {
                if let Ok(rel_target) = tc.path.strip_prefix(&sc.path) {
                    let rel_str = rel_target.to_string_lossy().to_string();
                    if !rel_str.is_empty() {
                        let already_excluded = tc.exclude.contains(&rel_str);
                        if !already_excluded {
                            info!("Config: Detected Target {:?} is inside Source {:?}. Auto-adding exclusion.", tc.path, sc.path);
                            tc.exclude.push(rel_str);
                        }
                    }
                }
                tc.rwf_uncached_ok.store(security::probe_rwf_uncached(&tc.path), Ordering::Relaxed);
                tc.direct_io_ok.store(security::probe_direct_io(&tc.path), Ordering::Relaxed);
                tc.compile(config.worker_count, config.io_buffer_size_mib)?;
            }
        }

        info!("Configuration loaded successfully.");
        Ok(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub path: PathBuf,
    #[serde(default)]
    pub targets: Vec<TargetConfig>,
    #[serde(skip, default)]
    pub rwf_uncached_ok: Arc<AtomicBool>,
    #[serde(default)]
    pub cross_subvolumes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    pub path: PathBuf,
    pub profile: TargetProfile,
    #[serde(default = "TargetConfig::default_autotune_latency")]
    pub autotune_target_latency_ms: u64,
    #[serde(default)]
    pub target_bandwidth_mbps: Option<u64>,
    #[serde(default)]
    pub target_iops: Option<u64>,
    #[serde(default)]
    pub initial_sync: bool,
    #[serde(skip, default)]
    pub supports_reflink: Arc<AtomicBool>,
    #[serde(default)]
    pub vdo_optimization: bool,
    #[serde(default = "TargetConfig::default_vdo_stall_threshold")]
    pub vdo_stall_threshold: u32,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub enable_versioning: bool,
    #[serde(default = "TargetConfig::default_max_versions")]
    pub max_versions: usize,
    #[serde(default = "TargetConfig::default_max_versions_size_mb")]
    pub max_versions_size_mb: u64,
    #[serde(default)]
    pub force_retention_files: Vec<String>,
    #[serde(default = "TargetConfig::default_force_retention_count")]
    pub force_retention_count: usize,
    #[serde(default = "TargetConfig::default_worker_count")]
    pub worker_count: usize,
    #[serde(default = "TargetConfig::default_batch_size")]
    pub batch_size: usize,
    
    // CHANGED: MS -> US
    #[serde(default = "TargetConfig::default_flush_interval")]
    pub worker_flush_interval_us: u64,
    
    #[serde(default = "TargetConfig::default_io_buffer_size_mib")]
    pub io_buffer_size_mib: u64,
    #[serde(default = "TargetConfig::default_ordering_scan_depth")]
    pub ordering_scan_depth: usize,
    #[serde(default = "TargetConfig::default_hibernation_secs")]
    pub worker_hibernation_secs: u64,
    #[serde(default = "TargetConfig::default_retry_initial_ms")]
    pub worker_retry_initial_ms: u64,
    #[serde(default = "TargetConfig::default_retry_max_ms")]
    pub worker_retry_max_ms: u64,
    #[serde(default = "TargetConfig::default_atomic_writes")]
    pub atomic_writes: bool,
    #[serde(default = "TargetConfig::default_source_uncached")]
    pub source_uncached: bool,
    #[serde(default = "TargetConfig::default_target_uncached")]
    pub target_uncached: bool,
    /// Override adaptive segment stall timeout (seconds). None = use adaptive value.
    #[serde(default)]
    pub segment_stall_timeout_override: Option<u64>,
    /// Override adaptive segment overall timeout (seconds). None = use adaptive value.
    #[serde(default)]
    pub segment_overall_timeout_override: Option<u64>,
    /// Override adaptive post-copy timeout (seconds). None = use adaptive value.
    #[serde(default)]
    pub postcopy_timeout_override: Option<u64>,
    #[serde(skip, default)]
    pub xattr_supported: Arc<AtomicBool>,
    #[serde(skip, default)]
    pub direct_io_ok: Arc<AtomicBool>,
    #[serde(skip, default)]
    pub rwf_uncached_ok: Arc<AtomicBool>,
    #[serde(skip, default)]
    pub rwf_atomic_ok: Arc<AtomicBool>,
    #[serde(skip)]
    pub include_regexes: Vec<Regex>,
    #[serde(skip)]
    pub exclude_regexes: Vec<Regex>,
    #[serde(skip)]
    pub force_retention_regexes: Vec<Regex>,
    #[serde(skip, default)]
    pub label: Arc<str>,
    #[serde(skip, default)]
    pub paused: Arc<AtomicBool>,
    #[serde(skip, default)]
    pub outage_journal: Arc<dashmap::DashSet<PathBuf>>,
    #[serde(skip)]
    pub tombstone_journal: Option<Arc<fxcp_core::tombstone::TombstoneJournal>>,
}

impl Default for TargetConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            profile: TargetProfile::Auto,
            autotune_target_latency_ms: TargetConfig::default_autotune_latency(),
            target_bandwidth_mbps: None,
            target_iops: None,
            initial_sync: false,
            supports_reflink: Arc::new(AtomicBool::new(false)),
            vdo_optimization: false,
            vdo_stall_threshold: TargetConfig::default_vdo_stall_threshold(),
            include: vec![],
            exclude: vec![],
            enable_versioning: false,
            max_versions: TargetConfig::default_max_versions(),
            max_versions_size_mb: TargetConfig::default_max_versions_size_mb(),
            force_retention_files: vec![],
            force_retention_count: TargetConfig::default_force_retention_count(),
            worker_count: TargetConfig::default_worker_count(),
            batch_size: TargetConfig::default_batch_size(),
            worker_flush_interval_us: TargetConfig::default_flush_interval(),
            io_buffer_size_mib: TargetConfig::default_io_buffer_size_mib(),
            ordering_scan_depth: TargetConfig::default_ordering_scan_depth(),
            worker_hibernation_secs: TargetConfig::default_hibernation_secs(),
            worker_retry_initial_ms: TargetConfig::default_retry_initial_ms(),
            worker_retry_max_ms: TargetConfig::default_retry_max_ms(),
            atomic_writes: TargetConfig::default_atomic_writes(),
            source_uncached: TargetConfig::default_source_uncached(),
            target_uncached: TargetConfig::default_target_uncached(),
            segment_stall_timeout_override: None,
            segment_overall_timeout_override: None,
            postcopy_timeout_override: None,
            xattr_supported: Arc::new(AtomicBool::new(false)),
            direct_io_ok: Arc::new(AtomicBool::new(false)),
            rwf_uncached_ok: Arc::new(AtomicBool::new(false)),
            rwf_atomic_ok: Arc::new(AtomicBool::new(false)),
            include_regexes: vec![],
            exclude_regexes: vec![],
            force_retention_regexes: vec![],
            label: "".into(),
            paused: Arc::new(AtomicBool::new(false)),
            outage_journal: Arc::new(dashmap::DashSet::new()),
            tombstone_journal: None,
        }
    }
}

impl TargetConfig {
    fn default_worker_count() -> usize { 4 }
    fn default_batch_size() -> usize { 64 }
    
    // CHANGED: Default is now 100,000us (100ms)
    fn default_flush_interval() -> u64 { 100_000 }
    
    fn default_io_buffer_size_mib() -> u64 { 2 }
    fn default_ordering_scan_depth() -> usize { 16 }
    fn default_hibernation_secs() -> u64 { constants::DEFAULT_HIBERNATION_SECS }
    fn default_retry_initial_ms() -> u64 { 5 }
    fn default_retry_max_ms() -> u64 { 1000 }
    fn default_vdo_stall_threshold() -> u32 { 1000 }
    fn default_autotune_latency() -> u64 { 50 }
    fn default_max_versions() -> usize { 24 }
    fn default_max_versions_size_mb() -> u64 { 1024 }
    fn default_force_retention_count() -> usize { 100 }
    fn default_atomic_writes() -> bool { false }
    fn default_source_uncached() -> bool { false }
    fn default_target_uncached() -> bool { false }

    fn compile_filters(patterns: &[String]) -> Result<Vec<Regex>> {
        patterns.iter().map(|p| {
            let re_str = regex::escape(p).replace("\\*", ".*").replace("\\?", ".");
            Regex::new(&re_str).map_err(|e| FoxingError::Config(format!("Invalid filter regex: {}: {}", p, e)))
        }).collect()
    }

    pub fn compile(&mut self, default_workers: usize, default_io_buffer_mib: u64) -> Result<()> {
        self.worker_count = self.worker_count.min(default_workers).max(1);
        self.io_buffer_size_mib = self.io_buffer_size_mib.min(default_io_buffer_mib).max(1);
        self.label = self.path.to_string_lossy().into();

        let internal_excludes = vec![
            ".foxing_reflink_probe*".to_string(),
            ".foxing_latency_probe*".to_string(),
            ".foxing_mount_epoch".to_string(),
            ".foxing_mount_probe".to_string(),
            "*.tmp.*".to_string(),
            "*.swap_tmp".to_string(),
        ];
        
        for p in internal_excludes {
            if !self.exclude.iter().any(|e| e == &p) {
                self.exclude.push(p);
            }
        }

        self.include_regexes = Self::compile_filters(&self.include)?;
        self.exclude_regexes = Self::compile_filters(&self.exclude)?;
        self.force_retention_regexes = Self::compile_filters(&self.force_retention_files)?;

        // Ensure microsecond alignment if defaults were used
        if self.worker_flush_interval_us == TargetConfig::default_flush_interval() {
            let (min, _) = get_flush_multiplier_bounds(&self.profile);
            // 50ms * multiplier -> microseconds
            self.worker_flush_interval_us = (min as u64) * 50_000;
        }

        // Initialize tombstone journal on the target
        let tombstone_path = self.path.join(".foxing_tombstones.jsonl");
        match fxcp_core::tombstone::TombstoneJournal::open(&tombstone_path) {
            Ok(journal) => {
                let count = journal.len();
                if count > 0 {
                    tracing::info!("Tombstone journal: {} pending entries at {:?}", count, tombstone_path);
                }
                self.tombstone_journal = Some(std::sync::Arc::new(journal));
            }
            Err(e) => {
                tracing::warn!("Failed to open tombstone journal {:?}: {} — deletions will not be persisted", tombstone_path, e);
            }
        }

        // Add tombstone journal to internal exclude patterns
        if !self.exclude.iter().any(|e| e.contains("foxing_tombstones")) {
            self.exclude.push(".foxing_tombstones*".to_string());
        }

        Ok(())
    }

    pub fn is_path_excluded(&self, rel_path: &Path) -> bool {
        let path_str = rel_path.to_string_lossy();
        for re in &self.exclude_regexes {
            if re.is_match(&path_str) {
                return true;
            }
        }
        if !self.include_regexes.is_empty() {
            for re in &self.include_regexes {
                if re.is_match(&path_str) {
                    return false;
                }
            }
            return true;
        }
        false
    }
}
