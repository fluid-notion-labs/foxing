use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, fs, sync::{Arc, atomic::AtomicBool}, collections::HashSet};
use crate::error::{Result, FoxingError as MirrorError};
use regex::RegexSet;
use sysinfo::{System};
use nix::sys::statfs::statfs;

pub const MAX_FAILURE_BACKOFF: u64 = 600;
pub const ERROR_LIMITER_SECS: u64 = 60;
const BASE_AUTOTUNE_VDO_THRESHOLD: u32 = 128;

const TMPFS_MAGIC: i64 = 0x01021994;
const RAMFS_MAGIC: i64 = 0x858458f6;

fn d_bool_false() -> bool { false }
fn d_bool_true() -> bool { true }
fn d_zero_usize() -> usize { 0 }
fn d_zero_u64() -> u64 { 0 }
fn d_zero_f64() -> f64 { 0.0 }
fn d_mp() -> u16 { 9100 }
fn d_st() -> u64 { 30 }
fn def_abool() -> Arc<AtomicBool> { Arc::new(AtomicBool::new(true)) }
fn d_zero_u32() -> u32 { 0 }

fn d_journal_dir() -> PathBuf { PathBuf::new() }
fn d_journal_size_mb() -> u64 { 0 }
fn d_journal_retention() -> usize { 0 }

fn default_worker_count(sys: &System) -> usize {
    sys.cpus().len().max(2)
}
fn default_global_buffer_limit_mb(sys: &System) -> u64 {
    let total_mem = sys.total_memory() / 1024 / 1024;
    (total_mem as f64 * 0.70) as u64
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub enum TargetProfile {
    Auto,
    NVMe,
    SSD,
    HDD,
    NFS,
    Network,
}
fn d_profile() -> TargetProfile { TargetProfile::Auto }

pub fn autotune_buffer_chunk_size(profile: &TargetProfile, global_limit_mib: u64) -> u64 {
    let base_mib = if global_limit_mib > 16384 { 8 } else { 2 };
    match profile {
        TargetProfile::NVMe => base_mib * 2,
        TargetProfile::SSD => base_mib,
        TargetProfile::Network | TargetProfile::NFS => base_mib,
        TargetProfile::HDD | TargetProfile::Auto => base_mib.max(1),
    }
}
pub fn autotune_vdo_stall_threshold(profile: &TargetProfile) -> u32 {
    match profile {
        TargetProfile::NVMe => 1024,
        TargetProfile::SSD => 512,
        TargetProfile::HDD => 128,
        TargetProfile::Network | TargetProfile::NFS => 256,
        TargetProfile::Auto => BASE_AUTOTUNE_VDO_THRESHOLD,
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default="d_zero_usize")] pub worker_count: usize,
    #[serde(default="d_zero_usize")] pub queue_max: usize,
    #[serde(default="d_mp")] pub metrics_port: u16,
    #[serde(default="d_st")] pub shutdown_timeout_secs: u64,
    #[serde(default="d_zero_u64")] pub coalesce_max_bytes: u64,
    #[serde(default="d_zero_u64")] pub capacity_threshold_mb: u64,
    #[serde(default="d_zero_u64")] pub breaker_interval_secs: u64,
    #[serde(default="d_bool_false")] pub fatal_metrics_bind: bool,
    #[serde(default="d_zero_u64")] pub force_flush_interval_secs: u64,
    #[serde(default="d_zero_u64")] pub global_buffer_limit: u64,
    #[serde(default="d_bool_false")] pub quiesce_mode: bool,
    #[serde(default="d_zero_f64")] pub max_system_load_avg: f64,
    #[serde(default="d_zero_u64")] pub hydration_delay_ms: u64,
    #[serde(default="String::new")] pub io_priority: String,
    #[serde(default="d_zero_u64")] pub io_buffer_size_mib: u64,
    #[serde(default="d_zero_f64")] pub governor_psi_io_threshold: f64,
    #[serde(default="d_zero_f64")] pub governor_psi_cpu_threshold: f64,
    
    #[serde(default="d_journal_dir")] pub journal_dir: PathBuf,
    #[serde(default="d_journal_size_mb")] pub journal_size_limit_mb: u64,
    #[serde(default="d_journal_retention")] pub journal_retention_count: usize,
    
    #[serde(default="d_zero_usize", skip)] pub max_workers_sys: usize,
    #[serde(default)] pub sources: Vec<SourceConfig>
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    pub path: PathBuf,
    pub targets: Vec<TargetConfig>,
    #[serde(skip, default="def_abool")] pub rwf_uncached_ok: Arc<AtomicBool>,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TargetConfig {
    pub path: PathBuf,
    #[serde(default="d_profile")] pub profile: TargetProfile,
    #[serde(default="d_bool_true")] pub initial_sync: bool,
    #[serde(default="d_bool_false")] pub vdo_optimization: bool,
    #[serde(default="d_bool_false")] pub btrfs_compression: bool,
    #[serde(default="d_bool_false")] pub f2fs_compression: bool,
    #[serde(default="d_bool_false")] pub f2fs_pinning: bool,
    #[serde(default="d_bool_false")] pub enable_versioning: bool,
    #[serde(default="d_bool_true")] pub paranoid_deduplication: bool,
    #[serde(default="d_zero_usize")] pub max_versions: usize,
    #[serde(default="d_zero_u64")] pub max_versions_size_mb: u64,
    #[serde(default)] pub version_excludes: Vec<String>,
    #[serde(default)] pub version_includes: Vec<String>,
    #[serde(default="d_bool_false")] pub force_versioning: bool,
    #[serde(default)] pub force_version_includes: Vec<String>,
    pub force_retention_count: Option<usize>,
    #[serde(default="d_zero_usize")] pub worker_count: usize,
    #[serde(default="d_zero_usize")] pub queue_max: usize,
    #[serde(default="d_zero_usize")] pub batch_size: usize,
    #[serde(default="d_zero_u64")] pub io_buffer_size_mib: u64,
    #[serde(default="d_zero_u64")] pub autotune_target_latency_ms: u64,
    #[serde(default)] pub excludes: Vec<String>,
    #[serde(default)] pub includes: Vec<String>,
    #[serde(skip)] regex_ex: Option<RegexSet>,
    #[serde(skip)] regex_in: Option<RegexSet>,
    #[serde(skip)] regex_vex: Option<RegexSet>,
    #[serde(skip)] regex_vin: Option<RegexSet>,
    #[serde(skip)] regex_force_vin: Option<RegexSet>,
    #[serde(skip, default="def_abool")] pub supports_reflink: Arc<AtomicBool>,
    #[serde(skip, default="def_abool")] pub direct_io_ok: Arc<AtomicBool>,
    #[serde(skip, default="def_abool")] pub rwf_uncached_ok: Arc<AtomicBool>,
    #[serde(skip, default="def_abool")] pub xattr_supported: Arc<AtomicBool>,
    #[serde(default = "d_zero_u32")] pub vdo_stall_threshold: u32,
    #[serde(default="d_zero_u64")] pub ordering_max_pending_bytes: u64,
    #[serde(default="d_zero_usize")] pub ordering_scan_depth: usize,
    #[serde(default="d_zero_u64")] pub worker_hibernation_secs: u64,
    #[serde(default="d_zero_u64")] pub worker_flush_interval_ms: u64,
    #[serde(default="d_zero_u64")] pub worker_gap_recovery_secs: u64,
    #[serde(default="d_zero_usize")] pub worker_critical_drain_threshold: usize,
    #[serde(default="d_zero_u64")] pub worker_capacity_check_interval_ms: u64,
    #[serde(default="d_zero_u64")] pub worker_gap_recovery_batch: u64,
    #[serde(default="d_zero_u64")] pub worker_gap_recovery_max_backoff: u64,
    #[serde(default="d_zero_u64")] pub hydration_large_file_threshold_startup_mb: u64,
    #[serde(default="d_zero_u64")] pub hydration_large_file_threshold_drain_mb: u64,
}

impl TargetConfig {
    pub fn compile(&mut self, max_workers_sys: usize, global_mem_limit_mb: u64) -> Result<()> {
        if self.worker_count == 0 {
            self.worker_count = (max_workers_sys / 2).max(2);
        }
        if self.batch_size == 0 {
            self.batch_size = match self.profile {
                TargetProfile::NVMe => 256,
                TargetProfile::SSD => 128,
                TargetProfile::Network => 64,
                _ => 32,
            };
        }
        if self.queue_max == 0 {
            self.queue_max = match self.profile {
                TargetProfile::NVMe => 1_000_000,
                _ => 200_000,
            };
        }
        if self.autotune_target_latency_ms == 0 {
            self.autotune_target_latency_ms = match self.profile {
                TargetProfile::NVMe => 10,
                TargetProfile::SSD => 50,
                _ => 200,
            };
        }
        if self.ordering_max_pending_bytes == 0 {
            let share = (global_mem_limit_mb * 1024 * 1024) / 5;
            self.ordering_max_pending_bytes = share.max(128 * 1024 * 1024).min(2 * 1024 * 1024 * 1024);
        }
        if self.ordering_scan_depth == 0 {
            self.ordering_scan_depth = 5000;
        }
        if self.worker_flush_interval_ms == 0 {
            self.worker_flush_interval_ms = 10;
        }
        if self.worker_hibernation_secs == 0 {
            self.worker_hibernation_secs = 300;
        }
        if self.worker_gap_recovery_secs == 0 { self.worker_gap_recovery_secs = 5; }
        if self.worker_gap_recovery_batch == 0 { self.worker_gap_recovery_batch = 100; }
        if self.worker_gap_recovery_max_backoff == 0 { self.worker_gap_recovery_max_backoff = 30; }
        if self.worker_critical_drain_threshold == 0 { self.worker_critical_drain_threshold = 100; }
        if self.worker_capacity_check_interval_ms == 0 { self.worker_capacity_check_interval_ms = 1000; }
        if self.vdo_stall_threshold == 0 {
            self.vdo_stall_threshold = autotune_vdo_stall_threshold(&self.profile);
        }
        if self.hydration_large_file_threshold_startup_mb == 0 {
            self.hydration_large_file_threshold_startup_mb = match self.profile {
                TargetProfile::HDD => 16,
                _ => 128,
            };
        }
        if self.hydration_large_file_threshold_drain_mb == 0 {
            self.hydration_large_file_threshold_drain_mb = 5;
        }
        if self.max_versions == 0 { self.max_versions = 5; }
        if self.max_versions_size_mb == 0 { self.max_versions_size_mb = 10240; }
        if !self.excludes.is_empty() {
            self.regex_ex = Some(RegexSet::new(&self.excludes).map_err(|e| MirrorError::Config(format!("Invalid exclude regex: {}", e)))?);
        }
        if !self.includes.is_empty() {
            self.regex_in = Some(RegexSet::new(&self.includes).map_err(|e| MirrorError::Config(format!("Invalid include regex: {}", e)))?);
        }
        if !self.version_excludes.is_empty() {
            self.regex_vex = Some(RegexSet::new(&self.version_excludes).map_err(|e| MirrorError::Config(format!("Invalid version exclude regex: {}", e)))?);
        }
        if !self.version_includes.is_empty() {
            self.regex_vin = Some(RegexSet::new(&self.version_includes).map_err(|e| MirrorError::Config(format!("Invalid version include regex: {}", e)))?);
        }
        if !self.force_version_includes.is_empty() {
            self.regex_force_vin = Some(RegexSet::new(&self.force_version_includes).map_err(|e| MirrorError::Config(format!("Invalid force version include regex: {}", e)))?);
        }
        Ok(())
    }
    pub fn allow(&self, p: &std::path::Path) -> bool {
        let s = p.to_str().unwrap_or("");
        if let Some(r) = &self.regex_in {
            if !r.is_match(s) { return false; }
        }
        self.regex_ex.as_ref().map_or(true, |r| !r.is_match(s))
    }
    pub fn allow_versioning(&self, p: &std::path::Path) -> bool {
        let s = p.to_str().unwrap_or("");
        if let Some(r) = &self.regex_vin {
            if !r.is_match(s) { return false; }
        }
        self.regex_vex.as_ref().map_or(true, |r| !r.is_match(s))
    }
    pub fn is_forced_version(&self, p: &std::path::Path) -> bool {
        if self.force_versioning { return true; }
        let s = p.to_str().unwrap_or("");
        if let Some(r) = &self.regex_force_vin {
            return r.is_match(s);
        }
        false
    }
}

impl Config {
    pub fn calculate_defaults(mut self) -> Self {
        let mut sys = System::new_all();
        sys.refresh_memory();

        if self.worker_count == 0 {
            self.worker_count = default_worker_count(&sys);
            tracing::info!("Auto-Config: Worker Count set to {} (All Cores)", self.worker_count);
        }
        self.max_workers_sys = self.worker_count;
        if self.global_buffer_limit == 0 {
            self.global_buffer_limit = default_global_buffer_limit_mb(&sys);
            tracing::info!("Auto-Config: Global Buffer Limit set to {} MB (70% RAM)", self.global_buffer_limit);
        }
        if self.queue_max == 0 { self.queue_max = 500_000; }
        if self.max_system_load_avg == 0.0 {
            let cores = sys.cpus().len() as f64;
            self.max_system_load_avg = cores * 4.0;
        }
        if self.governor_psi_io_threshold == 0.0 { self.governor_psi_io_threshold = 60.0; }
        if self.governor_psi_cpu_threshold == 0.0 { self.governor_psi_cpu_threshold = 80.0; }
        if self.capacity_threshold_mb == 0 { self.capacity_threshold_mb = 500; }
        if self.breaker_interval_secs == 0 { self.breaker_interval_secs = 60; }
        if self.force_flush_interval_secs == 0 { self.force_flush_interval_secs = 5; }
        if self.hydration_delay_ms == 0 { self.hydration_delay_ms = 1; }
        if self.io_priority.is_empty() { self.io_priority = "Realtime".to_string(); }
        
        if self.journal_dir.as_os_str().is_empty() {
            self.journal_dir = Self::probe_ephemeral_storage();

            let is_rescue = Self::is_likely_rescue_env();

            let total_mem_mb = sys.total_memory() / 1024 / 1024;
            
            let target_total_mb = if is_rescue {
                let avail_mem_mb = sys.available_memory() / 1024 / 1024;
                tracing::info!("Config: Rescue Mode Detected! Scaling journal based on AVAILABLE memory ({} MB) instead of TOTAL.", avail_mem_mb);
                (avail_mem_mb as f64 * 0.01) as u64
            } else {
                (total_mem_mb as f64 * 0.05) as u64
            };
            
            let (min_cap, max_cap) = if is_rescue { (32, 512) } else { (64, 4096) };
            let effective_total_mb = target_total_mb.max(min_cap).min(max_cap);
            
            self.journal_retention_count = 10;
            self.journal_size_limit_mb = effective_total_mb / self.journal_retention_count as u64;
            
            if self.journal_size_limit_mb < 8 { self.journal_size_limit_mb = 8; }

            tracing::info!(
                "Config: Journal autotuned to Ephemeral (RAM/Tmp). Path: {:?}. Total Buffer: ~{} MB ({} segs x {} MB). RescueMode: {}", 
                self.journal_dir,
                self.journal_size_limit_mb * self.journal_retention_count as u64,
                self.journal_retention_count,
                self.journal_size_limit_mb,
                is_rescue
            );
        } else {
            if self.journal_size_limit_mb == 0 { self.journal_size_limit_mb = 100; }
            if self.journal_retention_count == 0 { self.journal_retention_count = 10; }
        }

        self.ensure_journal_safety();
        
        self
    }

    fn is_likely_rescue_env() -> bool {
        if Path::new("/run/ostree-booted").exists() {
            return false;
        }

        if let Ok(cmdline) = std::fs::read_to_string("/proc/cmdline") {
            let s = cmdline.to_lowercase();
            if s.contains("rd.live.image") || s.contains("boot=live") || s.contains("casper") || s.contains("archisobasedir") {
                return true;
            }
        }

        let root = Path::new("/");
        if let Ok(stat) = statfs(root) {
            let magic = stat.filesystem_type().0 as i64;
            if magic == TMPFS_MAGIC || magic == RAMFS_MAGIC {
                return true;
            }
        }
        false
    }

    fn probe_ephemeral_storage() -> PathBuf {
        let candidates = ["/dev/shm", "/run/shm", "/tmp", "/run"];
        for &path_str in &candidates {
            let path = Path::new(path_str);
            if !path.exists() { continue; }
            
            if let Ok(stat) = statfs(path) {
                let magic = stat.filesystem_type().0 as i64;
                if magic == TMPFS_MAGIC || magic == RAMFS_MAGIC {
                    let mut p = path.to_path_buf();
                    // Stable directory for reuse
                    p.push("foxing_ephemeral_data");
                    return p;
                }
            }
        }
        
        let mut temp = std::env::temp_dir();
        temp.push("foxing_ephemeral_data");
        tracing::warn!("Config: Could not find explicit RAM disk. Falling back to system temp: {:?}", temp);
        temp
    }

    fn ensure_journal_safety(&mut self) {
        if !self.journal_dir.exists() {
            let _ = std::fs::create_dir_all(&self.journal_dir);
        }
        
        let abs_journal = self.journal_dir.canonicalize().unwrap_or(self.journal_dir.clone());
        
        for source in &mut self.sources {
            let abs_source = source.path.canonicalize().unwrap_or(source.path.clone());
            
            if abs_journal.starts_with(&abs_source) {
                let rel_journal = if abs_journal == abs_source {
                    PathBuf::from("") 
                } else {
                    abs_journal.strip_prefix(&abs_source).unwrap_or(Path::new("")).to_path_buf()
                };
                
                let pattern_base = if rel_journal.as_os_str().is_empty() {
                    "source_.*\\.wal$".to_string()
                } else {
                    let s = rel_journal.to_string_lossy();
                    format!("^{}.*", regex::escape(&s))
                };
                
                for target in &mut source.targets {
                    if !target.excludes.iter().any(|e| e == &pattern_base) {
                        tracing::warn!("SAFETY: Journal {:?} is inside Source {:?}. Auto-excluding '{}' to prevent loops.", abs_journal, abs_source, pattern_base);
                        target.excludes.push(pattern_base.clone());
                    }
                }
            }
        }
    }

    pub fn load(p: &str) -> Result<Self> {
        let s = fs::read_to_string(p)?;
        let mut c: Config = toml::from_str(&s).map_err(|e| MirrorError::Config(e.to_string()))?;
        c = c.calculate_defaults();
        if c.shutdown_timeout_secs == 0 { return Err(MirrorError::Config("shutdown_timeout_secs must be > 0".into())); }
        let global_limit_mib = c.global_buffer_limit;
        let system_max_workers = c.max_workers_sys;
        let mut target_paths = HashSet::new();
        for s in &mut c.sources {
            let src_uncached_ok = crate::security::probe_rwf_uncached(&s.path);
            s.rwf_uncached_ok.store(src_uncached_ok, std::sync::atomic::Ordering::Relaxed);
            for t in &mut s.targets {
                t.compile(system_max_workers, global_limit_mib)?;
                if t.io_buffer_size_mib == 0 {
                    let tuned_mib = autotune_buffer_chunk_size(&t.profile, global_limit_mib);
                    t.io_buffer_size_mib = tuned_mib;
                    tracing::info!("Target {:?}: Autotuning buffer chunk size to {} MiB", t.path, tuned_mib);
                }
                let dio_ok = crate::security::probe_direct_io(&t.path);
                t.direct_io_ok.store(dio_ok, std::sync::atomic::Ordering::Relaxed);
                let tgt_uncached_ok = crate::security::probe_rwf_uncached(&t.path);
                t.rwf_uncached_ok.store(tgt_uncached_ok, std::sync::atomic::Ordering::Relaxed);
                let abs = t.path.canonicalize().map_err(|e| MirrorError::Config(format!("Invalid path {:?}: {}", t.path, e)))?;
                if !target_paths.insert(abs.clone()) {
                    return Err(MirrorError::Config(format!("Duplicate target path detected: {:?}.", abs)));
                }
            }
        }
        Ok(c)
    }
}
