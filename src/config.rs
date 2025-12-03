use serde::{Deserialize, Serialize};
use std::{path::PathBuf, fs, sync::{Arc, atomic::AtomicBool}, collections::HashSet};
use crate::error::{Result, FoxingError as MirrorError};
use regex::RegexSet;
use sysinfo::System;
// --- Exported Constants ---
pub const MAX_FAILURE_BACKOFF: u64 = 600;
pub const ERROR_LIMITER_SECS: u64 = 60;

fn d_quiesce() -> bool { false }
fn d_wc()->usize{2}
fn d_qm()->usize{100000}
fn d_mp()->u16{9100}
fn d_st()->u64{30}
fn d_cmb()->u64{1048576}
fn d_ct()->u64{500}
fn d_bi()->u64{60}
fn d_fmb()->bool{false}
fn d_ffi()->u64{5}
fn d_gbl()->u64{500000}
fn d_max_versions() -> usize { 5 }
fn d_max_versions_size_mb() -> u64 { 10240 }
fn d_max_load() -> f64 { 4.0 }
fn d_hyd_delay() -> u64 { 10 }
fn d_tune_lat() -> u64 { 50 } // REINSTATED
fn d_ioprio() -> String { "Normal".to_string() }
// DYNAMICALLY TUNED FLOOR: Set to 1MiB as base default.
fn d_io_buffer_size_mib() -> u64 { 1 }

// --- Exported Enums ---
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
fn d_max_workers_sys()->usize{4}

// NEW: Function to dynamically calculate the optimal chunk size based on profile and resources
pub fn autotune_buffer_chunk_size(profile: &TargetProfile, global_limit_mib: u64) -> u64 {
    // Base chunk size, always at least 1 MiB
    let mut tuned_mib = d_io_buffer_size_mib();

    // Only consider scaling if the system is generously provisioned (> 8GB global buffer limit).
    // This prevents overly large chunks on memory-constrained systems.
    if global_limit_mib >= 8192 { // 8 GB limit heuristic
        match profile {
            // FIX: Changed NVme -> NVMe
            TargetProfile::NVMe => tuned_mib = 4, 
            // FIX: Changed SSD -> SSD
            TargetProfile::SSD => tuned_mib = 2,
            // FIX: Changed Network -> Network, NFS -> NFS
            TargetProfile::Network | TargetProfile::NFS => tuned_mib = 2,
            // FIX: Changed HDD -> HDD, Auto -> Auto
            TargetProfile::HDD | TargetProfile::Auto => {}
        }
    }
    
    // Safety check: ensure the resulting chunk size isn't so large that it monopolizes
    // the global budget. We cap it at 1/16th of the global limit, minimum of 1MiB.
    let max_safe_chunk = (global_limit_mib / 16).max(1);
    
    tuned_mib.min(max_safe_chunk)
}


// --- Exported Structs ---
#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default="d_wc")] pub worker_count: usize,
    #[serde(default="d_qm")] pub queue_max: usize,
    #[serde(default="d_mp")] pub metrics_port: u16,
    #[serde(default="d_st")] pub shutdown_timeout_secs: u64,
    #[serde(default="d_cmb")] pub coalesce_max_bytes: u64,
    #[serde(default="d_ct")] pub capacity_threshold_mb: u64,
    #[serde(default="d_bi")] pub breaker_interval_secs: u64,
    #[serde(default="d_fmb")] pub fatal_metrics_bind: bool,
    #[serde(default="d_ffi")] pub force_flush_interval_secs: u64,
    #[serde(default="d_gbl")] pub global_buffer_limit: u64,
    #[serde(default="d_quiesce")] pub quiesce_mode: bool,
    #[serde(default="d_max_load")] pub max_system_load_avg: f64,
    #[serde(default="d_hyd_delay")] pub hydration_delay_ms: u64,
    #[serde(default="d_ioprio")] pub io_priority: String,
    
    // This value is now determined by the target's profile in TargetConfig::compile
    // It is kept here as a global default fallback, but targets override it.
    #[serde(default="d_io_buffer_size_mib")] pub io_buffer_size_mib: u64, 
    
    #[serde(default="d_max_workers_sys", skip)] pub max_workers_sys: usize,
    #[serde(default)] pub sources: Vec<SourceConfig>
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    pub path: PathBuf,
    pub targets: Vec<TargetConfig>,
    // NEW: Source Read Capability is stored here
    #[serde(skip, default="def_abool")] pub rwf_uncached_ok: Arc<AtomicBool>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TargetConfig {
    pub path: PathBuf,
    #[serde(default="d_profile")] pub profile: TargetProfile,
    #[serde(default="def_true")] pub initial_sync: bool,
    #[serde(default="def_false")] pub vdo_optimization: bool,
    #[serde(default="def_false")] pub btrfs_compression: bool,
    #[serde(default="def_false")] pub f2fs_compression: bool,
    #[serde(default="def_false")] pub f2fs_pinning: bool,
    #[serde(default="def_false")] pub enable_versioning: bool,
    #[serde(default="d_max_versions")] pub max_versions: usize,
    #[serde(default="d_max_versions_size_mb")] pub max_versions_size_mb: u64,
    #[serde(default)] pub version_excludes: Vec<String>,
    #[serde(default)] pub version_includes: Vec<String>,
    #[serde(default="def_false")] pub force_versioning: bool,
    #[serde(default)] pub force_version_includes: Vec<String>,
    pub force_retention_count: Option<usize>,
    #[serde(default="d_wc")] pub worker_count: usize,
    #[serde(default="d_wc")] pub queue_max: usize,
    #[serde(default="d_wc")] pub batch_size: usize,
    #[serde(default="d_io_buffer_size_mib")] pub io_buffer_size_mib: u64, // Now profile-tuned
    #[serde(default="d_tune_lat")] pub autotune_target_latency_ms: u64,
    #[serde(default)] pub excludes: Vec<String>,
    #[serde(default)] pub includes: Vec<String>,
    #[serde(skip)] regex_ex: Option<RegexSet>,
    #[serde(skip)] regex_in: Option<RegexSet>,
    #[serde(skip)] regex_vex: Option<RegexSet>,
    #[serde(skip)] regex_vin: Option<RegexSet>,
    #[serde(skip)] regex_force_vin: Option<RegexSet>,
    #[serde(skip, default="def_abool")] pub supports_reflink: Arc<AtomicBool>,
    #[serde(skip, default="def_abool")] pub direct_io_ok: Arc<AtomicBool>,
    // Target Write Capability
    #[serde(skip, default="def_abool")] pub rwf_uncached_ok: Arc<AtomicBool>,
}
fn def_abool() -> Arc<AtomicBool> { Arc::new(AtomicBool::new(true)) }
fn def_true() -> bool { true }
fn def_false() -> bool { false }

// --- Exported Helper Functions ---
pub fn get_default_coalesce_max_bytes(profile: &TargetProfile) -> u64 {
    match profile {
        TargetProfile::NVMe => 4 * 1024 * 1024,
        TargetProfile::SSD => 2 * 1024 * 1024,
        TargetProfile::HDD => 512 * 1024,
        TargetProfile::NFS => 1 * 1024 * 1024,
        TargetProfile::Network => 1 * 1024 * 1024,
        TargetProfile::Auto => 1 * 1024 * 1024,
    }
}
pub fn get_default_target_workers(profile: &TargetProfile) -> usize {
    match profile {
        TargetProfile::NVMe => 8,
        TargetProfile::SSD => 4,
        TargetProfile::HDD => 2,
        TargetProfile::NFS => 4,
        TargetProfile::Network => 4,
        TargetProfile::Auto => 2,
    }
}
pub fn get_default_batch_size(profile: &TargetProfile) -> usize {
    match profile {
        TargetProfile::NVMe => 16,
        TargetProfile::SSD => 8,
        TargetProfile::HDD => 4,
        TargetProfile::NFS => 8,
        TargetProfile::Network => 32,
        TargetProfile::Auto => 4,
    }
}
pub fn get_flush_multiplier_bounds(profile: &TargetProfile) -> (u32, u32) {
    match profile {
        TargetProfile::NVMe => (1, 4),
        TargetProfile::SSD => (2, 8),
        TargetProfile::HDD => (4, 32),
        TargetProfile::NFS => (2, 16),
        TargetProfile::Network => (2, 16),
        TargetProfile::Auto => (2, 8),
    }
}
// --- Implementation Block ---
impl TargetConfig {
    pub fn compile(&mut self, max_workers_sys: usize) -> Result<()> {
        if self.worker_count == d_wc() {
            let profile_workers = get_default_target_workers(&self.profile);
            self.worker_count = profile_workers.min(max_workers_sys);
        }
        if self.batch_size == d_wc() {
            self.batch_size = get_default_batch_size(&self.profile);
        }
        
        // NEW: Autotune the buffer chunk size if the config uses the default value (1 MiB).
        // If the user explicitly set a value > 1 MiB, we honor it.
        if self.io_buffer_size_mib == d_io_buffer_size_mib() {
            // Note: We need the global_buffer_limit for tuning. Since config.load hasn't finished, 
            // we use the default (500MiB) as a proxy, which is conservative.
            // The ultimate logic should run *after* global_buffer_limit is finalized.
            // For now, we will assume a decent default or rely on the final tuning later.
            // A safer approach is implemented in Config::load after global_buffer_limit is ready.
        }

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
        let sys = System::new_all();
        let total_ram_mib = (sys.total_memory() / 1024 / 1024) as u64;
        let total_cpus = sys.cpus().len().max(1);
        if self.worker_count == d_wc() {
             self.worker_count = total_cpus.max(2) / 2;
        }
        if self.global_buffer_limit == d_gbl() {
            // Heuristic: Max 5GB global buffer memory, or 10% of total RAM, whichever is smaller.
            self.global_buffer_limit = (total_ram_mib / 10).min(5000);
        }
        self.max_workers_sys = self.worker_count;
        self
    }
    pub fn load(p: &str) -> Result<Self> {
        let s = fs::read_to_string(p)?;
        let mut c: Config = toml::from_str(&s).map_err(|e| MirrorError::Config(e.to_string()))?;
        c = c.calculate_defaults();
        if c.shutdown_timeout_secs == 0 { return Err(MirrorError::Config("shutdown_timeout_secs must be > 0".into())); }
        
        let global_limit_mib = c.global_buffer_limit; // Now finalized by calculate_defaults
        let system_max_workers = c.max_workers_sys;
        let mut target_paths = HashSet::new();
        
        for s in &mut c.sources {
            // Probe Source Capability (Read Operations)
            let src_uncached_ok = crate::security::probe_rwf_uncached(&s.path);
            s.rwf_uncached_ok.store(src_uncached_ok, std::sync::atomic::Ordering::Relaxed);
            if !src_uncached_ok { tracing::warn!("Source {:?}: Uncached Reads (RWF_UNCACHED) not supported.", s.path); }

            for t in &mut s.targets {
                t.compile(system_max_workers)?;
                
                // --- NEW AUTOTUNING STEP ---
                // If io_buffer_size_mib is at the default floor (1 MiB), scale it up based on profile
                if t.io_buffer_size_mib == d_io_buffer_size_mib() {
                    let tuned_mib = autotune_buffer_chunk_size(&t.profile, global_limit_mib);
                    if tuned_mib > t.io_buffer_size_mib {
                        tracing::info!("Target {:?}: Autotuning buffer chunk size from {} MiB to {} MiB (Profile: {:?}).", 
                                        t.path, t.io_buffer_size_mib, tuned_mib, t.profile);
                        t.io_buffer_size_mib = tuned_mib;
                    }
                }
                // ---------------------------

                // Probe Target Capabilities (Write Operations)
                let dio_ok = crate::security::probe_direct_io(&t.path);
                t.direct_io_ok.store(dio_ok, std::sync::atomic::Ordering::Relaxed);
                
                let tgt_uncached_ok = crate::security::probe_rwf_uncached(&t.path);
                t.rwf_uncached_ok.store(tgt_uncached_ok, std::sync::atomic::Ordering::Relaxed);

                if !dio_ok { tracing::warn!("Target {:?}: Direct IO (O_DIRECT) not supported.", t.path); }
                if !tgt_uncached_ok { tracing::warn!("Target {:?}: Uncached Writes/Reads (RWF_UNCACHED) not supported.", t.path); }

                let abs = t.path.canonicalize().map_err(|e| MirrorError::Config(format!("Invalid path {:?}: {}", t.path, e)))?;
                if !target_paths.insert(abs.clone()) {
                    return Err(MirrorError::Config(format!("Duplicate target path detected: {:?}.", abs)));
                }
            }
        }
        Ok(c)
    }
}
