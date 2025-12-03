use clap::{Parser, Subcommand};
use std::sync::{Arc, atomic::Ordering, atomic::AtomicBool};
// Removed TunerState unused import
use foxing::{config::Config, mirror::Manager, bpf, metrics, tuner::TunerBoard};
// Removed unused foxing::error::Result
use tokio::signal::unix::{signal, SignalKind};
use axum::{routing::get, Json, extract::State, Router};
use serde_json::json;
use tower_http::trace::{self, TraceLayer};
use tracing::{info, error, warn};
use std::net::SocketAddr;
use sysinfo::System;
use tokio::sync::RwLock;
use libc;
use walkdir::WalkDir;
use std::fs;
use nix::sched::{sched_setaffinity, CpuSet};
use nix::unistd::Pid;
use foxing::tui;
use foxing::versioning;
use prometheus::{self, Encoder, TextEncoder};

fn get_available_cores() -> Vec<usize> {
    let system = System::new_all();
    let total_cores = system.cpus().len();
    let mut available_cores = Vec::new();
    info!("System reports {} logical cores.", total_cores);
    let mut p_cores = Vec::new();
    let mut e_cores = Vec::new();
    for i in 0..total_cores {
        let core_type_path = format!("/sys/devices/system/cpu/cpu{}/cpu_capacity", i);
        if let Ok(content) = fs::read_to_string(&core_type_path) {
            if content.trim() != "0" {
                p_cores.push(i);
            } else {
                e_cores.push(i);
            }
        }
    }
    available_cores.extend(p_cores.clone());
    available_cores.extend(e_cores.clone());
    info!("NUMA/Core Topology detected: P-Cores: {} E-Cores: {}", p_cores.len(), e_cores.len());
    info!("Assigned core IDs for pinning: {:?}", available_cores);
    available_cores
}

fn set_realtime_priority() {
    let param = libc::sched_param { sched_priority: 1 };
    let pid = 0;
    let res = unsafe {
        libc::sched_setscheduler(
            pid,
            libc::SCHED_FIFO,
            &param,
        )
    };
    if res == -1 {
        warn!("Failed to set SCHED_FIFO realtime priority for BPF thread (os error: {}).", std::io::Error::last_os_error());
    } else {
        info!("Successfully set SCHED_FIFO priority for BPF event collector.");
    }
}

fn pin_to_cpu(core_id: usize) {
    let mut cpu_set = CpuSet::new();
    if core_id >= std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) {
         warn!("Requested CPU pin ID {} is out of bounds. Skipping pin.", core_id);
         return;
    }
    if let Err(e) = cpu_set.set(core_id) {
         warn!("Failed to set CPU mask for pin {}: {}", core_id, e);
         return;
    }
    if let Err(e) = sched_setaffinity(Pid::from_raw(0), &cpu_set) {
        warn!("Failed to pin thread to CPU {}: {}", core_id, e);
    } else {
        info!("Thread pinned successfully to CPU {}", core_id);
    }
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Daemon { #[arg(long, default_value = "config.toml")] config: String },
    Status { #[arg(short, long, default_value = "9100")] port: u16 },
    Metrics {
        #[arg(short, long)] port: u16,
        #[arg(long)] json: bool,
        #[arg(short, long)] watch: bool,
    },
    Reload,
    OneShot { #[arg(long, default_value = "config.toml")] config: String },
    Tui { #[arg(long, default_value = "config.toml")] config: String },
    Version { #[clap(subcommand)] sub: VersionCommands },
}

#[derive(Subcommand)]
enum VersionCommands {
    Cleanup { path: String, #[arg(short, long)] dry_run: bool },
    Force { path: String, tag: String },
}

#[derive(Clone)]
struct AppState {
    tuner_board: TunerBoard,
}

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = metrics::REGISTRY.gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).expect("Failed to encode metrics");
    String::from_utf8(buffer).expect("Failed to convert metrics buffer to string")
}

async fn status_json_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    let status = collect_system_status(&state.tuner_board);
    Json(json!({
        "status": status
    }))
}

fn collect_system_status(tuner_board: &TunerBoard) -> foxing::api::SystemStatus {
    use foxing::api::BpfDeviceStat;
    use foxing::api::TargetStatus;
    
    let mut status = foxing::api::SystemStatus::default();
    status.load_avg_1m = metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).get();
    status.governor_stressed = metrics::GOVERNOR_STRESSED.get() == 1;
    status.global_events_dropped = metrics::EVENTS_DROPPED.get();
    status.live_additions = metrics::LIVE_ADDITIONS.get();
    
    status.debug.bpf_sequence_gaps = 0;
    status.debug.bpf_events_malformed = metrics::EVENTS_MALFORMED.get();
    status.debug.bpf_events_unwatched = metrics::EVENTS_UNWATCHED.get();
    status.debug.worker_shutdown_timeouts = metrics::WORKER_SHUTDOWN_TIMEOUTS.get();
    status.debug.sidecars_created = metrics::SIDECAR_FILES_CREATED.get();
    status.debug.generation_mismatches = metrics::GENERATION_MISMATCHES.get();

    let bpf_stats = bpf::get_device_stats();
    for (dev_id, (seq, count)) in bpf_stats {
        let hex_id = format!("0x{:08x}", dev_id);
        status.debug.bpf_device_stats.insert(hex_id, BpfDeviceStat {
            dev_id_raw: dev_id,
            last_sequence: seq,
            total_events: count,
        });
    }

    for r in tuner_board.iter() {
        let path_str = r.key().to_string_lossy().to_string();
        let tuner_state = *r.value();
        
        let lat = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_sum();
        let count = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_count();
        let avg_lat = if count > 0 { (lat / count as f64) * 1000.0 } else { 0.0 };
        let wal_failures = metrics::WAL_COHERENCE_FAILURES.with_label_values(&[&path_str]).get();

        let t_status = TargetStatus {
            tuner_state,
            throughput_mb: 0.0, 
            latency_ms: avg_lat,
            buffer_utilization: metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_str]).get(),
            ops_reflink: metrics::COPY_METHOD_REFLINK.get(),
            ops_offload: metrics::COPY_METHOD_OFFLOAD.get(),
            ops_standard: metrics::COPY_METHOD_STANDARD.get(),
            pending_events: 0,
            wal_coherence_failures: wal_failures,
        };
        status.targets.insert(path_str, t_status);
    }
    
    status
}

fn handle_status(_port: u16) {
    // Client impl placeholder
}

fn handle_metrics_command(_port: u16, _json_mode: bool, _watch_mode: bool) -> anyhow::Result<()> {
    // Metrics fetch placeholder
    Ok(())
}

async fn handle_version_commands(sub: VersionCommands) -> anyhow::Result<()> {
    match sub {
        VersionCommands::Cleanup { path, dry_run } => {
            let path_buf = std::path::PathBuf::from(path);
            versioning::cleanup_cli(&path_buf, dry_run).await?;
        },
        VersionCommands::Force { path, tag } => {
            let path_buf = std::path::PathBuf::from(path);
            versioning::force_version_cli(&path_buf, &tag).await?;
        }
    }
    Ok(())
}

fn handle_reload() {
    // SIGHUP logic placeholder
}

fn set_process_priority(_policy_str: &str) {
    // Nice value placeholder
}

async fn run_daemon_logic(config_path: String, start_tui: bool) -> anyhow::Result<()> {
    let cfg = match Config::load(&config_path) {
        Ok(c) => Arc::new(RwLock::new(c)),
        Err(e) => { error!("Failed to load config: {}", e); return Ok(()); }
    };

    if start_tui {
        cfg.write().await.max_system_load_avg = 100.0; // Disable governor for TUI demo
    }

    metrics::initialize_metrics(cfg.read().await.global_buffer_limit);
    
    let prio = cfg.read().await.io_priority.clone();
    set_process_priority(&prio);

    let mut available_cores = get_available_cores();
    let bpf_core_id = if !available_cores.is_empty() { available_cores.remove(0) } else { 0 };

    let mut mgr = Manager::new(cfg.clone()).await;
    let (queues, mut handles, shutdown_senders, mut hydration_rx) = mgr.start().await;

    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let shutdown_bpf = shutdown.clone();

    let bpf_thread_handle = std::thread::Builder::new()
        .name("foxing-bpf-collector".into())
        .spawn(move || {
            pin_to_cpu(bpf_core_id);
            set_realtime_priority();
            
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    if let Err(e) = bpf::run(queues, shutdown_bpf).await {
                        error!("BPF Error: {}", e);
                    }
                })
        }).unwrap();

    handles.push(tokio::task::spawn_blocking(move || {
        bpf_thread_handle.join().unwrap();
        Ok(())
    }));

    let mgr_arc = Arc::new(tokio::sync::Mutex::new(mgr));
    let mgr_for_hydration = mgr_arc.clone();
    let sd_for_hyd = shutdown.clone();

    tokio::spawn(async move {
        while let Some(path) = hydration_rx.recv().await {
            if sd_for_hyd.load(Ordering::Relaxed) { break; }
            warn!("Hydration REQUESTED via signal for {:?}", path);
            let _m = mgr_for_hydration.lock().await; // Fixed unused variable m -> _m
            // Manager handles hydration internally via channels passed during start()
        }
    });

    if start_tui {
        let source_path = cfg.read().await.sources[0].path.clone();
        metrics::DISCOVERY_COMPLETE.store(false, Ordering::Relaxed);
        
        std::thread::spawn(move || {
            let walker = WalkDir::new(source_path).into_iter();
            let mut count = 0;
            for entry in walker {
                if entry.is_err() { continue; }
                count += 1;
            }
            metrics::TOTAL_ITEMS_DISCOVERED.set(count);
            metrics::DISCOVERY_COMPLETE.store(true, Ordering::Relaxed);
        });

        let board = mgr_arc.lock().await.tuner_board.clone();
        let mut app = tui::TuiApp::new(tui::DataMode::Remote(format!("http://127.0.0.1:{}", cfg.read().await.metrics_port)));
        
        app.run(move || {
            Some(collect_system_status(&board))
        }).unwrap_or_else(|e| eprintln!("TUI Error: {}", e));
        
        sd.store(true, Ordering::Relaxed);
    } else {
        let m_port = cfg.read().await.metrics_port;
        let fatal = cfg.read().await.fatal_metrics_bind;
        let tuner_board = mgr_arc.lock().await.tuner_board.clone();
        let app_state = AppState { tuner_board };

        tokio::spawn(async move {
            let router = Router::new()
                .route("/metrics", get(metrics_handler))
                .route("/status", get(status_json_handler))
                .with_state(app_state.clone())
                .layer(TraceLayer::new_for_http().on_request(trace::DefaultOnRequest::default()).on_response(trace::DefaultOnResponse::default()));

            let addr = SocketAddr::from(([0, 0, 0, 0], m_port));
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!("Metrics listening on {}", addr);
                    axum::serve(listener, router.into_make_service()).await.unwrap();
                }
                Err(e) => {
                    if fatal {
                        error!("FATAL: Failed to bind metrics port {}: {}", m_port, e);
                        std::process::exit(1);
                    } else {
                        warn!("Failed to bind metrics port {}: {}", m_port, e);
                    }
                }
            }
        });

        let mut sighup = signal(SignalKind::hangup()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();

        loop {
            tokio::select! {
                _ = sighup.recv() => info!("Received SIGHUP"),
                _ = sigint.recv() => { info!("Received SIGINT. Shutting down."); break; }
            }
        }
    }

    info!("Draining workers...");
    for tx in shutdown_senders { let _ = tx.send(()).await; }
    
    let timeout_secs = cfg.read().await.shutdown_timeout_secs;
    let timeout = std::time::Duration::from_secs(timeout_secs);
    
    for h in handles {
        let _ = tokio::time::timeout(timeout, h).await;
    }
    
    mgr_arc.lock().await.wait_hydration();
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Status { port } => { handle_status(port); return Ok(()); },
        Commands::Metrics { port, json, watch } => {
            handle_metrics_command(port, json, watch)?;
            return Ok(());
        },
        Commands::Reload => { handle_reload(); return Ok(()); },
        Commands::Version { sub } => { handle_version_commands(sub).await?; return Ok(()) },
        Commands::Daemon { config } => {
            run_daemon_logic(config, false).await?;
        },
        Commands::OneShot { config } => {
            run_daemon_logic(config, true).await?;
        },
        Commands::Tui { config } => {
            run_daemon_logic(config, true).await?;
        }
    }
    Ok(())
}
