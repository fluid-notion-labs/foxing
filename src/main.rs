use clap::{Parser, Subcommand};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use foxing::{config::Config, mirror::Manager, bpf, metrics, versioning, api, worker};
use tracing::{info, error, warn};
use axum::{Router, routing::get, extract::State, Json};
use std::net::SocketAddr;
use prometheus::{Encoder, TextEncoder};
use reqwest::blocking::get as http_get;
use comfy_table::Table;
use tokio::signal::unix::{signal, SignalKind};
use sysinfo::System;
use std::path::PathBuf;
use tokio::sync::RwLock;
use libc;
use walkdir::WalkDir;

mod tui;

#[derive(Parser)]
#[command(name = "xfs-mirror", version, about = "High-perf replication")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Daemon { #[arg(long, default_value = "config.toml")] config: String },
    Status { #[arg(short, long, default_value_t = 9100)] port: u16 },
    Metrics { 
        #[arg(short, long, default_value_t = 9100)] port: u16,
        #[arg(long)] json: bool,
        #[arg(short, long)] watch: bool,
    },
    Reload,
    Version { #[command(subcommand)] sub: VersionCommands },
    Oneshot { #[arg(long, default_value = "config.toml")] config: String },
}

#[derive(Subcommand)]
enum VersionCommands {
    List {
        #[arg(value_parser)] target_file: PathBuf,
        #[arg(short, long, default_value_t = 10)] limit: usize,
    },
    Revert {
        #[arg(value_parser)] target_file: PathBuf,
        #[arg(long)] epoch: u64,
        #[arg(long)] copy_to: Option<PathBuf>,
    },
    Copy {
        #[arg(value_parser)] target_file: PathBuf,
        #[arg(long)] epoch: u64,
        #[arg(value_parser)] destination: PathBuf,
    },
}

#[derive(Clone)]
struct AppState {
    tuner_board: worker::TunerBoard,
}

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let families = metrics::REGISTRY.gather();
    let mut buf = vec![];
    encoder.encode(&families, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

// --- SHARED STATS COLLECTOR ---
fn collect_system_status(tuner_board: &worker::TunerBoard) -> api::SystemStatus {
    let mut status = api::SystemStatus::default();
    
    status.load_avg_1m = metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).get();
    status.governor_stressed = metrics::GOVERNOR_STRESSED.get() == 1;
    status.global_events_dropped = metrics::EVENTS_DROPPED.get();
    status.live_additions = metrics::LIVE_ADDITIONS.get();

    // Populate Debug Info
    status.debug.bpf_sequence_gaps = metrics::SEQUENCE_GAPS.with_label_values(&[]).get(); 
    status.debug.bpf_events_malformed = metrics::EVENTS_MALFORMED.get();
    status.debug.bpf_events_unwatched = metrics::EVENTS_UNWATCHED.get();
    status.debug.worker_shutdown_timeouts = metrics::WORKER_SHUTDOWN_TIMEOUTS.get();
    status.debug.sidecars_created = metrics::SIDECAR_FILES_CREATED.get();
    status.debug.generation_mismatches = metrics::GENERATION_MISMATCHES.get();
    
    let bpf_stats = bpf::get_device_stats();
    for (dev_id, (seq, count)) in bpf_stats {
        let hex_id = format!("0x{:08x}", dev_id);
        status.debug.bpf_device_stats.insert(hex_id, api::BpfDeviceStat {
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

        let t_status = api::TargetStatus {
            tuner_state,
            throughput_mb: 0.0, 
            latency_ms: avg_lat,
            buffer_utilization: metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_str]).get(),
            ops_reflink: metrics::COPY_METHOD_REFLINK.get(),
            ops_offload: metrics::COPY_METHOD_OFFLOAD.get(),
            ops_standard: metrics::COPY_METHOD_STANDARD.get(),
            pending_events: 0,
        };
        status.targets.insert(path_str, t_status);
    }
    status
}

async fn status_json_handler(State(state): State<AppState>) -> Json<api::SystemStatus> {
    Json(collect_system_status(&state.tuner_board))
}

fn handle_status(port: u16) {
    match http_get(format!("http://127.0.0.1:{}/metrics", port)) {
        Ok(r) => {
            println!("Daemon: ONLINE (v{})", env!("CARGO_PKG_VERSION"));
            let mut table = Table::new();
            table.set_header(vec!["Metric", "Value"]);
            for line in r.text().unwrap().lines() {
                if line.starts_with("foxing") { 
                    table.add_row(line.split_whitespace().collect::<Vec<&str>>());
                }
            }
            println!("{table}");
        },
        Err(_) => println!("Daemon: OFFLINE")
    }
}

fn handle_metrics_command(port: u16, json_mode: bool, watch_mode: bool) -> anyhow::Result<()> {
    let url = format!("http://127.0.0.1:{}/status", port);

    if watch_mode {
        let mut app = tui::TuiApp::new(tui::DataMode::Remote(url.clone()));
        app.run(| | {
            match http_get(&url) {
                Ok(resp) => resp.json::<api::SystemStatus>().ok(),
                Err(_) => None
            }
        })?;
    } else if json_mode {
        let resp = http_get(&url)?;
        println!("{}", resp.text()?);
    } else {
        let resp = http_get(format!("http://127.0.0.1:{}/metrics", port))?;
        println!("{}", resp.text()?);
    }
    Ok(())
}

fn handle_reload() {
    let mut sys = System::new_all();
    sys.refresh_all();
    let me = std::process::id();
    for (pid, proc) in sys.processes() {
        if proc.name() == "foxing" && pid.as_u32() != me {
            let _ = std::process::Command::new("kill").arg("-HUP").arg(pid.to_string()).status();
            println!("Sent SIGHUP to PID {}", pid);
            return;
        }
    }
    println!("Daemon not found.");
}

fn set_process_priority(policy_str: &str) {
    const IOPRIO_WHO_PROCESS: i32 = 1;
    const IOPRIO_CLASS_BE: i32 = 2;
    const IOPRIO_CLASS_IDLE: i32 = 3;
    
    let (class, data) = match policy_str {
        "Idle" => (IOPRIO_CLASS_IDLE, 0),
        "Low" => (IOPRIO_CLASS_BE, 7), 
        "Normal" => (IOPRIO_CLASS_BE, 4), 
        "High" => (IOPRIO_CLASS_BE, 0), 
        _ => {
            warn!("Unknown io_priority '{}'. Using Normal.", policy_str);
            (IOPRIO_CLASS_BE, 4)
        }
    };

    let ioprio = (class << 13) | data;
    let res = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, ioprio) };
    
    if res < 0 {
        warn!("Failed to set I/O priority to {}: {}", policy_str, std::io::Error::last_os_error());
    } else {
        info!("Set process I/O priority to {}", policy_str);
    }
}

async fn handle_version_commands(cmd: VersionCommands) -> anyhow::Result<()> {
    match cmd {
        VersionCommands::List { target_file, limit } => {
            let versions = versioning::list_versions(&target_file)?;
            versioning::print_versions_table(versions, limit);
        }
        VersionCommands::Revert { target_file, epoch, copy_to } => {
            if let Some(dest) = copy_to {
                versioning::copy_version_to_path(&target_file, epoch, &dest)?;
                info!("Successfully copied version {} to {:?}. Proceeding with revert.", epoch, dest);
            }
            versioning::revert_file(&target_file, epoch)?;
            info!("Successfully reverted {:?} to version {}. The file now reflects the state at epoch {}.", target_file, epoch, epoch);
        }
        VersionCommands::Copy { target_file, epoch, destination } => {
            versioning::copy_version_to_path(&target_file, epoch, &destination)?;
            info!("Successfully copied version {} to {:?}.", epoch, destination);
        }
    }
    Ok(())
}

async fn run_daemon_logic(config_path: String, start_tui: bool) -> anyhow::Result<()> {
    if !start_tui {
        tracing_subscriber::fmt().init();
    }

    let cfg = match Config::load(&config_path) {
        Ok(c) => Arc::new(RwLock::new(c)), 
        Err(e) => { error!("Failed to load config: {}", e); return Ok(()); }
    };
    
    // OVERRIDE: Disable Governor for OneShot Mode
    if start_tui {
        cfg.write().await.max_system_load_avg = 100.0; // Effectively disabled
    }
    
    metrics::initialize_metrics(cfg.read().await.global_buffer_limit);

    let prio = cfg.read().await.io_priority.clone();
    set_process_priority(&prio);

    let mut mgr = Manager::new(cfg.clone()).await;
    let (queues, handles, shutdown_senders, _hydration_rx) = mgr.start().await;
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();

    let shutdown_bpf = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) = bpf::run(queues, shutdown_bpf).await { 
            error!("BPF Error: {}", e); 
        }
    });

    if start_tui {
        let source_path = cfg.read().await.sources[0].path.clone(); 
        metrics::DISCOVERY_COMPLETE.store(false, Ordering::Relaxed);

        std::thread::spawn(move || {
            let walker = WalkDir::new(source_path).into_iter();
            let mut count = 0;
            for _ in walker {
                count += 1;
                if count % 1000 == 0 { metrics::TOTAL_ITEMS_DISCOVERED.set(count); }
            }
            metrics::TOTAL_ITEMS_DISCOVERED.set(count);
            metrics::DISCOVERY_COMPLETE.store(true, Ordering::Relaxed);
        });

        // Launch TUI in Local Mode
        let mut app = tui::TuiApp::new(tui::DataMode::Local);
        let board = mgr.tuner_board.clone();
        
        app.run(move || {
            Some(collect_system_status(&board))
        }).unwrap_or_else(|e| eprintln!("TUI Error: {}", e));

        sd.store(true, Ordering::Relaxed);
    } else {
        let m_port = cfg.read().await.metrics_port; 
        let fatal = cfg.read().await.fatal_metrics_bind;
        let app_state = AppState { tuner_board: mgr.tuner_board.clone() };

        tokio::spawn(async move {
            let app = Router::new()
                .route("/metrics", get(metrics_handler))
                .route("/status", get(status_json_handler))
                .with_state(app_state);
                
            let addr = SocketAddr::from(([0, 0, 0, 0], m_port));
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!("Metrics listening on {}", addr);
                    if let Err(e) = axum::serve(listener, app).await { error!("Metrics server error: {}", e); }
                }
                Err(e) => {
                    error!("Failed to bind metrics port {}: {}. Metrics unavailable.", m_port, e);
                    if fatal { std::process::exit(1); }
                }
            }
        });

        let mut sighup = signal(SignalKind::hangup()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        loop {
            tokio::select! {
                _ = sighup.recv() => info!("Received SIGHUP"),
                _ = sigint.recv() => { info!("Shutting down..."); sd.store(true, Ordering::Relaxed); break; }
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
    
    mgr.wait_hydration();
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
        Commands::Oneshot { config } => {
            run_daemon_logic(config, true).await?;
        }
    }

    Ok(())
}
