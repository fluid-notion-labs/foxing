use std::path::PathBuf;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use clap::{Parser, Subcommand};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use foxing::config::Config;
use foxing::mirror::Manager;
use foxing::tuner::TunerBoard;
use foxing::metrics;
use tokio::signal::unix::{signal, SignalKind};
use axum::{routing::get, Router, Json, extract::State};
use tower_http::trace::TraceLayer;
use std::net::SocketAddr;
use tokio::sync::RwLock;
use foxing::tui;
use foxing::versioning;

#[derive(Parser)]
#[command(name = "foxing")]
#[command(about = "High-Performance Filesystem Replication Daemon", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Daemon {
        #[arg(short, long, default_value = "config.toml")]
        config: String,
        #[arg(short, long)]
        tui: bool,
    },
    Check {
        #[arg(short, long)]
        config: String,
    },
    Version {
        #[command(subcommand)]
        cmd: VersionCommands,
    },
    Status,
    Metrics,
}

#[derive(Subcommand)]
enum VersionCommands {
    List { path: String },
    Revert { path: String, epoch: u64 },
    Copy { path: String, epoch: u64, destination: String },
    Cleanup {
        path: String,
        #[arg(long)]
        dry_run: bool
    },
    Force {
        path: String,
        #[arg(short, long)]
        tag: String
    }
}

#[derive(Clone)]
struct AppState {
    tuner_board: TunerBoard,
}

fn collect_system_status(tuner_board: &TunerBoard) -> foxing::api::SystemStatus {
    use foxing::api::BpfDeviceStat;
    use foxing::api::TargetStatus;
    
    let mut status = foxing::api::SystemStatus::default();
    
    // Global Metrics
    status.load_avg_1m = metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).get();
    status.governor_stressed = metrics::GOVERNOR_STRESSED.get() == 1.0;
    status.global_events_dropped = metrics::EVENTS_DROPPED.get() as u64;
    status.live_additions = metrics::LIVE_ADDITIONS.get() as u64;

    // Debug Metrics
    status.debug.bpf_events_malformed = metrics::EVENTS_MALFORMED.get() as u64;
    status.debug.bpf_events_unwatched = metrics::EVENTS_UNWATCHED.get() as u64;
    status.debug.worker_shutdown_timeouts = metrics::WORKER_SHUTDOWN_TIMEOUTS.get() as u64;
    status.debug.sidecars_created = metrics::SIDECAR_FILES_CREATED.get() as u64;
    status.debug.generation_mismatches = metrics::GENERATION_MISMATCHES.get() as u64;

    // BPF Device Stats
    let dev_stats = foxing::bpf::get_device_stats();
    for (dev_id, (seq, count)) in dev_stats {
        let hex_id = format!("0x{:08x}", dev_id);
        status.debug.bpf_device_stats.insert(hex_id, BpfDeviceStat {
            sequence: seq,
            event_count: count,
        });
    }

    // Per-Target Status from TunerBoard
    for r in tuner_board.iter() {
        let path_str = r.key().to_string_lossy().to_string();
        let tuner_state = *r.value();
        
        let lat = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_sum();
        let count = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_count();
        let latency_ms = if count > 0 { (lat / count as f64) * 1000.0 } else { 0.0 };

        let wal_failures = metrics::WAL_COHERENCE_FAILURES.with_label_values(&[&path_str]).get() as u64;
        let batch_size = metrics::TARGET_BATCH_SIZE.with_label_values(&[&path_str]).get() as usize;
        let coalesce_bytes = metrics::TARGET_COALESCE_BYTES.with_label_values(&[&path_str]).get() as u64;
        
        let t_status = TargetStatus {
            latency_ms,
            pending_events: metrics::ORDERING_BUF_SIZE.with_label_values(&[&path_str]).get() as usize,
            tuner_state,
            batch_size,
            coalesce_window_kb: coalesce_bytes / 1024,
            buffer_utilization: metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_str]).get(),
            ops_reflink: metrics::COPY_METHOD_REFLINK.get() as u64,
            ops_offload: metrics::COPY_METHOD_OFFLOAD.get() as u64,
            ops_standard: metrics::COPY_METHOD_STANDARD.get() as u64,
            wal_failures,
        };
        status.targets.insert(path_str, t_status);
    }
    status
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Daemon { config, tui }) => {
            run_daemon_logic(config, tui).await?;
        },
        Some(Commands::Check { config }) => {
            match Config::load(&config) {
                Ok(_) => info!("Configuration is valid."),
                Err(e) => error!("Configuration error: {}", e),
            }
        },
        Some(Commands::Version { cmd }) => {
            match cmd {
                VersionCommands::List { path } => {
                    let p = PathBuf::from(path);
                    let versions = versioning::list_versions(&p)?;
                    versioning::print_versions_table(versions, 20);
                },
                VersionCommands::Revert { path, epoch } => {
                    let p = PathBuf::from(path);
                    versioning::revert_file(&p, epoch)?;
                    info!("Successfully reverted {:?} to epoch {}", p, epoch);
                },
                VersionCommands::Copy { path, epoch, destination } => {
                    let p = PathBuf::from(path);
                    let d = PathBuf::from(destination);
                    versioning::copy_version_to_path(&p, epoch, &d)?;
                    info!("Extracted version {} of {:?} to {:?}", epoch, p, d);
                },
                VersionCommands::Cleanup { path, dry_run } => {
                    let p = PathBuf::from(path);
                    versioning::cleanup_cli(&p, dry_run).await?;
                },
                VersionCommands::Force { path, tag } => {
                    let p = PathBuf::from(path);
                    versioning::force_version_cli(&p, &tag).await?;
                }
            }
        },
        Some(Commands::Status) => {
            let resp = reqwest::get("http://127.0.0.1:9100/status").await?.json::<foxing::api::SystemStatus>().await?;
            println!("{:#?}", resp);
        },
        Some(Commands::Metrics) => {
            let resp = reqwest::get("http://127.0.0.1:9100/metrics").await?.text().await?;
            println!("{}", resp);
        },
        None => {
            println!("Foxing Daemon. Use --help for usage.");
        }
    }

    Ok(())
}

async fn run_daemon_logic(config_path: String, start_tui: bool) -> anyhow::Result<()> {
    info!("Starting Foxing Daemon (Config: {})", config_path);
    
    let config = Config::load(&config_path)?;
    let global_limit = config.global_buffer_limit;
    let metrics_port = config.metrics_port;
    
    metrics::initialize_metrics(global_limit);

    let shared_config = Arc::new(RwLock::new(config));
    let mut manager = Manager::new(shared_config.clone()).await;

    // Start Manager (Spawns Workers, Hydrators)
    let (queues, handles, mut shutdowns, _) = manager.start().await;
    
    // Retrieve initial sequence for BPF resumption
    let sources_map = manager.sources.clone();
    let mut initial_seq = 0;
    for src in sources_map.values() {
        if let Some(journal) = &src.journal {
            let last = journal.get_last_sequence();
            if last > initial_seq {
                initial_seq = last;
            }
        }
    }

    // Start BPF Thread
    let bpf_shutdown = Arc::new(AtomicBool::new(false));
    let bpf_shutdown_clone = bpf_shutdown.clone();
    
    let bpf_handle = std::thread::spawn(move || {
        if let Err(e) = foxing::bpf::run(queues, bpf_shutdown_clone, sources_map, initial_seq) {
            error!("BPF Thread crashed: {}", e);
        }
    });

    // Start API Server
    let tuner_board = manager.tuner_board.clone();
    let app_state = AppState { tuner_board: tuner_board.clone() };
    
    let api_app = Router::new()
        .route("/metrics", get(|| async { metrics_wrapper::gather() }))
        .route("/status", get(move |State(s): State<AppState>| async move {
            Json(collect_system_status(&s.tuner_board))
        }))
        .with_state(app_state)
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], metrics_port));
    let api_server = axum::serve(tokio::net::TcpListener::bind(addr).await?, api_app);
    
    let api_handle = tokio::spawn(async move {
        if let Err(e) = api_server.await {
            error!("API Server Error: {}", e);
        }
    });

    info!("Metrics API listening on http://{}", addr);

    // TUI or Signal Handling
    if start_tui {
        let t_board = manager.tuner_board.clone();
        let mut app = tui::TuiApp::new(tui::DataMode::Local);
        // TUI runs in a blocking thread to not stall the reactor, though here we just join it.
        let _ = std::thread::spawn(move || {
            let _ = app.run(|| Some(collect_system_status(&t_board)));
        }).join();
        info!("TUI exited. Shutting down daemon...");
    } else {
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
    }

    // Graceful Shutdown
    info!("Initiating Graceful Shutdown...");
    
    // 1. Stop BPF
    bpf_shutdown.store(true, Ordering::Relaxed);
    let _ = bpf_handle.join();
    
    // 2. Stop Workers
    for tx in shutdowns.drain(..) {
        let _ = tx.send(()).await;
    }
    for h in handles {
        let _ = h.await;
    }
    
    // 3. Stop Hydrators
    manager.wait_hydration();
    
    // 4. Stop API
    api_handle.abort();
    
    info!("Shutdown Complete.");
    Ok(())
}

mod metrics_wrapper {
    use prometheus::{Encoder, TextEncoder};
    
    pub fn gather() -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}
