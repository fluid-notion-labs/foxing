use std::path::{Path, PathBuf};
use tracing::{info, error, debug, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use clap::{Parser, Subcommand};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use foxing::config::{Config, SourceConfig, TargetConfig, TargetProfile};
use foxing::mirror::Manager;
use foxing::tuner::{TunerBoard, GLOBAL_TUNER_REGISTRY};
use foxing::metrics;
use tokio::signal::unix::{signal, SignalKind};
use axum::{routing::get, Router, Json, extract::State};
use tower_http::trace::TraceLayer;
use std::net::SocketAddr;
use tokio::sync::{RwLock, mpsc};
use foxing::tui;
use foxing::versioning;
use std::time::Duration;
use foxing::hydration_worker::HydrationMode;
use foxing::constants;

const CONFIG_HELP: &str = r#"
FOXING CONFIGURATION CHEATSHEET & USAGE GUIDE
===================================================
[GLOBAL SETTINGS]
worker_count = 8           # Number of parallel threads. Set to match logical CPU cores.
global_buffer_limit = 8192 # Max RAM usage in MB.
                           # Recommendation: 20% of RAM for desktop, 70% for dedicated server.
[SOURCE CONFIGURATION]
[[source]]
path = "/mnt/source"
  [source.targets]
  path = "/mnt/backup"
  # I/O PROFILES (Optimizes batching & coalescing)
  # - "NVMe":    Low latency (<15ms), small batches.
  # - "HDD":     High throughput, large batches to linearize writes.
  # - "Network": Tolerates high latency, huge buffers.
  profile = "NVMe"
  # HYDRATION (Initial Sync)
  initial_sync = true        # Run full scan on startup.
  # VERSIONING (Time Travel)
  enable_versioning = true   # Keep history using Reflinks (CoW).
  max_versions = 24          # Max snapshots to keep.
  max_versions_size_mb = 1024
  # FILTERS
  exclude = ["*.tmp", ".cache/**"]
[COMMANDS]
- foxing daemon --config config.toml
  Starts the replication service. Add --ui for interactive monitor.
- foxing snap list <path>
  Shows available versions for a file.
- foxing snap revert <path> <epoch>
  Instantly reverts a file to a previous version (CoW swap).
- foxing sync <src> <dst>
  Runs a one-off synchronization (rsync-like mode).
"#;

#[derive(Parser)]
#[command(name = "foxing")]
#[command(version, about = "High-Performance Filesystem Replication Daemon", long_about = None)]
#[command(after_help = CONFIG_HELP)]
struct Cli {
    #[arg(short, long, global = true, action = clap::ArgAction::Count, help = "Increase verbosity (-v: Debug, -vv: Trace)")]
    verbose: u8,
    #[arg(long, help = "Launch the interactive TUI (Setup Wizard or Daemon Monitor)")]
    ui: bool,
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
    #[command(name = "sync", visible_alias = "cp", visible_alias = "copy", about = "Rsync-like one-shot synchronization")]
    Sync {
        #[arg(help = "Source path")]
        source: PathBuf,
        #[arg(help = "Destination path")]
        destination: PathBuf,
        #[arg(short = 'a', long, help = "Archive mode (recursively copy, preserving attributes)")]
        archive: bool,
        #[arg(short = 'r', long, help = "Recursive copy (implied by -a)")]
        recursive: bool,
        #[arg(long, help = "Preserve versions using Reflinks/CoW")]
        snapshot: bool,
        #[arg(long, help = "Watch for changes after initial sync (Daemon mode)")]
        watch: bool,
        #[arg(short = 'e', long, help = "Exclude pattern (glob)")]
        exclude: Vec<String>,
        #[arg(long, help = "I/O Profile: NVMe, SSD, HDD, Network")]
        profile: Option<TargetProfile>,
        #[arg(short = 'n', long, help = "Dry run (Not fully implemented)")]
        dry_run: bool,
    },
    Check {
        #[arg(short, long)]
        config: String,
    },
    #[command(visible_alias = "snap")]
    Snapshot {
        #[command(subcommand)]
        cmd: SnapshotCommands,
    },
    Explain,
    Status,
    Metrics {
        #[arg(long, help = "Launch TUI monitor instead of raw output")]
        ui: bool,
    },
}

#[derive(Subcommand)]
enum SnapshotCommands {
    List { path: String },
    Revert { path: String, epoch: u64 },
    Copy { path: String, epoch: u64, destination: String },
    Cleanup { path: String, #[arg(long)] dry_run: bool },
    Force { path: String, #[arg(short, long)] tag: String }
}

#[derive(Clone)]
struct AppState {
    tuner_board: TunerBoard,
}

#[derive(Clone)]
struct TuiLogWriter {
    tx: mpsc::UnboundedSender<foxing::tui::UiEvent>,
}

impl std::io::Write for TuiLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s = String::from_utf8_lossy(buf).to_string();
        let _ = self.tx.send(foxing::tui::UiEvent::Log(s));
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn collect_system_status(tuner_board: &TunerBoard) -> foxing::api::SystemStatus {
    use foxing::api::{BpfDeviceStat, TargetStatus};
    let mut status = foxing::api::SystemStatus::default();
    
    status.load_avg_1m = metrics::GOVERNOR_LOAD_AVERAGE.with_label_values(&["1m"]).get();
    status.governor_stressed = metrics::GOVERNOR_STRESSED.get() == 1.0;
    status.global_events_dropped = metrics::EVENTS_DROPPED.get() as u64;
    status.live_additions = metrics::LIVE_ADDITIONS.get() as u64;
    
    status.debug.bpf_events_malformed = metrics::EVENTS_MALFORMED.get() as u64;
    status.debug.bpf_events_unwatched = metrics::EVENTS_UNWATCHED.get() as u64;
    status.debug.worker_shutdown_timeouts = metrics::WORKER_SHUTDOWN_TIMEOUTS.get() as u64;
    status.debug.sidecars_created = metrics::SIDECAR_FILES_CREATED.get() as u64;
    status.debug.generation_mismatches = metrics::GENERATION_MISMATCHES.get() as u64;

    let dev_stats = foxing::bpf::get_device_stats();
    for (dev_id, (seq, count)) in dev_stats {
        let hex_id = format!("0x{:08x}", dev_id);
        status.debug.bpf_device_stats.insert(hex_id, BpfDeviceStat {
            sequence: seq,
            event_count: count,
        });
    }

    for r in tuner_board.iter() {
        let path_str = r.key().to_string_lossy().to_string();
        let tuner_state = *r.value();
        
        let lat = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_sum();
        let count = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_count();
        let latency_ms = if count > 0 { (lat / count as f64) * 1000.0 } else { 0.0 };
        
        let mut avg_batch_size = 0.0;
        let mut avg_coalesce = 0.0;
        let mut avg_buf_util = 0.0;
        let mut worker_count = 0;
        
        for entry in GLOBAL_TUNER_REGISTRY.iter() {
            if entry.key().0 == path_str {
                let output = entry.value();
                avg_batch_size += output.batch_size as f64;
                avg_coalesce += output.coalesce_bytes as f64;
                
                let worker_id_str = entry.key().1.to_string();
                avg_buf_util += metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_str, &worker_id_str]).get();
                worker_count += 1;
            }
        }
        
        if worker_count > 0 {
            avg_batch_size /= worker_count as f64;
            avg_coalesce /= worker_count as f64;
            avg_buf_util /= worker_count as f64;
        }
        
        let history_point = (avg_coalesce / 1024.0 / 1024.0, latency_ms);
        
        let t_status = TargetStatus {
            latency_ms,
            pending_events: metrics::ORDERING_BUF_SIZE.with_label_values(&[&path_str]).get() as usize,
            tuner_state,
            batch_size: avg_batch_size as usize,
            coalesce_window_kb: (avg_coalesce / 1024.0) as u64,
            buffer_utilization: avg_buf_util,
            ops_reflink: metrics::COPY_METHOD_REFLINK.with_label_values(&[&path_str]).get() as u64,
            ops_offload: metrics::COPY_METHOD_OFFLOAD.with_label_values(&[&path_str]).get() as u64,
            ops_standard: metrics::COPY_METHOD_STANDARD.with_label_values(&[&path_str]).get() as u64,
            history: vec![history_point],
        };
        
        status.targets.insert(path_str, t_status);
    }
    
    status
}

fn print_usage_patterns() {
    println!("{}", CONFIG_HELP);
}

async fn is_daemon_running(port: u16) -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok()
}

async fn run_monitor(base_url: String) -> anyhow::Result<()> {
    let (ui_tx, rx) = tokio::sync::mpsc::channel(32);
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel(32);
    
    let _ = ui_tx.send(foxing::tui::UiEvent::Log("Connected to remote daemon. Real-time logs not available in monitor mode.".to_string())).await;
    
    let url_clone = base_url.clone();
    let ui_tx_clone = ui_tx.clone();
    
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let status_url = format!("{}/status", url_clone);
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        
        loop {
            interval.tick().await;
            match client.get(&status_url).send().await {
                Ok(resp) => {
                    if let Ok(status) = resp.json::<foxing::api::SystemStatus>().await {
                        if ui_tx_clone.send(foxing::tui::UiEvent::SystemUpdate(Box::new(status))).await.is_err() {
                            break;
                        }
                    }
                },
                Err(_) => {
                }
            }
        }
    });
    
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            debug!("Remote command ignored (Read-Only Monitor): {:?}", cmd);
        }
    });
    
    let mut app = foxing::tui::TuiApp::new(foxing::tui::DataMode::Remote(base_url), cmd_tx, ui_tx, CONFIG_HELP.to_string())?;
    app.run(rx).await?;
    Ok(())
}

fn format_bytes(bytes: f64) -> String {
    const UNIT: f64 = 1024.0;
    if bytes < UNIT { return format!("{:.0} B", bytes); }
    let div = UNIT;
    if bytes < div * UNIT { return format!("{:.2} KiB", bytes / div); }
    let div = div * UNIT;
    if bytes < div * UNIT { return format!("{:.2} MiB", bytes / div); }
    let div = div * UNIT;
    format!("{:.2} GiB", bytes / div)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    
    let one_shot_mode = matches!(cli.command, Some(Commands::Sync { watch: false, .. }));
    if one_shot_mode {
        constants::ONE_SHOT_MODE.store(true, Ordering::Relaxed);
    }

    let default_filter = match cli.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    let (log_tx, log_rx) = mpsc::unbounded_channel();
    let use_ui_logging = cli.ui || matches!(cli.command, Some(Commands::Daemon { tui: true, .. }));

    if use_ui_logging {
        let writer = TuiLogWriter { tx: log_tx.clone() };
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(
                std::env::var("RUST_LOG").unwrap_or_else(|_| default_filter.into()),
            ))
            .with(tracing_subscriber::fmt::layer()
                .with_writer(move || writer.clone())
                .with_ansi(false)
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(
                std::env::var("RUST_LOG").unwrap_or_else(|_| default_filter.into()),
            ))
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    if cli.verbose > 0 {
        debug!("Verbosity Level: {} (Filter: {})", cli.verbose, default_filter);
    }

    if cli.ui {
        let config_path = "config.toml";
        let config = if Path::new(config_path).exists() {
             Config::load(config_path).unwrap_or_default()
        } else {
             Config::default()
        };

        if is_daemon_running(config.metrics_port).await {
             run_monitor(format!("http://127.0.0.1:{}", config.metrics_port)).await?;
             return Ok(());
        }

        if Path::new(config_path).exists() {
            run_runtime(config, true, false, Some(log_rx), cli.verbose).await?;
        } else {
            println!("No config found and no daemon running. Launching Setup Wizard...");
            let config_opt = tokio::task::spawn_blocking(move || {
                tui::setup::run_interactive_setup()
            }).await??;
            
            if let Some(config) = config_opt {
                let toml_str = toml::to_string_pretty(&config)
                    .map_err(|e| anyhow::anyhow!("Serialization error: {}", e))?;
                std::fs::write(config_path, toml_str)?;
                println!("Configuration generated at ./{}. Run 'foxing daemon' to start.", config_path);
            } else {
                println!("Setup aborted.");
            }
        }
        return Ok(());
    }

    match cli.command {
        Some(Commands::Daemon { config, tui }) => {
            let cfg = Config::load(&config)?;
            if tui && is_daemon_running(cfg.metrics_port).await {
                warn!("Daemon already running on port {}. Attaching Monitor instead of starting new instance.", cfg.metrics_port);
                run_monitor(format!("http://127.0.0.1:{}", cfg.metrics_port)).await?;
                return Ok(());
            }
            let logs = if tui { Some(log_rx) } else { None };
            run_runtime(cfg, tui, false, logs, cli.verbose).await?;
        },
        Some(Commands::Sync { source, destination, archive, recursive, snapshot, watch, exclude, profile, dry_run }) => {
            let one_shot_mode_flag = !watch;
            if one_shot_mode_flag {
                constants::ONE_SHOT_MODE.store(true, Ordering::Relaxed);
            }
            
            if dry_run {
                info!("Dry run requested. (Verifying config only)");
            }
            
            let is_recursive = archive || recursive;
            if source.is_dir() && !is_recursive {
                anyhow::bail!("Source is a directory. Use -r or -a to copy directories.");
            }

            let profile = profile.unwrap_or(TargetProfile::Auto);
            
            let target_config = TargetConfig {
                path: destination,
                profile,
                autotune_target_latency_ms: 50,
                target_bandwidth_mbps: None,
                target_iops: None,
                initial_sync: true,
                supports_reflink: Arc::new(AtomicBool::new(false)),
                vdo_optimization: true,
                vdo_stall_threshold: 1000,
                include: vec![],
                exclude,
                enable_versioning: snapshot,
                max_versions: 24,
                max_versions_size_mb: 1024,
                force_retention_files: vec![],
                force_retention_count: 0,
                worker_count: foxing::config::SYS.logical_cores.min(8),
                batch_size: 64,
                worker_flush_interval_us: 100_000,
                io_buffer_size_mib: 4,
                ordering_scan_depth: 16,
                worker_hibernation_secs: 10,
                worker_retry_initial_ms: 10,
                worker_retry_max_ms: 1000,
                atomic_writes: false,
                source_uncached: false,
                target_uncached: false,
                xattr_supported: Arc::new(AtomicBool::new(true)),
                direct_io_ok: Arc::new(AtomicBool::new(false)),
                rwf_uncached_ok: Arc::new(AtomicBool::new(false)),
                rwf_atomic_ok: Arc::new(AtomicBool::new(false)),
                include_regexes: vec![],
                exclude_regexes: vec![],
                force_retention_regexes: vec![],
                label: "".into(),
            };

            let source_config = SourceConfig {
                path: source,
                targets: vec![target_config],
                rwf_uncached_ok: Arc::new(AtomicBool::new(false)),
                cross_subvolumes: false,
            };

            let mut config = Config::default();
            config.sources = vec![source_config];
            
            // Pre-compile config to validate paths
            for sc in &mut config.sources {
                sc.rwf_uncached_ok.store(foxing::security::probe_rwf_uncached(&sc.path), Ordering::Relaxed);
                for tc in &mut sc.targets {
                    tc.rwf_uncached_ok.store(foxing::security::probe_rwf_uncached(&tc.path), Ordering::Relaxed);
                    tc.direct_io_ok.store(foxing::security::probe_direct_io(&tc.path), Ordering::Relaxed);
                    tc.compile(config.worker_count, config.io_buffer_size_mib)?;
                }
            }

            info!("Starting Sync (One-Shot: {})", one_shot_mode_flag);
            run_runtime(config, false, one_shot_mode_flag, None, cli.verbose).await?;
        },
        Some(Commands::Check { config }) => {
            match Config::load(&config) {
                Ok(_) => info!("Configuration is valid."),
                Err(e) => error!("Configuration error: {}", e),
            }
        },
        Some(Commands::Snapshot { cmd }) => {
            match cmd {
                SnapshotCommands::List { path } => {
                    let p = PathBuf::from(path);
                    let versions = versioning::list_versions(&p)?;
                    versioning::print_versions_table(versions, 20);
                },
                SnapshotCommands::Revert { path, epoch } => {
                    let p = PathBuf::from(path);
                    versioning::revert_file(&p, epoch)?;
                    info!("Successfully reverted {:?} to epoch {}", p, epoch);
                },
                SnapshotCommands::Copy { path, epoch, destination } => {
                    let p = PathBuf::from(path);
                    let d = PathBuf::from(destination);
                    versioning::copy_version_to_path(&p, epoch, &d)?;
                    info!("Extracted version {} of {:?} to {:?}", epoch, p, d);
                },
                SnapshotCommands::Cleanup { path, dry_run } => {
                    let p = PathBuf::from(path);
                    versioning::cleanup_cli(&p, dry_run).await?;
                },
                SnapshotCommands::Force { path, tag } => {
                    let p = PathBuf::from(path);
                    versioning::force_version_cli(&p, &tag).await?;
                }
            }
        },
        Some(Commands::Explain) => {
            println!("{}", CONFIG_HELP);
        },
        Some(Commands::Status) => {
            let url = "http://127.0.0.1:9100";
            let target_url = format!("{}/status", url);
            let response = reqwest::get(&target_url).await;
            match response {
                Ok(resp) => println!("{}", resp.text().await?),
                Err(e) => error!("Failed to connect to daemon at {}: {}", url, e),
            }
        },
        Some(Commands::Metrics { ui }) => {
            let url = "http://127.0.0.1:9100";
            if ui {
                if is_daemon_running(9100).await {
                    run_monitor(url.to_string()).await?;
                } else {
                    error!("Daemon not running on port 9100. Cannot attach monitor.");
                }
            } else {
                let target_url = format!("{}/metrics", url);
                let response = reqwest::get(&target_url).await;
                match response {
                    Ok(resp) => println!("{}", resp.text().await?),
                    Err(e) => error!("Failed to connect to daemon at {}: {}", url, e),
                }
            }
        },
        None => {
            print_usage_patterns();
        }
    }
    
    constants::ONE_SHOT_MODE.store(false, Ordering::Relaxed);
    Ok(())
}

async fn run_runtime(
    config: Config,
    start_tui: bool,
    one_shot_mode: bool,
    log_receiver: Option<mpsc::UnboundedReceiver<foxing::tui::UiEvent>>,
    verbosity: u8
) -> anyhow::Result<()> {
    if one_shot_mode {
        constants::ONE_SHOT_MODE.store(true, Ordering::Relaxed);
    }

    let global_limit = config.global_buffer_limit;
    let metrics_port = config.metrics_port;

    if !one_shot_mode && is_daemon_running(metrics_port).await {
        anyhow::bail!("Port {} is already in use. Is Foxing already running? Use --ui to attach or 'killall foxing' to stop.", metrics_port);
    }

    metrics::initialize_metrics(global_limit);

    let shared_config = Arc::new(RwLock::new(config));
    let mut manager = Manager::new(shared_config.clone()).await;

    if one_shot_mode {
        manager.set_hydration_mode(HydrationMode::PrioritizeStructure);
        info!("Running in One-Shot Mode: Prioritizing directory structure scan before data movement.");
    } else {
        manager.set_hydration_mode(HydrationMode::Streaming);
    }

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
    
    let api_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await;
        match listener {
            Ok(l) => {
                if let Err(e) = axum::serve(l, api_app).await {
                    error!("API Server Error: {}", e);
                }
            },
            Err(e) => error!("Failed to bind API port {}: {}", metrics_port, e),
        }
    });

    if !one_shot_mode {
        info!("Metrics API listening on http://{}", addr);
    }

    let (queues, mut tasks_set, mut shutdowns, _hydration_rx_dummy) = manager.start().await?;
    let sources_map = manager.sources.clone();
    let initial_seq = 0;
    
    let bpf_shutdown = Arc::new(AtomicBool::new(false));
    let bpf_shutdown_clone = bpf_shutdown.clone();
    
    let bpf_handle = std::thread::spawn(move || {
        if let Err(e) = foxing::bpf::run(queues, bpf_shutdown_clone, sources_map, initial_seq) {
            error!("BPF Thread crashed: {}", e);
        }
    });

    if one_shot_mode {
        let start_time = std::time::Instant::now();
        info!("Sync Mode: Waiting for hydration to complete...");
        
        let mut check_interval = tokio::time::interval(Duration::from_millis(500));
        let mut last_log = std::time::Instant::now();
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;

        loop {
            tokio::select! {
                _ = sigint.recv() => {
                    info!("\nInterrupt received. Cancelling sync and generating report...");
                    break;
                }
                _ = sigterm.recv() => {
                    info!("\nTermination signal received. Cancelling sync and generating report...");
                    break;
                }
                _ = check_interval.tick() => {
                    let mut all_done = true;
                    let mut total_pending = 0;
                    let mut active_scans = 0;
                    
                    for src in manager.sources.values() {
                        if src.hydration.active.load(Ordering::Relaxed) {
                            active_scans += 1;
                            all_done = false;
                        }
                        
                        // FIXED: Use try_lock to prevent deadlocking main loop if scanner thread is blocked
                        // on channel pressure while holding the lock.
                        if let Some(queue_guard) = src.bulk_job_queue.try_lock() {
                            if let Some(queue) = queue_guard.as_ref() {
                                let pending = queue.pending_count.load(Ordering::Relaxed);
                                if pending > 0 {
                                    total_pending += pending;
                                    all_done = false;
                                }
                            }
                        } else {
                            // If locked, we assume work is happening but don't block.
                            // We mark as not done because we can't verify emptiness.
                            all_done = false;
                        }
                    }

                    if all_done {
                        info!("Sync Complete. All files processed.");
                        break;
                    }

                    if last_log.elapsed().as_secs() >= 5 {
                        info!("Sync Progress: {} scans active, {} files pending processing...", active_scans, total_pending);
                        last_log = std::time::Instant::now();
                    }
                }
            }
        }

        let duration = start_time.elapsed();
        let seconds = duration.as_secs_f64();
        
        println!("\nFOXING SYNC SUMMARY");
        println!("================================================================================");
        println!("Total Duration:  {:.2} seconds", seconds);
        
        let mut total_bytes = 0.0;
        let mut total_reflink = 0u64;
        let mut total_offload = 0u64;
        let mut total_standard = 0u64;
        
        use comfy_table::{Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Cell, Color, Attribute};
        let mut table = Table::new();
        table.load_preset(UTF8_FULL).apply_modifier(UTF8_ROUND_CORNERS);
        table.set_header(vec![
            Cell::new("Target Path").add_attribute(Attribute::Bold),
            Cell::new("Data Moved").add_attribute(Attribute::Bold),
            Cell::new("Throughput").add_attribute(Attribute::Bold),
            Cell::new("Avg Latency").add_attribute(Attribute::Bold),
            Cell::new("Buffer Util").add_attribute(Attribute::Bold),
            Cell::new("Tuner State").add_attribute(Attribute::Bold),
        ]);

        for r in manager.tuner_board.iter() {
            let path_str = r.key().to_string_lossy().to_string();
            let tuner_state = r.value();
            
            let bytes = metrics::BYTES_REPLICATED.with_label_values(&[&path_str]).get();
            total_bytes += bytes;
            let throughput_mbps = if seconds > 0.0 { (bytes / 1024.0 / 1024.0) / seconds } else { 0.0 };
            
            let lat_sum = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_sum();
            let lat_count = metrics::REPLICATION_LATENCY.with_label_values(&[&path_str]).get_sample_count();
            let avg_lat = if lat_count > 0 { (lat_sum / lat_count as f64) * 1000.0 } else { 0.0 };
            
            let mut avg_buf_util = 0.0;
            let mut worker_count = 0;
            for entry in GLOBAL_TUNER_REGISTRY.iter() {
                if entry.key().0 == path_str {
                    // FIXED: Read through Arc
                    let _output = entry.value();
                    let worker_id_str = entry.key().1.to_string();
                    avg_buf_util += metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[&path_str, &worker_id_str]).get();
                    worker_count += 1;
                }
            }
            if worker_count > 0 { avg_buf_util /= worker_count as f64; }
            avg_buf_util *= 100.0;

            total_reflink += metrics::COPY_METHOD_REFLINK.with_label_values(&[&path_str]).get() as u64;
            total_offload += metrics::COPY_METHOD_OFFLOAD.with_label_values(&[&path_str]).get() as u64;
            total_standard += metrics::COPY_METHOD_STANDARD.with_label_values(&[&path_str]).get() as u64;

            table.add_row(vec![
                Cell::new(&path_str).fg(Color::Cyan),
                Cell::new(format_bytes(bytes)),
                Cell::new(format!("{:.2} MB/s", throughput_mbps)),
                Cell::new(format!("{:.2} ms", avg_lat)),
                Cell::new(format!("{:.1}%", avg_buf_util)),
                Cell::new(format!("{:?}", tuner_state)),
            ]);
        }

        let global_throughput = if seconds > 0.0 { (total_bytes / 1024.0 / 1024.0) / seconds } else { 0.0 };
        
        println!("Total Data:      {}", format_bytes(total_bytes));
        println!("Agg. Throughput: {:.2} MB/s\n", global_throughput);
        println!("{table}");
        
        println!("\nOPERATIONAL BREAKDOWN");
        println!("---------------------");
        println!("• Methods:");
        println!("  ├─ Reflinks (CoW):      {}", total_reflink);
        println!("  ├─ Standard Copies:     {}", total_standard);
        println!("  └─ Network/Offload:     {}", total_offload);
        println!("• Discovery:");
        println!("  ├─ Hydration Items:     {}", metrics::LIVE_ADDITIONS.get());
        println!("  └─ Skipped (Unchanged): {}", metrics::HYDRATION_HASH_SKIPPED.get());
        println!("\nRELIABILITY & HEALTH");
        println!("--------------------");
        println!("• Versioning:");
        println!("  ├─ Snapshots Created:   {}", metrics::VERSIONING_SUCCESS.get());
        println!("  └─ Failures:            {}", metrics::VERSIONING_FAILURES.get());
        println!("• Diagnostics:");
        println!("  ├─ Events Dropped:      {}", metrics::EVENTS_DROPPED.get());
        println!("  ├─ Malformed Events:    {}", metrics::EVENTS_MALFORMED.get());
        println!("  └─ Retry Failures:      {}", metrics::POISON_CABINET_ACTIVE.get());

        if verbosity > 0 {
            println!("\nFULL METRICS DUMP (Verbose Mode)");
            println!("================================================================================");
            println!("{}", metrics_wrapper::gather());
        } else {
            println!("================================================================================\n");
        }
        
        tokio::time::sleep(Duration::from_millis(500)).await;

    } else if start_tui {
        let t_board = manager.tuner_board.clone();
        let (ui_tx, rx) = tokio::sync::mpsc::channel(32);
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel(32);
        
        let ui_tx_clone = ui_tx.clone();
        let poller_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let status = collect_system_status(&t_board);
                if ui_tx_clone.send(foxing::tui::UiEvent::SystemUpdate(Box::new(status))).await.is_err() {
                    break;
                }
            }
        });

        if let Some(mut log_src) = log_receiver {
            let ui_tx_logs = ui_tx.clone();
            tokio::spawn(async move {
                while let Some(evt) = log_src.recv().await {
                    let _ = ui_tx_logs.send(evt).await;
                }
            });
        }

        let command_handle = tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                debug!("Received Daemon Command: {:?}", cmd);
            }
        });

        let mut app = foxing::tui::TuiApp::new(foxing::tui::DataMode::Local, cmd_tx, ui_tx, CONFIG_HELP.to_string()).expect("Failed to init TUI");
        let _ = app.run(rx).await;
        
        poller_handle.abort();
        command_handle.abort();
        
        info!("TUI exited. Shutting down daemon...");
    } else {
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        
        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
    }

    info!("Initiating Graceful Shutdown...");
    bpf_shutdown.store(true, Ordering::Relaxed);
    let _ = bpf_handle.join();
    
    for src in manager.sources.values() {
        if let Some(queue) = src.bulk_job_queue.lock().as_ref() {
            queue.signal_shutdown();
            
            let pending = queue.pending_count.load(Ordering::SeqCst);
            if pending > 0 {
                warn!("Shutdown: Force-draining {} pending hydration jobs for source {:?}", pending, src.path);
                queue.pending_count.store(0, Ordering::SeqCst);
                metrics::EVENTS_DROPPED.inc_by(pending as f64);
            }
        }
    }

    for tx in shutdowns.drain(..) {
        let _ = tx.send(()).await;
    }

    tasks_set.abort_all();
    while let Some(_) = tasks_set.join_next().await {}
    
    api_handle.abort();
    info!("Shutdown Complete.");
    
    constants::ONE_SHOT_MODE.store(false, Ordering::Relaxed);
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
