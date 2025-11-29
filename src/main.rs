// File: foxing/src/main.rs | Index: 18 of 24 | Function: Entry point with I/O Priority enforcement.
use clap::{Parser, Subcommand};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use xfs_mirror::{config::Config, mirror::Manager, bpf, metrics, versioning};
use tracing::{info, error, warn};
use axum::{Router, routing::get};
use std::net::SocketAddr;
use prometheus::{Encoder, TextEncoder};
use reqwest::blocking::get as http_get;
use comfy_table::Table;
use tokio::signal::unix::{signal, SignalKind};
use sysinfo::System;
use std::path::PathBuf;
use tokio::sync::RwLock;
use libc;

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
    Metrics { #[arg(short, long, default_value_t = 9100)] port: u16 },
    Reload,
    Version { #[command(subcommand)] sub: VersionCommands },
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

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let families = metrics::REGISTRY.gather();
    let mut buf = vec![];
    encoder.encode(&families, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}

fn handle_status(port: u16) {
    match http_get(format!("http://127.0.0.1:{}/metrics", port)) {
        Ok(r) => {
            println!("Daemon: ONLINE (v{})", env!("CARGO_PKG_VERSION"));
            let mut table = Table::new();
            table.set_header(vec!["Metric", "Value"]);
            for line in r.text().unwrap().lines() {
                if line.starts_with("foxing") { // Namespace Updated
                    table.add_row(line.split_whitespace().collect::<Vec<&str>>());
                }
            }
            println!("{table}");
        },
        Err(_) => println!("Daemon: OFFLINE")
    }
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

// Set Process I/O Priority (Linux CFQ/BFQ)
fn set_process_priority(policy_str: &str) {
    // Constants from linux/ioprio.h
    const IOPRIO_WHO_PROCESS: i32 = 1;
    const IOPRIO_CLASS_RT: i32 = 1;
    const IOPRIO_CLASS_BE: i32 = 2;
    const IOPRIO_CLASS_IDLE: i32 = 3;
    
    let (class, data) = match policy_str {
        "Idle" => (IOPRIO_CLASS_IDLE, 0),
        "Low" => (IOPRIO_CLASS_BE, 7), // Best Effort, Lowest Priority
        "Normal" => (IOPRIO_CLASS_BE, 4), // Best Effort, Default
        "High" => (IOPRIO_CLASS_BE, 0), // Best Effort, Highest Priority
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Status { port } => { handle_status(port); return Ok(()); },
        Commands::Metrics { port } => { 
            println!("{}", http_get(format!("http://127.0.0.1:{}/metrics", port))?.text()?); 
            return Ok(()); 
        },
        Commands::Reload => { handle_reload(); return Ok(()); },
        Commands::Version { sub } => { handle_version_commands(sub).await?; return Ok(()) },
        Commands::Daemon { config } => {
            let cfg = match Config::load(&config) {
                Ok(c) => Arc::new(RwLock::new(c)), 
                Err(e) => { error!("Failed to load config: {}", e); return Ok(()); }
            };
            
            // Apply I/O Priority from Config
            let prio = cfg.read().await.io_priority.clone();
            set_process_priority(&prio);

            let mut mgr = Manager::new(cfg.clone());
            let (queues, handles, shutdown_senders, _hydration_rx) = mgr.start();
            let shutdown = Arc::new(AtomicBool::new(false));
            let sd = shutdown.clone();

            let m_port = cfg.read().await.metrics_port; 
            let fatal = cfg.read().await.fatal_metrics_bind;

            tokio::spawn(async move {
                let app = Router::new().route("/metrics", get(metrics_handler));
                let addr = SocketAddr::from(([0, 0, 0, 0], m_port));
                
                match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => {
                        info!("Metrics listening on {}", addr);
                        if let Err(e) = axum::serve(listener, app).await {
                            error!("Metrics server error: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to bind metrics port {}: {}. Metrics unavailable.", m_port, e);
                        if fatal { std::process::exit(1); }
                    }
                }
            });

            tokio::spawn(async move {
                let mut sighup = signal(SignalKind::hangup()).unwrap();
                let mut sigint = signal(SignalKind::interrupt()).unwrap();
                loop {
                    tokio::select! {
                        _ = sighup.recv() => info!("Received SIGHUP (Hot reload not implemented in this version, restart recommended)"),
                        _ = sigint.recv() => { info!("Shutting down..."); sd.store(true, Ordering::Relaxed); break; }
                    }
                }
            });

            if let Err(e) = bpf::run(queues, shutdown.clone()).await { 
                error!("BPF Error: {}", e); 
                shutdown.store(true, Ordering::Relaxed);
            }

            info!("Draining workers...");
            for tx in shutdown_senders {
                let _ = tx.send(()).await;
            }

            let timeout_secs = cfg.read().await.shutdown_timeout_secs;
            let timeout = std::time::Duration::from_secs(timeout_secs);
            let mut failures = 0;
            for h in handles {
                match tokio::time::timeout(timeout, h).await {
                    Ok(res) => if let Err(e) = res {
                        error!("Worker join error: {}", e);
                        failures += 1;
                    },
                    Err(_) => {
                        warn!("Worker timed out during shutdown");
                        metrics::WORKER_SHUTDOWN_TIMEOUTS.inc();
                        failures += 1;
                    }
                }
            }
            
            info!("Waiting for hydration threads...");
            mgr.wait_hydration();
            
            if failures > 0 {
                warn!("Shutdown complete with {} worker errors/timeouts.", failures);
            } else {
                info!("Shutdown complete gracefully.");
            }
        }
    }

    Ok(())
}
