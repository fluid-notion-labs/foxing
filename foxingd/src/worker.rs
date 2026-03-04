use std::sync::Arc;
use tokio::sync::{mpsc, broadcast, Mutex};
use tracing::{error, info, debug, warn};
use crate::error::{Result, FoxingError};
use fxcp_core::FxcpError;
use crate::mirror::{SourceInfo, SharedConfig};
use crate::config::{TargetConfig};
use fxcp_core::operations::{SmartCopier, CopyStats, probe_capabilities, OptimizedFs, FsyncLatencyTracker};
use std::sync::atomic::Ordering;
use crate::tuner::{BbrTuner, TunerBoard, TunerOutput, GLOBAL_TUNER_REGISTRY, check_global_consistency};
use fxcp_core::governor::Governor;
use std::time::{Duration, Instant};
use crate::identity::{self, ResolveResult};
use fxcp_core::buffer::BufferPool;
use crate::event::{Event, EventType};
use crate::ordering::{Coalescer};
use fxcp_core::consistency::{InMemoryWal, WalOpKind};
use crate::resilience::{PoisonCabinet, CircuitBreaker};
use io_uring::IoUring;
use std::path::{Path, PathBuf};
use libc;
use fxcp_core::sidecar::{AsyncSidecar, SyncSignature, set_sync_signature};
use std::collections::{HashSet, VecDeque};
use fxcp_core::constants;
use tokio::io::unix::AsyncFd;
use tokio::task::spawn_blocking;
use fxcp_core::security;
use std::os::unix::fs::MetadataExt;
use crate::metrics;

const STATIC_RING_DEPTH: u32 = 4096;

#[derive(Debug, Clone)]
pub struct HydrationSender(pub mpsc::UnboundedSender<(PathBuf, Option<u64>)>);

impl HydrationSender {
    pub async fn send_repair_job(&self, path: PathBuf, inode: Option<u64>) {
        let _ = self.0.send((path, inode));
    }
}

#[derive(Debug)]
enum ErrorClass {
    TargetNotFound,
    SourceNotFound,
    Transient,
    Permanent,
}

fn classify_error(err: &FoxingError, event_type: &EventType) -> ErrorClass {
    match err {
        FoxingError::Io(io_err) => match io_err.kind() {
            std::io::ErrorKind::NotFound => {
                // Write-like ops target an existing file — ENOENT means the target
                // hasn't been created yet, so route to repair (full copy).
                // For other ops the source is likely gone (transient lifecycle).
                if matches!(event_type,
                    EventType::Write | EventType::WriteRange | EventType::Clone
                    | EventType::Truncate | EventType::Fallocate)
                {
                    ErrorClass::TargetNotFound
                } else {
                    ErrorClass::SourceNotFound
                }
            },
            std::io::ErrorKind::PermissionDenied => ErrorClass::Permanent,
            std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::Interrupted => ErrorClass::Transient,
            _ => ErrorClass::Transient,
        },
        _ => ErrorClass::Transient,
    }
}

pub struct BarrierCoordinator {
    pub worker_id: usize,
    pub total_workers: usize,
    pub cmd_tx: broadcast::Sender<BarrierCommand>,
    pub cmd_rx: broadcast::Receiver<BarrierCommand>,
    pub ack_tx: mpsc::Sender<usize>,
    pub ack_rx: Option<Arc<Mutex<mpsc::Receiver<usize>>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BarrierCommand { PauseAndFlush, Resume }

struct RetryQueue {
    queue: VecDeque<(Arc<Event>, Instant, u32)>,
    max_retries: u32,
    base_backoff_ms: u64,
    capacity: usize,
}

impl RetryQueue {
    fn new(max_retries: u32, base_backoff_ms: u64, capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            max_retries,
            base_backoff_ms,
            capacity,
        }
    }

    fn push(&mut self, event: Arc<Event>, attempts: u32) {
        if attempts >= self.max_retries {
            error!("RetryQueue: Dropping event after {} attempts: {:?} (Inode: {})", attempts, event.event_type, event.inode);
            return;
        }
        if self.queue.len() >= self.capacity {
            warn!("RetryQueue Overflow: Refusing retry for inode {} (Queue Full)", event.inode);
            crate::metrics::EVENTS_DROPPED.inc();
            return;
        }
        let backoff_ms = (self.base_backoff_ms * 2u64.pow(attempts)).min(5000);
        let backoff = Duration::from_millis(backoff_ms);
        let next_attempt = Instant::now() + backoff;
        
        debug!("RetryQueue: Scheduling retry #{} for inode {} in {:?}", attempts + 1, event.inode, backoff);
        self.queue.push_back((event, next_attempt, attempts + 1));
    }

    #[allow(dead_code)]
    fn pop_ready(&mut self) -> Option<(Arc<Event>, u32)> {
        if let Some((_, time, _)) = self.queue.front() {
            if Instant::now() >= *time {
                if let Some((evt, _, attempts)) = self.queue.pop_front() {
                    return Some((evt, attempts));
                }
            }
        }
        None
    }

    fn pop_ready_batch(&mut self, max: usize) -> Vec<(Arc<Event>, u32)> {
        let mut batch = Vec::new();
        let now = Instant::now();
        while batch.len() < max {
            if let Some((_, time, _)) = self.queue.front() {
                if now >= *time {
                    if let Some((evt, _, attempts)) = self.queue.pop_front() {
                        batch.push((evt, attempts));
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        batch
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

pub async fn run_worker(
    mut event_rx: mpsc::Receiver<Arc<Event>>,
    source: Arc<SourceInfo>,
    target_cfg: TargetConfig,
    config: SharedConfig,
    mut shutdown_rx: mpsc::Receiver<()>,
    hydration_tx: Arc<HydrationSender>,
    _repair_txs: Arc<Vec<mpsc::Sender<Arc<Event>>>>,
    governor: Arc<Governor>,
    tuner_board: TunerBoard,
    worker_id: usize,
    _resume_state: Option<()>,
    mut barrier: BarrierCoordinator,
    daemon_id: String,
    initial_latency: Duration,
    mut external_stats_rx: mpsc::UnboundedReceiver<CopyStats>,
) -> Result<()> {
    info!("Worker {}: Started for target {:?}", worker_id, target_cfg.path);
    let worker_id_str = worker_id.to_string();
    let wal = InMemoryWal::new();
    let poison_cabinet = Arc::new(Mutex::new(PoisonCabinet::new()));
    let circuit_breaker = Arc::new(Mutex::new(CircuitBreaker::new(30)));
    let sidecar = AsyncSidecar::new();
    
    let source_caps = probe_capabilities(&source.path);
    let target_caps = probe_capabilities(&target_cfg.path);

    let mut tuner = BbrTuner::new(
        &target_cfg,
        initial_latency,
        config.read().await.global_buffer_limit * 1024 * 1024,
        target_caps.btrfs_quotas.load(Ordering::Relaxed)
    );

    let mut coalescer = Coalescer::new(target_cfg.ordering_scan_depth);
    let mut retry_queue = RetryQueue::new(
        constants::WORKER_RETRY_QUEUE_MAX_RETRIES,
        constants::WORKER_RETRY_QUEUE_BASE_BACKOFF_MS,
        1000
    );

    let ring_depth = STATIC_RING_DEPTH;
    let ring = IoUring::new(ring_depth).map_err(FoxingError::Io)?;
    let eventfd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    ring.submitter().register_eventfd(eventfd).map_err(FoxingError::Io)?;
    let async_fd = Arc::new(AsyncFd::new(eventfd).map_err(FoxingError::Io)?);

    let io_buffer_size = target_cfg.io_buffer_size_mib * 1024 * 1024;
    let num_buffers = (io_buffer_size / 4096).max(ring_depth as u64) as usize;
    let mut buffer_pool = BufferPool::new(num_buffers, 4096, constants::MINIMUM_ALIGNMENT_BYTES)?;
    
    {
        let iovs = buffer_pool.as_io_vecs();
        unsafe { ring.submitter().register_buffers(&iovs) }.map_err(|e| FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
    }

    let skip_fsync = constants::ONE_SHOT_MODE.load(Ordering::Relaxed);

    let mut smart_copier = SmartCopier {
        ring,
        buffer_pool,
        atomic_buffer_pool: None,
        async_fd,
        vdo_opt: target_cfg.vdo_optimization,
        direct_io_ok: target_cfg.direct_io_ok.load(Ordering::Relaxed),
        source_caps: source_caps.clone(),
        target_caps: target_caps.clone(),
        vdo_stall_threshold: target_cfg.vdo_stall_threshold,
        barrier_callback: None,
        source_uncached: target_cfg.source_uncached,
        target_uncached: target_cfg.target_uncached,
        governor: Some(governor.clone()),
        fsync_tracker: FsyncLatencyTracker::default(),
        skip_fsync,
    };

    let mut dirty_tracker: HashSet<u64> = HashSet::new();
    
    let mut flush_interval = tokio::time::interval(Duration::from_micros(tuner.current_flush_us));
    let mut tune_interval = tokio::time::interval(Duration::from_micros(constants::WORKER_TUNE_INTERVAL_US));
    tune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut retry_interval = tokio::time::interval(Duration::from_millis(constants::WORKER_RETRY_QUEUE_BASE_BACKOFF_MS));
    retry_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut last_tune = Instant::now();
    let mut bytes_since_tune = 0u64;
    let mut ops_since_tune = 0u64;
    let mut _events_since_tune = 0;
    
    let mut accumulated_latency = Duration::ZERO;
    let mut max_latency_in_window = Duration::ZERO;
    let mut latency_samples_count = 0;
    let mut peak_coalescer_len = 0;

    let path_label = &target_cfg.label;
    
    let (telemetry_tx, mut telemetry_rx) = tokio::sync::mpsc::unbounded_channel::<TunerOutput>();
    let board_clone = tuner_board.clone();
    let path_label_clone = path_label.to_string();
    let worker_id_label = worker_id_str.clone();
    let worker_id_idx = worker_id;

    tokio::spawn(async move {
        let mut last_update = Instant::now();
        let mut sanity_check_interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                Some(output) = telemetry_rx.recv() => {
                    if last_update.elapsed() > Duration::from_millis(250) {
                        board_clone.insert(PathBuf::from(&path_label_clone), output.state);
                        
                        // FIXED: Wrap TunerOutput in Arc to prevent type mismatch with DashMap<..., Arc<TunerOutput>>
                        GLOBAL_TUNER_REGISTRY.insert((path_label_clone.clone(), worker_id_idx), Arc::new(output.clone()));
                        
                        metrics::TARGET_BATCH_SIZE.with_label_values(&[&path_label_clone, &worker_id_label]).set(output.batch_size as f64);
                        metrics::TARGET_COALESCE_BYTES.with_label_values(&[&path_label_clone, &worker_id_label]).set(output.coalesce_bytes as f64);
                        metrics::TUNER_STATE.with_label_values(&[&path_label_clone, &worker_id_label]).set(output.state as i64 as f64);
                        metrics::TARGET_FLUSH_INTERVAL_MS.with_label_values(&[&path_label_clone, &worker_id_label]).set(output.flush_us as f64 / 1000.0);
                        metrics::TARGET_STORAGE_CLASS.with_label_values(&[&path_label_clone, &worker_id_label]).set(output.storage_class as i64 as f64);
                        
                        last_update = Instant::now();
                    }
                }
                _ = sanity_check_interval.tick() => {
                    if worker_id_idx == 0 {
                        check_global_consistency();
                    }
                }
                else => break,
            }
        }
    });

    loop {
        if last_tune.elapsed().as_micros() > constants::WORKER_TUNE_INTERVAL_US as u128 {
            let elapsed = last_tune.elapsed().as_secs_f64();
            let current_usage = crate::metrics::GLOBAL_BUFFER_COUNT.load(Ordering::Relaxed);
            
            let sample_latency = if latency_samples_count > 0 {
                accumulated_latency / latency_samples_count
            } else {
                Duration::ZERO
            };

            let output = tuner.tune(
                elapsed,
                bytes_since_tune,
                ops_since_tune,
                governor.current_stress_score(),
                coalescer.len(),
                10_000,
                4096,
                governor.current_memory_usage_pct(),
                sample_latency,
                max_latency_in_window,
                current_usage,
                constants::AVG_EVENT_OVERHEAD_BYTES,
                target_caps.btrfs_quotas.load(Ordering::Relaxed)
            );
            
            let _ = telemetry_tx.send(output.clone());
            
            let flush_micros = output.flush_us;
            flush_interval = tokio::time::interval(Duration::from_micros(flush_micros));
            
            let util_pct = (peak_coalescer_len as f64 / 10_000.0).clamp(0.0, 1.0);
            metrics::WORKER_BUFFER_UTILIZATION.with_label_values(&[path_label, &worker_id_str]).set(util_pct);
            metrics::WORKER_RETRY_QUEUE_SIZE.with_label_values(&[path_label, &worker_id_str]).set(retry_queue.len() as f64);

            last_tune = Instant::now();
            bytes_since_tune = 0;
            ops_since_tune = 0;
            _events_since_tune = 0;
            accumulated_latency = Duration::ZERO;
            max_latency_in_window = Duration::ZERO;
            latency_samples_count = 0;
            peak_coalescer_len = 0;
        }

        // Idle detection: when no pending work, reduce polling to save CPU
        if coalescer.is_empty() && retry_queue.is_empty() {
            // No work pending — next interesting event is either:
            // 1. A new event from BPF (event_rx)
            // 2. A stats update from hydration (external_stats_rx)
            // 3. Shutdown signal
            // All of these will properly wake through tokio channels.
            // Don't spin on timer branches.
        }

        // Retry pressure: force-flush coalescer when retry queue is growing
        // This prevents event accumulation while retries stagnate on slow targets
        if retry_queue.len() > 100 && !coalescer.is_empty() {
            let flush_count = (retry_queue.len() / 100).min(8);
            for _ in 0..flush_count {
                if let Some(evt) = coalescer.pop_batch(0, Duration::ZERO, false) {
                    let start_time = Instant::now();
                    let (res, sc, dt) = process_single_event_with_wal(
                        evt.clone(), source.clone(), target_cfg.clone(), wal.clone(),
                        poison_cabinet.clone(), circuit_breaker.clone(), daemon_id.clone(),
                        smart_copier, sidecar.clone(), dirty_tracker, worker_id, hydration_tx.clone(), 0
                    ).await;
                    smart_copier = sc;
                    dirty_tracker = dt;
                    let duration = start_time.elapsed();
                    match res {
                        Ok(stats) => {
                            bytes_since_tune += stats.bytes_processed;
                            ops_since_tune += stats.ops_count.max(1);
                            accumulated_latency += duration;
                            if duration > max_latency_in_window { max_latency_in_window = duration; }
                            latency_samples_count += 1;
                        },
                        Err(e) => {
                            accumulated_latency += duration;
                            if duration > max_latency_in_window { max_latency_in_window = duration; }
                            latency_samples_count += 1;

                            match classify_error(&e, &evt.event_type) {
                                ErrorClass::TargetNotFound => {
                                    let abs_path = source.path.join(&evt.name);
                                    hydration_tx.send_repair_job(abs_path, Some(evt.inode)).await;
                                    metrics::EVENTS_REPAIR_QUEUED.inc();
                                },
                                ErrorClass::SourceNotFound => {
                                    debug!("Worker {}: Source gone for inode {} (transient lifecycle)", worker_id, evt.inode);
                                    metrics::EVENTS_SOURCE_GONE.inc();
                                },
                                ErrorClass::Transient => {
                                    retry_queue.push(evt, 0);
                                },
                                ErrorClass::Permanent => {
                                    error!("Worker {}: Permanent error for inode {}: {:?}", worker_id, evt.inode, e);
                                    metrics::EVENTS_DROPPED.inc();
                                },
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }

        tokio::select! {
            biased;

            _ = shutdown_rx.recv() => {
                info!("Worker {}: Shutdown signal received.", worker_id);
                break;
            }
            
            Some(stats) = external_stats_rx.recv() => {
                if !stats.io_duration.is_zero() && stats.ops_count > 0 {
                    let avg_op_latency = stats.io_duration.div_f64(stats.ops_count as f64);
                    accumulated_latency += avg_op_latency;
                    if avg_op_latency > max_latency_in_window { max_latency_in_window = avg_op_latency; }
                    latency_samples_count += 1;
                }
                bytes_since_tune += stats.bytes_processed;
                ops_since_tune += stats.ops_count;
            }

            Ok(cmd) = barrier.cmd_rx.recv() => {
                match cmd {
                    BarrierCommand::PauseAndFlush => {
                        debug!("Worker {}: Pausing for barrier...", worker_id);
                        // Drain pending events
                        while let Some(evt) = coalescer.pop_batch(0, Duration::ZERO, false) {
                             let start_time = Instant::now();
                             let (res, sc, dt) = process_single_event_with_wal(
                                evt, source.clone(), target_cfg.clone(), wal.clone(), 
                                poison_cabinet.clone(), circuit_breaker.clone(), daemon_id.clone(),
                                smart_copier, sidecar.clone(), dirty_tracker, worker_id, hydration_tx.clone(), 0
                            ).await;
                            
                            smart_copier = sc;
                            dirty_tracker = dt;
                            let duration = start_time.elapsed();
                            
                            if let Ok(stats) = res {
                                bytes_since_tune += stats.bytes_processed;
                                ops_since_tune += stats.ops_count.max(1);
                                accumulated_latency += duration;
                                if duration > max_latency_in_window { max_latency_in_window = duration; }
                                latency_samples_count += 1;
                            }
                        }
                        
                        let _ = barrier.ack_tx.send(worker_id).await;
                        
                        // Wait for resume
                        loop {
                            match barrier.cmd_rx.recv().await {
                                Ok(BarrierCommand::Resume) => {
                                    debug!("Worker {}: Resuming...", worker_id);
                                    break;
                                },
                                Ok(_) => {},
                                Err(_) => break,
                            }
                        }
                    },
                    BarrierCommand::Resume => {}
                }
            }

            _ = retry_interval.tick(), if !retry_queue.is_empty() => {
                let batch = retry_queue.pop_ready_batch(16);
                for (evt, attempts) in batch {
                    let start_time = Instant::now();
                    let (res, sc, dt) = process_single_event_with_wal(
                        evt.clone(), source.clone(), target_cfg.clone(), wal.clone(),
                        poison_cabinet.clone(), circuit_breaker.clone(), daemon_id.clone(),
                        smart_copier, sidecar.clone(), dirty_tracker, worker_id, hydration_tx.clone(), attempts
                    ).await;

                    smart_copier = sc;
                    dirty_tracker = dt;
                    let duration = start_time.elapsed();

                    match res {
                        Ok(stats) => {
                            bytes_since_tune += stats.bytes_processed;
                            ops_since_tune += stats.ops_count.max(1);
                            accumulated_latency += duration;
                            if duration > max_latency_in_window { max_latency_in_window = duration; }
                            latency_samples_count += 1;
                            let e2e = evt.created_at.elapsed().as_secs_f64();
                            metrics::REPLICATION_LATENCY.with_label_values(&[path_label]).observe(e2e);
                            let epoch_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as f64;
                            metrics::WORKER_LAST_COPY_EPOCH_MS.with_label_values(&[path_label, &worker_id_str]).set(epoch_ms);
                        },
                        Err(e) => {
                            accumulated_latency += duration;
                            if duration > max_latency_in_window { max_latency_in_window = duration; }
                            latency_samples_count += 1;

                            match classify_error(&e, &evt.event_type) {
                                ErrorClass::TargetNotFound => {
                                    let abs_path = source.path.join(&evt.name);
                                    hydration_tx.send_repair_job(abs_path, Some(evt.inode)).await;
                                    metrics::EVENTS_REPAIR_QUEUED.inc();
                                },
                                ErrorClass::SourceNotFound => {
                                    debug!("Worker {}: Source gone for inode {} (transient lifecycle)", worker_id, evt.inode);
                                    metrics::EVENTS_SOURCE_GONE.inc();
                                },
                                ErrorClass::Transient => {
                                    warn!("Worker {}: Retry #{} failed for inode {}: {:?}", worker_id, attempts, evt.inode, e);
                                    retry_queue.push(evt, attempts);
                                },
                                ErrorClass::Permanent => {
                                    error!("Worker {}: Permanent error for inode {}: {:?}", worker_id, evt.inode, e);
                                    metrics::EVENTS_DROPPED.inc();
                                },
                            }
                        }
                    }
                }
            }

            _ = flush_interval.tick(), if !coalescer.is_empty() => {
                if let Some(evt) = coalescer.pop_batch(0, Duration::ZERO, false) {
                    let start_time = Instant::now();
                    let (res, sc, dt) = process_single_event_with_wal(
                        evt.clone(), source.clone(), target_cfg.clone(), wal.clone(), 
                        poison_cabinet.clone(), circuit_breaker.clone(), daemon_id.clone(),
                        smart_copier, sidecar.clone(), dirty_tracker, worker_id, hydration_tx.clone(), 0
                    ).await;
                    
                    smart_copier = sc;
                    dirty_tracker = dt;
                    let duration = start_time.elapsed();

                    if let Ok(stats) = res {
                        bytes_since_tune += stats.bytes_processed;
                        ops_since_tune += stats.ops_count.max(1);
                        accumulated_latency += duration;
                        if duration > max_latency_in_window { max_latency_in_window = duration; }
                        latency_samples_count += 1;
                        let e2e = evt.created_at.elapsed().as_secs_f64();
                        metrics::REPLICATION_LATENCY.with_label_values(&[path_label]).observe(e2e);
                        let epoch_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64;
                        metrics::WORKER_LAST_COPY_EPOCH_MS.with_label_values(&[path_label, &worker_id_str]).set(epoch_ms);
                    } else if let Err(e) = res {
                        accumulated_latency += duration;
                        if duration > max_latency_in_window { max_latency_in_window = duration; }
                        latency_samples_count += 1;

                        match classify_error(&e, &evt.event_type) {
                            ErrorClass::TargetNotFound => {
                                let abs_path = source.path.join(&evt.name);
                                hydration_tx.send_repair_job(abs_path, Some(evt.inode)).await;
                                metrics::EVENTS_REPAIR_QUEUED.inc();
                            },
                            ErrorClass::SourceNotFound => {
                                debug!("Worker {}: Source gone for inode {} (transient lifecycle)", worker_id, evt.inode);
                                metrics::EVENTS_SOURCE_GONE.inc();
                            },
                            ErrorClass::Transient => {
                                warn!("Worker {}: Event failed (queued for retry): {:?}", worker_id, e);
                                retry_queue.push(evt, 0);
                            },
                            ErrorClass::Permanent => {
                                error!("Worker {}: Permanent error for inode {}: {:?}", worker_id, evt.inode, e);
                                metrics::EVENTS_DROPPED.inc();
                            },
                        }
                    }
                }
            }
            
            // Idle detection: when no events and no work pending, sleep to avoid spinning.
            // The event_rx.recv() below will properly wake when events arrive.
            _ = tune_interval.tick(), if !coalescer.is_empty() || !retry_queue.is_empty() => {}

            Some(evt) = event_rx.recv(), if coalescer.len() < 10000 => {
                coalescer.push(evt);
                if coalescer.len() > peak_coalescer_len { peak_coalescer_len = coalescer.len(); }
                
                if let Some(batch_evt) = coalescer.pop_batch(tuner.current_coalesce_bytes, Duration::from_micros(tuner.current_flush_us), true) {
                    let start_time = Instant::now();
                    let (res, sc, dt) = process_single_event_with_wal(
                        batch_evt.clone(), source.clone(), target_cfg.clone(), wal.clone(), 
                        poison_cabinet.clone(), circuit_breaker.clone(), daemon_id.clone(),
                        smart_copier, sidecar.clone(), dirty_tracker, worker_id, hydration_tx.clone(), 0
                    ).await;
                    
                    smart_copier = sc;
                    dirty_tracker = dt;
                    let duration = start_time.elapsed();

                    match res {
                        Ok(stats) => {
                            bytes_since_tune += stats.bytes_processed;
                            ops_since_tune += stats.ops_count.max(1);
                            accumulated_latency += duration;
                            if duration > max_latency_in_window { max_latency_in_window = duration; }
                            latency_samples_count += 1;
                            let e2e = batch_evt.created_at.elapsed().as_secs_f64();
                            metrics::REPLICATION_LATENCY.with_label_values(&[path_label]).observe(e2e);
                            let epoch_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as f64;
                            metrics::WORKER_LAST_COPY_EPOCH_MS.with_label_values(&[path_label, &worker_id_str]).set(epoch_ms);
                        },
                        Err(e) => {
                            accumulated_latency += duration;
                            if duration > max_latency_in_window { max_latency_in_window = duration; }
                            latency_samples_count += 1;

                            match classify_error(&e, &batch_evt.event_type) {
                                ErrorClass::TargetNotFound => {
                                    let abs_path = source.path.join(&batch_evt.name);
                                    hydration_tx.send_repair_job(abs_path, Some(batch_evt.inode)).await;
                                    metrics::EVENTS_REPAIR_QUEUED.inc();
                                },
                                ErrorClass::SourceNotFound => {
                                    debug!("Worker {}: Source gone for inode {} (transient lifecycle)", worker_id, batch_evt.inode);
                                    metrics::EVENTS_SOURCE_GONE.inc();
                                },
                                ErrorClass::Transient => {
                                    warn!("Worker {}: Batch failed: {:?}", worker_id, e);
                                    retry_queue.push(batch_evt, 0);
                                },
                                ErrorClass::Permanent => {
                                    error!("Worker {}: Permanent error for inode {}: {:?}", worker_id, batch_evt.inode, e);
                                    metrics::EVENTS_DROPPED.inc();
                                },
                            }
                        }
                    }
                }
            }
        }
    }

    info!("Worker {}: Shutdown complete.", worker_id);
    Ok(())
}

async fn resolve_event_path(source: &Arc<SourceInfo>, parent_inode: u64, name: &str) -> Result<PathBuf> {
    if parent_inode == 0 { return Ok(PathBuf::from(name)); }
    if let Some(dir_entry) = source.dir_map.get(&parent_inode) {
        return Ok(dir_entry.value().join(name));
    }
    Ok(PathBuf::from(name))
}

#[allow(clippy::too_many_arguments)]
async fn process_single_event_with_wal(
    event: Arc<Event>,
    source: Arc<SourceInfo>,
    target_cfg: TargetConfig,
    wal: Arc<InMemoryWal>,
    _poison_cabinet: Arc<Mutex<PoisonCabinet>>,
    _circuit_breaker: Arc<Mutex<CircuitBreaker>>,
    _daemon_id: String,
    mut smart_copier: SmartCopier,
    sidecar: AsyncSidecar,
    mut dirty_tracker: HashSet<u64>,
    worker_id: usize,
    hydration_tx: Arc<HydrationSender>,
    _attempts: u32,
) -> (Result<CopyStats>, SmartCopier, HashSet<u64>) {
    let target_label = target_cfg.label.to_string();
    
    // Validate generation to prevent recycled inode corruption
    if event.generation != std::u32::MAX {
        if let Some(entry) = source.inode_map.get(&event.inode) {
            let stored_gen = *entry.generation.read();
            if stored_gen != std::u32::MAX && stored_gen != event.generation {
                return (
                    Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Generation mismatch - retry"))),
                    smart_copier,
                    dirty_tracker
                );
            }
        }
    }

    let op_kind = match event.event_type {
        EventType::Rename | EventType::RenameIncomplete => WalOpKind::Rename,
        EventType::Truncate => WalOpKind::Truncate,
        EventType::Write | EventType::WriteRange => WalOpKind::Write,
        EventType::Barrier => WalOpKind::Barrier,
        _ => WalOpKind::Barrier,
    };

    // WAL Barrier - Ensures ordering correctness for dependent operations
    let op_guard = wal.acquire_barrier(event.inode, op_kind).await;

    let target_path_res = identity::resolve_target(&source.inode_map, &source.dir_map, &event, &target_cfg.path);
    let target_path = match target_path_res {
        ResolveResult::Success(p, _, _) => p,
        ResolveResult::NeedsRepair(p) => p,
        ResolveResult::SecurityBlock => {
            error!("Worker: Security Policy Violation - Access denied for inode {} (Path Traversal/Symlink Race)", event.inode);
            return (Err(FoxingError::Security("Path traversal or symlink race detected".into())), smart_copier, dirty_tracker);
        }
    };

    if let Ok(rel_path) = target_path.strip_prefix(&target_cfg.path) {
        if target_cfg.is_path_excluded(rel_path) {
            debug!("Worker: Dropping event for excluded path: {:?}", rel_path);
            crate::metrics::EVENTS_FILTERED.with_label_values(&[&target_label]).inc();
            return (Ok(CopyStats::default()), smart_copier, dirty_tracker);
        }
    }

    // Hydration gate: during initial sync, proactively route events for
    // unhydrated files to repair instead of attempting (and failing) partial writes
    if source.hydration.active.load(std::sync::atomic::Ordering::Relaxed)
        && !source.hydrated_inodes.contains(&event.inode)
        && matches!(event.event_type,
            EventType::Write | EventType::WriteRange | EventType::Clone
            | EventType::Truncate | EventType::Fallocate)
    {
        if !target_path.exists() {
            let abs_path = source.path.join(&event.name);
            hydration_tx.send_repair_job(abs_path, Some(event.inode)).await;
            metrics::HYDRATION_GATE_REDIRECTED.inc();
            return (Ok(CopyStats::default()), smart_copier, dirty_tracker);
        }
    }

    let op_stats = CopyStats::default();
    let mut op_result: Result<CopyStats> = Ok(op_stats);

    match event.event_type {
        EventType::Clone => {
            let rel = target_path.strip_prefix(&target_cfg.path).unwrap_or(Path::new(""));
            let source_path = source.mount.join(rel);

            if !source_path.exists() {
                debug!("Worker {}: Source {:?} gone, skipping clone", worker_id, source_path);
                metrics::EVENTS_SOURCE_GONE.inc();
                return (Ok(CopyStats::default()), smart_copier, dirty_tracker);
            }

            let size = match std::fs::metadata(&source_path) {
                Ok(m) => m.len(),
                Err(_) => event.length
            };

            if target_cfg.enable_versioning {
                let root_path = target_cfg.path.clone();
                let target_path_clone = target_path.clone();
                let event_clone = event.clone();
                let snapshot_result = spawn_blocking(move || {
                    fxcp_core::security::create_version_snapshot(
                        &target_path_clone,
                        event_clone.seq_num,
                        &root_path,
                        event_clone.inode,
                    )
                }).await.map_err(FoxingError::Join);

                if let Ok(res) = snapshot_result {
                    match res {
                        Ok(Some(version)) => {
                            source.version_index.register(version);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!("Worker {}: Versioning failed for clone op on {:?}: {}", worker_id, target_path, e);
                            crate::metrics::VERSIONING_FAILURES.inc();
                        }
                    }
                }
            }

            if !dirty_tracker.contains(&event.inode) {
                sidecar.set_dirty_blind(target_path.clone());
                dirty_tracker.insert(event.inode);
            }

            let copy_timeout = {
                let base_secs = 60u64;
                let size_mb = (size / (10 * 1024 * 1024)).max(1) as u64;
                std::time::Duration::from_secs((base_secs + size_mb * 60).min(300))
            };
            metrics::WORKER_COPY_IN_FLIGHT.with_label_values(&[&target_label, &worker_id.to_string()]).inc();
            match tokio::time::timeout(copy_timeout, smart_copier.optimized_copy_range(
                source_path,
                target_path.clone(),
                event.offset,
                event.length,
                size,
                target_label.clone(),
                None,
                false,
            )).await {
                Ok(inner) => { op_result = inner.map_err(Into::into); }
                Err(_elapsed) => {
                    warn!("Worker {}: Copy timed out after {:?} for inode {} (size={})",
                          worker_id, copy_timeout, event.inode, size);
                    metrics::COPY_TIMEOUT_TOTAL.with_label_values(&[&target_label]).inc();
                    op_result = Err(FoxingError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("Copy timed out after {:?}", copy_timeout)
                    )));
                }
            };
            metrics::WORKER_COPY_IN_FLIGHT.with_label_values(&[&target_label, &worker_id.to_string()]).dec();
        },
        EventType::Write | EventType::WriteRange => {
            let rel = target_path.strip_prefix(&target_cfg.path).unwrap_or(Path::new(""));
            let source_path = source.mount.join(rel);

            if !source_path.exists() {
                debug!("Worker {}: Source {:?} gone, skipping write", worker_id, source_path);
                metrics::EVENTS_SOURCE_GONE.inc();
                return (Ok(CopyStats::default()), smart_copier, dirty_tracker);
            }

            let size = match std::fs::metadata(&source_path) {
                Ok(m) => m.len(),
                Err(_) => event.length
            };

            if target_cfg.enable_versioning {
                let root_path = target_cfg.path.clone();
                let target_path_clone = target_path.clone();
                let event_clone = event.clone();
                
                let snapshot_result = spawn_blocking(move || {
                    fxcp_core::security::create_version_snapshot(
                        &target_path_clone,
                        event_clone.seq_num,
                        &root_path,
                        event_clone.inode,
                    )
                }).await.map_err(FoxingError::Join);

                if let Ok(res) = snapshot_result {
                    match res {
                        Ok(Some(version)) => {
                            source.version_index.register(version);
                            debug!("Worker {}: Snapshot registered for inode {}", worker_id, event.inode);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            let is_unsupported = if let FxcpError::Io(io_err) = &e {
                                io_err.kind() == std::io::ErrorKind::Unsupported
                            } else {
                                false
                            };
                            
                            if is_unsupported {
                                 warn!("Worker {}: Versioning skipped for {:?}: Filesystem does not support required features (Reflink/CoW).", worker_id, target_path);
                            } else {
                                 error!("Worker {}: Versioning CRITICAL FAILURE for {:?}: {}. Data loss risk.", worker_id, target_path, e);
                            }
                            crate::metrics::VERSIONING_FAILURES.inc();
                        }
                    }
                }
            }

            if !dirty_tracker.contains(&event.inode) {
                sidecar.set_dirty_blind(target_path.clone());
                dirty_tracker.insert(event.inode);
            }

            let copy_timeout = {
                let base_secs = 60u64;
                let size_mb = (size / (10 * 1024 * 1024)).max(1) as u64;
                std::time::Duration::from_secs((base_secs + size_mb * 60).min(300))
            };
            metrics::WORKER_COPY_IN_FLIGHT.with_label_values(&[&target_label, &worker_id.to_string()]).inc();
            match tokio::time::timeout(copy_timeout, smart_copier.optimized_copy_range(
                source_path,
                target_path.clone(),
                event.offset,
                event.length,
                size,
                target_label.clone(),
                None,
                false,
            )).await {
                Ok(inner) => { op_result = inner.map_err(Into::into); }
                Err(_elapsed) => {
                    warn!("Worker {}: Copy timed out after {:?} for inode {} (size={})",
                          worker_id, copy_timeout, event.inode, size);
                    metrics::COPY_TIMEOUT_TOTAL.with_label_values(&[&target_label]).inc();
                    op_result = Err(FoxingError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("Copy timed out after {:?}", copy_timeout)
                    )));
                }
            };
            metrics::WORKER_COPY_IN_FLIGHT.with_label_values(&[&target_label, &worker_id.to_string()]).dec();
        },
        EventType::Rename => {
            let old_rel_res = resolve_event_path(&source, event.parent_inode, &event.name).await;
            match old_rel_res {
                Ok(old_rel) => {
                    let old_path = target_cfg.path.join(old_rel);
                    
                    if let Some(parent) = target_path.parent() {
                        if !parent.exists() { let _ = std::fs::create_dir_all(parent); }
                    }
                    
                    let rename_timeout = std::time::Duration::from_secs(60);
                    match tokio::time::timeout(rename_timeout, smart_copier.optimized_rename(old_path.clone(), target_path.clone(), event.flags)).await {
                        Ok(inner) => { op_result = inner.map_err(Into::into); }
                        Err(_elapsed) => {
                            warn!("Worker {}: Rename timed out after {:?} for inode {}",
                                  worker_id, rename_timeout, event.inode);
                            metrics::COPY_TIMEOUT_TOTAL.with_label_values(&[&target_label]).inc();
                            op_result = Err(FoxingError::Io(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                format!("Rename timed out after {:?}", rename_timeout)
                            )));
                        }
                    };

                    // Update identity map if directory
                    if event.mode & libc::S_IFDIR as u32 != 0 {
                        if let Some(new_name) = &event.new_name {
                            if let Ok(new_rel) = resolve_event_path(&source, event.new_parent_inode, new_name).await {
                                identity::update_map_after_rename(
                                    &source.inode_map, &source.dir_map, event.dev_id, event.inode,
                                    new_rel.clone(), event.generation, true, event.timestamp_ns, event.seq_num
                                );
                            }
                        }
                    }
                },
                Err(e) => op_result = Err(e),
            }
        },
        EventType::RenameIncomplete => {
            // Best effort recovery for partial rename events
            let old_rel = if let Some(entry) = source.inode_map.get(&event.inode) {
                entry.primary_path()
            } else {
                PathBuf::from(&event.name)
            };
            let old_path = target_cfg.path.join(&old_rel);
            
            let new_path_res = spawn_blocking({
                let src_clone = source.clone();
                let inode = event.inode;
                let generation = event.generation;
                move || identity::resolve_and_update_path(&src_clone, inode, generation, 0, 0)
            }).await.map_err(FoxingError::Join);

            if let Ok(Ok(new_path_full)) = new_path_res {
                if let Ok(new_rel) = new_path_full.strip_prefix(&source.mount) {
                    let new_target_path = target_cfg.path.join(new_rel);
                    
                    if let Some(parent) = new_target_path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    
                    info!("Worker: Executing recovered rename: {:?} -> {:?}", old_path, new_target_path);
                    let rename_timeout = std::time::Duration::from_secs(60);
                    match tokio::time::timeout(rename_timeout, smart_copier.optimized_rename(old_path, new_target_path, 0)).await {
                        Ok(inner) => { op_result = inner.map_err(Into::into); }
                        Err(_elapsed) => {
                            warn!("Worker {}: RenameIncomplete timed out after {:?} for inode {}",
                                  worker_id, rename_timeout, event.inode);
                            metrics::COPY_TIMEOUT_TOTAL.with_label_values(&[&target_label]).inc();
                            op_result = Err(FoxingError::Io(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                format!("RenameIncomplete timed out after {:?}", rename_timeout)
                            )));
                        }
                    };
                } else {
                    op_result = Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to strip prefix from recovered path")));
                }
            } else {
                 op_result = Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to recover path for incomplete rename")));
            }
        },
        EventType::Truncate => {
            if !dirty_tracker.contains(&event.inode) {
                sidecar.set_dirty_blind(target_path.clone());
                dirty_tracker.insert(event.inode);
            }
            let truncate_timeout = std::time::Duration::from_secs(60);
            match tokio::time::timeout(truncate_timeout, smart_copier.optimized_truncate(target_path.clone(), event.length)).await {
                Ok(inner) => { op_result = inner.map_err(Into::into); }
                Err(_elapsed) => {
                    warn!("Worker {}: Truncate timed out after {:?} for inode {}",
                          worker_id, truncate_timeout, event.inode);
                    metrics::COPY_TIMEOUT_TOTAL.with_label_values(&[&target_label]).inc();
                    op_result = Err(FoxingError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("Truncate timed out after {:?}", truncate_timeout)
                    )));
                }
            };
        },
        EventType::Unlink | EventType::Rmdir => {
             let event_clone = event.clone();
             dirty_tracker.remove(&event.inode);
             let path_to_remove = target_path.clone();
             
             let res = spawn_blocking(move || {
                if event_clone.event_type == EventType::Rmdir {
                    std::fs::remove_dir(path_to_remove)
                } else {
                    std::fs::remove_file(path_to_remove)
                }
            }).await.map_err(FoxingError::Join);

            match res {
                Ok(Ok(_)) => {
                    identity::remove_entry(&source.inode_map, &source.dir_map, event.dev_id, event.inode);
                },
                Ok(Err(e)) => op_result = Err(FoxingError::Io(e)),
                Err(e) => op_result = Err(e.into()),
            }
        },
        EventType::Mkdir | EventType::Create | EventType::Mknod => {
             let target_path_clone = target_path.clone();
             let event_clone = event.clone();
             
             let fs_res = spawn_blocking(move || {
                 if let Some(parent) = target_path_clone.parent() {
                     let _ = std::fs::create_dir_all(parent);
                 }
                 
                 if event_clone.event_type == EventType::Mkdir {
                     let _ = std::fs::create_dir_all(&target_path_clone);
                 } else if event_clone.event_type == EventType::Mknod {
                     security::create_mknod(&target_path_clone, event_clone.mode, event_clone.dev_id as u64)?;
                 } else {
                     let _ = std::fs::File::create(&target_path_clone);
                 }
                 
                 security::set_ownership(&target_path_clone, event_clone.uid, event_clone.gid)?;
                 Ok::<(), FoxingError>(())
             }).await.map_err(FoxingError::Join);

             if let Ok(Ok(_)) = fs_res {
                 let rel = target_path.strip_prefix(&target_cfg.path).unwrap_or(Path::new(""));
                 let source_path = source.mount.join(rel);
                 let target_path_meta = target_path.clone();
                 
                 let _ = spawn_blocking(move || {
                     security::apply_metadata(&source_path, &target_path_meta)?;
                     security::sync_xattrs(&source_path, &target_path_meta);
                     
                     if let Ok(sig) = SyncSignature::compute(&target_path_meta) {
                        let _ = set_sync_signature(&target_path_meta, &sig);
                     }
                     Ok::<(), FoxingError>(())
                 }).await;

                 if let Ok(rel) = target_path.strip_prefix(&target_cfg.path) {
                      identity::update_map(
                        &source.inode_map, &source.dir_map, event.dev_id, event.inode,
                        rel.to_path_buf(), event.generation, false, event.event_type == EventType::Mkdir,
                        event.timestamp_ns, event.seq_num
                      );
                 }
             } else if let Ok(Err(e)) = fs_res {
                 op_result = Err(e);
             } else {
                 op_result = Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Thread join error")));
             }
        },
        EventType::Link => {
             if let Some(entry) = source.inode_map.get(&event.inode) {
                 let existing_rel = entry.primary_path();
                 let existing_path = target_cfg.path.join(existing_rel);
                 let new_path = target_path.clone();
                 let new_path_link = new_path.clone();
                 
                 let link_res = spawn_blocking(move || {
                     security::create_hard_link(&existing_path, &new_path_link)
                 }).await.map_err(FoxingError::Join);

                 match link_res {
                     Ok(Ok(_)) => {
                         let meta = spawn_blocking(move || {
                             std::fs::metadata(&new_path)
                         }).await.map_err(FoxingError::Join);
                         
                         if let Ok(Ok(meta)) = meta {
                             if meta.nlink() as u32 != event.nlink {
                                 warn!("Worker {}: Hardlink count mismatch for inode {}. Event: {}, Target: {}. Triggering repair.",
                                       worker_id, event.inode, event.nlink, meta.nlink());
                                 let rel_path = target_path.strip_prefix(&target_cfg.path)
                                    .unwrap_or(Path::new(""))
                                    .to_path_buf();
                                 let abs_source_path = source.path.join(&rel_path);
                                 hydration_tx.send_repair_job(abs_source_path, Some(event.inode)).await;
                             }
                         }
                         
                         if let Ok(rel) = target_path.strip_prefix(&target_cfg.path) {
                             identity::update_map(
                                &source.inode_map, &source.dir_map, event.dev_id, event.inode,
                                rel.to_path_buf(), event.generation, false, false,
                                event.timestamp_ns, event.seq_num
                             );
                         }
                     },
                     Ok(Err(e)) => op_result = Err(e.into()),
                     Err(e) => op_result = Err(e.into()),
                 }
             } else {
                 warn!("Worker {}: Cannot link inode {}. Source path not found in map. Triggering repair.", worker_id, event.inode);
                 let rel_path = target_path.strip_prefix(&target_cfg.path)
                    .unwrap_or(Path::new(""))
                    .to_path_buf();
                 let abs_source_path = source.path.join(&rel_path);
                 hydration_tx.send_repair_job(abs_source_path, Some(event.inode)).await;
             }
        },
        EventType::SetFlags => {
             let p = target_path.clone();
             let flags = event.flags;
             let res = spawn_blocking(move || security::set_file_attr(&p, flags)).await.map_err(FoxingError::Join);
             if let Ok(Err(e)) = res { op_result = Err(e.into()); }
             else if let Err(e) = res { op_result = Err(e.into()); }
        },
        EventType::Lock | EventType::Flock => {
             let p = target_path.clone();
             let flags = event.flags;
             let map = source.lock_map.clone();
             let res = spawn_blocking(move || fxcp_core::operations::apply_lock(&p, flags, &map)).await.map_err(FoxingError::Join);
             if let Ok(Err(e)) = res { op_result = Err(e.into()); }
             else if let Err(e) = res { op_result = Err(e.into()); }
        },
        EventType::Fallocate => {
            if !dirty_tracker.contains(&event.inode) {
                sidecar.set_dirty_blind(target_path.clone());
                dirty_tracker.insert(event.inode);
            }
            let mode = event.flags as i32;
            let fallocate_timeout = std::time::Duration::from_secs(60);
            match tokio::time::timeout(fallocate_timeout, smart_copier.optimized_fallocate(target_path.clone(), mode, event.offset, event.length)).await {
                Ok(inner) => { op_result = inner.map_err(Into::into); }
                Err(_elapsed) => {
                    warn!("Worker {}: Fallocate timed out after {:?} for inode {}",
                          worker_id, fallocate_timeout, event.inode);
                    metrics::COPY_TIMEOUT_TOTAL.with_label_values(&[&target_label]).inc();
                    op_result = Err(FoxingError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("Fallocate timed out after {:?}", fallocate_timeout)
                    )));
                }
            };
        },
        _ => {}
    }

    if op_result.is_ok() && matches!(event.event_type, EventType::Write | EventType::WriteRange | EventType::Clone | EventType::Truncate | EventType::Fallocate) {
        let target_path_clone = target_path.clone();
        let _ = spawn_blocking(move || {
            if let Ok(sig) = SyncSignature::compute(&target_path_clone) {
                if let Err(e) = set_sync_signature(&target_path_clone, &sig) {
                    debug!("Failed to persist signature for {:?}: {}", target_path_clone, e);
                }
            }
        }).await;
    }

    // Clear dirty flag on successful data operations — catches flags from
    // any source (hydration, previous sessions, external processes)
    if op_result.is_ok() && matches!(event.event_type,
        EventType::Write | EventType::WriteRange | EventType::Clone
        | EventType::Truncate | EventType::Fallocate
        | EventType::Create | EventType::Mkdir)
    {
        sidecar.clear_dirty(target_path.clone());
        dirty_tracker.remove(&event.inode);
    } else if dirty_tracker.contains(&event.inode) {
        // Failed op but we set dirty earlier in this session — leave the xattr
        // so next attempt knows it's still dirty, but keep tracker in sync
    }

    drop(op_guard);
    (op_result, smart_copier, dirty_tracker)
}
