use std::sync::Arc;
use std::path::PathBuf;
use tokio::sync::mpsc::{self, Sender, UnboundedSender};
use tracing::{debug, warn};
use crate::error::Result;
use crate::mirror::{SourceInfo, SharedConfig};
use crate::config::TargetConfig;
use crate::tuner::TunerBoard;
use fxcp_core::governor::Governor;
use tokio::task::JoinSet;
use dashmap::DashMap;
use fxcp_core::constants;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};
use std::time::Duration;
use std::collections::HashMap;
use fxcp_core::operations::CopyStats;
use crate::hydration_worker::SignatureCache;

#[derive(Debug, Clone)]
pub struct HydrationJob {
    pub rel_path: PathBuf,
    pub target_cfg: TargetConfig,
    pub inode: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct HydrationQueue {
    /// Per-worker senders — round-robin distribution, no shared receiver mutex
    senders: Vec<Sender<HydrationJob>>,
    next_worker: Arc<AtomicUsize>,
    pub pending_count: Arc<AtomicUsize>,
    pub shutdown: Arc<AtomicBool>,
}

impl HydrationQueue {
    pub fn new_in_scope(
        source: Arc<SourceInfo>,
        config: SharedConfig,
        governor: Arc<Governor>,
        tuner_board: TunerBoard,
        worker_count: usize,
        scope: &mut JoinSet<Result<()>>,
        stats_senders: Arc<HashMap<PathBuf, Vec<UnboundedSender<CopyStats>>>>,
    ) -> Self {
        let config_reader = futures::executor::block_on(config.read());
        let queue_capacity = config_reader.queue_max.min(constants::HYDRATION_QUEUE_CAPACITY);
        let per_worker_capacity = (queue_capacity / worker_count).max(64);
        drop(config_reader);

        let tracker = Arc::new(DashMap::new());
        let pending_count = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut senders = Vec::with_capacity(worker_count);
        // Shared signature cache for tiered cloning — first target to complete
        // a file caches its Merkle/SyncSignature, subsequent targets reuse it.
        let sig_cache: SignatureCache = Arc::new(DashMap::new());

        for id in 0..worker_count {
            let (tx, rx) = mpsc::channel(per_worker_capacity);
            senders.push(tx);
            let source_clone = source.clone();
            let config_clone = config.clone();
            let governor_clone = governor.clone();
            let tuner_clone = tuner_board.clone();
            let tracker_clone = tracker.clone();
            let pending_clone = pending_count.clone();
            let stats_senders_clone = stats_senders.clone();
            let sig_cache_clone = sig_cache.clone();

            scope.spawn(async move {
                crate::hydration_worker::run_hydration_worker_loop(
                    rx, source_clone, config_clone, governor_clone,
                    tuner_clone, worker_count, id, tracker_clone, pending_clone,
                    stats_senders_clone, sig_cache_clone
                ).await
            });
        }

        Self {
            senders,
            next_worker: Arc::new(AtomicUsize::new(0)),
            pending_count,
            shutdown,
        }
    }

    pub fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub async fn submit_job(&self, rel_path: PathBuf, target_cfg: TargetConfig, inode: Option<u64>) {
        if self.shutdown.load(Ordering::Relaxed) || self.senders.is_empty() {
            return;
        }

        let job = HydrationJob { rel_path, target_cfg, inode };
        self.pending_count.fetch_add(1, Ordering::SeqCst);

        // Round-robin across per-worker channels
        let worker_idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let sender = &self.senders[worker_idx];
        let capacity = sender.capacity();
        let max_cap = sender.max_capacity();

        let mut attempts = 0;

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                self.pending_count.fetch_sub(1, Ordering::SeqCst);
                break;
            }

            match sender.try_send(job.clone()) {
                Ok(_) => {
                    debug!("submit_job: worker={} capacity={}/{} path={:?}",
                           worker_idx, capacity - 1, max_cap, job.rel_path);
                    return;
                },
                Err(mpsc::error::TrySendError::Full(_)) => {
                    attempts += 1;
                    if attempts >= fxcp_core::constants::HYDRATION_BACKPRESSURE_TIMEOUT_ATTEMPTS {
                        warn!("Hydration Queue: Backpressure timeout after {} attempts - dropping job {:?}",
                              attempts, job.rel_path);
                        crate::metrics::EVENTS_DROPPED.inc();
                        self.pending_count.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }
                    debug!("submit_job: worker={} FULL (capacity={}/{}) attempt {}/{}",
                           worker_idx, capacity, max_cap, attempts, fxcp_core::constants::HYDRATION_BACKPRESSURE_TIMEOUT_ATTEMPTS);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                },
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!("Hydration Queue Dropped: Worker {} receiver closed", worker_idx);
                    self.pending_count.fetch_sub(1, Ordering::SeqCst);
                    return;
                }
            }
        }
    }
}
