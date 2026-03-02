use std::sync::Arc;
use std::path::PathBuf;
use tokio::sync::mpsc::{self, Sender, UnboundedSender};
use tracing::debug;
use crate::error::Result;
use crate::mirror::{SourceInfo, SharedConfig};
use crate::config::TargetConfig;
use crate::tuner::TunerBoard;
use fxcp_core::governor::Governor;
use tokio::task::JoinSet;
use dashmap::DashMap;
use fxcp_core::constants;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};
use std::time::{Instant, Duration};
use std::collections::HashMap;
use fxcp_core::operations::CopyStats;

#[derive(Debug, Clone)]
pub struct HydrationJob {
    pub rel_path: PathBuf,
    pub target_cfg: TargetConfig,
    pub inode: Option<u64>,
}

#[derive(Debug)]
pub struct HydrationQueue {
    pub sender: Sender<HydrationJob>,
    pub pending_count: Arc<AtomicUsize>,
    pub shutdown: Arc<AtomicBool>,
    #[allow(dead_code)]
    rename_failure_tracker: Arc<DashMap<u64, (u32, Instant)>>,
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
        drop(config_reader);

        let (tx, rx) = mpsc::channel(queue_capacity);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let tracker = Arc::new(DashMap::new());
        let pending_count = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        for id in 0..worker_count {
            let rx_clone = rx.clone();
            let source_clone = source.clone();
            let config_clone = config.clone();
            let governor_clone = governor.clone();
            let tuner_clone = tuner_board.clone();
            let tracker_clone = tracker.clone();
            let pending_clone = pending_count.clone();
            let stats_senders_clone = stats_senders.clone();
            
            scope.spawn(async move {
                crate::hydration_worker::run_hydration_worker_loop(
                    rx_clone, source_clone, config_clone, governor_clone,
                    tuner_clone, worker_count, id, tracker_clone, pending_clone,
                    stats_senders_clone
                ).await
            });
        }

        Self {
            sender: tx,
            pending_count,
            shutdown,
            rename_failure_tracker: tracker
        }
    }

    pub fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn submit_job(&self, rel_path: PathBuf, target_cfg: TargetConfig, inode: Option<u64>) {
        if self.shutdown.load(Ordering::Relaxed) {
            return;
        }

        let job = HydrationJob { rel_path, target_cfg, inode };
        self.pending_count.fetch_add(1, Ordering::SeqCst);

        // FIXED: Use blocking_send with a timeout/check loop to allow Ctrl+C to interrupt
        // stalling caused by a full queue.
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                self.pending_count.fetch_sub(1, Ordering::SeqCst);
                break;
            }

            // Try to send without blocking first
            match self.sender.try_send(job.clone()) {
                Ok(_) => return,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Queue full, wait a bit and check shutdown flag again
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                },
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!("Hydration Queue Dropped: Receiver closed");
                    self.pending_count.fetch_sub(1, Ordering::SeqCst);
                    return;
                }
            }
        }
    }
}
