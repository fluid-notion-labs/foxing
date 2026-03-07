use crate::event::{Event, EventType, EventQueue};
use crate::error::{FoxingError, Result};
use libbpf_rs::RingBufferBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use libbpf_rs::skel::{SkelBuilder, OpenSkel};
use dashmap::DashMap;
use crate::metrics;
use std::mem;
use libbpf_rs::MapCore;
use tracing::{info, warn, debug, error};
use crate::ordering::ReorderBuffer;
use std::sync::Mutex;
use crate::mirror::SourceInfo;
use fxcp_core::constants;

/// P1: Early transient lifecycle filter — prunes create→unlink chains
/// before events enter the worker fanout queues. This eliminates
/// disk I/O for files that are created and immediately deleted
/// (e.g. rm -rf node_modules, compiler temps, build artifacts).
struct TransientFilter {
    /// Inodes recently created (Create/Mkdir) — tracked for pruning
    recent_creates: HashSet<u64>,
    /// Counter of pruned transient events
    pruned_count: u64,
}

impl TransientFilter {
    fn new() -> Self {
        Self { recent_creates: HashSet::new(), pruned_count: 0 }
    }

    /// Returns true if the event should be SUPPRESSED (pruned).
    fn should_prune(&mut self, evt: &Event) -> bool {
        match evt.event_type {
            EventType::Create | EventType::Mkdir | EventType::Mknod => {
                self.recent_creates.insert(evt.inode);
                false // Don't prune creates — we need them if no unlink follows
            }
            EventType::Unlink | EventType::Rmdir => {
                if self.recent_creates.remove(&evt.inode) {
                    // This inode was recently created and is now being deleted
                    // — it's a transient file. Prune both the unlink AND
                    // the create was already queued, but the worker's coalescer
                    // will handle that. We suppress the unlink to reduce queue load.
                    self.pruned_count += 1;
                    debug!("TransientFilter: Pruned unlink for transient inode {} (total pruned: {})", evt.inode, self.pruned_count);
                    return true;
                }
                false
            }
            _ => false,
        }
    }

    /// Periodic cleanup: remove stale entries to prevent memory growth.
    /// Call every ~30s from the poll loop.
    fn gc(&mut self) {
        if self.recent_creates.len() > 100_000 {
            // Under extreme churn, cap the set size
            self.recent_creates.clear();
        }
    }
}

mod skel { include!(concat!(env!("OUT_DIR"), "/mirror.skel.rs")); }
use skel::*;

const MAX_TRACKED_DEVICES: usize = 256;

lazy_static::lazy_static! {
    static ref SEQUENCE_TRACKER: Vec<AtomicU64> = (0..MAX_TRACKED_DEVICES).map(|_| AtomicU64::new(0)).collect();
    static ref DEVICE_INDEX_MAP: DashMap<u32, usize> = DashMap::new();
    static ref DEVICE_EVENT_COUNTER: DashMap<u32, AtomicU64> = DashMap::new();
}

pub fn get_device_stats() -> HashMap<u32, (u64, u64)> {
    let mut stats = HashMap::new();
    for r in DEVICE_EVENT_COUNTER.iter() {
        let dev = *r.key();
        let count = r.value().load(Ordering::Relaxed);
        let seq = if let Some(idx) = DEVICE_INDEX_MAP.get(&dev) {
            SEQUENCE_TRACKER[*idx].load(Ordering::Relaxed)
        } else {
            0
        };
        stats.insert(dev, (seq, count));
    }
    stats
}

#[repr(C)]
#[derive(Copy, Clone)]
struct RawEvent {
    type_: u8, ver: u8, interactive: u8, _pad0: [u8;1],
    dev: u32, seq: u64, ts: u64, p_ino: u64, ino: u64,
    np_ino: u64, r#gen: u32, mode: u32, off: u64, len: u64, uid: u32, gid: u32,
    nlink: u32, flags: u32, sz: u64,
    projid: u32,
    _pad1: u32,
    name: [u8;256], nname: [u8;256],
    comm: [u8;16]
}

fn resolve_missing_parent_chain(
    source: &Arc<SourceInfo>,
    parent_inode: u64,
    seq: u64,
    dev_id: u32,
    queues: &HashMap<u32, Vec<Arc<EventQueue>>>
) {
    if parent_inode == 0 { return; }
    if source.dir_map.contains_key(&parent_inode) { return; }
    
    if let Some(path) = crate::identity::resolve_live_path(source, parent_inode, std::u32::MAX) {
        debug!("Synthetic Event: Resolved missing parent {} to {:?}", parent_inode, path);
        let synthetic_evt = Arc::new(Event {
            event_type: EventType::Mkdir,
            dev_id,
            inode: parent_inode,
            parent_inode: 0,
            new_parent_inode: 0,
            seq_num: seq,
            timestamp_ns: 0,
            offset: 0, length: 0,
            name: path.to_string_lossy().to_string(),
            new_name: None, generation: 0, projid: 0,
            uid: 0, gid: 0,
            mode: 0o755, flags: 0,
            nlink: 2,
            process_name: "SYNTHETIC".into(), interactive: false,
            created_at: std::time::Instant::now()
        });
        
        if let Some(qs) = queues.get(&dev_id) {
            for q in qs {
                let _ = q.push(synthetic_evt.clone());
            }
        }
        
        crate::identity::update_map(
            &source.inode_map, &source.dir_map, dev_id, parent_inode,
            path, std::u32::MAX, true, true, 0, 0
        );
    }
}

pub fn run(
    queues: HashMap<u32, Vec<Arc<EventQueue>>>,
    shutdown: Arc<AtomicBool>,
    sources: HashMap<u32, Arc<SourceInfo>>,
    initial_seq: u64
) -> Result<()> {
    let skel_builder = MirrorSkelBuilder::default();
    let mut open_obj = mem::MaybeUninit::uninit();
    let open_skel = skel_builder.open(unsafe { &mut *open_obj.as_mut_ptr() }).map_err(|e| FoxingError::Bpf(e.to_string()))?;
    let skel = open_skel.load().map_err(|e| FoxingError::Bpf(e.to_string()))?;

    if initial_seq > 0 {
        let key: u32 = 0;
        let next_seq = initial_seq + 1;
        let val_bytes = next_seq.to_ne_bytes();
        if let Err(e) = skel.maps.local_seq_map.update(&key.to_ne_bytes(), &val_bytes, libbpf_rs::MapFlags::ANY) {
            warn!("BPF: Failed to restore global sequence number {}: {}", next_seq, e);
        } else {
            info!("BPF: Restored Global Sequence to {}", next_seq);
        }
    }

    let self_pid = std::process::id();
    let pid_val: u8 = 1;
    skel.maps.ignored_pids.update(&self_pid.to_ne_bytes(), &pid_val.to_ne_bytes(), libbpf_rs::MapFlags::ANY)
        .map_err(|e| FoxingError::Bpf(format!("Failed to register PID filter: {}", e)))?;

    let queues_in_closure = Arc::new(queues);
    let sources_in_closure = Arc::new(sources);
    
    let queues_in_loop = queues_in_closure.clone();
    let sources_in_loop = sources_in_closure.clone();

    let mut reorder_buffers_map: HashMap<u32, ReorderBuffer> = HashMap::new();
    let mut device_index_local_map: HashMap<u32, usize> = HashMap::new();
    
    let mut next_free_index = 0;
    
    for dev in queues_in_closure.keys() {
        let key = dev.to_ne_bytes();
        let val = 1u8;
        info!("BPF: Watching device 0x{:08x} ({})", dev, dev);
        
        skel.maps.watched_devs.update(&key, &val.to_ne_bytes(), libbpf_rs::MapFlags::ANY)
            .map_err(|e| FoxingError::Bpf(e.to_string()))?;
            
        DEVICE_EVENT_COUNTER.insert(*dev, AtomicU64::new(0));
        
        // FIXED: Return error instead of silently failing when device limit is exceeded
        if next_free_index < MAX_TRACKED_DEVICES {
            DEVICE_INDEX_MAP.insert(*dev, next_free_index);
            device_index_local_map.insert(*dev, next_free_index);
            SEQUENCE_TRACKER[next_free_index].store(0, Ordering::Relaxed);
            next_free_index += 1;
        } else {
            return Err(FoxingError::Bpf(format!(
                "Device limit exceeded ({}/{}). Cannot watch device 0x{:x}",
                next_free_index, MAX_TRACKED_DEVICES, dev
            )));
        }
        
        reorder_buffers_map.insert(*dev, ReorderBuffer::new(50, constants::REORDER_BUFFER_BYTES));
    }

    let reorder_buffers = Arc::new(Mutex::new(reorder_buffers_map));
    let reorder_buffers_in_closure = reorder_buffers.clone();
    let device_index_closure_map = Arc::new(device_index_local_map);

    let mut _held_links = Vec::new();
    let mut attached_count = 0;
    let progs = &skel.progs;

    let mut vfs_write_attached = false;
    match progs.trace_vfs_write_iter.attach() {
        Ok(link) => {
            _held_links.push(link);
            attached_count += 1;
            vfs_write_attached = true;
            info!("BPF: Attached Generic VFS Write Hook (Gold Standard)");
        },
        Err(e) => {
            warn!("BPF: Failed to attach vfs_write_iter (Error: {}). Falling back to specific filesystem hooks.", e);
        }
    }

    if !vfs_write_attached {
        let fallback_probes = [
            ("trace_xfs_write_iter", &progs.trace_xfs_write_iter),
            ("trace_btrfs_write_iter", &progs.trace_btrfs_write_iter),
            ("trace_ext4_write_iter", &progs.trace_ext4_write_iter),
            ("trace_f2fs_write_iter", &progs.trace_f2fs_write_iter),
            ("trace_nfs_file_write", &progs.trace_nfs_file_write),
        ];
        for (name, prog) in fallback_probes.iter() {
            match prog.attach() {
                Ok(link) => {
                    _held_links.push(link);
                    attached_count += 1;
                    debug!("BPF: Attached fallback write probe {}", name);
                },
                Err(_) => {}
            }
        }
    }

    let essential_probes = [
        ("trace_vfs_fsync", &progs.trace_vfs_fsync),
        ("trace_create_entry", &progs.trace_create_entry),
        ("trace_create_exit", &progs.trace_create_exit),
        ("trace_security_create_entry", &progs.trace_security_create_entry),
        ("trace_security_mkdir_entry", &progs.trace_security_mkdir_entry),
        ("trace_d_instantiate", &progs.trace_d_instantiate),
        ("trace_mkdir_entry", &progs.trace_mkdir_entry),
        ("trace_mkdir_exit", &progs.trace_mkdir_exit),
        ("trace_mknod_entry", &progs.trace_mknod_entry),
        ("trace_mknod_exit", &progs.trace_mknod_exit),
        ("trace_link_entry", &progs.trace_link_entry),
        ("trace_link_exit", &progs.trace_link_exit),
        ("trace_symlink_entry", &progs.trace_symlink_entry),
        ("trace_symlink_exit", &progs.trace_symlink_exit),
        ("trace_unlink_entry", &progs.trace_unlink_entry),
        ("trace_unlink_exit", &progs.trace_unlink_exit),
        ("trace_rmdir_entry", &progs.trace_rmdir_entry),
        ("trace_rmdir_exit", &progs.trace_rmdir_exit),
        ("trace_rename_entry", &progs.trace_rename_entry),
        ("trace_rename_exit", &progs.trace_rename_exit),
        ("trace_notify_change", &progs.trace_notify_change),
        ("trace_setxattr", &progs.trace_setxattr),
        ("trace_removexattr", &progs.trace_removexattr),
        ("trace_fallocate", &progs.trace_fallocate),
        ("trace_xfs_commit", &progs.trace_xfs_commit),
        ("trace_fileattr", &progs.trace_fileattr),
        ("trace_vfs_lock_file", &progs.trace_vfs_lock_file),
        ("trace_flock_lock_inode_wait", &progs.trace_flock_lock_inode_wait),
        ("trace_flock_lock_inode", &progs.trace_flock_lock_inode),
        ("trace_clone_file_range", &progs.trace_clone_file_range),
        ("trace_filemap_fdatawrite_range", &progs.trace_filemap_fdatawrite_range),
    ];

    for (name, prog) in essential_probes.iter() {
        match prog.attach() {
            Ok(link) => {
                _held_links.push(link);
                attached_count += 1;
                debug!("BPF: Attached probe {}", name);
            }
            Err(e) => {
                debug!("BPF: Skipped probe {} (Kernel unsupported/Module missing): {}", name, e);
            }
        }
    }

    if attached_count == 0 {
        return Err(FoxingError::Bpf("Failed to attach ANY BPF probes.".into()));
    }

    info!("BPF: Successfully attached {} probes", attached_count);

    // P1: Early transient lifecycle filter
    let transient_filter = Arc::new(Mutex::new(TransientFilter::new()));
    let transient_filter_callback = transient_filter.clone();
    let transient_filter_loop = transient_filter.clone();

    let maps = skel.maps;
    let events_map: &dyn MapCore = &maps.events;
    let mut builder = RingBufferBuilder::new();

    builder.add(events_map, move |data| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if data.len() != std::mem::size_of::<RawEvent>() {
                crate::metrics::EVENTS_MALFORMED.inc();
                return 0;
            }
            let raw = unsafe {
                std::ptr::read_unaligned(data.as_ptr() as *const RawEvent)
            };
            
            let counter = DEVICE_EVENT_COUNTER.entry(raw.dev).or_insert(AtomicU64::new(0));
            let _event_count = counter.fetch_add(1, Ordering::Relaxed);
            
            let mut current_seq = 0;
            if let Some(idx) = device_index_closure_map.get(&raw.dev) {
                let tracker = &SEQUENCE_TRACKER[*idx];
                current_seq = tracker.load(Ordering::Relaxed);
                
                if raw.seq > current_seq + 1 {
                    let delta = raw.seq - current_seq;
                    if delta > 1000 {
                        warn!("BPF: Large sequence gap detected on dev 0x{:x} (delta: {}). Triggering Panic Mode hydration.", raw.dev, delta);
                        metrics::LARGE_SEQUENCE_GAPS.inc();
                    }
                }
                
                // Optimized sequence tracking using atomic CAS
                loop {
                    if raw.seq <= current_seq { break; }
                    match tracker.compare_exchange_weak(current_seq, raw.seq, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(updated) => current_seq = updated,
                    }
                }
            }

            let mut synthetic_gap_evt = None;
            if raw.seq > current_seq + 1 && (raw.seq - current_seq) > 1000 {
                 synthetic_gap_evt = Some(Arc::new(Event {
                        event_type: EventType::SequenceGap,
                        dev_id: raw.dev,
                        inode: raw.p_ino,
                        parent_inode: raw.p_ino,
                        new_parent_inode: 0,
                        seq_num: current_seq + 1,
                        timestamp_ns: 0, offset: 0, length: 0,
                        name: "PANIC_GAP".into(), new_name: None, generation: 0, projid: 0,
                        uid: 0, gid: 0,
                        mode: 0, flags: 0, nlink: 0,
                        process_name: "GAP_DETECTOR".into(), interactive: false, created_at: std::time::Instant::now()
                    }));
            }

            if !queues_in_closure.contains_key(&raw.dev) {
                metrics::EVENTS_UNWATCHED.inc();
                return 0;
            }

            let name_len = raw.name.iter().position(|&c| c == 0).unwrap_or(raw.name.len());
            let name = String::from_utf8_lossy(&raw.name[..name_len]).to_string();
            let comm_len = raw.comm.iter().position(|&c| c == 0).unwrap_or(raw.comm.len());
            let comm = String::from_utf8_lossy(&raw.comm[..comm_len]).to_string();
            
            let new_name = if raw.type_ == 7 {
                    let nname_len = raw.nname.iter().position(|&c| c == 0).unwrap_or(raw.nname.len());
                    let s = String::from_utf8_lossy(&raw.nname[..nname_len]).to_string();
                    if s.is_empty() {
                        metrics::BPF_RENAME_INCOMPLETE_DATA.inc();
                    }
                    Some(s)
            } else { None };

            let event_type = EventType::from(raw.type_);
            
            if event_type == EventType::Create || event_type == EventType::Mkdir {
                if let Some(src) = sources_in_closure.get(&raw.dev) {
                    resolve_missing_parent_chain(src, raw.p_ino, raw.seq, raw.dev, &queues_in_closure);
                }
            }

            let evt = Arc::new(Event {
                event_type,
                dev_id: raw.dev,
                inode: raw.ino,
                parent_inode: raw.p_ino,
                new_parent_inode: raw.np_ino,
                seq_num: raw.seq,
                timestamp_ns: raw.ts,
                offset: raw.off,
                length: raw.len,
                name,
                new_name,
                generation: raw.r#gen,
                projid: raw.projid,
                uid: raw.uid,
                gid: raw.gid,
                mode: raw.mode,
                flags: raw.flags,
                nlink: raw.nlink,
                process_name: comm,
                interactive: raw.interactive == 1,
                created_at: std::time::Instant::now()
            });

            if let Ok(mut buffers) = reorder_buffers_in_closure.lock() {
                if let Some(buf) = buffers.get_mut(&raw.dev) {
                    if let Some(gap) = synthetic_gap_evt {
                        let _ = buf.push(gap);
                    }
                    if buf.push(evt.clone()) {
                        // Diagnostic: periodically log ReorderBuffer state
                        static PUSH_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                        let pc = PUSH_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if pc % 5000 == 0 {
                            info!("ReorderBuffer state: next_seq={}, buffer_len={}, structural_len={}, pushed_total={}",
                                  buf.next_seq, buf.len(), 0, pc);
                        }
                        while let Some(ordered_evt) = buf.pop() {
                            // P1: Early transient filter — prune create→unlink chains
                            if let Ok(mut filter) = transient_filter_callback.lock() {
                                if filter.should_prune(&ordered_evt) {
                                    continue; // Skip this event entirely
                                }
                            }
                             if let Some(src_info) = sources_in_closure.get(&ordered_evt.dev_id).cloned() {
                                if let Some(projector) = &src_info.projector {
                                    projector.project(&ordered_evt);
                                }
                                if let Some(qs) = queues_in_closure.get(&ordered_evt.dev_id) {
                                    for q in qs {
                                        if q.push(ordered_evt.clone()) {
                                            metrics::GLOBAL_BUFFER_COUNT.fetch_add(1, Ordering::SeqCst);
                                        }
                                    }
                                } else {
                                    static QUEUE_MISS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                    let m = QUEUE_MISS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    if m % 1000 == 0 {
                                        warn!("BPF: No EventQueue for dev_id=0x{:x} (miss #{})", ordered_evt.dev_id, m);
                                    }
                                }
                            } else {
                                static SRC_MISS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                let m = SRC_MISS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if m % 1000 == 0 {
                                    warn!("BPF: No SourceInfo for dev_id=0x{:x} (miss #{})", ordered_evt.dev_id, m);
                                }
                            }
                        }
                    } else {
                        metrics::EVENTS_DROPPED.inc();
                    }
                }
            }
            0
        }));
        
        match result {
            Ok(ret) => ret,
            Err(e) => {
                error!("BPF Callback Panicked: {:?}", e);
                crate::metrics::EVENTS_MALFORMED.inc();
                0
            }
        }
    }).map_err(|e| FoxingError::Bpf(e.to_string()))?;

    let ring = builder.build().map_err(|e| FoxingError::Bpf(e.to_string()))?;
    
    info!("BPF: Event processing started");
    
    let mut last_report = std::time::Instant::now();
    
    while !shutdown.load(Ordering::Relaxed) {
        match ring.poll(std::time::Duration::from_millis(100)) {
            Ok(_) => {
                if let Ok(mut buffers) = reorder_buffers.lock() {
                    for (_dev_id, buf) in buffers.iter_mut() {
                        while let Some(ordered_evt) = buf.pop() {
                            // P1: Early transient filter
                            if let Ok(mut filter) = transient_filter_loop.lock() {
                                if filter.should_prune(&ordered_evt) {
                                    continue;
                                }
                            }
                             if let Some(src_info) = sources_in_loop.get(&ordered_evt.dev_id).cloned() {
                                if let Some(projector) = &src_info.projector {
                                    projector.project(&ordered_evt);
                                }
                                if let Some(qs) = queues_in_loop.get(&ordered_evt.dev_id) {
                                    for q in qs {
                                        if q.push(ordered_evt.clone()) {
                                            metrics::GLOBAL_BUFFER_COUNT.fetch_add(1, Ordering::SeqCst);
                                        }
                                    }
                                }
                             }
                        }
                    }
                }
                // P1: Periodic GC of transient filter
                if let Ok(mut filter) = transient_filter_loop.lock() {
                    filter.gc();
                }
                
                if last_report.elapsed().as_secs() >= 30 {
                    for entry in DEVICE_EVENT_COUNTER.iter() {
                        let dev_id = entry.key();
                        let count = entry.value().load(Ordering::Relaxed);
                        info!("BPF Stats: Device 0x{:08x} ({}) - {} events processed",
                              dev_id, dev_id, count);
                    }
                    last_report = std::time::Instant::now();
                }
            },
            Err(e) => {
                warn!("BPF Ring Poll Warning (will retry): {}", e);
            }
        }
    }

    info!("BPF: Shutting down");
    Ok(())
}
