use crate::event::{Event, EventType, EventQueue};
use crate::error::{FoxingError, Result};
use libbpf_rs::RingBufferBuilder;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use libbpf_rs::skel::{SkelBuilder, OpenSkel};
use dashmap::DashMap;
use crate::metrics::{self, GLOBAL_BUFFER_LIMIT};
use std::mem;
use libbpf_rs::MapCore;
use tracing::{info, warn, debug};
use crate::ordering::ReorderBuffer;
use std::sync::Mutex;
use crate::mirror::SourceInfo;
use crate::tuner::{BbrTuner};
use crate::config::{TargetConfig, TargetProfile};
use std::path::PathBuf;

mod skel { include!(concat!(env!("OUT_DIR"), "/mirror.skel.rs")); }
use skel::*;

lazy_static::lazy_static! {
    static ref SEQUENCE_TRACKER: DashMap<u32, AtomicU64> = DashMap::new();
    static ref DEVICE_EVENT_COUNTER: DashMap<u32, AtomicU64> = DashMap::new();
}

pub fn get_device_stats() -> HashMap<u32, (u64, u64)> {
    let mut stats = HashMap::new();
    for r in DEVICE_EVENT_COUNTER.iter() {
        let dev = *r.key();
        let count = r.value().load(Ordering::Relaxed);
        let seq = SEQUENCE_TRACKER.get(&dev).map(|v| v.load(Ordering::Relaxed)).unwrap_or(0);
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

pub fn run(
    queues: HashMap<u32, Vec<Arc<EventQueue>>>, 
    shutdown: Arc<AtomicBool>,
    sources: HashMap<u32, Arc<SourceInfo>>,
    initial_seq: u64
) -> Result<()> {
    let skel_builder = MirrorSkelBuilder::default();
    let mut open_obj = mem::MaybeUninit::uninit();
    let open_skel = skel_builder.open(&mut open_obj).map_err(|e| FoxingError::Bpf(e.to_string()))?;
    let mut skel = open_skel.load().map_err(|e| FoxingError::Bpf(e.to_string()))?;
    
    if initial_seq > 0 {
        let key: u32 = 0;
        let val = initial_seq + 1;
        if let Err(e) = skel.maps.global_seq_map.update(&key.to_ne_bytes(), &val.to_ne_bytes(), libbpf_rs::MapFlags::ANY) {
            warn!("BPF: Failed to restore sequence number {}: {}", val, e);
        } else {
            info!("BPF: Restored Global Sequence to {}", val);
        }
    }

    let self_pid = std::process::id();
    let pid_val: u8 = 1;
    skel.maps.ignored_pids.update(&self_pid.to_ne_bytes(), &pid_val.to_ne_bytes(), libbpf_rs::MapFlags::ANY)
        .map_err(|e| FoxingError::Bpf(format!("Failed to register PID filter: {}", e)))?;

    info!("BPF: Registering {} watched device(s)", queues.len());
    
    let queues_in_closure = Arc::new(queues);
    let sources_in_closure = Arc::new(sources);
    let queues_in_loop = queues_in_closure.clone();
    let sources_in_loop = sources_in_closure.clone();
    
    let mut reorder_buffers_map: HashMap<u32, ReorderBuffer> = HashMap::new();
    
    // Shared state for journal buffers across threads/closures
    let journal_buffers: Arc<Mutex<HashMap<u32, Vec<Arc<Event>>>>> = Arc::new(Mutex::new(HashMap::new()));
    let journal_buffers_closure = journal_buffers.clone();
    
    let mut journal_tuners: HashMap<u32, BbrTuner> = HashMap::new();
    let dummy_board = Arc::new(DashMap::new());

    for dev in queues_in_closure.keys() {
        let key = dev.to_ne_bytes();
        let val = 1u8;
        info!("BPF: Watching device 0x{:08x} ({})", dev, dev);
        skel.maps.watched_devs.update(&key, &val.to_ne_bytes(), libbpf_rs::MapFlags::ANY)
            .map_err(|e| FoxingError::Bpf(e.to_string()))?;
        DEVICE_EVENT_COUNTER.insert(*dev, AtomicU64::new(0));
        SEQUENCE_TRACKER.insert(*dev, AtomicU64::new(0));
        reorder_buffers_map.insert(*dev, ReorderBuffer::new(250, 64 * 1024 * 1024));
        
        journal_buffers.lock().unwrap().insert(*dev, Vec::with_capacity(128));
        
        let mut journal_cfg = TargetConfig {
            path: PathBuf::from("journal"),
            profile: TargetProfile::SSD, 
            ..Default::default()
        };
        let _ = journal_cfg.compile(2, 1024); 
        journal_tuners.insert(*dev, BbrTuner::new(&journal_cfg));
    }

    let reorder_buffers = Arc::new(Mutex::new(reorder_buffers_map));
    let reorder_buffers_in_closure = reorder_buffers.clone();
    let mut _held_links = Vec::new();
    let mut attached_count = 0;
    
    let progs = &skel.progs;
    let probes = [
        ("trace_xfs_write", &progs.trace_xfs_write),
        ("trace_btrfs_write", &progs.trace_btrfs_write),
        ("trace_f2fs_write", &progs.trace_f2fs_write),
        ("trace_gen_write", &progs.trace_gen_write),
        ("trace_vfs_fsync", &progs.trace_vfs_fsync),
        ("trace_create_entry", &progs.trace_create_entry),
        ("trace_create_exit", &progs.trace_create_exit),
        ("trace_mkdir_entry", &progs.trace_mkdir_entry),
        ("trace_mkdir_exit", &progs.trace_mkdir_exit),
        ("trace_mknod_entry", &progs.trace_mknod_entry),
        ("trace_mknod_exit", &progs.trace_mknod_exit),
        ("trace_link_entry", &progs.trace_link_entry),
        ("trace_link_exit", &progs.trace_link_exit),
        ("trace_symlink_entry", &progs.trace_symlink_entry),
        ("trace_symlink_exit", &progs.trace_symlink_exit),
        ("trace_unlink", &progs.trace_unlink),
        ("trace_rmdir", &progs.trace_rmdir),
        ("trace_rename", &progs.trace_rename),
        ("trace_notify_change", &progs.trace_notify_change),
        ("trace_setxattr", &progs.trace_setxattr),
        ("trace_removexattr", &progs.trace_removexattr),
        ("trace_fallocate", &progs.trace_fallocate),
        ("trace_xfs_commit", &progs.trace_xfs_commit),
    ];
    for (name, prog) in probes.iter() {
        match prog.attach() {
            Ok(link) => {
                _held_links.push(link);
                attached_count += 1;
                info!("BPF: Attached probe {}", name);
            }
            Err(e) => {
                warn!("BPF: Skipped probe {} (Kernel unsupported: {})", name, e);
            }
        }
    }

    if attached_count == 0 {
        return Err(FoxingError::Bpf("Failed to attach ANY BPF probes.".into()));
    }
    info!("BPF: Successfully attached {} probes", attached_count);

    let maps = skel.maps;
    let events_map: &dyn MapCore = &maps.events;
    let mut builder = RingBufferBuilder::new();

    // Move journal tuners into closure (exclusive access within the ringbuf thread)
    let mut journal_tuners_closure = journal_tuners;

    builder.add(events_map, move |data| {
        let current_count = metrics::GLOBAL_BUFFER_COUNT.load(Ordering::SeqCst);
        let global_limit = GLOBAL_BUFFER_LIMIT.get() as u64;
        
        if data.len() != std::mem::size_of::<RawEvent>() {
            crate::metrics::EVENTS_MALFORMED.inc();
            return 0;
        }
        let raw = unsafe { std::ptr::read_unaligned(data.as_ptr() as *const RawEvent) };
        if current_count >= global_limit * 3 / 4 {
            metrics::EVENTS_DROPPED.inc();
            return 0;
        }
        let counter = DEVICE_EVENT_COUNTER.entry(raw.dev).or_insert(AtomicU64::new(0));
        let _event_count = counter.fetch_add(1, Ordering::Relaxed);
        let name_len = raw.name.iter().position(|&c| c == 0).unwrap_or(raw.name.len());
        let name = String::from_utf8_lossy(&raw.name[..name_len]).to_string();
        let comm_len = raw.comm.iter().position(|&c| c == 0).unwrap_or(raw.comm.len());
        let comm = String::from_utf8_lossy(&raw.comm[..comm_len]).to_string();
        
        if !queues_in_closure.contains_key(&raw.dev) {
            metrics::EVENTS_UNWATCHED.inc();
            return 0;
        }
        let tracker = SEQUENCE_TRACKER.entry(raw.dev).or_insert(AtomicU64::new(0));
        let _ = tracker.fetch_max(raw.seq, Ordering::Relaxed);
        let new_name = if raw.type_ == 7 {
                let nname_len = raw.nname.iter().position(|&c| c == 0).unwrap_or(raw.nname.len());
                Some(String::from_utf8_lossy(&raw.nname[..nname_len]).to_string())
        } else { None };
        
        let evt = Arc::new(Event {
            event_type: EventType::from(raw.type_),
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
            mode: raw.mode,
            flags: raw.flags,
            process_name: comm,
            interactive: raw.interactive == 1,
            created_at: std::time::Instant::now()
        });

        if let Some(src_info) = sources_in_closure.get(&raw.dev) {
            if let Some(journal) = &src_info.journal {
                if let Ok(mut buffers_map) = journal_buffers_closure.lock() {
                    if let Some(buffer) = buffers_map.get_mut(&raw.dev) {
                        buffer.push(evt.clone());
                        
                        let tuner = journal_tuners_closure.get_mut(&raw.dev).unwrap();
                        if buffer.len() >= tuner.current_batch_size {
                            let start = std::time::Instant::now();
                            let res = journal.append_batch(buffer);
                            let duration = start.elapsed().as_secs_f64();
                            
                            let bytes = res.unwrap_or(0);
                            tuner.tune(duration, bytes, false, 0, 10000, &dummy_board, "journal", 0);
                            buffer.clear();
                        }
                    }
                }
            }
        }

        if let Ok(mut buffers) = reorder_buffers_in_closure.lock() {
            if let Some(buf) = buffers.get_mut(&raw.dev) {
                if buf.push(evt) {
                    while let Some(ordered_evt) = buf.pop() {
                        if let Some(src_info) = sources_in_closure.get(&ordered_evt.dev_id) {
                            if let Some(projector) = &src_info.projector {
                                projector.project(&ordered_evt);
                            }
                        }

                        metrics::GLOBAL_BUFFER_COUNT.fetch_add(1, Ordering::SeqCst);
                        if let Some(qs) = queues_in_closure.get(&ordered_evt.dev_id) {
                            for q in qs { q.push(ordered_evt.clone()); }
                        }
                    }
                } else {
                    metrics::EVENTS_DROPPED.inc();
                }
            }
        }
        0
    }).map_err(|e| FoxingError::Bpf(e.to_string()))?;
    
    let ring = builder.build().map_err(|e| FoxingError::Bpf(e.to_string()))?;
    info!("BPF: Event processing started");
    
    let mut last_report = std::time::Instant::now();
    while !shutdown.load(Ordering::Relaxed) {
        match ring.poll(std::time::Duration::from_millis(100)) {
            Ok(_) => {
                // Flush Journal Buffers for all devices
                if let Ok(mut buffers_map) = journal_buffers.lock() {
                    for (dev_id, buffer) in buffers_map.iter_mut() {
                        if !buffer.is_empty() {
                            if let Some(src_info) = sources_in_loop.get(dev_id) {
                                if let Some(journal) = &src_info.journal {
                                    let _ = journal.append_batch(buffer);
                                    buffer.clear();
                                }
                            }
                        }
                    }
                }

                if let Ok(mut buffers) = reorder_buffers.lock() {
                    for (_dev_id, buf) in buffers.iter_mut() {
                        while let Some(ordered_evt) = buf.pop() {
                            if let Some(src_info) = sources_in_loop.get(&ordered_evt.dev_id) {
                                if let Some(projector) = &src_info.projector {
                                    projector.project(&ordered_evt);
                                }
                            }

                            metrics::GLOBAL_BUFFER_COUNT.fetch_add(1, Ordering::SeqCst);
                            if let Some(qs) = queues_in_loop.get(&ordered_evt.dev_id) {
                                for q in qs { q.push(ordered_evt.clone()); }
                            }
                        }
                    }
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
