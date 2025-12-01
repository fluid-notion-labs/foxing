//! # BPF Event Collector
//!
//! This module initializes the eBPF subsystem, attaches kprobes to VFS/XFS
//! functions, and polls the kernel ring buffer. It handles the raw event stream
//! and applies backpressure if the userspace consumers fall behind.

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

mod skel { include!(concat!(env!("OUT_DIR"), "/mirror.skel.rs")); }
use skel::*;

lazy_static::lazy_static! { 
    static ref SEQUENCE_TRACKER: DashMap<u32, AtomicU64> = DashMap::new(); 
    static ref DEVICE_EVENT_COUNTER: DashMap<u32, AtomicU64> = DashMap::new();
}

/// Public accessor for debug UI to see BPF event counts per device.
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

/// Main BPF Event Loop.
///
/// This function loads the BPF program and enters a continuous polling loop.
/// It monitors `metrics::GLOBAL_BUFFER_COUNT` to apply backpressure by
/// skipping kernel buffer consumption if userspace is overloaded.
pub async fn run(queues: HashMap<u32, Vec<Arc<EventQueue>>>, shutdown: Arc<AtomicBool>) -> Result<()> {
    let skel_builder = MirrorSkelBuilder::default();
    
    let mut open_obj = mem::MaybeUninit::uninit();
    let open_skel = skel_builder.open(&mut open_obj).map_err(|e| FoxingError::Bpf(e.to_string()))?;
    let skel = open_skel.load().map_err(|e| FoxingError::Bpf(e.to_string()))?;
    
    let self_pid = std::process::id();
    let pid_val: u8 = 1;
    skel.maps.ignored_pids.update(&self_pid.to_ne_bytes(), &pid_val.to_ne_bytes(), libbpf_rs::MapFlags::ANY)
        .map_err(|e| FoxingError::Bpf(format!("Failed to register PID filter: {}", e)))?;

    info!("BPF: Registering {} watched device(s)", queues.len());
    for dev in queues.keys() {
        let key = dev.to_ne_bytes(); 
        let val = 1u8;
        info!("BPF: Watching device 0x{:08x} ({})", dev, dev);
        skel.maps.watched_devs.update(&key, &val.to_ne_bytes(), libbpf_rs::MapFlags::ANY)
            .map_err(|e| FoxingError::Bpf(e.to_string()))?;
            
        DEVICE_EVENT_COUNTER.insert(*dev, AtomicU64::new(0));
        SEQUENCE_TRACKER.insert(*dev, AtomicU64::new(0));
    }
    
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
    builder.add(events_map, move |data| {
        let current_count = metrics::GLOBAL_BUFFER_COUNT.load(Ordering::Relaxed);
        let global_limit = GLOBAL_BUFFER_LIMIT.get() as u64; 
        
        // Backpressure: If userspace buffer is > 75% full, skip acquisition
        // This prevents the kernel ring buffer from becoming stuck if userspace halts.
        if current_count >= global_limit * 3 / 4 {
            metrics::EVENTS_DROPPED.inc();
            return 0; 
        }
        
        if data.len() != std::mem::size_of::<RawEvent>() { 
            crate::metrics::EVENTS_MALFORMED.inc();
            return 0; 
        }
        
        let raw = unsafe { std::ptr::read_unaligned(data.as_ptr() as *const RawEvent) };
        
        let counter = DEVICE_EVENT_COUNTER.entry(raw.dev).or_insert(AtomicU64::new(0));
        let event_count = counter.fetch_add(1, Ordering::Relaxed);
        
        // Decode strings
        let name_len = raw.name.iter().position(|&c| c == 0).unwrap_or(raw.name.len());
        let name = String::from_utf8_lossy(&raw.name[..name_len]).to_string();
        
        let comm_len = raw.comm.iter().position(|&c| c == 0).unwrap_or(raw.comm.len());
        let comm = String::from_utf8_lossy(&raw.comm[..comm_len]).to_string();

        if event_count < 100 { 
            debug!("BPF Event #{} (Seq {}) from {} ({}): type={}, inode={}, interactive={}", 
                   event_count, raw.seq, comm, raw.dev, raw.type_, raw.ino, raw.interactive);
        }
        
        if !queues.contains_key(&raw.dev) {
            metrics::EVENTS_UNWATCHED.inc();
            return 0;
        }
        
        let tracker = SEQUENCE_TRACKER.entry(raw.dev).or_insert(AtomicU64::new(0));
        
        let prev = tracker.fetch_max(raw.seq, Ordering::Relaxed);
        let is_wraparound = prev > (u64::MAX - 1000000) && raw.seq < 1000000;
        
        if !is_wraparound && raw.seq > prev + 1 {
            warn!("BPF: Sequence gap detected on device 0x{:08x}: {} -> {} (gap of {})", 
                  raw.dev, prev, raw.seq, raw.seq - prev - 1);
            crate::metrics::SEQUENCE_GAPS.with_label_values(&[&raw.dev.to_string()]).inc();
            let gap = Arc::new(Event { 
                event_type: EventType::SequenceGap, dev_id: raw.dev, inode: 0, 
                parent_inode: 0, seq_num: raw.seq, offset: 0, length: 0, 
                name: "".into(), new_name: None, generation: 0, projid: 0, 
                mode: 0, flags: 0, process_name: "kernel".into(), interactive: false, 
                created_at: std::time::Instant::now() 
            });
            if let Some(qs) = queues.get(&raw.dev) { 
                for q in qs { q.push(gap.clone()); } 
            }
        }
        
        let new_name = if raw.type_ == 7 { 
                let nname_len = raw.nname.iter().position(|&c| c == 0).unwrap_or(raw.nname.len());
                Some(String::from_utf8_lossy(&raw.nname[..nname_len]).to_string()) 
        } else { None };

        let evt = Arc::new(Event {
            event_type: EventType::from(raw.type_), dev_id: raw.dev, inode: raw.ino,
            parent_inode: raw.p_ino, seq_num: raw.seq, offset: raw.off, length: raw.len,
            name, new_name, generation: raw.r#gen, projid: raw.projid,
            mode: raw.mode,
            flags: raw.flags,
            process_name: comm, 
            interactive: raw.interactive == 1,
            created_at: std::time::Instant::now()
        });
        
        metrics::GLOBAL_BUFFER_COUNT.fetch_add(1, Ordering::Relaxed);
        
        if let Some(qs) = queues.get(&raw.dev) { 
            for q in qs { q.push(evt.clone()); } 
        }
        
        0
    }).map_err(|e| FoxingError::Bpf(e.to_string()))?; 
    
    let ring = builder.build().map_err(|e| FoxingError::Bpf(e.to_string()))?;
    
    info!("BPF: Event processing started");
    
    let mut last_report = std::time::Instant::now();
    
    while !shutdown.load(Ordering::Relaxed) { 
        match ring.poll(std::time::Duration::from_millis(100)) {
            Ok(_) => {
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
                // Log warning but keep running to tolerate EINTR signals under load
                warn!("BPF Ring Poll Warning (will retry): {}", e);
            }
        }
    }
    
    info!("BPF: Shutting down");
    Ok(())
}
