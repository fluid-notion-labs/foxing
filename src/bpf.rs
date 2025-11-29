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

mod skel { include!(concat!(env!("OUT_DIR"), "/mirror.skel.rs")); }
use skel::*;

lazy_static::lazy_static! { 
    static ref SEQUENCE_TRACKER: DashMap<u32, AtomicU64> = DashMap::new(); 
}

#[repr(C)]
#[derive(Copy, Clone)]
struct RawEvent {
    type_: u8, ver: u8, _p: [u8;2], dev: u32, seq: u64, ts: u64, p_ino: u64, ino: u64,
    np_ino: u64, r#gen: u32, mode: u32, off: u64, len: u64, uid: u32, gid: u32,
    nlink: u32, flags: u32, sz: u64, 
    projid: u32, _pad3: u32, 
    name: [u8;256], nname: [u8;256]
}

pub async fn run(queues: HashMap<u32, Vec<Arc<EventQueue>>>, shutdown: Arc<AtomicBool>) -> Result<()> {
    let skel_builder = MirrorSkelBuilder::default();
    
    // FIX: Provide OpenObject placeholder for open()
    let mut open_obj = mem::MaybeUninit::uninit();
    let open_skel = skel_builder.open(&mut open_obj).map_err(|e| FoxingError::Bpf(e.to_string()))?;
    let skel = open_skel.load().map_err(|e| FoxingError::Bpf(e.to_string()))?;
    
    let self_pid = std::process::id();
    let pid_val: u8 = 1;
    skel.maps.ignored_pids.update(&self_pid.to_ne_bytes(), &pid_val.to_ne_bytes(), libbpf_rs::MapFlags::ANY)
        .map_err(|e| FoxingError::Bpf(format!("Failed to register PID filter: {}", e)))?;

    for dev in queues.keys() {
        let key = dev.to_le_bytes(); 
        let val = 1u8;
        skel.maps.watched_devs.update(&key, &val.to_le_bytes(), libbpf_rs::MapFlags::ANY).map_err(|e| FoxingError::Bpf(e.to_string()))?;
    }
    
    
    
    // Manual Feature Probing
    let mut _held_links = Vec::new();
    let mut attached_count = 0;

    // Use the generated 'maps' to access the underlying object, or just specific fields.
    // Since 'obj' is private, we must rely on the specific probe fields in `skel.progs`.
    // We will manually try to attach the known ones.
    
    let progs = &skel.progs;
    
    // List of known probes from mirror.bpf.c
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
                tracing::info!("BPF: Attached probe {}", name);
            }
            Err(e) => {
                // Ignore missing FS modules (ENOENT/EPERM)
                tracing::warn!("BPF: Skipped probe {} (Kernel unsupported: {})", name, e);
            }
        }
    }

    if attached_count == 0 {
        return Err(FoxingError::Bpf("Failed to attach ANY BPF probes.".into()));
    }
    
    let maps = skel.maps;
    let events_map: &dyn MapCore = &maps.events;
    
    let mut builder = RingBufferBuilder::new();
    builder.add(events_map, move |data| {
        let current_count = metrics::GLOBAL_BUFFER_COUNT.load(Ordering::Relaxed);
        let global_limit = GLOBAL_BUFFER_LIMIT.get() as u64; 
        
        if current_count >= global_limit {
            metrics::EVENTS_DROPPED.inc();
            metrics::GLOBAL_BUFFER_COUNT.fetch_sub(1, Ordering::Relaxed);
            return 0; 
        }
        
        if data.len() != std::mem::size_of::<RawEvent>() { 
            crate::metrics::EVENTS_MALFORMED.inc();
            return 0; 
        }
        
        let raw = unsafe { std::ptr::read_unaligned(data.as_ptr() as *const RawEvent) };
        let tracker = SEQUENCE_TRACKER.entry(raw.dev).or_insert(AtomicU64::new(0));
        
        let prev = tracker.fetch_max(raw.seq, Ordering::Relaxed);
        let is_wraparound = prev > (u64::MAX - 1000000) && raw.seq < 1000000;
        
        if !is_wraparound && raw.seq > prev + 1 {
            crate::metrics::SEQUENCE_GAPS.with_label_values(&[&raw.dev.to_string()]).inc();
            let gap = Arc::new(Event { 
                event_type: EventType::SequenceGap, dev_id: raw.dev, inode: 0, 
                parent_inode: 0, seq_num: raw.seq, offset: 0, length: 0, 
                name: "".into(), new_name: None, generation: 0, projid: 0, 
                mode: 0,
                created_at: std::time::Instant::now() 
            });
            if let Some(qs) = queues.get(&raw.dev) { 
                for q in qs { q.push(gap.clone()); } 
            }
        }
        
        let name_len = raw.name.iter().position(|&c| c == 0).unwrap_or(raw.name.len());
        let name = String::from_utf8_lossy(&raw.name[..name_len]).to_string();
        
        let new_name = if raw.type_ == 7 { 
                let nname_len = raw.nname.iter().position(|&c| c == 0).unwrap_or(raw.nname.len());
                Some(String::from_utf8_lossy(&raw.nname[..nname_len]).to_string()) 
        } else { None };

        let evt = Arc::new(Event {
            event_type: EventType::from(raw.type_), dev_id: raw.dev, inode: raw.ino,
            parent_inode: raw.p_ino, seq_num: raw.seq, offset: raw.off, length: raw.len,
            name, new_name, generation: raw.r#gen, projid: raw.projid,
            mode: raw.mode,
            created_at: std::time::Instant::now()
        });
        
        if let Some(qs) = queues.get(&raw.dev) { 
            for q in qs { q.push(evt.clone()); } 
        }
        
        metrics::GLOBAL_BUFFER_COUNT.fetch_add(1, Ordering::Relaxed);
        
        0
    }).map_err(|e| FoxingError::Bpf(e.to_string()))?; 
    
    let ring = builder.build().map_err(|e| FoxingError::Bpf(e.to_string()))?;
    
    while !shutdown.load(Ordering::Relaxed) { 
        match ring.poll(std::time::Duration::from_millis(100)) {
            Ok(_) => {},
            Err(e) => return Err(FoxingError::Bpf(e.to_string())),
        }
    }
    Ok(())
}
