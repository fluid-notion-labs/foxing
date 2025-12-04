use std::path::{PathBuf};
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::{Event};
use std::fs;
use std::os::unix::fs::MetadataExt;
use tracing::{warn, info, debug};
use walkdir;
use std::io;

#[derive(Clone, Debug)]
pub struct IdentityEntry {
    pub path: PathBuf,
    pub generation: u32,
    pub timestamp_ns: u64,
    pub seq_num: u64,
}

impl IdentityEntry {
    pub fn from_stat(path: PathBuf, generation: u32) -> Self {
        Self {
            path,
            generation,
            timestamp_ns: u64::MAX, // stat() beats buffered events
            seq_num: u64::MAX,
        }
    }
}

// FIX #6: Composite key (DeviceID, Inode) to support multi-device sources safely
pub type InodeMap = Arc<Mutex<LruCache<(u32, u64), IdentityEntry>>>;
pub type DirMap = Arc<Mutex<LruCache<(u32, u64), PathBuf>>>;

pub fn update_map(map: &InodeMap, dir_map: &DirMap, dev: u32, inode: u64, path: PathBuf, generation: u32, is_synthetic: bool, is_dir: bool, ts: u64, seq: u64) {
    let mut cache = map.lock();
    let key = (dev, inode);
    if let Some(entry) = cache.get(&key) {
        if entry.timestamp_ns > ts {
            return;
        }
        if entry.timestamp_ns == ts && entry.seq_num >= seq {
            return;
        }
    }
    if !is_synthetic {
        cache.put(key, IdentityEntry {
            path: path.clone(),
            generation,
            timestamp_ns: ts,
            seq_num: seq,
        });
    }
    if is_dir {
        let mut d_cache = dir_map.lock();
        d_cache.put(key, path);
    }
}

pub fn update_map_after_rename(map: &InodeMap, dir_map: &DirMap, dev: u32, inode: u64, new_path: PathBuf, generation: u32, is_dir: bool, ts: u64, seq: u64) {
    let mut cache = map.lock();
    let key = (dev, inode);
    if let Some(entry) = cache.get(&key) {
        if entry.timestamp_ns > ts {
            return;
        }
        if entry.timestamp_ns == ts && entry.seq_num >= seq {
            return;
        }
    }
    cache.put(key, IdentityEntry {
        path: new_path.clone(),
        generation,
        timestamp_ns: ts,
        seq_num: seq,
    });
    if is_dir {
        let mut d_cache = dir_map.lock();
        d_cache.put(key, new_path);
    }
}

pub fn resolve_target(map: &InodeMap, event: &Event, target_root: &std::path::Path) -> (PathBuf, bool, bool) {
    let mut cache = map.lock();
    let key = (event.dev_id, event.inode);
    
    if let Some(entry) = cache.get(&key) {
        if entry.generation != event.generation && entry.generation != std::u32::MAX {
             warn!("Identity Mismatch: Device {} Inode {} cached gen {} != event gen {}. Invalidating cache.",
                   event.dev_id, event.inode, entry.generation, event.generation);
             cache.pop(&key);
        } else {
             return (target_root.join(&entry.path), false, false);
        }
    }
    
    if event.parent_inode != 0 {
        let parent_key = (event.dev_id, event.parent_inode);
        if let Some(parent_entry) = cache.get(&parent_key) {
            let full_path = parent_entry.path.join(&event.name);
            cache.put(key, IdentityEntry {
                path: full_path.clone(),
                generation: event.generation,
                timestamp_ns: event.timestamp_ns,
                seq_num: event.seq_num,
            });
            return (target_root.join(full_path), false, false);
        }
    }
    
    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() && !event.name.contains('/') {
        // Root relative path in name, heuristic fallback
    } else if !event.name.is_empty() {
        cache.put(key, IdentityEntry {
            path: event_path.clone(),
            generation: event.generation,
            timestamp_ns: event.timestamp_ns,
            seq_num: event.seq_num,
        });
        return (target_root.join(event_path), false, false);
    }
    
    let identity_dir = target_root.join(".mirror").join(".by-identity");
    let filename = format!("{}_{}_{}", event.dev_id, event.inode, event.generation);
    let synthetic_path = identity_dir.join(filename);
    (synthetic_path, true, true)
}

pub fn resolve_directory(dir_map: &DirMap, inode_map: &InodeMap, dev: u32, inode: u64) -> Option<PathBuf> {
    let key = (dev, inode);
    {
        let mut d_cache = dir_map.lock();
        if let Some(p) = d_cache.get(&key) {
            return Some(p.clone());
        }
    }
    let mut i_cache = inode_map.lock();
    if let Some(entry) = i_cache.get(&key) {
        let p_clone = entry.path.clone();
        drop(i_cache);
        let mut d_cache = dir_map.lock();
        d_cache.put(key, p_clone.clone());
        return Some(p_clone);
    }
    None
}

pub fn resolve_and_update_path(map: &InodeMap, dir_map: &DirMap, source_mount_root: &std::path::Path, inode: u64) -> Result<PathBuf, io::Error> {
    info!("IDENTITY: Starting aggressive lookup for Inode {} in {:?}", inode, source_mount_root);
    
    // Scan logic modified to capture dev_id from filesystem metadata
    let mut found_path = None;
    let mut found_dev = 0;
    
    for entry in walkdir::WalkDir::new(source_mount_root).min_depth(1) {
        if let Ok(entry) = entry {
            if entry.depth() > 20 { continue; }
            if let Ok(metadata) = entry.metadata() {
                if metadata.ino() == inode {
                    if let Ok(rel_path) = entry.path().strip_prefix(source_mount_root) {
                        found_path = Some(rel_path.to_path_buf());
                        found_dev = metadata.dev() as u32; 
                        
                        info!("IDENTITY: FOUND Inode {} (Dev {}) at {:?}", inode, found_dev, rel_path);
                        let is_dir = metadata.is_dir();
                        let generation = 0u32;
                        let rel_path_buf = rel_path.to_path_buf();
                        let key = (found_dev, inode);
                        
                        let mut cache = map.lock();
                        cache.put(key, IdentityEntry::from_stat(rel_path_buf.clone(), generation));
                        
                        if is_dir {
                             let mut d_cache = dir_map.lock();
                             d_cache.put(key, rel_path_buf.clone());
                             debug!("IDENTITY: Aggressive scan cached directory inode {} in DirMap.", inode);
                        }
                        break;
                    }
                }
            }
        } else if let Err(e) = entry {
            warn!("Error during aggressive path resolution walk: {}", e);
        }
    }
    if let Some(path) = found_path {
        if fs::metadata(source_mount_root.join(&path)).is_ok() {
            return Ok(path);
        }
    }
    warn!("IDENTITY: Failed to resolve Inode {} after full walk.", inode);
    Err(io::Error::new(io::ErrorKind::NotFound, "Inode not found after aggressive search."))
}
