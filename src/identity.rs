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
    pub fn new(path: PathBuf, generation: u32, timestamp_ns: u64, seq_num: u64) -> Self {
        Self {
            path,
            generation,
            timestamp_ns,
            seq_num,
        }
    }

    pub fn from_stat(path: PathBuf, generation: u32) -> Self {
        Self {
            path,
            generation,
            timestamp_ns: u64::MAX,
            seq_num: u64::MAX,
        }
    }
}

pub type InodeMap = Arc<Mutex<LruCache<(u32, u64), IdentityEntry>>>;
pub type DirMap = Arc<Mutex<LruCache<(u32, u64), PathBuf>>>;

pub fn update_map(map: &InodeMap, dir_map: &DirMap, dev: u32, inode: u64, path: PathBuf, generation: u32, is_synthetic: bool, is_dir: bool, ts: u64, seq: u64) {
    let mut cache = map.lock();
    let key = (dev, inode);
    
    // Fix for Hydration vs Live Event race:
    // If cache has a Hydration Entry (TS=MAX), we MUST allow the Live Event to overwrite it
    // because the Live Event has the authoritative generation/path for the current point in time.
    // Otherwise, stale hydration data blocks new 'Create' events, leading to Identity Mismatch later.
    if let Some(entry) = cache.get(&key) {
        let is_hydration = entry.timestamp_ns == u64::MAX;
        if !is_hydration {
            if entry.timestamp_ns > ts { return; }
            if entry.timestamp_ns == ts && entry.seq_num >= seq { return; }
        }
    }

    if !is_synthetic {
        cache.put(key, IdentityEntry::new(path.clone(), generation, ts, seq));
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
        let is_hydration = entry.timestamp_ns == u64::MAX;
        if !is_hydration {
            if entry.timestamp_ns > ts { return; }
            if entry.timestamp_ns == ts && entry.seq_num >= seq { return; }
        }
    }

    cache.put(key, IdentityEntry::new(new_path.clone(), generation, ts, seq));
    
    if is_dir {
        let mut d_cache = dir_map.lock();
        d_cache.put(key, new_path);
    }
}

pub fn remove_entry(map: &InodeMap, dir_map: &DirMap, dev: u32, inode: u64) {
    let key = (dev, inode);
    {
        let mut cache = map.lock();
        if cache.pop(&key).is_some() {
            debug!("IDENTITY: Removed inode {} (dev {}) from InodeMap", inode, dev);
        }
    }
    {
        let mut d_cache = dir_map.lock();
        if d_cache.pop(&key).is_some() {
            debug!("IDENTITY: Removed inode {} (dev {}) from DirMap", inode, dev);
        }
    }
}

pub fn resolve_target(map: &InodeMap, event: &Event, target_root: &std::path::Path) -> (PathBuf, bool, bool) {
    let mut cache = map.lock();
    let key = (event.dev_id, event.inode);

    if let Some(entry) = cache.get_mut(&key) {
        // Verify Generation matches to detect inode reuse
        let match_gen = entry.generation == event.generation 
                        || entry.generation == 0 // 0 implies uncached/unknown gen
                        || entry.generation == std::u32::MAX; // MAX implies manual override

        if !match_gen {
             warn!("Identity Mismatch: Device {} Inode {} cached gen {} != event gen {}. Invalidating cache.", 
                   event.dev_id, event.inode, entry.generation, event.generation);
             cache.pop(&key);
             // Fall through to standard resolution
        } else {
             // Update generation if we now know it
             if entry.generation == 0 && event.generation != 0 {
                 entry.generation = event.generation;
             }
             return (target_root.join(&entry.path), false, false);
        }
    }

    // Fallback 1: Parent Relative Path (if parent is known)
    if event.parent_inode != 0 {
        let parent_key = (event.dev_id, event.parent_inode);
        // We need to check 'cache' here, but it's locked.
        // However, we are looking up a *different* key.
        // Since LruCache isn't concurrent, we are holding the lock.
        if let Some(parent_entry) = cache.get(&parent_key) {
            let full_path = parent_entry.path.join(&event.name);
            
            // Speculatively cache this result
            cache.put(key, IdentityEntry::new(
                full_path.clone(), 
                event.generation, 
                event.timestamp_ns,
                event.seq_num,
            ));
            
            return (target_root.join(full_path), false, false);
        }
    }

    // Fallback 2: Direct Path (if event has full path info or is root-relative)
    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() && !event.name.contains('/') {
        // Likely just a filename, cannot resolve without parent.
    } else if !event.name.is_empty() {
        // Has slashes, might be relative to watch root.
        // We assume the BPF filter provided a relative path if it could.
        cache.put(key, IdentityEntry::new(
            event_path.clone(), 
            event.generation,
            event.timestamp_ns,
            event.seq_num,
        ));
        return (target_root.join(event_path), false, false);
    }

    // Fallback 3: Synthetic Identity Path (The "Lost & Found" strategy)
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
    
    // If not in DirMap, check InodeMap and promote it
    let mut i_cache = inode_map.lock();
    if let Some(entry) = i_cache.get(&key) {
        let p_clone = entry.path.clone();
        // Drop lock before acquiring d_cache lock to prevent deadlock (though order is consistent here)
        drop(i_cache); 
        
        let mut d_cache = dir_map.lock();
        d_cache.put(key, p_clone.clone());
        return Some(p_clone);
    }
    
    None
}

pub fn resolve_and_update_path(
    map: &InodeMap, 
    dir_map: &DirMap, 
    source_mount_root: &std::path::Path, 
    inode: u64,
    generation_hint: u32,
    ts_hint: u64,
    seq_hint: u64
) -> Result<PathBuf, io::Error> {
    info!("IDENTITY: Starting aggressive lookup for Inode {} in {:?}", inode, source_mount_root);
    
    // This is expensive: Walk the entire source tree to find the inode.
    // Used only for hydration/repair when identity is lost.
    let mut found_path = None;
    
    // Using walkdir for recursive scan
    for entry in walkdir::WalkDir::new(source_mount_root).min_depth(1) {
        if let Ok(entry) = entry {
            // Limit depth to prevent infinite loops in complex binds?
            // Currently unbounded, but maybe sensible default needed.
            if entry.depth() > 20 { continue; } 

            if let Ok(metadata) = entry.metadata() {
                if metadata.ino() == inode {
                    // Found it!
                    if let Ok(rel_path) = entry.path().strip_prefix(source_mount_root) {
                        found_path = Some(rel_path.to_path_buf());
                        let found_dev = metadata.dev() as u32;
                        info!("IDENTITY: FOUND Inode {} (Dev {}) at {:?}", inode, found_dev, rel_path);

                        // Update Caches
                        let is_dir = metadata.is_dir();
                        let rel_path_buf = rel_path.to_path_buf();
                        let key = (found_dev, inode);

                        let mut cache = map.lock();
                        cache.put(key, IdentityEntry::new(rel_path_buf.clone(), generation_hint, ts_hint, seq_hint));

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
        // Double check existence?
        if fs::metadata(source_mount_root.join(&path)).is_ok() {
            return Ok(path);
        }
    }

    warn!("IDENTITY: Failed to resolve Inode {} after full walk.", inode);
    Err(io::Error::new(io::ErrorKind::NotFound, "Inode not found after aggressive search."))
}
