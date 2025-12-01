use std::path::{PathBuf};
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::{Event};
use std::fs; 
use std::os::unix::fs::MetadataExt; 
use tracing::debug; 
use walkdir; // Explicitly import walkdir for the recursive lookup

pub type InodeMap = Arc<Mutex<LruCache<u64, (PathBuf, u32)>>>; // Path, Generation

pub fn update_map(map: &InodeMap, inode: u64, path: PathBuf, generation: u32, is_synthetic: bool) {
    let mut cache = map.lock();
    if !is_synthetic {
        cache.put(inode, (path, generation));
    }
}

pub fn update_map_after_rename(map: &InodeMap, inode: u64, new_path: PathBuf, generation: u32) {
    let mut cache = map.lock();
    cache.put(inode, (new_path, generation));
}

pub fn resolve_target(map: &InodeMap, event: &Event, target_root: &std::path::Path) -> (PathBuf, bool, bool) {
    let mut cache = map.lock();
    
    // 1. Try to find existing mapping
    if let Some((path, cached_gen)) = cache.get(&event.inode) {
        if *cached_gen == event.generation || *cached_gen == std::u32::MAX {
             return (target_root.join(path), false, false);
        }
    }

    // 2. Optimistic name usage
    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() {
        cache.put(event.inode, (event_path.clone(), event.generation));
        return (target_root.join(event_path), false, false);
    }

    // 3. Fallback: Synthetic Identity
    // Fixed path from .foxing to .mirror
    let identity_dir = target_root.join(".mirror").join(".by-identity");
    let filename = format!("{}_{}", event.inode, event.generation);
    let synthetic_path = identity_dir.join(filename);
    
    (synthetic_path, true, true) 
}

/// Attempts to resolve the canonical path for an inode by performing a focused, aggressive search.
/// 
/// This is intended for immediate use by the Worker thread to fix a suspected stale map entry 
/// due to inode reuse, avoiding a slow hydration cycle.
/// 
/// # Returns
/// `Ok(PathBuf)` containing the file's relative path on success.
/// `Err(std::io::Error)` if the file cannot be found after search.
pub fn resolve_and_update_path(map: &InodeMap, source_mount_root: &std::path::Path, inode: u64) -> Result<PathBuf, std::io::Error> {
    let mut found_path = None;
    
    // Perform a constrained recursive search starting from the source root.
    // Limit depth to avoid excessive blocking time.
    for entry in walkdir::WalkDir::new(source_mount_root).min_depth(1) {
        if let Ok(entry) = entry {
            // Hard limit depth to prevent catastrophic blocking (e.g., in /proc)
            if entry.depth() > 5 { continue; } 
            
            if let Ok(metadata) = entry.metadata() {
                if metadata.ino() == inode {
                    if let Ok(rel_path) = entry.path().strip_prefix(source_mount_root) {
                        found_path = Some(rel_path.to_path_buf());
                        break;
                    }
                }
            }
        } else if let Err(e) = entry {
            debug!("Error during aggressive path resolution walk: {}", e);
        }
    }

    if let Some(path) = found_path {
        // We found the path. Now, update the map with the correct metadata.
        if let Ok(metadata) = fs::metadata(source_mount_root.join(&path)) {
            debug!("Fast refresh successful: Inode {} resolved to {:?}", inode, path);
            let rel_path = path.clone();
            // Update the map aggressively with the new, correct relative path
            map.lock().put(inode, (rel_path, metadata.generation()));
            return Ok(path);
        }
    }

    debug!("Fast refresh failed to find inode {}.", inode);
    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Inode not found after aggressive search."))
}
