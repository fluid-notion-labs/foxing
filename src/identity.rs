use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::{Event};

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
