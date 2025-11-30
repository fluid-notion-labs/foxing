use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::{Event, EventType};

pub type InodeMap = Arc<Mutex<LruCache<u64, (PathBuf, u32)>>>; // Path, Generation

pub fn update_map(map: &InodeMap, inode: u64, path: PathBuf, gen: u32, is_synthetic: bool) {
    let mut cache = map.lock();
    if !is_synthetic {
        cache.put(inode, (path, gen));
    }
}

pub fn update_map_after_rename(map: &InodeMap, inode: u64, new_path: PathBuf, gen: u32) {
    let mut cache = map.lock();
    cache.put(inode, (new_path, gen));
}

pub fn resolve_target(map: &InodeMap, event: &Event, target_root: &std::path::Path) -> (PathBuf, bool, bool) {
    let mut cache = map.lock();
    
    // 1. Try to find existing mapping
    if let Some((path, gen)) = cache.get(&event.inode) {
        if *gen == event.generation || *gen == std::u32::MAX {
             return (target_root.join(path), false, false);
        }
    }

    // 2. If not found, or generation mismatch, check if we can use the name from the event
    // (Only if it looks like a valid relative path)
    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() {
        // Optimistic: Use the name provided by BPF/Inotify
        // We update the map so subsequent writes use this path
        cache.put(event.inode, (event_path.clone(), event.generation));
        return (target_root.join(event_path), false, false);
    }

    // 3. Fallback: Synthetic Identity
    // Corrected Path: .mirror/.by-identity (Was .foxing/.by-identity)
    let identity_dir = target_root.join(".mirror").join(".by-identity");
    let filename = format!("{}_{}", event.inode, event.generation);
    let synthetic_path = identity_dir.join(filename);
    
    (synthetic_path, true, true) // Path, is_synthetic, needs_creation
}
