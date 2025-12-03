use std::path::{PathBuf};
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::{Event};
use std::fs;
use std::os::unix::fs::MetadataExt;
use tracing::debug;
use walkdir;

pub type InodeMap = Arc<Mutex<LruCache<u64, (PathBuf, u32)>>>;

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
    
    // 1. Try exact Inode Match
    if let Some((path, cached_gen)) = cache.get(&event.inode) {
        if *cached_gen == event.generation || *cached_gen == std::u32::MAX {
             return (target_root.join(path), false, false);
        }
    }

    // 2. Try Parent Resolution
    if event.parent_inode != 0 {
        if let Some((parent_path, _)) = cache.get(&event.parent_inode) {
            let full_path = parent_path.join(&event.name);
            cache.put(event.inode, (full_path.clone(), event.generation));
            return (target_root.join(full_path), false, false);
        }
    }

    // 3. Fallback: Path construction from name
    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() {
        cache.put(event.inode, (event_path.clone(), event.generation));
        return (target_root.join(event_path), false, false);
    }

    // 4. Synthetic Identity
    let identity_dir = target_root.join(".mirror").join(".by-identity");
    let filename = format!("{}_{}", event.inode, event.generation);
    let synthetic_path = identity_dir.join(filename);
    (synthetic_path, true, true)
}

// Fixed: Made public for worker logic
pub fn resolve_directory(map: &InodeMap, inode: u64) -> Option<PathBuf> {
    let mut cache = map.lock();
    cache.get(&inode).map(|(p, _)| p.clone())
}

pub fn resolve_and_update_path(map: &InodeMap, source_mount_root: &std::path::Path, inode: u64) -> Result<PathBuf, std::io::Error> {
    let mut found_path = None;
    for entry in walkdir::WalkDir::new(source_mount_root).min_depth(1) {
        if let Ok(entry) = entry {
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
        if let Ok(_metadata) = fs::metadata(source_mount_root.join(&path)) {
            debug!("Fast refresh successful: Inode {} resolved to {:?}", inode, path);
            let rel_path = path.clone();
            map.lock().put(inode, (rel_path, 0));
            return Ok(path);
        }
    }

    debug!("Fast refresh failed to find inode {}.", inode);
    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Inode not found after aggressive search."))
}
