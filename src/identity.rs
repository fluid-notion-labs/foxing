use std::path::{PathBuf};
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::{Event};
use std::fs;
use std::os::unix::fs::MetadataExt;
use tracing::{warn, info, debug};
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
    
    if let Some((path, cached_gen)) = cache.get(&event.inode) {
        // FIX: Inode Mismatch Handling Gap #2
        if *cached_gen != event.generation && *cached_gen != std::u32::MAX {
             warn!("Identity Mismatch: Inode {} cached gen {} != event gen {}. Invalidating cache.", 
                   event.inode, cached_gen, event.generation);
             cache.pop(&event.inode);
             // Fallthrough to standard resolution/synthetic logic
        } else {
             return (target_root.join(path), false, false);
        }
    }

    if event.parent_inode != 0 {
        if let Some((parent_path, _)) = cache.get(&event.parent_inode) {
            let full_path = parent_path.join(&event.name);
            cache.put(event.inode, (full_path.clone(), event.generation));
            return (target_root.join(full_path), false, false);
        }
    }

    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() && !event.name.contains('/') {
        // Only simple names usually work here
    } else if !event.name.is_empty() {
        cache.put(event.inode, (event_path.clone(), event.generation));
        return (target_root.join(event_path), false, false);
    }

    // Fallback to synthetic identity
    let identity_dir = target_root.join(".mirror").join(".by-identity");
    let filename = format!("{}_{}", event.inode, event.generation);
    let synthetic_path = identity_dir.join(filename);
    
    (synthetic_path, true, true)
}

pub fn resolve_directory(map: &InodeMap, inode: u64) -> Option<PathBuf> {
    let mut cache = map.lock();
    cache.get(&inode).map(|(p, _)| p.clone())
}

pub fn resolve_and_update_path(map: &InodeMap, source_mount_root: &std::path::Path, inode: u64) -> Result<PathBuf, std::io::Error> {
    info!("IDENTITY: Starting aggressive lookup for Inode {} in {:?}", inode, source_mount_root);
    
    if let Some((cached_path, _gen)) = map.lock().get(&inode) {
        debug!("IDENTITY: Found cached path for inode {}: {:?}", inode, cached_path);
        return Ok(cached_path.clone());
    }

    let mut found_path = None;
    
    for entry in walkdir::WalkDir::new(source_mount_root).min_depth(1) {
        if let Ok(entry) = entry {
            if entry.depth() > 10 { continue; }
            
            if let Ok(metadata) = entry.metadata() {
                if metadata.ino() == inode {
                    if let Ok(rel_path) = entry.path().strip_prefix(source_mount_root) {
                        found_path = Some(rel_path.to_path_buf());
                        info!("IDENTITY: FOUND Inode {} at {:?}", inode, rel_path);
                        break;
                    }
                }
            }
        } else if let Err(e) = entry {
            warn!("Error during aggressive path resolution walk: {}", e);
        }
    }

    if let Some(path) = found_path {
        if let Ok(_metadata) = fs::metadata(source_mount_root.join(&path)) {
            let rel_path = path.clone();
            // We set gen to MAX because we don't have it cheaply here, 
            // but the aggressive lookup implies trust.
            map.lock().put(inode, (rel_path, std::u32::MAX)); 
            return Ok(path);
        }
    }

    warn!("IDENTITY: Failed to resolve Inode {} after full walk.", inode);
    Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Inode not found after aggressive search."))
}
