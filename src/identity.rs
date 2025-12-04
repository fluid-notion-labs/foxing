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

pub type InodeMap = Arc<Mutex<LruCache<u64, (PathBuf, u32)>>>;
pub type DirMap = Arc<Mutex<LruCache<u64, PathBuf>>>;

pub fn update_map(map: &InodeMap, dir_map: &DirMap, inode: u64, path: PathBuf, generation: u32, is_synthetic: bool, is_dir: bool) {
    let mut cache = map.lock();
    if !is_synthetic {
        cache.put(inode, (path.clone(), generation));
    }
    // Critical Issue 1 & 3 & 6: Explicitly manage directory map
    if is_dir {
        let mut d_cache = dir_map.lock();
        d_cache.put(inode, path);
    }
}

pub fn update_map_after_rename(map: &InodeMap, dir_map: &DirMap, inode: u64, new_path: PathBuf, generation: u32, is_dir: bool) {
    let mut cache = map.lock();
    cache.put(inode, (new_path.clone(), generation));
    if is_dir {
        let mut d_cache = dir_map.lock();
        d_cache.put(inode, new_path);
    }
}

pub fn resolve_target(map: &InodeMap, event: &Event, target_root: &std::path::Path) -> (PathBuf, bool, bool) {
    let mut cache = map.lock();
    if let Some((path, cached_gen)) = cache.get(&event.inode) {
        if *cached_gen != event.generation && *cached_gen != std::u32::MAX {
             warn!("Identity Mismatch: Inode {} cached gen {} != event gen {}. Invalidating cache.",
                   event.inode, cached_gen, event.generation);
             cache.pop(&event.inode);
        } else {
             return (target_root.join(path), false, false);
        }
    }
    
    // Fallback: Try parent resolution if inode not found
    if event.parent_inode != 0 {
        if let Some((parent_path, _)) = cache.get(&event.parent_inode) {
            let full_path = parent_path.join(&event.name);
            cache.put(event.inode, (full_path.clone(), event.generation));
            return (target_root.join(full_path), false, false);
        }
    }

    // Fallback: Try name resolution
    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() && !event.name.contains('/') {
        // Simple filename, likely at root or we lost context
    } else if !event.name.is_empty() {
        cache.put(event.inode, (event_path.clone(), event.generation));
        return (target_root.join(event_path), false, false);
    }

    // Synthetic Fallback
    let identity_dir = target_root.join(".mirror").join(".by-identity");
    let filename = format!("{}_{}", event.inode, event.generation);
    let synthetic_path = identity_dir.join(filename);
    (synthetic_path, true, true)
}

/// Critical Issue 5: Directory Path Resolution Logic
/// Checks DirMap first (O(1)), falls back to InodeMap if not found.
pub fn resolve_directory(dir_map: &DirMap, inode_map: &InodeMap, inode: u64) -> Option<PathBuf> {
    // Check dedicated directory cache first
    {
        let mut d_cache = dir_map.lock();
        if let Some(p) = d_cache.get(&inode) {
            return Some(p.clone());
        }
    }
    
    // Fallback to general inode map (directories are also stored here)
    let mut i_cache = inode_map.lock();
    if let Some((p, _)) = i_cache.get(&inode) {
        // We found it in the file map, promote it to dir map for future speed
        let p_clone = p.clone();
        drop(i_cache); // Drop lock before acquiring dir_map lock
        
        let mut d_cache = dir_map.lock();
        d_cache.put(inode, p_clone.clone());
        
        return Some(p_clone);
    }
    
    None
}

pub fn resolve_and_update_path(map: &InodeMap, dir_map: &DirMap, source_mount_root: &std::path::Path, inode: u64) -> Result<PathBuf, io::Error> {
    info!("IDENTITY: Starting aggressive lookup for Inode {} in {:?}", inode, source_mount_root);
    
    // Quick check before walk
    if let Some((cached_path, _gen)) = map.lock().get(&inode) {
        debug!("IDENTITY: Found cached path for inode {}: {:?}", inode, cached_path);
        return Ok(cached_path.clone());
    }

    let mut found_path = None;
    // Limit depth to avoid massive IO storms on deep trees during fallback
    for entry in walkdir::WalkDir::new(source_mount_root).min_depth(1) {
        if let Ok(entry) = entry {
            if entry.depth() > 20 { continue; } // Safety brake
            
            if let Ok(metadata) = entry.metadata() {
                if metadata.ino() == inode {
                    if let Ok(rel_path) = entry.path().strip_prefix(source_mount_root) {
                        found_path = Some(rel_path.to_path_buf());
                        info!("IDENTITY: FOUND Inode {} at {:?}", inode, rel_path);
                        
                        let is_dir = metadata.is_dir();
                        let generation = 0u32; // We don't easily get generation from std::fs, assume 0 or handle externally
                        let rel_path_buf = rel_path.to_path_buf();
                        
                        // Update both maps
                        let mut cache = map.lock();
                        cache.put(inode, (rel_path_buf.clone(), generation));
                        
                        // Critical Issue 6: Aggressive Scan Updates DirMap
                        if is_dir {
                             let mut d_cache = dir_map.lock();
                             d_cache.put(inode, rel_path_buf.clone());
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
        // Verify it actually exists (stat) before returning
        if fs::metadata(source_mount_root.join(&path)).is_ok() {
            return Ok(path);
        }
    }

    warn!("IDENTITY: Failed to resolve Inode {} after full walk.", inode);
    Err(io::Error::new(io::ErrorKind::NotFound, "Inode not found after aggressive search."))
}
