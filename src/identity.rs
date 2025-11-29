use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::Event;

pub type InodeMap = Arc<Mutex<LruCache<u64, (PathBuf, u32, bool)>>>;

pub fn resolve_target(map: &InodeMap, e: &Event, target_root: &std::path::Path) -> (PathBuf, bool, bool) {
    let mut map = map.lock();
    let mut need_creation = false;
    let mut is_existing_synthetic = false;
    let rel = PathBuf::from(&e.name);
    let mut dst = target_root.join(&rel);
    let mut is_synthetic = false;

    if let Some((_, cached_gen, is_syn)) = map.peek(&e.inode) {
        if *cached_gen != std::u32::MAX && *cached_gen != e.generation {
            crate::metrics::GENERATION_MISMATCHES.inc();
            map.pop(&e.inode);
            need_creation = true;
        } else {
            is_existing_synthetic = *is_syn;
        }
    } else {
        need_creation = true;
    }

    if need_creation || is_existing_synthetic {
        is_synthetic = true;
        // CRITICAL FIX: Changed .foxing to .mirror to match worker.rs directory creation logic.
        // Previously this caused "No such file or directory" errors on new file creation.
        let identity_dir = target_root.join(".mirror").join(".by-identity");
        let syn_dst = identity_dir.join(format!("{}_{}", e.inode, e.generation));
        
        if need_creation {
            dst = syn_dst;
        } else if is_existing_synthetic {
            dst = syn_dst;
            need_creation = false; 
        }
    }
    
    (dst, is_synthetic, need_creation)
}

pub fn update_map(map: &InodeMap, inode: u64, rel: PathBuf, r#gen: u32, is_syn: bool) {
    map.lock().put(inode, (rel, r#gen, is_syn));
}

pub fn update_map_after_rename(map: &InodeMap, inode: u64, new_rel: PathBuf, r#gen: u32) {
    let mut map = map.lock();
    if let Some((_, old_gen, is_syn)) = map.get(&inode).map(|v| v.clone()) {
        let final_gen = if r#gen != 0 { r#gen } else { old_gen };
        map.put(inode, (new_rel, final_gen, is_syn));
    } else {
        map.put(inode, (new_rel, r#gen, false));
    }
}
