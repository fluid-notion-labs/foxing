use std::path::{PathBuf};
use std::sync::Arc;
use parking_lot::Mutex;
use lru::LruCache;
use crate::event::{Event};
use std::fs;
use std::os::unix::fs::MetadataExt;
use tracing::{warn, debug};
use walkdir;
use std::io;
use crate::metrics;
use std::num::NonZeroUsize;

#[derive(Clone, Debug)]
pub struct IdentityEntry {
    pub path: PathBuf,
    pub generation: u32,
    pub timestamp_ns: u64,
    pub seq_num: u64,
}

impl IdentityEntry {
    pub fn new(path: PathBuf, generation: u32, timestamp_ns: u64, seq_num: u64) -> Self {
        Self { path, generation, timestamp_ns, seq_num }
    }
}

// ISSUE 2 FIX: Explicit Result Type for Repair Handling
#[derive(Debug)]
pub enum ResolveResult {
    Success(PathBuf, bool, bool), // path, is_synthetic, needs_creation
    NeedsRepair(PathBuf),         // synthetic_path_for_repair_queueing
}

const SHARD_COUNT: usize = 64;

#[derive(Debug)]
pub struct ShardedInodeMap {
    shards: Vec<Mutex<LruCache<u64, IdentityEntry>>>,
}

impl ShardedInodeMap {
    pub fn new(capacity: usize) -> Arc<Self> {
        let per_shard = NonZeroUsize::new((capacity / SHARD_COUNT).max(100)).unwrap();
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Mutex::new(LruCache::new(per_shard)));
        }
        Arc::new(Self { shards })
    }

    fn get_shard(&self, inode: u64) -> &Mutex<LruCache<u64, IdentityEntry>> {
        &self.shards[(inode as usize) % SHARD_COUNT]
    }

    pub fn put(&self, inode: u64, entry: IdentityEntry) {
        let mut shard = self.get_shard(inode).lock();
        if let Some(existing) = shard.get(&inode) {
            let is_hydration = existing.timestamp_ns == u64::MAX;
            if !is_hydration {
                if existing.timestamp_ns > entry.timestamp_ns { return; }
                if existing.timestamp_ns == entry.timestamp_ns && existing.seq_num >= entry.seq_num { return; }
            }
        }
        shard.put(inode, entry);
    }

    pub fn get_path(&self, inode: u64) -> Option<PathBuf> {
        let mut shard = self.get_shard(inode).lock();
        shard.get(&inode).map(|e| e.path.clone())
    }

    pub fn get_entry_clone(&self, inode: u64) -> Option<IdentityEntry> {
        let mut shard = self.get_shard(inode).lock();
        shard.get(&inode).cloned()
    }

    pub fn remove(&self, inode: u64) {
        let mut shard = self.get_shard(inode).lock();
        if shard.pop(&inode).is_some() {
            debug!("IDENTITY: Removed inode {} from ShardedMap", inode);
        }
    }
}

#[derive(Debug)]
pub struct ShardedDirMap {
    shards: Vec<Mutex<LruCache<u64, PathBuf>>>,
}

impl ShardedDirMap {
    pub fn new(capacity: usize) -> Arc<Self> {
        let per_shard = NonZeroUsize::new((capacity / SHARD_COUNT).max(100)).unwrap();
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Mutex::new(LruCache::new(per_shard)));
        }
        Arc::new(Self { shards })
    }

    fn get_shard(&self, inode: u64) -> &Mutex<LruCache<u64, PathBuf>> {
        &self.shards[(inode as usize) % SHARD_COUNT]
    }

    pub fn put(&self, inode: u64, path: PathBuf) {
        self.get_shard(inode).lock().put(inode, path);
    }

    pub fn get(&self, inode: u64) -> Option<PathBuf> {
        self.get_shard(inode).lock().get(&inode).cloned()
    }

    pub fn remove(&self, inode: u64) {
        self.get_shard(inode).lock().pop(&inode);
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
    }
}

pub fn update_map(map: &ShardedInodeMap, dir_map: &ShardedDirMap, _dev: u32, inode: u64, path: PathBuf, generation: u32, is_synthetic: bool, is_dir: bool, ts: u64, seq: u64) {
    if !is_synthetic {
        map.put(inode, IdentityEntry::new(path.clone(), generation, ts, seq));
    }
    if is_dir {
        dir_map.put(inode, path);
    }
}

pub fn update_map_after_rename(map: &ShardedInodeMap, dir_map: &ShardedDirMap, _dev: u32, inode: u64, new_path: PathBuf, generation: u32, is_dir: bool, ts: u64, seq: u64) {
    map.put(inode, IdentityEntry::new(new_path.clone(), generation, ts, seq));
    if is_dir {
        dir_map.put(inode, new_path);
    }
}

pub fn remove_entry(map: &ShardedInodeMap, dir_map: &ShardedDirMap, _dev: u32, inode: u64) {
    map.remove(inode);
    dir_map.remove(inode);
}

pub fn resolve_target(
    inode_map: &ShardedInodeMap,
    event: &Event,
    target_root: &std::path::Path
) -> ResolveResult {
    // 1. Check Cache
    if let Some(entry) = inode_map.get_entry_clone(event.inode) {
        let match_gen = entry.generation == event.generation
                        || entry.generation == 0
                        || entry.generation == std::u32::MAX;
        
        if !match_gen {
            warn!("Identity Mismatch: Inode {} cached gen {} != event gen {}. Invalidating.",
                  event.inode, entry.generation, event.generation);
            inode_map.remove(event.inode);
            metrics::IDENTITY_CACHE_HIT_RATE.set(0.0);
            metrics::GENERATION_MISMATCHES.inc();
            
            let identity_dir = target_root.join(".mirror").join(".by-identity");
            let filename = format!("{}_{}_{}", event.dev_id, event.inode, event.generation);
            return ResolveResult::NeedsRepair(identity_dir.join(filename));
        } else {
             metrics::IDENTITY_CACHE_HIT_RATE.set(1.0);
             return ResolveResult::Success(target_root.join(&entry.path), false, false);
        }
    } else {
        metrics::IDENTITY_CACHE_HIT_RATE.set(0.0);
    }

    // 2. Resolve via Parent
    if event.parent_inode != 0 {
        if let Some(parent_path) = inode_map.get_path(event.parent_inode) {
            let full_path = parent_path.join(&event.name);
            inode_map.put(event.inode, IdentityEntry::new(
                full_path.clone(),
                event.generation,
                event.timestamp_ns,
                event.seq_num,
            ));
            return ResolveResult::Success(target_root.join(full_path), false, false);
        }
    }

    // 3. Simple Path Fallback
    let event_path = PathBuf::from(&event.name);
    if !event.name.is_empty() && !event.name.contains('/') {
        // Name is a single component but we failed parent lookup (or root)
    } else if !event.name.is_empty() {
        inode_map.put(event.inode, IdentityEntry::new(
            event_path.clone(),
            event.generation,
            event.timestamp_ns,
            event.seq_num,
        ));
        return ResolveResult::Success(target_root.join(event_path), false, false);
    }

    // 4. Synthetic Identity Fallback
    let identity_dir = target_root.join(".mirror").join(".by-identity");
    let filename = format!("{}_{}_{}", event.dev_id, event.inode, event.generation);
    let synthetic_path = identity_dir.join(filename);
    
    ResolveResult::Success(synthetic_path, true, true)
}

pub fn resolve_directory(dir_map: &ShardedDirMap, inode_map: &ShardedInodeMap, _dev: u32, inode: u64) -> Option<PathBuf> {
    if let Some(p) = dir_map.get(inode) {
        return Some(p);
    }
    if let Some(entry) = inode_map.get_entry_clone(inode) {
        dir_map.put(inode, entry.path.clone());
        return Some(entry.path);
    }
    None
}

pub fn resolve_and_update_path(
    source: &crate::mirror::SourceInfo,
    inode: u64,
    generation_hint: u32,
    ts_hint: u64,
    seq_hint: u64
) -> io::Result<PathBuf> {
    let start_time = std::time::Instant::now();
    let _timer = metrics::INODE_LOOKUP_DURATION.start_timer();

    if let Some(watcher) = &source.identity_watcher {
        if let Some(path) = watcher.resolve(inode) {
             debug!("IDENTITY: Reverse Index HIT for Inode {} -> {:?}", inode, path);
             source.inode_map.put(inode, IdentityEntry::new(path.clone(), generation_hint, ts_hint, seq_hint));
             return Ok(path);
        }
    }

    debug!("IDENTITY: Starting aggressive lookup for Inode {} in {:?}", inode, source.mount);
    let mut found_path = None;
    
    for entry in walkdir::WalkDir::new(&source.mount).min_depth(1) {
        if let Ok(entry) = entry {
            if entry.depth() > 20 { continue; }
            if let Ok(metadata) = entry.metadata() {
                if metadata.ino() == inode {
                    if let Ok(rel_path) = entry.path().strip_prefix(&source.mount) {
                        found_path = Some(rel_path.to_path_buf());
                        let rel_path_buf = rel_path.to_path_buf();
                        source.inode_map.put(inode, IdentityEntry::new(rel_path_buf.clone(), generation_hint, ts_hint, seq_hint));
                        if metadata.is_dir() {
                             source.dir_map.put(inode, rel_path_buf.clone());
                        }
                        break;
                    }
                }
            }
        }
    }

    if let Some(path) = found_path {
        if fs::metadata(source.mount.join(&path)).is_ok() {
            return Ok(path);
        }
    }

    warn!("IDENTITY: Failed to resolve Inode {} after full walk. Duration: {:?}", inode, start_time.elapsed());
    Err(io::Error::new(io::ErrorKind::NotFound, "Inode not found after aggressive search."))
}
