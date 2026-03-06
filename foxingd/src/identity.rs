use std::path::{Path, PathBuf, Component};
use crate::error::{FoxingError, Result};
use std::sync::atomic::{Ordering, AtomicU64};
use std::time::Instant;
use parking_lot::RwLock;
use tracing::{debug, warn};
use dashmap::DashMap;
use std::sync::Arc;
use crate::event::Event;
use crate::metrics;
use fxcp_core::constants;

#[derive(Debug)]
pub struct IdentityEntry {
    pub paths: RwLock<Vec<PathBuf>>,
    pub generation: RwLock<u32>,
    pub timestamp_ns: RwLock<u64>,
    pub seq_num: RwLock<u64>,
    pub last_accessed_seq: AtomicU64,
    pub unlinked_at: RwLock<Option<Instant>>,
}

impl IdentityEntry {
    pub fn new(path: PathBuf, generation: u32, timestamp_ns: u64, seq_num: u64) -> Self {
        Self {
            paths: RwLock::new(vec![path]),
            generation: RwLock::new(generation),
            timestamp_ns: RwLock::new(timestamp_ns),
            seq_num: RwLock::new(seq_num),
            last_accessed_seq: AtomicU64::new(seq_num),
            unlinked_at: RwLock::new(None),
        }
    }

    pub fn primary_path(&self) -> PathBuf {
        let paths = self.paths.read();
        paths[0].clone()
    }

    pub fn add_path(&self, path: PathBuf) {
        let mut paths = self.paths.write();
        if !paths.contains(&path) {
            paths.push(path);
        }
        *self.unlinked_at.write() = None;
    }

    pub fn record_access(&self, seq: u64) {
        let _ = self.last_accessed_seq.fetch_max(seq, Ordering::Relaxed);
    }

    pub fn record_use(&self) {
        let mut unlinked = self.unlinked_at.write();
        if unlinked.is_some() {
             *unlinked = None;
        }
    }

    pub fn record_unlinked(&self) {
        let mut unlinked = self.unlinked_at.write();
        if unlinked.is_none() {
             *unlinked = Some(Instant::now());
        }
    }

    pub fn should_evict(&self, current_global_seq: u64) -> bool {
        if self.unlinked_at.read().is_some() {
             return true;
        }
        let last = self.last_accessed_seq.load(Ordering::Relaxed);
        if current_global_seq > last && (current_global_seq - last) > constants::CACHE_TTL_EVENTS {
            return true;
        }
        false
    }

    pub fn remove_path(&self, path: &Path) {
        let mut paths = self.paths.write();
        paths.retain(|p| p != path);
        if paths.is_empty() {
             drop(paths);
             self.record_unlinked();
        }
    }
}

pub type ShardedInodeMap = Arc<DashMap<u64, IdentityEntry>>;
pub type ShardedDirMap = Arc<DashMap<u64, PathBuf>>;

#[derive(Debug, PartialEq)]
pub enum ResolveResult {
    Success(PathBuf, bool, bool),
    NeedsRepair(PathBuf),
    SecurityBlock,
}

fn validate_target_root_containment(target_root: &Path, rel_path: &Path) -> bool {
    for component in rel_path.components() {
        match component {
            Component::Normal(_) => {},
            Component::CurDir => {},
            _ => {
                warn!("Security: Path Traversal attempt detected in relative path: {:?}", rel_path);
                return false;
            }
        }
    }
    let full_path = target_root.join(rel_path);
    if full_path.starts_with(target_root) { true } else { false }
}

pub fn resolve_target(
    inode_map: &ShardedInodeMap,
    dir_map: &ShardedDirMap,
    event: &Event,
    target_root: &Path
) -> ResolveResult {
    std::sync::atomic::fence(Ordering::Acquire);
    
    // Optimistic read first
    let (result, rel_path_check) = if let Some(entry) = inode_map.get(&event.inode) {
        entry.record_access(event.seq_num);
        
        let specific_rel_path = if event.parent_inode != 0 && !event.name.is_empty() {
             if let Some(parent_path) = dir_map.get(&event.parent_inode) {
                 Some(parent_path.join(&event.name))
             } else {
                 None
             }
        } else {
            None
        };

        let entry_gen = *entry.generation.read();
        
        if entry_gen != std::u32::MAX && entry_gen != event.generation {
            metrics::GENERATION_MISMATCHES.inc();
            let fallback = if let Some(p) = specific_rel_path {
                target_root.join(p)
            } else {
                target_root.join(&event.name)
            };
            (ResolveResult::NeedsRepair(fallback), None)
        } else if let Some(rel_path) = specific_rel_path {
            let paths_guard = entry.paths.read();
            let known = paths_guard.contains(&rel_path);
            drop(paths_guard);
            
            if !known {
                debug!("Identity: Lazy tree repair for inode {}. Updating path to {:?}", event.inode, rel_path);
                if event.nlink <= 1 {
                    entry.paths.write().clear();
                }
                entry.add_path(rel_path.clone());
            }
            let full_target_path = target_root.join(&rel_path);
            (ResolveResult::Success(full_target_path, false, false), Some(rel_path))
        } else {
            let primary = target_root.join(entry.primary_path());
            (ResolveResult::Success(primary, false, false), Some(entry.primary_path()))
        }
    } else {
        // Inode not in map — resolve path via parent_inode + name from dir_map.
        // If parent not in dir_map, try to resolve it from the inode_map
        // (the Mkdir event may have populated it there).
        let rel_path = if event.parent_inode != 0 && !event.name.is_empty() {
            if let Some(parent_path) = dir_map.get(&event.parent_inode) {
                parent_path.join(&event.name)
            } else if let Some(parent_entry) = inode_map.get(&event.parent_inode) {
                // Parent is in inode_map but not dir_map — add it to dir_map
                let parent_path = parent_entry.primary_path();
                dir_map.insert(event.parent_inode, parent_path.clone());
                parent_path.join(&event.name)
            } else {
                PathBuf::from(&event.name)
            }
        } else {
            PathBuf::from(&event.name)
        };
        let fallback_path = target_root.join(&rel_path);
        let r = if !event.name.is_empty() {
            ResolveResult::Success(fallback_path.clone(), true, true)
        } else {
            ResolveResult::NeedsRepair(fallback_path.clone())
        };
        (r, Some(rel_path))
    };

    if let Some(rel) = rel_path_check {
        if !validate_target_root_containment(target_root, &rel) {
            return ResolveResult::SecurityBlock;
        }
    }
    result
}

pub fn prune_expired_entries(inode_map: &ShardedInodeMap, limit: usize, current_global_seq: u64) {
    if inode_map.len() > limit {
        debug!("Identity Map exceeded high water mark ({}). Starting intelligent eviction (Epoch: {}).", limit, current_global_seq);
        let target_len = limit * 9 / 10;
        let excess_count = inode_map.len().saturating_sub(target_len);
        
        if excess_count == 0 { return; }

        let mut candidates: Vec<(u64, bool, u64)> = inode_map.iter()
            .map(|r| {
                let unlinked = r.value().unlinked_at.read().is_some();
                let last = r.value().last_accessed_seq.load(Ordering::Relaxed);
                (*r.key(), unlinked, last)
            })
            .collect();

        candidates.sort_unstable_by(|a, b| {
            if a.1 && !b.1 {
                std::cmp::Ordering::Less
            } else if !a.1 && b.1 {
                std::cmp::Ordering::Greater
            } else {
                a.2.cmp(&b.2)
            }
        });

        let removed_count = candidates.iter().take(excess_count).filter(|(inode, _, _)| {
            inode_map.remove(inode).is_some()
        }).count();

        debug!("Identity Map eviction complete. Removed {} entries (Target: {}).", removed_count, excess_count);
    } else {
        inode_map.retain(|_, v| !v.should_evict(current_global_seq));
    }
}

pub fn update_map(
    inode_map: &ShardedInodeMap,
    dir_map: &ShardedDirMap,
    _dev: u32,
    inode: u64,
    rel_path: PathBuf,
    generation: u32,
    is_synthetic: bool,
    is_dir: bool,
    timestamp_ns: u64,
    seq_num: u64,
) {
    if inode == 0 { return; }
    
    if is_dir {
        dir_map.insert(inode, rel_path.clone());
    }

    match inode_map.entry(inode) {
        dashmap::mapref::entry::Entry::Occupied(entry) => {
            let e = entry.get();
            e.add_path(rel_path);
            e.record_access(seq_num);
        },
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(IdentityEntry::new(rel_path, generation, timestamp_ns, seq_num));
        }
    }

    if is_synthetic {
        metrics::SYNTHETIC_IDENTITY_FILES.inc();
    }
}

pub fn update_map_after_rename(
    inode_map: &ShardedInodeMap,
    dir_map: &ShardedDirMap,
    _dev: u32,
    inode: u64,
    new_rel_path: PathBuf,
    generation: u32,
    is_dir: bool,
    timestamp_ns: u64,
    seq_num: u64,
) {
    if inode == 0 { return; }

    match inode_map.entry(inode) {
        dashmap::mapref::entry::Entry::Occupied(entry) => {
            let e = entry.get();
            e.add_path(new_rel_path.clone());
            *e.generation.write() = generation;
            *e.timestamp_ns.write() = timestamp_ns;
            *e.seq_num.write() = seq_num;
            e.record_access(seq_num);
        },
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(IdentityEntry::new(new_rel_path.clone(), generation, timestamp_ns, seq_num));
        }
    }

    if is_dir {
        dir_map.insert(inode, new_rel_path);
    }
}

pub fn remove_entry(inode_map: &ShardedInodeMap, dir_map: &ShardedDirMap, _dev: u32, inode: u64) {
    if inode == 0 { return; }
    if let Some(entry) = inode_map.get(&inode) {
        entry.record_unlinked();
    }
    dir_map.remove(&inode);
}

pub fn resolve_live_path(source: &Arc<crate::mirror::SourceInfo>, inode: u64, generation: u32) -> Option<PathBuf> {
    if let Some(index) = &source.identity_index {
        if let Some(rel_path) = index.resolve(inode) {
            return Some(rel_path);
        }
    }
    if let Some(entry) = source.inode_map.get(&inode) {
        let entry_gen = *entry.generation.read();
        if entry_gen == std::u32::MAX || entry_gen == generation {
            return Some(entry.primary_path());
        }
    }
    use std::fs::File;
    use std::os::unix::io::AsRawFd;
    if let Ok(root_file) = File::open(&source.mount) {
        let fd = root_file.as_raw_fd();
        match fxcp_core::operations::btrfs_resolve_inode(fd, inode) {
            Ok(path) => {
                if let Some(ref root_offset) = source.fs_root_relative_path {
                    if let Ok(stripped) = path.strip_prefix(root_offset) {
                        return Some(stripped.to_path_buf());
                    }
                }
                return Some(path);
            },
            Err(_) => {}
        }
    }
    None
}

pub fn resolve_and_update_path(source: &Arc<crate::mirror::SourceInfo>, inode: u64, generation: u32, timestamp_ns: u64, seq_num: u64) -> Result<PathBuf> {
    if let Some(live_path) = resolve_live_path(source, inode, generation) {
        let is_dir = std::fs::metadata(&source.mount.join(&live_path)).map(|m| m.is_dir()).unwrap_or(false);
        update_map(&source.inode_map, &source.dir_map, source.dev, inode, live_path.clone(), generation, false, is_dir, timestamp_ns, seq_num);
        return Ok(live_path);
    }
    Err(FoxingError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, format!("Failed to resolve or update path for inode {}", inode))))
}
