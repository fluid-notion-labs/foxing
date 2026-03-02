use crate::identity::{ShardedInodeMap, ShardedDirMap, IdentityEntry};
use crate::event::{Event, EventType};
use std::path::{Path, PathBuf};
use libc;
use tracing::{debug, warn, error};
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::collections::HashMap;

#[derive(Debug)]
pub struct IdentityProjector {
    inode_map: ShardedInodeMap,
    dir_map: ShardedDirMap,
    _source_dev: u32,
    pending_renames: Mutex<HashMap<u64, Vec<Event>>>,
}

impl IdentityProjector {
    pub fn new(inode_map: ShardedInodeMap, dir_map: ShardedDirMap, source_dev: u32) -> Self {
        Self {
            inode_map,
            dir_map,
            _source_dev: source_dev,
            pending_renames: Mutex::new(HashMap::new()),
        }
    }

    fn find_inode_by_rel_path(&self, rel_path: &Path) -> Option<u64> {
        for entry in self.inode_map.iter() {
            if entry.paths.read().iter().any(|p| p == rel_path) {
                return Some(*entry.key());
            }
        }
        None
    }

    pub fn project(&self, event: &Event) {
        match event.event_type {
            EventType::Rename => {
                if !self.inode_map.contains_key(&event.inode) {
                    debug!("Projector: Deferring rename for unknown inode {}", event.inode);
                    let mut pending = self.pending_renames.lock().unwrap();
                    pending.entry(event.inode).or_default().push(event.clone());
                    return;
                }

                if let Some(new_name) = &event.new_name {
                    let is_dir = (event.mode & libc::S_IFMT) == libc::S_IFDIR;
                    let (path_a, path_b) = {
                        let parent_a = self.dir_map.get(&event.parent_inode).map(|r| r.value().clone());
                        let parent_b = if event.new_parent_inode != 0 {
                            self.dir_map.get(&event.new_parent_inode).map(|r| r.value().clone())
                        } else {
                            None
                        };
                        let rel_path_a = if let Some(p) = parent_a { p.join(&event.name) } else { PathBuf::from(&event.name) };
                        let rel_path_b = if let Some(p) = parent_b { p.join(new_name) } else { PathBuf::from(new_name) };
                        (rel_path_a, rel_path_b)
                    };

                    if (event.flags & libc::RENAME_EXCHANGE as u32) != 0 {
                        let inode_b_opt = self.find_inode_by_rel_path(&path_b);
                        if let Some(inode_b) = inode_b_opt {
                            debug!("Projector: RENAME_EXCHANGE detected. Swapping Inode {} (A) with Inode {} (B).", event.inode, inode_b);
                            let is_dir_b = self.dir_map.contains_key(&inode_b);
                            // Correctly handle the swap for both inodes
                            self.swap_identity_path(inode_b, &path_b, path_a.clone(), is_dir_b);
                            self.swap_identity_path(event.inode, &path_a, path_b.clone(), is_dir);
                            
                            if is_dir {
                                self.dir_map.insert(event.inode, path_b);
                            }
                            if is_dir_b {
                                self.dir_map.insert(inode_b, path_a);
                            }
                        } else {
                            let parent_of_b = path_b.parent().unwrap_or(Path::new(""));
                            warn!("Projector: RENAME_EXCHANGE inconsistency. Destination Inode (B) not found for path {:?}. \
                                   Identity map partially corrupted. ACTION REQUIRED: Re-scan directory {:?} to recover Inode B.",
                                   path_b, parent_of_b);
                            self.update_identity(event.inode, path_b.clone(), event.generation, event.timestamp_ns, event.seq_num);
                            if is_dir {
                                self.dir_map.insert(event.inode, path_b);
                            }
                        }
                    } else {
                        // Standard Rename
                        self.remove_identity_path(event.inode, &path_a);
                        self.update_identity(event.inode, path_b.clone(), event.generation, event.timestamp_ns, event.seq_num);
                        if is_dir {
                            self.dir_map.insert(event.inode, path_b.clone());
                        }
                        
                        // Fence check to ensure visibility
                        if let Some(entry) = self.inode_map.get(&event.inode) {
                            let paths = entry.paths.read();
                            if !paths.contains(&path_b) {
                                error!("Projector CONSISTENCY FAILURE: Rename update for inode {} not visible immediately. Path: {:?}",
                                       event.inode, path_b);
                                drop(paths);
                                self.update_identity(event.inode, path_b.clone(), event.generation, event.timestamp_ns, event.seq_num);
                            }
                        }
                        debug!("Projector: Standard Rename detected. Updated Inode {} to {:?}", event.inode, path_b);
                    }
                }
            },
            EventType::Mkdir | EventType::Create | EventType::Mknod | EventType::Symlink => {
                if event.parent_inode != 0 {
                    if let Some(parent_path) = self.dir_map.get(&event.parent_inode) {
                        let full_path = parent_path.join(&event.name);
                        self.update_identity(event.inode, full_path.clone(), event.generation, event.timestamp_ns, event.seq_num);
                        if event.event_type == EventType::Mkdir {
                            self.dir_map.insert(event.inode, full_path);
                        }
                    }
                }
                // Replay deferred renames: Now that the inode exists, apply any queued renames.
                let deferred_events: Option<Vec<Event>> = {
                    let mut pending = self.pending_renames.lock().unwrap();
                    pending.remove(&event.inode)
                };
                if let Some(events) = deferred_events {
                    debug!("Projector: Replaying {} deferred renames for newly created Inode {}", events.len(), event.inode);
                    for e in events {
                        self.project(&e);
                    }
                }
            },
            EventType::Link => {
                if event.parent_inode != 0 {
                    if let Some(parent_path) = self.dir_map.get(&event.parent_inode) {
                        let full_path = parent_path.join(&event.name);
                        self.add_identity_path(event.inode, full_path, event.generation, event.timestamp_ns, event.seq_num);
                    }
                }
            },
            EventType::Unlink => {
                if event.parent_inode != 0 {
                     if let Some(parent_path) = self.dir_map.get(&event.parent_inode) {
                        let full_path = parent_path.join(&event.name);
                        self.remove_identity_path(event.inode, &full_path);
                    }
                }
            },
            EventType::Rmdir => {
                self.dir_map.remove(&event.inode);
                self.inode_map.remove(&event.inode);
            },
            _ => {}
        }
    }

    fn update_identity(&self, inode: u64, path: PathBuf, generation: u32, ts: u64, seq: u64) {
        match self.inode_map.entry(inode) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let e = entry.get();
                {
                    let mut paths = e.paths.write();
                    paths.retain(|p| p != &path);
                    if paths.last() != Some(&path) {
                        paths.push(path.clone());
                    }
                    std::sync::atomic::fence(Ordering::SeqCst);
                }
                *e.generation.write() = generation;
                *e.timestamp_ns.write() = ts;
                *e.seq_num.write() = seq;
                e.record_use();
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(IdentityEntry::new(path, generation, ts, seq));
                std::sync::atomic::fence(Ordering::SeqCst);
            }
        }
    }

    // FIXED: Critical Race Condition Fix
    // Instead of read -> clone -> modify -> write (check), hold write lock entire time.
    fn swap_identity_path(&self, inode: u64, old_path: &Path, new_path: PathBuf, _is_dir: bool) {
        if let Some(entry) = self.inode_map.get(&inode) {
            let mut guard = entry.paths.write();
            guard.retain(|p| p != old_path && p != &new_path);
            guard.push(new_path.clone());
            
            // Release lock implicitly when guard drops
            drop(guard);
            
            std::sync::atomic::fence(Ordering::SeqCst);
            entry.record_use();
        }
    }

    fn add_identity_path(&self, inode: u64, path: PathBuf, generation: u32, ts: u64, seq: u64) {
        match self.inode_map.entry(inode) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let e = entry.get();
                {
                    let mut paths = e.paths.write();
                    paths.retain(|p| p != &path);
                    if paths.last() != Some(&path) {
                        paths.push(path);
                    }
                    std::sync::atomic::fence(Ordering::SeqCst);
                }
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(IdentityEntry::new(path, generation, ts, seq));
                std::sync::atomic::fence(Ordering::SeqCst);
            }
        }
    }

    fn remove_identity_path(&self, inode: u64, path: &PathBuf) {
        let should_remove_entry = if let Some(entry) = self.inode_map.get(&inode) {
            let mut paths = entry.paths.write();
            paths.retain(|p| p != path);
            let empty = paths.is_empty();
            std::sync::atomic::fence(Ordering::SeqCst);
            drop(paths);
            if empty {
                entry.remove_path(path);
                true
            } else {
                false
            }
        } else {
            false
        };
        
        if should_remove_entry {
            self.inode_map.remove(&inode);
        }
    }
}
