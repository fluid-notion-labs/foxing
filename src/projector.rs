use std::sync::Arc;
use crate::identity::{ShardedInodeMap, ShardedDirMap, IdentityEntry};
use crate::event::{Event, EventType};
use std::path::PathBuf;
use libc;

#[derive(Debug)]
pub struct IdentityProjector {
    inode_map: Arc<ShardedInodeMap>,
    dir_map: Arc<ShardedDirMap>,
    _source_dev: u32,
}

impl IdentityProjector {
    pub fn new(inode_map: Arc<ShardedInodeMap>, dir_map: Arc<ShardedDirMap>, source_dev: u32) -> Self {
        Self {
            inode_map,
            dir_map,
            _source_dev: source_dev,
        }
    }

    pub fn project(&self, event: &Event) {
        match event.event_type {
            EventType::Rename => {
                if let Some(new_name) = &event.new_name {
                    let is_dir = (event.mode & libc::S_IFMT) == libc::S_IFDIR;
                    
                    let parent_path = if event.new_parent_inode != 0 {
                        self.dir_map.get(event.new_parent_inode)
                    } else {
                        None
                    };

                    let new_rel_path = if let Some(pp) = parent_path {
                        pp.join(new_name)
                    } else {
                        PathBuf::from(new_name)
                    };

                    self.update_identity(event.inode, new_rel_path.clone(), event.generation, event.timestamp_ns, event.seq_num);
                    
                    if is_dir {
                        self.dir_map.put(event.inode, new_rel_path);
                    }
                }
            },
            EventType::Mkdir => {
                if event.parent_inode != 0 {
                    if let Some(parent_path) = self.dir_map.get(event.parent_inode) {
                        let full_path = parent_path.join(&event.name);
                        self.update_identity(event.inode, full_path.clone(), event.generation, event.timestamp_ns, event.seq_num);
                        self.dir_map.put(event.inode, full_path);
                    }
                }
            },
            EventType::Unlink | EventType::Rmdir => {
                self.inode_map.remove(event.inode);
                self.dir_map.remove(event.inode);
            },
            _ => {}
        }
    }

    fn update_identity(&self, inode: u64, path: PathBuf, generation: u32, ts: u64, seq: u64) {
        self.inode_map.put(inode, IdentityEntry::new(path, generation, ts, seq));
    }
}
