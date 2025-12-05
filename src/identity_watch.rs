use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use notify::{RecommendedWatcher, RecursiveMode, Watcher, Event, EventKind};
use std::os::unix::fs::MetadataExt;
use tracing::{info, warn, error};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct InotifyIndex {
    index: DashMap<u64, PathBuf>,
    root: PathBuf,
    _watcher: Option<RecommendedWatcher>,
}

impl InotifyIndex {
    pub fn new(root: PathBuf) -> Arc<Self> {
        let index = DashMap::new();
        
        let (tx, rx) = std::sync::mpsc::channel();
        
        let mut watcher = match notify::recommended_watcher(tx) {
            Ok(w) => w,
            Err(e) => {
                error!("IDENTITY: Failed to create Inotify watcher: {}. Fallback to walkdir enabled.", e);
                return Arc::new(Self { index: DashMap::new(), root, _watcher: None });
            }
        };

        if let Err(e) = watcher.watch(&root, RecursiveMode::Recursive) {
            error!("IDENTITY: Failed to start watching {:?}: {}", root, e);
            return Arc::new(Self { index: DashMap::new(), root, _watcher: None });
        }

        let instance = Arc::new(Self {
            index,
            root: root.clone(),
            _watcher: Some(watcher),
        });

        let index_clone = instance.index.clone();
        let root_for_thread = root.clone();
        
        std::thread::Builder::new()
            .name("foxing-identity-watch".into())
            .spawn(move || {
                info!("IDENTITY: Reverse Index Watcher started for {:?}", root_for_thread);
                
                let start = std::time::Instant::now();
                let mut count = 0;
                for entry in WalkDir::new(&root_for_thread).into_iter().filter_map(|e| e.ok()) {
                    if let Ok(meta) = entry.metadata() {
                        let inode = meta.ino();
                        if let Ok(rel) = entry.path().strip_prefix(&root_for_thread) {
                            if !rel.as_os_str().is_empty() {
                                index_clone.insert(inode, rel.to_path_buf());
                                count += 1;
                            }
                        }
                    }
                }
                info!("IDENTITY: Initial scan indexed {} items in {:?}", count, start.elapsed());

                while let Ok(res) = rx.recv() {
                    match res {
                        Ok(event) => Self::handle_event(&index_clone, &root_for_thread, event),
                        Err(e) => warn!("IDENTITY: Watch error: {:?}", e),
                    }
                }
            })
            .expect("Failed to spawn identity watcher thread");

        instance
    }

    fn handle_event(index: &DashMap<u64, PathBuf>, root: &Path, event: Event) {
        match event.kind {
            EventKind::Create(_) => {
                for path in event.paths {
                    if path.exists() {
                        if let Ok(meta) = std::fs::symlink_metadata(&path) {
                            let inode = meta.ino();
                            if let Ok(rel) = path.strip_prefix(root) {
                                index.insert(inode, rel.to_path_buf());
                            }
                        }
                    }
                }
            },
            EventKind::Remove(_) => {
            },
            _ => {}
        }
    }

    pub fn resolve(&self, inode: u64) -> Option<PathBuf> {
        if let Some(entry) = self.index.get(&inode) {
            let rel_path = entry.value();
            let full_path = self.root.join(rel_path);
            
            if let Ok(meta) = std::fs::symlink_metadata(&full_path) {
                if meta.ino() == inode {
                    return Some(rel_path.clone());
                } else {
                    drop(entry);
                    self.index.remove(&inode);
                }
            } else {
                drop(entry);
                self.index.remove(&inode);
            }
        }
        None
    }
}
