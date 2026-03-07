// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/src/identity_watch.rs — Identity map change watcher

//! Monitors identity map changes for proactive cache invalidation.

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use notify::{RecommendedWatcher, RecursiveMode, Watcher, Event, EventKind};
use std::os::unix::fs::MetadataExt;
use tracing::{info, warn, error, debug};
use walkdir::WalkDir;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct ProactiveIndex {
    index: DashMap<u64, PathBuf>,
    root: PathBuf,
    _watcher: Arc<Mutex<Option<RecommendedWatcher>>>,
    scan_complete: AtomicBool,
}

impl ProactiveIndex {
    pub fn new(root: PathBuf) -> Arc<Self> {
        let index = DashMap::new();
        let (tx, rx) = std::sync::mpsc::channel();
        
        let watcher_opt = match notify::recommended_watcher(tx) {
            Ok(w) => Some(w),
            Err(e) => {
                error!("IDENTITY: Failed to create Inotify watcher: {}. Index will be static.", e);
                None
            }
        };

        let watcher_arc = Arc::new(Mutex::new(watcher_opt));
        
        let instance = Arc::new(Self {
            index,
            root: root.clone(),
            _watcher: watcher_arc.clone(),
            scan_complete: AtomicBool::new(false),
        });

        let index_clone = instance.index.clone();
        let root_for_thread = root.clone();
        let instance_clone = instance.clone();
        let watcher_in_thread = watcher_arc.clone();

        std::thread::Builder::new()
            .name("foxing-identity-scan".into())
            .spawn(move || {
                info!("IDENTITY: Full scan started for {:?}", root_for_thread);
                let start = std::time::Instant::now();
                let mut count = 0;

                // Initial Scan
                for entry in WalkDir::new(&root_for_thread)
                    .follow_links(false)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    if let Ok(meta) = entry.metadata() {
                        let inode = meta.ino();
                        if let Ok(rel) = entry.path().strip_prefix(&root_for_thread) {
                            if !rel.as_os_str().is_empty() {
                                index_clone.insert(inode, rel.to_path_buf());
                                count += 1;
                            }
                        }
                    }
                    if count % 100 == 0 {
                        std::thread::yield_now();
                    }
                    if count % 1000 == 0 {
                        std::thread::sleep(std::time::Duration::from_micros(50));
                    }
                }

                instance_clone.scan_complete.store(true, Ordering::SeqCst);
                info!("IDENTITY: Full scan complete. Indexed {} items in {:?}.", count, start.elapsed());

                // Enable Watcher
                {
                    let mut lock = watcher_in_thread.lock().unwrap();
                    if let Some(w) = lock.as_mut() {
                        info!("IDENTITY: Enabling recursive watcher post-scan.");
                        if let Err(e) = w.watch(&root_for_thread, RecursiveMode::Recursive) {
                            if e.to_string().contains("OS file watch limit reached") {
                                warn!("IDENTITY: OS file watch limit reached. Continuing in polling/scan mode. (Hint: Increase fs.inotify.max_user_watches)");
                            } else {
                                warn!("IDENTITY: Failed to activate watcher: {}", e);
                            }
                        }
                    }
                }

                // Event Loop
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
            EventKind::Modify(_) | EventKind::Access(_) => return,
            EventKind::Create(_) => {
                for path in event.paths {
                    if path.exists() {
                        // Critical Fix: When a directory is created (e.g. `cp -r`), 
                        // we must immediately scan it to catch any files that were 
                        // created inside it before we processed this event.
                        if path.is_dir() {
                            debug!("IDENTITY: New directory detected: {:?}. Deep scanning...", path);
                            // We use a small buffer or immediate insertion to minimize latency
                            for entry in WalkDir::new(&path)
                                .follow_links(false)
                                .into_iter()
                                .filter_map(|e| e.ok()) 
                            {
                                if let Ok(meta) = entry.metadata() {
                                    let inode = meta.ino();
                                    if let Ok(rel) = entry.path().strip_prefix(root) {
                                        // Insert/Update Identity Map
                                        index.insert(inode, rel.to_path_buf());
                                    }
                                }
                            }
                        } else {
                            // Standard file creation
                            if let Ok(meta) = std::fs::symlink_metadata(&path) {
                                let inode = meta.ino();
                                if let Ok(rel) = path.strip_prefix(root) {
                                    index.insert(inode, rel.to_path_buf());
                                }
                            }
                        }
                    }
                }
            },
            EventKind::Remove(_) => {
                // For removals, we generally let the inode map lazily evict or handle via BPF events.
                // Immediate removal here could race with a Rename event if not careful.
            },
            _ => {}
        }
    }

    pub fn resolve(&self, inode: u64) -> Option<PathBuf> {
        if let Some(entry) = self.index.get(&inode) {
            let rel_path = entry.value();
            let full_path = self.root.join(rel_path);
            
            // Verify inode match to prevent stale cache issues (Identity collision)
            // This checks if the path we *think* maps to the inode actually still does.
            if let Ok(meta) = std::fs::symlink_metadata(&full_path) {
                if meta.ino() == inode {
                    return Some(rel_path.clone());
                } else {
                    // Stale entry detected (Inode mismatch)
                    drop(entry);
                    self.index.remove(&inode);
                }
            } else {
                // File gone
                drop(entry);
                self.index.remove(&inode);
            }
        }
        None
    }

    pub fn wait_for_scan(&self) {
        while !self.scan_complete.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}
