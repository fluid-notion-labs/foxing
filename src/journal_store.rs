use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write, BufReader, BufRead};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use serde::{Serialize};
use crate::event::Event;
use sysinfo::{Disks};
use chrono::Utc;
use tracing::{info, error, debug, warn};
use glob::glob;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Serialize)]
struct RecoveryLayout {
    timestamp: String,
    source_path: PathBuf,
    daemon_id: String,
    disks: Vec<DiskInfo>,
    tuning_profile: String, 
}

#[derive(Serialize)]
struct DiskInfo {
    name: String,
    kind: String,
    file_system: String,
    mount_point: String,
    total_space: u64,
    is_removable: bool,
}

#[derive(Debug)]
pub struct JournalStore {
    event_writer: Arc<Mutex<BufWriter<File>>>,
    bytes_written: Arc<Mutex<u64>>,
    journal_path: PathBuf,
    size_limit_bytes: u64,
    retention_count: usize,
    buffer_capacity: usize,
    last_recovered_seq: AtomicU64,
}

impl JournalStore {
    pub fn new(
        source_journal_path: &Path, 
        target_root: &Path, 
        source_mount: &Path, 
        daemon_id: &str,
        buffer_size_bytes: usize,
        tuning_profile: &str,
        size_limit_mb: u64,
        retention_count: usize,
    ) -> io::Result<Self> {
        if let Some(parent) = source_journal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(source_journal_path)?;
            
        debug!("JOURNAL: Initializing with write buffer: {} KB", buffer_size_bytes / 1024);
        let writer = BufWriter::with_capacity(buffer_size_bytes, file);

        let target_meta_path = target_root.join(".mirror").join(format!("source_layout_{}.json", daemon_id));
        if let Err(e) = Self::write_recovery_layout(&target_meta_path, source_mount, daemon_id, tuning_profile) {
            warn!("JOURNAL: Could not write recovery layout: {}", e);
        } else {
            info!("JOURNAL: Wrote source recovery layout to {:?}", target_meta_path);
        }

        let current_size = std::fs::metadata(source_journal_path).map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            event_writer: Arc::new(Mutex::new(writer)),
            bytes_written: Arc::new(Mutex::new(current_size)),
            journal_path: source_journal_path.to_path_buf(),
            size_limit_bytes: size_limit_mb * 1024 * 1024,
            retention_count,
            buffer_capacity: buffer_size_bytes,
            last_recovered_seq: AtomicU64::new(0),
        })
    }

    pub fn get_last_sequence(&self) -> u64 {
        self.last_recovered_seq.load(Ordering::Relaxed)
    }

    pub fn replay<F>(&self, mut callback: F) -> io::Result<usize> 
    where F: FnMut(Event) {
        if !self.journal_path.exists() { return Ok(0); }
        
        let file = File::open(&self.journal_path)?;
        let reader = BufReader::new(file);
        let mut count = 0;
        let mut max_seq = 0;

        for line in reader.lines() {
            if let Ok(l) = line {
                if let Ok(evt) = serde_json::from_str::<Event>(&l) {
                    if evt.seq_num > max_seq {
                        max_seq = evt.seq_num;
                    }
                    callback(evt);
                    count += 1;
                }
            }
        }
        
        self.last_recovered_seq.store(max_seq, Ordering::Relaxed);
        info!("JOURNAL: Replay Complete. Loaded {} events. Max Sequence: {}", count, max_seq);
        Ok(count)
    }

    pub fn append(&self, event: &Event) {
        if let Ok(json) = serde_json::to_string(event) {
            self.check_rotation_needed();
            let mut w = self.event_writer.lock().unwrap();
            if let Err(e) = writeln!(w, "{}", json) {
                error!("JOURNAL: Failed to write event to disk: {}", e);
            } else {
                let len = json.len() as u64 + 1;
                if let Ok(mut bw) = self.bytes_written.lock() { *bw += len; }
            }
        }
    }

    pub fn append_batch(&self, events: &[Arc<Event>]) -> io::Result<u64> {
        let mut buffer = Vec::with_capacity(events.len() * 128);
        for evt in events {
            // FIX: Explicitly dereference Arc (&**evt) to get &Event, which implements Serialize
            if let Ok(json) = serde_json::to_string(&**evt) {
                buffer.extend_from_slice(json.as_bytes());
                buffer.push(b'\n');
            }
        }

        let len = buffer.len();
        if len == 0 { return Ok(0); }

        self.check_rotation_needed();

        let mut w = self.event_writer.lock().unwrap();
        w.write_all(&buffer)?;
        
        if let Ok(mut bw) = self.bytes_written.lock() { *bw += len as u64; }
        
        Ok(len as u64)
    }

    pub fn flush(&self) -> io::Result<()> {
        let mut w = self.event_writer.lock().unwrap();
        w.flush()
    }

    fn check_rotation_needed(&self) {
        if let Ok(bw) = self.bytes_written.lock() {
            if *bw < self.size_limit_bytes { return; }
        }

        if let Ok(mut w) = self.event_writer.lock() {
            if let Ok(mut bw) = self.bytes_written.lock() {
                if *bw < self.size_limit_bytes { return; }
                
                info!("JOURNAL: Rotating log file (Size: {} bytes)", *bw);
                let _ = w.flush();
                
                let timestamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
                let archived_path = format!("{}.{}", self.journal_path.to_string_lossy(), timestamp);
                
                if let Err(e) = std::fs::rename(&self.journal_path, &archived_path) {
                    error!("JOURNAL: Failed to rotate (rename) log: {}", e);
                    return;
                }

                match OpenOptions::new().create(true).append(true).write(true).open(&self.journal_path) {
                    Ok(new_file) => {
                        *w = BufWriter::with_capacity(self.buffer_capacity, new_file);
                        *bw = 0;
                        info!("JOURNAL: Rotated to new log file.");
                    },
                    Err(e) => {
                        error!("JOURNAL: Failed to open new log file after rotation: {}", e);
                        let _ = std::fs::rename(&archived_path, &self.journal_path);
                    }
                }
            }
        }
        
        let pattern = format!("{}.*", self.journal_path.to_string_lossy());
        let retention = self.retention_count;
        std::thread::spawn(move || {
            Self::cleanup_old_logs(&pattern, retention);
        });
    }

    fn cleanup_old_logs(pattern: &str, retention: usize) {
        let mut files: Vec<PathBuf> = Vec::new();
        if let Ok(paths) = glob(pattern) {
            for entry in paths {
                if let Ok(path) = entry {
                    files.push(path);
                }
            }
        }
        files.sort_by_key(|f| std::fs::metadata(f).and_then(|m| m.modified()).ok());
        
        let total_files = files.len();
        if total_files > retention {
            let to_delete = total_files - retention;
            for i in 0..to_delete {
                if let Err(e) = std::fs::remove_file(&files[i]) {
                    warn!("JOURNAL: Failed to delete old log {:?}: {}", files[i], e);
                }
            }
        }
    }

    fn write_recovery_layout(path: &Path, source_mount: &Path, daemon_id: &str, tuning_profile: &str) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let disks = Disks::new_with_refreshed_list();

        let disk_infos: Vec<DiskInfo> = disks.iter().map(|d| {
            DiskInfo {
                name: d.name().to_string_lossy().to_string(),
                kind: format!("{:?}", d.kind()),
                file_system: d.file_system().to_string_lossy().to_string(),
                mount_point: d.mount_point().to_string_lossy().to_string(),
                total_space: d.total_space(),
                is_removable: d.is_removable(),
            }
        }).collect();

        let layout = RecoveryLayout {
            timestamp: Utc::now().to_rfc3339(),
            source_path: source_mount.to_path_buf(),
            daemon_id: daemon_id.to_string(),
            disks: disk_infos,
            tuning_profile: tuning_profile.to_string(),
        };

        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, &layout)?;
        Ok(())
    }
}
