use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json;
use std::os::unix::io::AsRawFd;
use libc;
use std::io::{self, Seek, SeekFrom};
use xattr;
use tracing::{warn};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum WalState {
    WriteBulk,
    FsyncCommit,
    PendingRename,
    Unknown
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PersistedWalEntry {
    pub seq: u64,
    pub state: WalState,
    pub timestamp: u64,
}

pub fn get_sidecar_path(target_path: &Path) -> Option<PathBuf> {
    let file_name = target_path.file_name()?.to_str()?;
    let sidecar_name = format!(".{}.foxing_meta", file_name);
    Some(target_path.with_file_name(sidecar_name))
}

fn lock_file(file: &File, exclusive: bool) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let op = if exclusive { libc::LOCK_EX } else { libc::LOCK_SH };
    let ret = unsafe { libc::flock(fd, op) };
    if ret == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

fn unlock_file(file: &File) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let ret = unsafe { libc::flock(fd, libc::LOCK_UN) };
    if ret == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

pub fn set_metadata(path: &Path, key: &str, value: &[u8]) {
    // Check for symlink first to avoid following it to a target we shouldn't touch
    // or failing on broken links. xattr on symlinks is rarely supported (user namespace).
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() {
            // Silently skip xattrs on symlinks to prevent noise/errors
            return;
        }
    } else {
        // Path doesn't exist
        return;
    }

    match xattr::set(path, key, value) {
        Ok(_) => {
            // If we successfully set xattr, ensure no legacy sidecar exists to avoid confusion
            if let Some(sp) = get_sidecar_path(path) {
                if sp.exists() {
                    let _ = fs::remove_file(sp);
                }
            }
            return;
        },
        Err(e) => {
            match e.kind() {
                io::ErrorKind::ReadOnlyFilesystem => return,
                io::ErrorKind::NotFound => return,
                _ => {
                    if let Some(code) = e.raw_os_error() {
                        if code != libc::EOPNOTSUPP && code != libc::ENOTSUP {
                            warn!("xattr::set failed for {:?} (Key: {}): {}. NOT falling back to sidecar.", path, key, e);
                            return;
                        }
                    } else {
                        warn!("xattr::set failed for {:?} with unknown error: {}. Aborting metadata save.", path, e);
                        return;
                    }
                }
            }
        }
    }
    
    // Legacy Sidecar Fallback (Only if xattr explicitly unsupported)
    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return,
    };

    if let Ok(mut file) = fs::OpenOptions::new().read(true).write(true).create(true).open(&sp) {
        if lock_file(&file, true).is_ok() {
            let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
            let val_str = hex::encode(value);
            map.insert(key.to_string(), val_str);
            
            if file.seek(SeekFrom::Start(0)).is_ok() && file.set_len(0).is_ok() {
                let _ = serde_json::to_writer(&file, &map);
            }
            let _ = unlock_file(&file);
        }
    }
}

pub fn get_metadata(path: &Path, key: &str) -> Option<Vec<u8>> {
    // Skip symlinks for xattr read too
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return None; }
    } else { return None; }

    if let Ok(Some(val)) = xattr::get(path, key) {
        return Some(val);
    }
    
    if let Some(sp) = get_sidecar_path(path) {
        if sp.exists() {
            if let Ok(file) = File::open(&sp) {
                if lock_file(&file, false).is_ok() {
                    let map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
                    let _ = unlock_file(&file);
                    
                    if let Some(val_str) = map.get(key) {
                        if let Ok(val) = hex::decode(val_str) {
                            // Migration: If we read from sidecar but have xattr support now, migrate it up.
                            if xattr::set(path, key, &val).is_ok() {
                                // Could delete sidecar entry here, but keeping it simple
                            }
                            return Some(val);
                        }
                    }
                }
            }
        }
    }
    None
}

// --- WAL PERSISTENCE LOGIC ---

pub fn persist_wal_state(path: &Path, seq: u64, state_str: &str) {
    let state_enum = match state_str {
        "WriteBulk" => WalState::WriteBulk,
        "FsyncCommit" => WalState::FsyncCommit,
        "PendingRename" => WalState::PendingRename,
        _ => WalState::Unknown,
    };

    let entry = PersistedWalEntry {
        seq,
        state: state_enum,
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
    };

    if let Ok(json) = serde_json::to_string(&entry) {
        set_metadata(path, "user.foxing.wal", json.as_bytes());
    }
}

pub fn clear_wal_state(path: &Path) {
    // We remove the attribute entirely to signal "Clean"
    // Skip symlinks check done inside set_metadata logic equivalent? 
    // xattr::remove follows symlinks. We should check if it's a symlink first to be safe/consistent.
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return; }
    }

    let _ = xattr::remove(path, "user.foxing.wal");
    // Also try to clear dirty bit for legacy compatibility
    let _ = xattr::remove(path, "user.foxing.dirty");
}

pub fn is_dirty(path: &Path) -> bool {
    // Skip symlinks
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return false; }
    } else { return false; }

    if xattr::get(path, "user.foxing.wal").ok().flatten().is_some() {
        return true;
    }
    // Legacy check
    if let Some(val) = get_metadata(path, "user.foxing.dirty") {
        return val == vec![1];
    }
    false
}

// Kept for backwards compatibility if needed, but `persist_wal_state` is preferred
pub fn set_dirty_flag(path: &Path, dirty: bool) {
    if dirty {
        // Default to unknown state if using legacy flag
        persist_wal_state(path, 0, "Unknown");
    } else {
        clear_wal_state(path);
    }
}
