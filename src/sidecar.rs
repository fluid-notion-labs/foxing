use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json;
use std::os::unix::io::AsRawFd;
use libc;
use std::io::{self, Seek, SeekFrom};
use xattr;
use tracing::{debug}; // Removed 'warn'
use std::hash::Hasher;
use std::collections::hash_map::DefaultHasher;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum WalState {
    None,
    IntentPending,
    InProgress,
    CommitPending,
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
    pub daemon_id: String,
    pub crc: u64,
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

pub fn set_metadata(path: &Path, key: &str, value: &[u8]) -> std::io::Result<()> {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return Ok(()); }
    } else { return Ok(()); }

    match xattr::set(path, key, value) {
        Ok(_) => {
            if let Some(sp) = get_sidecar_path(path) {
                if sp.exists() { let _ = fs::remove_file(sp); }
            }
            return Ok(());
        },
        Err(e) => {
            match e.kind() {
                io::ErrorKind::NotFound | io::ErrorKind::ReadOnlyFilesystem => return Err(e),
                _ => {
                    if let Some(code) = e.raw_os_error() {
                        if code != libc::EOPNOTSUPP && code != libc::ENOTSUP && code != libc::ENOSYS && code != libc::EPERM {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Cannot determine sidecar path")),
    };

    let mut file = fs::OpenOptions::new().read(true).write(true).create(true).open(&sp)?;
    lock_file(&file, true)?;
    
    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
    let val_str = hex::encode(value);
    map.insert(key.to_string(), val_str);
    
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    serde_json::to_writer(&file, &map)?;
    
    unlock_file(&file)?;
    Ok(())
}

pub fn get_metadata(path: &Path, key: &str) -> Option<Vec<u8>> {
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
                            return Some(val);
                        }
                    }
                }
            }
        }
    }
    None
}

pub fn remove_metadata(path: &Path, key: &str) -> std::io::Result<()> {
    let xattr_res = xattr::remove(path, key);
    
    if let Some(sp) = get_sidecar_path(path) {
        if sp.exists() {
            if let Ok(mut file) = fs::OpenOptions::new().read(true).write(true).open(&sp) {
                if lock_file(&file, true).is_ok() {
                    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
                    if map.remove(key).is_some() {
                        file.seek(SeekFrom::Start(0))?;
                        file.set_len(0)?;
                        if map.is_empty() {
                            let _ = fs::remove_file(&sp);
                        } else {
                            serde_json::to_writer(&file, &map)?;
                        }
                    }
                    let _ = unlock_file(&file);
                }
            }
        }
    }
    match xattr_res {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
             if let Some(code) = e.raw_os_error() {
                if code == libc::EOPNOTSUPP || code == libc::ENOTSUP || code == libc::ENOSYS || code == libc::EPERM {
                    return Ok(());
                }
            }
            Err(e)
        }
    }
}

pub fn atomic_wal_transition(path: &Path, from_state: WalState, to_state: WalState, daemon_id: &str, seq: u64) -> std::io::Result<bool> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };

    lock_file(&file, true)?;

    let current_state_entry = get_wal_state_internal(path);
    let current_state = current_state_entry.map(|e| e.state).unwrap_or(WalState::None);

    if current_state != from_state {
        debug!("WAL Transition Mismatch for {:?}: Expected {:?}, Found {:?}", path, from_state, current_state);
        unlock_file(&file)?;
        return Ok(false);
    }

    update_wal_internal(path, to_state, seq, daemon_id);
    
    unlock_file(&file)?;
    Ok(true)
}

fn update_wal_internal(path: &Path, state: WalState, seq: u64, daemon_id: &str) {
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(seq);
    hasher.write(daemon_id.as_bytes());
    let crc = hasher.finish();
    
    let entry = PersistedWalEntry {
        seq,
        state,
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
        daemon_id: daemon_id.to_string(),
        crc,
    };
    
    if let Ok(json) = serde_json::to_string(&entry) {
        let _ = set_metadata(path, "user.foxing.wal", json.as_bytes());
    }
}

fn get_wal_state_internal(path: &Path) -> Option<PersistedWalEntry> {
    if let Some(bytes) = get_metadata(path, "user.foxing.wal") {
        if let Ok(entry) = serde_json::from_slice::<PersistedWalEntry>(&bytes) {
            let mut hasher = DefaultHasher::new();
            hasher.write_u64(entry.seq);
            hasher.write(entry.daemon_id.as_bytes());
            if hasher.finish() == entry.crc {
                return Some(entry);
            }
        }
    }
    None
}

pub fn update_wal(path: &Path, state: WalState, seq: u64, daemon_id: &str) {
    update_wal_internal(path, state, seq, daemon_id);
}

pub fn get_wal_state(path: &Path) -> Option<PersistedWalEntry> {
    get_wal_state_internal(path)
}

pub fn clear_wal_state(path: &Path) {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return; }
    }
    let _ = remove_metadata(path, "user.foxing.wal");
    let _ = remove_metadata(path, "user.foxing.dirty");
}

pub fn is_dirty(path: &Path) -> bool {
    if get_metadata(path, "user.foxing.wal").is_some() {
        return true;
    }
    if let Some(val) = get_metadata(path, "user.foxing.dirty") {
        return val == vec![1];
    }
    false
}

pub fn set_dirty_flag(path: &Path, active: bool, _reason: &str) {
    if active {
        let _ = set_metadata(path, "user.foxing.dirty", &[1]);
    } else {
        let _ = remove_metadata(path, "user.foxing.dirty");
    }
}
