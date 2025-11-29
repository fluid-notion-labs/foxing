// File: foxing/src/sidecar.rs | Index: 11 of 24 | Function: Hybrid Metadata Manager (Native Xattrs with Sidecar Fallback).
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::io::{Seek, SeekFrom};
use libc;
use serde_json::Value;
use std::fs;
use crate::metrics;
use crate::error::{Result}; 
use uuid::Uuid;
use tracing::{error, debug};

/// Generates the sidecar path for a given file path.
pub fn get_sidecar_path(path: &Path) -> Option<PathBuf> {
    path.file_name().map(|n| {
        let mut s = String::from(".");
        s.push_str(&n.to_string_lossy());
        s.push_str(".foxing_meta");
        path.with_file_name(s)
    })
}

// Internal helper to read legacy/fallback sidecar files
fn read_sidecar_file(path: &Path) -> Result<HashMap<String, Vec<u8>>> {
    if !path.exists() { return Ok(HashMap::new()); }
    let file = fs::File::open(path)?; 
    let map_value: HashMap<String, Value> = serde_json::from_reader(file)?;
    let mut result_map = HashMap::new();
    for (key, val) in map_value {
        if let Value::String(s) = val {
            result_map.insert(key, s.into_bytes());
        } 
    }
    Ok(result_map)
}

/// Sets a metadata key-value pair.
/// STRATEGY: Try Native Xattr first. If that fails, use Sidecar file.
pub fn set_metadata(path: &Path, key: &str, val: &[u8]) {
    // 1. Try Native Xattr
    if xattr::set(path, key, val).is_ok() {
        // Success! Now checks if a legacy sidecar exists and nuke it to clean up.
        if let Some(sp) = get_sidecar_path(path) {
            if sp.exists() {
                let _ = fs::remove_file(sp);
                debug!("Cleaned up obsolete sidecar for {:?}", path);
            }
        }
        return;
    }

    // 2. Fallback: Sidecar File
    // This usually happens on NFS, FAT32, or tmpfs without xattr support.
    update_sidecar_file(path, key, val);
}

/// Gets a metadata value.
/// STRATEGY: Check Native Xattr first. Then check Sidecar file.
pub fn get_metadata(path: &Path, key: &str) -> Option<Vec<u8>> {
    // 1. Try Native Xattr
    if let Ok(Some(val)) = xattr::get(path, key) {
        return Some(val);
    }

    // 2. Fallback: Sidecar File
    if let Some(sp) = get_sidecar_path(path) {
        if let Ok(map) = read_sidecar_file(&sp) {
            return map.get(key).cloned();
        }
    }
    None
}

pub fn set_dirty_flag(path: &Path, dirty: bool) {
    set_metadata(path, "user.foxing.dirty", if dirty { b"true" } else { b"false" });
}

pub fn is_dirty(path: &Path) -> bool {
    if let Some(val) = get_metadata(path, "user.foxing.dirty") {
        return val == b"true";
    }
    false
}

// Legacy sidecar updater (only used as fallback now)
fn update_sidecar_file(path: &Path, key: &str, val: &[u8]) {
    if let Some(sp) = get_sidecar_path(path) {
        let file_exists = sp.exists();
        match fs::OpenOptions::new().read(true).write(true).create(true).open(&sp) {
            Ok(mut file) => {
                let fd = file.as_raw_fd();
                let flock = libc::flock { l_type: libc::F_WRLCK as i16, l_whence: libc::SEEK_SET as i16, l_start: 0, l_len: 0, l_pid: 0 };
                if unsafe { libc::fcntl(fd, libc::F_OFD_SETLKW, &flock) } < 0 {
                    error!("Failed to acquire sidecar lock for {:?}: {}", sp, std::io::Error::last_os_error());
                    return;
                }
                if !file_exists { metrics::SIDECAR_FILES_CREATED.inc(); }
                if file.seek(SeekFrom::Start(0)).is_err() { return; }

                let mut map: HashMap<String, Value> = match serde_json::from_reader(&file) {
                    Ok(m) => m,
                    Err(_) => HashMap::new(),
                };
                
                let val_string = String::from_utf8_lossy(val).to_string();
                map.insert(key.to_string(), Value::String(val_string));
                
                let temp_name = format!(".{}.tmp", Uuid::new_v4());
                let temp_path = sp.with_file_name(temp_name);

                match fs::OpenOptions::new().write(true).create(true).truncate(true).open(&temp_path) {
                    Ok(mut temp_file) => {
                        if serde_json::to_writer(&mut temp_file, &map).is_err() {
                            let _ = fs::remove_file(&temp_path); return;
                        }
                        if temp_file.sync_all().is_err() {
                             let _ = fs::remove_file(&temp_path); return;
                        }
                        let _ = fs::rename(&temp_path, &sp);
                    },
                    Err(_) => {}
                }
            },
            Err(_) => {}
        }
    }
}
