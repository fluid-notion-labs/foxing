use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::collections::HashMap;
use serde_json;
use std::os::unix::io::AsRawFd;
use libc;
use std::io::{self, Seek, SeekFrom};
use xattr;

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
    // 1. OPTIMIZED: Try Native XAttr First
    match xattr::set(path, key, value) {
        Ok(_) => {
            if let Some(sp) = get_sidecar_path(path) {
                if sp.exists() {
                    let _ = fs::remove_file(sp);
                }
            }
            return;
        },
        Err(e) => {
            if e.kind() == io::ErrorKind::ReadOnlyFilesystem {
                return;
            }
        }
    }

    // 3. FALLBACK: Sidecar File
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
    if let Some(sp) = get_sidecar_path(path) {
        if sp.exists() {
            if let Ok(file) = File::open(&sp) {
                if lock_file(&file, false).is_ok() {
                    let map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
                    let _ = unlock_file(&file);
                    
                    if let Some(val_str) = map.get(key) {
                        if let Ok(val) = hex::decode(val_str) {
                            if xattr::set(path, key, &val).is_ok() {
                                let _ = fs::remove_file(sp);
                            }
                            return Some(val);
                        }
                    }
                }
            }
        }
    }

    if let Ok(Some(val)) = xattr::get(path, key) {
        return Some(val);
    }

    None
}

pub fn set_dirty_flag(path: &Path, dirty: bool) {
    let val = if dirty { vec![1] } else { vec![0] };
    set_metadata(path, "user.foxing.dirty", &val);
}

pub fn is_dirty(path: &Path) -> bool {
    if let Some(val) = get_metadata(path, "user.foxing.dirty") {
        return val == vec![1];
    }
    false
}
