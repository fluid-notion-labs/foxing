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
            // If successful, we MUST clean up any potential sidecar to prevent
            // the "Split-Brain" scenario where a future read sees the sidecar
            // and ignores this new xattr.
            if let Some(sp) = get_sidecar_path(path) {
                if sp.exists() {
                    let _ = fs::remove_file(sp);
                }
            }
            return;
        },
        Err(e) => {
            // 2. EDGE CASE: Read-Only Filesystem (Rescue Mode)
            // If the FS is RO, writing a sidecar will also fail. 
            // Return early to avoid log spam/double errors.
            if e.kind() == io::ErrorKind::ReadOnly {
                return;
            }
            // Continue to fallback for other errors (ENOTSUP, EPERM, etc.)
        }
    }

    // 3. FALLBACK: Sidecar File
    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return,
    };

    // Attempt creation/update of sidecar
    // We accept that this might fail if we really are RO or ENOSPC, 
    // but we tried our best.
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
    // 1. PRIORITY CHECK: Sidecar
    // In a "Recovered" state (Rescue Mode -> Normal Mode), a sidecar indicates
    // that xattrs failed recently. We MUST trust the sidecar over the xattr
    // because the xattr might be stale (from before the Rescue Mode session).
    if let Some(sp) = get_sidecar_path(path) {
        if sp.exists() {
            if let Ok(file) = File::open(&sp) {
                if lock_file(&file, false).is_ok() {
                    let map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
                    let _ = unlock_file(&file);
                    
                    if let Some(val_str) = map.get(key) {
                        if let Ok(val) = hex::decode(val_str) {
                            // AUTO-HEALING:
                            // If we found valid data in a sidecar, but we are currently
                            // capable of writing xattrs, migrate the data and kill the sidecar.
                            // This "heals" the split-brain state.
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

    // 2. SECONDARY: Native XAttr
    // If no sidecar exists, the xattr is the source of truth.
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
