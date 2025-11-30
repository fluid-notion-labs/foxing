use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::collections::HashMap;
use serde_json;
use std::os::unix::io::AsRawFd;
use libc;

pub fn get_sidecar_path(target_path: &Path) -> Option<PathBuf> {
    let file_name = target_path.file_name()?.to_str()?;
    // Use a hidden file convention: .filename.foxing_meta
    let sidecar_name = format!(".{}.foxing_meta", file_name);
    Some(target_path.with_file_name(sidecar_name))
}

fn lock_file(file: &File) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let ret = unsafe { libc::flock(fd, libc::LOCK_EX) };
    if ret == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

fn unlock_file(file: &File) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let ret = unsafe { libc::flock(fd, libc::LOCK_UN) };
    if ret == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

pub fn set_metadata(path: &Path, key: &str, value: &[u8]) {
    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return,
    };

    // 1. Open (Create if needed)
    let file = match fs::OpenOptions::new().read(true).write(true).create(true).open(&sp) {
        Ok(f) => f,
        Err(_) => return,
    };

    // 2. Lock
    if lock_file(&file).is_err() { return; }

    // 3. Read existing
    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
    
    // 4. Modify
    // Store values as hex strings for JSON safety
    let val_str = hex::encode(value);
    map.insert(key.to_string(), val_str);

    // 5. Write (Truncate and rewrite)
    // We rewind and truncate to avoid temp file rename races inside the same dir if possible, 
    // but atomic rename is better. However, rename changes the inode/lock.
    // Better to write to temp and rename? But that breaks the lock held on 'file'.
    // Safe approach with lock: Truncate and Write in place.
    if file.set_len(0).is_ok() {
        use std::io::Seek;
        let _ = (&file).seek(std::io::SeekFrom::Start(0));
        let _ = serde_json::to_writer(&file, &map);
    }

    // 6. Unlock
    let _ = unlock_file(&file);
}

pub fn get_metadata(path: &Path, key: &str) -> Option<Vec<u8>> {
    let sp = get_sidecar_path(path)?;
    if !sp.exists() { return None; }

    let file = File::open(&sp).ok()?;
    // Shared lock for reading
    let fd = file.as_raw_fd();
    unsafe { libc::flock(fd, libc::LOCK_SH) };
    
    let map: HashMap<String, String> = serde_json::from_reader(&file).ok()?;
    
    unsafe { libc::flock(fd, libc::LOCK_UN) };

    map.get(key).and_then(|v| hex::decode(v).ok())
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
