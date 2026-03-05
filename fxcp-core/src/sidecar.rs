// Sidecar — metadata/dirty-flag management via xattr
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::collections::HashMap;
use serde_json;
use std::os::unix::io::AsRawFd;
use libc;
use std::io::{self, Seek, SeekFrom};
use xattr;
use tracing::{error, debug, warn};
use tokio::sync::{mpsc, oneshot};
use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::hashing;
use std::os::unix::fs::MetadataExt;

const NS_USER_PREFIX: &str = "user.foxing.";
const NS_TRUSTED_PREFIX: &str = "trusted.foxing.";
static FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyncSignature {
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub hash: Option<String>,         // Hex-encoded BLAKE3 Lite Hash
    pub merkle_root: Option<String>,  // Hex-encoded BLAKE3 Full Merkle Root
    pub chunk_size: Option<u64>,      // Fixed chunk boundary
    pub leaf_count: Option<u32>,      // Number of Merkle leaves (v4+)
    pub version: u8,                  // Schema version
}

impl SyncSignature {
    pub const CURRENT_VERSION: u8 = 4;
    
    pub fn compute(path: &Path) -> crate::error::Result<Self> {
        let meta = std::fs::metadata(path)?;
        let size = meta.len();
        
        let (hash, merkle_root) = if hashing::is_hashing_enabled() {
            // Updated to check threshold against dynamic config instead of hardcoded constant
            if size >= hashing::get_lite_threshold_bytes() {
                let lite = hashing::hash_file_lite(path, size)?
                    .map(|h| h.to_hex().to_string());
                let merkle = hashing::hash_file_full(path)?
                    .map(|h| h.to_hex().to_string());
                (lite, merkle)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };
        
        Ok(Self {
            size,
            mtime_sec: meta.mtime(),
            mtime_nsec: meta.mtime_nsec(),
            hash,
            merkle_root,
            chunk_size: Some(hashing::CHUNK_SIZE as u64),
            leaf_count: None,
            version: Self::CURRENT_VERSION,
        })
    }
    
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }
    
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        use bincode::Options;
        bincode::DefaultOptions::new()
            .with_limit(16 * 1024 * 1024) // 16MB max payload
            .deserialize(data).ok()
    }
    
    pub fn matches(&self, other: &Self) -> bool {
        if self.size != other.size { return false; }

        // Compare full file hash (merkle_root) FIRST — catches ALL changes
        // including middle-of-file modifications invisible to lite hash
        match (&self.merkle_root, &other.merkle_root) {
            (Some(a), Some(b)) => return a == b, // Definitive answer
            _ => {} // One or both lack full hash — fall through to mtime+lite
        }

        if self.mtime_sec != other.mtime_sec || self.mtime_nsec != other.mtime_nsec {
            return false;
        }

        match (&self.hash, &other.hash) {
            (Some(a), Some(b)) => a == b,
            _ => true, // If either side lacks hash, trust mtime
        }
    }
}

pub fn get_sidecar_path(target_path: &Path) -> Option<PathBuf> {
    let file_name = target_path.file_name()?.to_str()?;
    let sidecar_name = format!(".{}.foxing_meta", file_name);
    Some(target_path.with_file_name(sidecar_name))
}

/// Return the sidecar path for directory-level metadata.
///
/// For directories, metadata is stored INSIDE the directory as `.foxing_dir_meta`
/// rather than next to it (which is what `get_sidecar_path` would do via
/// `with_file_name()`). For non-directories, falls back to the regular sidecar path.
pub fn get_dir_sidecar_path(dir_path: &Path) -> Option<PathBuf> {
    if dir_path.is_dir() {
        Some(dir_path.join(".foxing_dir_meta"))
    } else {
        get_sidecar_path(dir_path)
    }
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

fn resolve_key_variants(key: &str) -> (String, String) {
    if key.starts_with("user.foxing.") {
        let suffix = key.strip_prefix("user.foxing.").unwrap();
        (format!("{}{}", NS_TRUSTED_PREFIX, suffix), key.to_string())
    } else if key.starts_with("trusted.foxing.") {
        let suffix = key.strip_prefix("trusted.foxing.").unwrap();
        (key.to_string(), format!("{}{}", NS_USER_PREFIX, suffix))
    } else {
        (format!("{}{}", NS_TRUSTED_PREFIX, key), format!("{}{}", NS_USER_PREFIX, key))
    }
}

pub fn set_metadata(path: &Path, key: &str, value: &[u8]) -> std::io::Result<()> {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return Ok(()); }
    } else { return Ok(()); }
    let (trusted_key, user_key) = resolve_key_variants(key);
    // Try trusted xattr first, then user xattr
    let xattr_ok = match xattr::set(path, &trusted_key, value) {
        Ok(_) => true,
        Err(e) => {
            if !FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                warn!("Security: 'trusted' xattr namespace unavailable ({:?}). Falling back to 'user' namespace. Anti-tamper protection disabled.", e);
            }
            xattr::set(path, &user_key, value).is_ok()
        }
    };

    // Always write sidecar file as well — xattr crate has reliability
    // issues on NFSv4.2 where writes succeed but reads return None
    // on subsequent process invocations. Sidecar is the reliable path.
    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Cannot determine sidecar path")),
    };
    let mut file = fs::OpenOptions::new().read(true).write(true).create(true).open(&sp)?;
    lock_file(&file, true)?;
    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
    let val_str = hex::encode(value);
    map.insert(user_key, val_str);
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    serde_json::to_writer(&file, &map)?;
    unlock_file(&file)?;
    crate::metrics::SIDECAR_FILES_CREATED.inc();
    Ok(())
}

pub fn get_metadata(path: &Path, key: &str) -> Option<Vec<u8>> {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return None; }
    } else { return None; }
    let (trusted_key, user_key) = resolve_key_variants(key);

    // Try sidecar file FIRST — reliable across NFS, containers, all filesystems.
    // xattr crate has reliability issues on NFSv4.2 (cross-process visibility).
    if let Some(sp) = get_sidecar_path(path) {
        if sp.exists() {
            if let Ok(file) = File::open(&sp) {
                if lock_file(&file, false).is_ok() {
                    let map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
                    let _ = unlock_file(&file);
                    if let Some(val_str) = map.get(&trusted_key).or_else(|| map.get(&user_key)) {
                        if let Ok(val) = hex::decode(val_str) {
                            return Some(val);
                        }
                    } else {
                        tracing::warn!("get_metadata sidecar {:?}: key {}|{} not found in {:?}", path, trusted_key, user_key, map.keys().collect::<Vec<_>>());
                    }
                } else {
                    tracing::warn!("get_metadata sidecar {:?}: lock failed", path);
                }
            } else {
                tracing::warn!("get_metadata sidecar {:?}: open failed", path);
            }
        }
    }

    // Fallback to xattr (works on local filesystems with xattr support)
    match xattr::get(path, &trusted_key) {
        Ok(Some(val)) => return Some(val),
        Ok(None) => {},
        Err(_) => {}
    }
    match xattr::get(path, &user_key) {
        Ok(Some(val)) => return Some(val),
        Ok(None) => {},
        Err(_) => {}
    }
    if let Some(sp) = get_sidecar_path(path) {
        if sp.exists() {
            if let Ok(file) = File::open(&sp) {
                if lock_file(&file, false).is_ok() {
                    let map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
                    let _ = unlock_file(&file);
                    if let Some(val_str) = map.get(&trusted_key).or_else(|| map.get(&user_key)) {
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
    let (trusted_key, user_key) = resolve_key_variants(key);
    let trusted_res = xattr::remove(path, &trusted_key);
    let user_res = xattr::remove(path, &user_key);
    if let Some(sp) = get_sidecar_path(path) {
        if sp.exists() {
            if let Ok(mut file) = fs::OpenOptions::new().read(true).write(true).open(&sp) {
                if lock_file(&file, true).is_ok() {
                    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
                    let rem1 = map.remove(&trusted_key).is_some();
                    let rem2 = map.remove(&user_key).is_some();
                    if rem1 || rem2 {
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
    if trusted_res.is_err() && user_res.is_err() {
        let err = trusted_res.unwrap_err();
        if err.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        if let Some(code) = err.raw_os_error() {
            if code == libc::EOPNOTSUPP || code == libc::ENOTSUP || code == libc::ENOSYS || code == libc::EPERM || code == 61 || code == 93 {
                return Ok(());
            }
        }
        error!("Failed to remove xattr {}: {}", key, err);
        return Err(err);
    }
    Ok(())
}

pub fn is_dirty(path: &Path) -> bool {
    if let Some(val) = get_metadata(path, "dirty") {
        return val == vec![1];
    }
    false
}

pub fn set_dirty_flag(path: &Path, active: bool, _reason: &str) -> std::io::Result<()> {
    if active {
        set_metadata(path, "dirty", &[1])
    } else {
        remove_metadata(path, "dirty")
    }
}

pub fn set_sync_signature(path: &Path, sig: &SyncSignature) -> std::io::Result<()> {
    set_metadata(path, "sig", &sig.serialize())
}

pub fn get_sync_signature(path: &Path) -> Option<SyncSignature> {
    get_metadata(path, "sig")
        .and_then(|bytes| SyncSignature::deserialize(&bytes))
}

/// Store a directory-level Merkle hash on the target directory.
///
/// For directories, writes the 32-byte hash directly into a sidecar file
/// INSIDE the directory (`.foxing_dir_meta`), avoiding the `with_file_name()`
/// bug that would place metadata next to the directory instead of inside it.
pub fn set_dir_hash(path: &Path, hash: &[u8; 32]) -> std::io::Result<()> {
    let sidecar = match get_dir_sidecar_path(path) {
        Some(p) => p,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Cannot determine dir sidecar path")),
    };
    match fs::write(&sidecar, hash) {
        Ok(()) => Ok(()),
        Err(e) => {
            error!("Failed to write dir hash sidecar {:?}: {}", sidecar, e);
            Ok(())
        }
    }
}

/// Retrieve a stored directory hash from the target directory.
///
/// Reads the 32-byte hash from the sidecar file inside the directory.
pub fn get_dir_hash(path: &Path) -> Option<[u8; 32]> {
    let sidecar = get_dir_sidecar_path(path)?;
    let bytes = match fs::read(&sidecar) {
        Ok(b) => b,
        Err(_) => return None,
    };
    if bytes.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(arr)
    } else {
        None
    }
}

/// Clear a stored directory hash (invalidation).
///
/// Removes the sidecar file inside the directory.
pub fn clear_dir_hash(path: &Path) -> std::io::Result<()> {
    let sidecar = match get_dir_sidecar_path(path) {
        Some(p) => p,
        None => return Ok(()),
    };
    match fs::remove_file(&sidecar) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn set_merkle_signature(path: &Path, sig: &hashing::MerkleSignature) -> std::io::Result<()> {
    let data = bincode::serialize(sig)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if data.len() > 64 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "merkle signature too large for xattr storage",
        ));
    }
    set_metadata(path, "merkle", &data)
}

pub fn get_merkle_signature(path: &Path) -> Option<hashing::MerkleSignature> {
    let bytes = get_metadata(path, "merkle")?;
    use bincode::Options;
    let sig: hashing::MerkleSignature = bincode::DefaultOptions::new()
        .with_limit(64 * 1024) // 64KB max (xattr limit)
        .deserialize(&bytes).ok()?;
    // Bounds check
    if sig.chunk_size == 0 { return None; }
    let expected_max = sig.file_size / sig.chunk_size + 2;
    if sig.leaf_hashes.len() as u64 > expected_max { return None; }
    Some(sig)
}

enum SidecarOp {
    SetDirty {
        path: PathBuf,
        response: Option<oneshot::Sender<io::Result<()>>>,
    },
    ClearDirty {
        path: PathBuf,
    },
}

#[derive(Clone)]
pub struct AsyncSidecar {
    tx: mpsc::UnboundedSender<SidecarOp>,
}

impl AsyncSidecar {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<SidecarOp>();
        thread::Builder::new()
            .name("foxing-sidecar-io".into())
            .spawn(move || {
                debug!("AsyncSidecar: Background thread started.");
                while let Some(op) = rx.blocking_recv() {
                    match op {
                        SidecarOp::SetDirty { path, response } => {
                            let res = set_dirty_flag(&path, true, "ASYNC_WRITE");
                            if let Some(resp_tx) = response {
                                let _ = resp_tx.send(res);
                            }
                        },
                        SidecarOp::ClearDirty { path } => {
                            if let Err(e) = set_dirty_flag(&path, false, "ASYNC_CLEAR") {
                                debug!("AsyncSidecar: Failed to clear dirty flag for {:?}: {}", path, e);
                            }
                        }
                    }
                }
                debug!("AsyncSidecar: Background thread stopping.");
            })
            .expect("Failed to spawn sidecar thread");
        Self { tx }
    }
    pub async fn set_dirty(&self, path: PathBuf) -> io::Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(SidecarOp::SetDirty { path, response: Some(resp_tx) }).is_err() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Sidecar worker dead"));
        }
        match resp_rx.await {
            Ok(res) => res,
            Err(_) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "Sidecar worker dropped response")),
        }
    }
    pub fn set_dirty_blind(&self, path: PathBuf) {
        let _ = self.tx.send(SidecarOp::SetDirty { path, response: None });
    }
    pub fn clear_dirty(&self, path: PathBuf) {
        let _ = self.tx.send(SidecarOp::ClearDirty { path });
    }

    /// Called when source file no longer exists — clear dirty flag
    /// since there's nothing to sync.
    pub fn clear_dirty_on_skip(&self, target_path: PathBuf) {
        let _ = self.tx.send(SidecarOp::ClearDirty { path: target_path });
    }
}
