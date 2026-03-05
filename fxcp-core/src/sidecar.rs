// Sidecar — metadata/dirty-flag management via xattr (primary) with JSON sidecar fallback
//
// Architecture: xattrs are the canonical store. Sidecar JSON files are ONLY
// used as a fallback for filesystems that lack xattr support (exfat, vfat, etc.).
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
/// Set once xattr is confirmed unsupported — all subsequent writes go to sidecar
static XATTR_UNSUPPORTED: AtomicBool = AtomicBool::new(false);

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
        bincode::deserialize(data).ok()
    }

    pub fn matches(&self, other: &Self) -> bool {
        if self.size != other.size { return false; }

        match (&self.merkle_root, &other.merkle_root) {
            (Some(a), Some(b)) => return a == b,
            _ => {}
        }

        if self.mtime_sec != other.mtime_sec || self.mtime_nsec != other.mtime_nsec {
            return false;
        }

        match (&self.hash, &other.hash) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }
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

/// Returns true if the error indicates xattrs are not supported on this filesystem.
fn is_xattr_unsupported(e: &io::Error) -> bool {
    if let Some(code) = e.raw_os_error() {
        code == libc::EOPNOTSUPP || code == libc::ENOTSUP || code == libc::ENOSYS
    } else {
        false
    }
}

// --- Sidecar fallback (only for filesystems without xattr support) ---

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

fn sidecar_set(path: &Path, key: &str, value: &[u8]) -> std::io::Result<()> {
    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Cannot determine sidecar path")),
    };
    let (_trusted_key, user_key) = resolve_key_variants(key);
    let mut file = fs::OpenOptions::new().read(true).write(true).create(true).open(&sp)?;
    lock_file(&file, true)?;
    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
    map.insert(user_key, hex::encode(value));
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    serde_json::to_writer(&file, &map)?;
    unlock_file(&file)?;
    crate::metrics::SIDECAR_FILES_CREATED.inc();
    Ok(())
}

fn sidecar_set_batch(path: &Path, entries: &[(&str, &[u8])]) -> std::io::Result<()> {
    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Cannot determine sidecar path")),
    };
    let mut file = fs::OpenOptions::new().read(true).write(true).create(true).open(&sp)?;
    lock_file(&file, true)?;
    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
    for &(key, value) in entries {
        let (_trusted_key, user_key) = resolve_key_variants(key);
        map.insert(user_key, hex::encode(value));
    }
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    serde_json::to_writer(&file, &map)?;
    unlock_file(&file)?;
    crate::metrics::SIDECAR_FILES_CREATED.inc();
    Ok(())
}

fn sidecar_get(path: &Path, key: &str) -> Option<Vec<u8>> {
    let sp = get_sidecar_path(path)?;
    let file = File::open(&sp).ok()?;
    if lock_file(&file, false).is_err() { return None; }
    let map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
    let _ = unlock_file(&file);
    let (trusted_key, user_key) = resolve_key_variants(key);
    let val_str = map.get(&trusted_key).or_else(|| map.get(&user_key))?;
    hex::decode(val_str).ok()
}

fn sidecar_remove(path: &Path, key: &str) -> std::io::Result<()> {
    let sp = match get_sidecar_path(path) {
        Some(p) => p,
        None => return Ok(()),
    };
    if !sp.exists() { return Ok(()); }
    let mut file = fs::OpenOptions::new().read(true).write(true).open(&sp)?;
    if lock_file(&file, true).is_err() { return Ok(()); }
    let mut map: HashMap<String, String> = serde_json::from_reader(&file).unwrap_or_default();
    let (trusted_key, user_key) = resolve_key_variants(key);
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
    Ok(())
}

// --- Public API ---

/// Try xattr set (trusted then user namespace). Returns Ok if xattr worked,
/// Err if xattr unsupported and caller should use sidecar fallback.
fn try_xattr_set(path: &Path, key: &str, value: &[u8]) -> std::result::Result<(), io::Error> {
    let (trusted_key, user_key) = resolve_key_variants(key);
    match xattr::set(path, &trusted_key, value) {
        Ok(_) => Ok(()),
        Err(e) => {
            if is_xattr_unsupported(&e) {
                // trusted namespace not available (unprivileged) — try user namespace
                match xattr::set(path, &user_key, value) {
                    Ok(_) => {
                        if !FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                            warn!("Security: 'trusted' xattr namespace unavailable. Using 'user' namespace.");
                        }
                        Ok(())
                    }
                    Err(e2) if is_xattr_unsupported(&e2) => Err(e2),
                    Err(e2) => {
                        // EPERM on user namespace likely means filesystem doesn't support xattrs at all
                        if e2.raw_os_error() == Some(libc::EPERM) {
                            Err(e2)
                        } else {
                            // Transient error — xattr is supported, just failed this time
                            warn!("xattr set failed for {:?}: {}", path, e2);
                            Ok(())
                        }
                    }
                }
            } else {
                // EPERM on trusted — expected for non-root, try user namespace
                if e.raw_os_error() == Some(libc::EPERM) {
                    match xattr::set(path, &user_key, value) {
                        Ok(_) => {
                            if !FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                                warn!("Security: 'trusted' xattr namespace unavailable. Using 'user' namespace.");
                            }
                            Ok(())
                        }
                        Err(e2) if is_xattr_unsupported(&e2) => Err(e2),
                        Err(e2) => {
                            warn!("xattr set failed for {:?}: {}", path, e2);
                            Ok(())
                        }
                    }
                } else {
                    warn!("xattr set failed for {:?}: {}", path, e);
                    Ok(())
                }
            }
        }
    }
}

pub fn set_metadata(path: &Path, key: &str, value: &[u8]) -> std::io::Result<()> {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return Ok(()); }
    } else { return Ok(()); }

    // Fast path: if we already know xattr is unsupported, go straight to sidecar
    if XATTR_UNSUPPORTED.load(Ordering::Relaxed) {
        return sidecar_set(path, key, value);
    }

    match try_xattr_set(path, key, value) {
        Ok(()) => Ok(()),
        Err(_) => {
            // xattr not supported on this filesystem — mark and use sidecar
            XATTR_UNSUPPORTED.store(true, Ordering::Relaxed);
            warn!("xattr unsupported on filesystem for {:?}. Falling back to sidecar JSON.", path);
            sidecar_set(path, key, value)
        }
    }
}

/// Write multiple metadata keys. Uses xattr per-key (kernel can't batch),
/// but avoids sidecar entirely unless xattr is unsupported.
pub fn set_metadata_batch(path: &Path, entries: &[(&str, &[u8])]) -> std::io::Result<()> {
    if entries.is_empty() { return Ok(()); }
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return Ok(()); }
    } else { return Ok(()); }

    if XATTR_UNSUPPORTED.load(Ordering::Relaxed) {
        return sidecar_set_batch(path, entries);
    }

    for &(key, value) in entries {
        if let Err(_) = try_xattr_set(path, key, value) {
            XATTR_UNSUPPORTED.store(true, Ordering::Relaxed);
            warn!("xattr unsupported on filesystem for {:?}. Falling back to sidecar JSON.", path);
            return sidecar_set_batch(path, entries);
        }
    }
    Ok(())
}

pub fn get_metadata(path: &Path, key: &str) -> Option<Vec<u8>> {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.is_symlink() { return None; }
    } else { return None; }

    // Fast path: if xattr is known-unsupported, go straight to sidecar
    if XATTR_UNSUPPORTED.load(Ordering::Relaxed) {
        return sidecar_get(path, key);
    }

    let (trusted_key, user_key) = resolve_key_variants(key);

    // Try trusted xattr first
    match xattr::get(path, &trusted_key) {
        Ok(Some(val)) => return Some(val),
        Ok(None) => {},
        Err(_) => {}
    }
    // Try user xattr
    match xattr::get(path, &user_key) {
        Ok(Some(val)) => return Some(val),
        Ok(None) => {},
        Err(_) => {}
    }

    // Fallback: check sidecar (for migration from old dual-write or xattr-less fs)
    sidecar_get(path, key)
}

pub fn remove_metadata(path: &Path, key: &str) -> std::io::Result<()> {
    let (trusted_key, user_key) = resolve_key_variants(key);
    let trusted_res = xattr::remove(path, &trusted_key);
    let user_res = xattr::remove(path, &user_key);

    // Also clean up any legacy sidecar entry
    let _ = sidecar_remove(path, key);

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
    let raw = get_metadata(path, "sig")?;
    let result = SyncSignature::deserialize(&raw);
    if result.is_none() {
        tracing::warn!("get_sync_signature {:?}: {} bytes but deserialize FAILED", path, raw.len());
    }
    result
}

/// Store a directory-level Merkle hash via xattr on the target directory.
pub fn set_dir_hash(path: &Path, hash: &[u8; 32]) -> std::io::Result<()> {
    set_metadata(path, "dir_hash", hash)
}

/// Retrieve a stored directory hash from xattr.
pub fn get_dir_hash(path: &Path) -> Option<[u8; 32]> {
    let bytes = get_metadata(path, "dir_hash")?;
    if bytes.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(arr)
    } else {
        None
    }
}

/// Clear a stored directory hash (invalidation).
pub fn clear_dir_hash(path: &Path) -> std::io::Result<()> {
    remove_metadata(path, "dir_hash")
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
    let sig: hashing::MerkleSignature = bincode::deserialize(&bytes).ok()?;
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

    pub fn clear_dirty_on_skip(&self, target_path: PathBuf) {
        let _ = self.tx.send(SidecarOp::ClearDirty { path: target_path });
    }
}
