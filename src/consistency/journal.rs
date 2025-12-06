use std::path::{Path, PathBuf};
use std::io;
use xattr;
use tracing::{debug, warn};

const INTENT_XATTR_KEY: &str = "user.foxing.intent";

pub enum Intent {
    Rename { dest: PathBuf },
}

impl Intent {
    fn serialize(&self) -> String {
        match self {
            Intent::Rename { dest } => format!("RENAME:{}", dest.to_string_lossy()),
        }
    }

    pub fn parse(val: &[u8]) -> Option<Self> {
        let s = String::from_utf8_lossy(val);
        if let Some(path_str) = s.strip_prefix("RENAME:") {
            return Some(Intent::Rename { dest: PathBuf::from(path_str) });
        }
        None
    }
}

pub struct Journal;

impl Journal {
    pub fn begin(path: &Path, intent: Intent) -> io::Result<()> {
        let val = intent.serialize();
        xattr::set(path, INTENT_XATTR_KEY, val.as_bytes())
    }

    pub fn end(path: &Path) -> io::Result<()> {
        match xattr::remove(path, INTENT_XATTR_KEY) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn recover(path: &Path) -> io::Result<bool> {
        if let Ok(Some(val)) = xattr::get(path, INTENT_XATTR_KEY) {
            if let Some(intent) = Intent::parse(&val) {
                match intent {
                    Intent::Rename { dest } => {
                        if path == dest {
                            let _ = Self::end(path);
                            return Ok(true);
                        }
                        
                        if let Some(parent) = dest.parent() {
                            if let Err(e) = std::fs::create_dir_all(parent) {
                                warn!("Journal Recovery: Failed to create parent {:?}: {}", parent, e);
                            }
                        }

                        match std::fs::rename(path, &dest) {
                            Ok(_) => {
                                let _ = Self::end(&dest);
                                // Ensure durability on recovery
                                if let Some(parent) = dest.parent() {
                                    if let Ok(f) = std::fs::File::open(parent) {
                                        let _ = f.sync_all();
                                    }
                                }
                                return Ok(true);
                            },
                            Err(_) => return Ok(false),
                        }
                    }
                }
            }
        }
        Ok(false)
    }
}

pub fn atomic_rename(src: &Path, dst: &Path) -> io::Result<()> {
    Journal::begin(src, Intent::Rename { dest: dst.to_path_buf() })?;

    // Ensure parent directory exists before rename
    if let Some(parent) = dst.parent() {
        if !parent.exists() {
            debug!("Atomic Rename: Auto-creating parent directory {:?}", parent);
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Atomic Rename: Failed to create parent directory {:?}: {}", parent, e);
                // We proceed to rename() so that if it fails, we get the correct FS error (e.g. ENOENT)
            }
        }
    }

    match std::fs::rename(src, dst) {
        Ok(_) => {
            Journal::end(dst)?;
            
            // CRITICAL FIX: fsync the parent directory to ensure the rename is durable.
            // This prevents "missing file" race conditions in tests and on crash.
            if let Some(parent) = dst.parent() {
                match std::fs::File::open(parent) {
                    Ok(f) => { let _ = f.sync_all(); },
                    Err(e) => debug!("Atomic Rename: Failed to open parent for sync: {}", e),
                }
            }
            Ok(())
        },
        Err(e) => {
            let _ = Journal::end(src);
            Err(e)
        }
    }
}
