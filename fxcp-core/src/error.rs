use thiserror::Error;

/// Classification of copy operation errors for retry/fallback decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyErrorKind {
    /// Source file no longer exists (transient lifecycle — skip event)
    SourceNotFound,
    /// Target file/parent doesn't exist (needs creation/repair)
    TargetNotFound,
    /// Operation exceeded deadline
    Timeout,
    /// Temporary issue (EAGAIN, interrupted, NFS hiccup) — retry with backoff
    Transient,
    /// Permanent failure (permission denied, read-only fs) — don't retry
    Permanent,
}

#[derive(Error, Debug)]
pub enum FxcpError {
    #[error("Config: {0}")]
    Config(String),
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("System: {0}")]
    System(#[from] nix::Error),
    #[error("Security: {0}")]
    Security(String),
    #[error("Encoding: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Join: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Versioning: {0}")]
    Versioning(String),
    #[error("CString Nul: {0}")]
    Nul(#[from] std::ffi::NulError),
    #[error("IOUring Push: {0}")]
    IouPush(String),
    #[error("Tracing Error ({correlation_id}): {message}")]
    Traced {
        correlation_id: u64,
        message: String,
    },
    #[error("Memory Limit Exhausted: {0}")]
    MemoryExhausted(String),
}

impl FxcpError {
    pub fn copy_error_kind(&self) -> CopyErrorKind {
        match self {
            FxcpError::Io(io_err) => match io_err.kind() {
                std::io::ErrorKind::NotFound => CopyErrorKind::TargetNotFound,
                std::io::ErrorKind::PermissionDenied => CopyErrorKind::Permanent,
                std::io::ErrorKind::TimedOut => CopyErrorKind::Timeout,
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted => CopyErrorKind::Transient,
                _ => CopyErrorKind::Transient,
            },
            FxcpError::Security(_) => CopyErrorKind::Permanent,
            _ => CopyErrorKind::Transient,
        }
    }
}

impl From<io_uring::squeue::PushError> for FxcpError {
    fn from(err: io_uring::squeue::PushError) -> Self {
        FxcpError::IouPush(format!("{:?}", err))
    }
}

pub type Result<T> = std::result::Result<T, FxcpError>;
