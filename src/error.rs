use thiserror::Error;

#[derive(Error, Debug)]
pub enum FoxingError {
    #[error("Config: {0}")]
    Config(String),
    #[error("BPF: {0}")]
    Bpf(String),
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("System: {0}")]
    System(#[from] nix::Error),
    #[error("Security: {0}")]
    Security(String),
    #[error("Encoding: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Libbpf: {0}")]
    Libbpf(#[from] libbpf_rs::Error),
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
    // Added for Task 2: Explicit memory exhaustion error
    #[error("Memory Limit Exhausted: {0}")]
    MemoryExhausted(String),
}

impl From<io_uring::squeue::PushError> for FoxingError {
    fn from(err: io_uring::squeue::PushError) -> Self {
        FoxingError::IouPush(format!("{:?}", err))
    }
}

pub type Result<T> = std::result::Result<T, FoxingError>;
