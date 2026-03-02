use thiserror::Error;
use fxcp_core::FxcpError;

#[derive(Error, Debug)]
pub enum FoxingError {
    #[error(transparent)]
    Core(#[from] FxcpError),
    #[error("BPF: {0}")]
    Bpf(String),
    #[error("Libbpf: {0}")]
    Libbpf(#[from] libbpf_rs::Error),
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("System: {0}")]
    System(#[from] nix::Error),
    #[error("Security: {0}")]
    Security(String),
    #[error("Config: {0}")]
    Config(String),
    #[error("Versioning: {0}")]
    Versioning(String),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Join: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("Encoding: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
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

impl From<io_uring::squeue::PushError> for FoxingError {
    fn from(err: io_uring::squeue::PushError) -> Self {
        FoxingError::IouPush(format!("{:?}", err))
    }
}

pub type Result<T> = std::result::Result<T, FoxingError>;
