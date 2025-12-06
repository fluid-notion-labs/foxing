use thiserror::Error;
#[derive(Error, Debug)]
pub enum FoxingError {
    #[error("Config: {0}")] Config(String),
    #[error("BPF: {0}")] Bpf(String),
    #[error("IO: {0}")] Io(#[from] std::io::Error),
    #[error("System: {0}")] System(#[from] nix::Error),
    #[error("Security: {0}")] Security(String),
    #[error("Encoding: {0}")] Utf8(#[from] std::string::FromUtf8Error),
    #[error("Libbpf: {0}")] Libbpf(#[from] libbpf_rs::Error),
    #[error("Join: {0}")] Join(#[from] tokio::task::JoinError),
    #[error("JSON: {0}")] Json(#[from] serde_json::Error),
    #[error("Versioning: {0}")] Versioning(String),
}
pub type Result<T> = std::result::Result<T, FoxingError>;
