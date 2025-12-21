use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum BsfsError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Bincode serialization error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Metadata CRC mismatch for inode {inode}")]
    CrcMismatch { inode: u64 },

    #[error("Inode not found: {0}")]
    InodeNotFound(u64),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Cloud storage error: {0}")]
    CloudStorage(String),

    #[error("Timestamp in future for inode {inode}: {field} is {timestamp:?}")]
    FutureTimestamp {
        inode: u64,
        field: &'static str,
        timestamp: std::time::SystemTime,
    },

    #[error("Recovery failed: {0}")]
    Recovery(String),

    #[error("Already mounted")]
    AlreadyMounted,

    #[error("Not mounted")]
    NotMounted,
}

pub type Result<T> = std::result::Result<T, BsfsError>;

impl BsfsError {
    pub fn to_errno(&self) -> libc::c_int {
        match self {
            BsfsError::Io(e) => e.raw_os_error().unwrap_or(libc::EIO),
            BsfsError::InodeNotFound(_) => libc::ENOENT,
            BsfsError::FileNotFound(_) => libc::ENOENT,
            BsfsError::InvalidPath(_) => libc::EINVAL,
            BsfsError::CrcMismatch { .. } => libc::EIO,
            BsfsError::Config(_) => libc::EINVAL,
            BsfsError::CloudStorage(_) => libc::EIO,
            BsfsError::FutureTimestamp { .. } => libc::EINVAL,
            BsfsError::Recovery(_) => libc::EIO,
            BsfsError::AlreadyMounted => libc::EBUSY,
            BsfsError::NotMounted => libc::EINVAL,
            BsfsError::Json(_) => libc::EINVAL,
            BsfsError::Bincode(_) => libc::EIO,
        }
    }
}
