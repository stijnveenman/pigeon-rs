use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PError {
    #[error("Offset cannot be added to index")]
    IndexOffsetNotAllowed,
    #[error("Failed to parse Index")]
    IndexParseFailed,
    #[error("Failed to open Index")]
    IndexOpenFailed,
    #[error("Failed to write to Index")]
    IndexWriteFailed,
    #[error("Index not found")]
    IndexNotFound,
    #[error("Index read failed")]
    IndexReadFailed,
    #[error("Faeild to open Log")]
    LogOpenFailed,
    #[error("Failed to write to Log")]
    LogWriteFailed,
}
