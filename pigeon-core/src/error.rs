use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum PError {
    #[error("Offset cannot be added to index")]
    IndexOffsetNotAllowed,
    #[error("Failed to parse Index")]
    IndexParseFailed,
    #[error("Failed to open Index")]
    IndexOpenFailed,
    #[error("Failed to write to Index")]
    IndexWriteFailed,
}
