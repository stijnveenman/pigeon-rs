use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum PError {
    #[error("Offset cannot be added to index")]
    IndexOffsetNotAllowed,
    #[error("Failed to parse Index")]
    IndexParseFailed,
}
