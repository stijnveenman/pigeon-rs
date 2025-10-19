use std::ffi::OsString;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Underlying IO error")]
    UnderlyingIO(#[from] tokio::io::Error),
    #[error("Segment is full and does not accept extra records")]
    SegmentFull,
    #[error("Partition ID does not exist")]
    PartitionNotFound,
    #[error("Failed to parse start offset from log filename({0:?})")]
    InvalidLogFilename(OsString),
}

pub type Result<T> = std::result::Result<T, Error>;
