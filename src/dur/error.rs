use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Underlying IO error")]
    UnderlyingIO(#[from] tokio::io::Error),
    #[error("Offset not found")]
    OffsetNotFound,
    #[error("Segment is full and does not accept extra records")]
    SegmentFull,
    #[error("Partition ID does not exist")]
    PartitionNotFound,
}

pub type Result<T> = std::result::Result<T, Error>;
