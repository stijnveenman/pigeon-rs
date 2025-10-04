use thiserror::Error;

use crate::dur;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Durrability Error")]
    Durrability(#[from] dur::error::Error),
    #[error("Topic with id ({0}) not found")]
    TopicIdNotFound(u64),
}

pub type Result<T> = std::result::Result<T, Error>;
