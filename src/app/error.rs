use axum::{http::StatusCode, response::IntoResponse};
use thiserror::Error;

use crate::dur;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Durrability Error")]
    Durrability(#[from] dur::error::Error),
    #[error("Topic with id ({0}) not found")]
    TopicIdNotFound(u64),
    #[error("Max Topic id reached")]
    MaxTopicIdReached,
    #[error("Topic id ({0}) is already in use")]
    TopicIdInUse(u64),
    #[error("Topic name ({0}) is already in use")]
    TopicNameInUse(String),
}

pub type Result<T> = std::result::Result<T, Error>;
