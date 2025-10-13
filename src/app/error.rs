use axum::{http::StatusCode, response::IntoResponse};
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;

use crate::{data::encoding, dur};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Durrability Error")]
    Durrability(#[from] dur::error::Error),
    #[error("Topic with id ({0}) not found")]
    TopicIdNotFound(u64),
    #[error("Topic with name ({0}) not found")]
    TopicNameNotFound(String),
    #[error("Max Topic id reached")]
    MaxTopicIdReached,
    #[error("Topic id ({0}) is already in use")]
    TopicIdInUse(u64),
    #[error("Topic name ({0}) is already in use")]
    TopicNameInUse(String),
    #[error("Topic with name ({0}) is internal")]
    InternalTopicName(String),
    #[error("Error encoding or decoding bytes")]
    EncodingError(#[from] encoding::Error),
    #[error("Fetch timed out waiting for records")]
    FetchTimeout,
    #[error("Error receiving from internal channel")]
    RecvError(#[from] RecvError),
}

pub type Result<T> = std::result::Result<T, Error>;
