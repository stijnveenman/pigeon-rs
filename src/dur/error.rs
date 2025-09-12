use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Underlying IO error")]
    UnderlyingIO(#[from] tokio::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
