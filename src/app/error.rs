use thiserror::Error;

use crate::dur;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Durrability Error")]
    Durrability(#[from] dur::error::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
