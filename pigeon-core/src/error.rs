use thiserror::Error;

#[derive(Error, Debug)]
pub enum PError {
    #[error("Failed to parse Index")]
    IndexParseFailed,
}
