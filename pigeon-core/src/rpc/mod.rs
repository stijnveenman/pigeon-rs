use serde::{Deserialize, Serialize};

use crate::PError;

pub mod append_record;
pub mod create_topic;

#[derive(Serialize, Deserialize)]
pub struct Error {
    pub error: PError,
}
