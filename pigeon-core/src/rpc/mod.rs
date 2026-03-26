use serde::{Deserialize, Serialize};

use crate::PError;

pub mod append_record;
pub mod consumer_group_respones;
pub mod create_topic;
pub mod join_consumer_group;

#[derive(Serialize, Deserialize)]
pub struct Error {
    pub error: PError,
}
