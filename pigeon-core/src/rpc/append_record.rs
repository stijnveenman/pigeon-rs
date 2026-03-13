use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendRecord {
    pub key: String,
    pub value: String,
}
