use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ProduceResponse {
    pub offset: u64,
}
