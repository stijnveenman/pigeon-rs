use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct JoinConsumerGroupParams {
    pub epoch: Option<usize>,
}
