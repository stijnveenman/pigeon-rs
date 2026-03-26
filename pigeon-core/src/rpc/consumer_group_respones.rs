use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum ConsumerGroupStateResponse {
    Empty,
    Rebalancing,
    Stable,
}

#[derive(Serialize, Deserialize)]
pub struct ConsumerGroupResponse {
    pub group_id: String,
    pub epoch: usize,
    pub consumers: Vec<String>,
    pub state: ConsumerGroupStateResponse,
}

impl Display for ConsumerGroupStateResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ConsumerGroupStateResponse::Empty => "Empty",
            ConsumerGroupStateResponse::Rebalancing => "Rebalancing",
            ConsumerGroupStateResponse::Stable => "Stable",
        };
        write!(f, "{s}")
    }
}

impl Display for ConsumerGroupResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}[Epoch: {}] - {} - Consumers ({})",
            self.group_id,
            self.epoch,
            self.state,
            self.consumers.join(",")
        )
    }
}
