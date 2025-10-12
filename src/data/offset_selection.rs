use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum OffsetSelection {
    Exact(u64),
    From(u64),
}

impl OffsetSelection {
    pub fn matches(&self, offset: u64) -> bool {
        match self {
            OffsetSelection::Exact(value) => offset == *value,
            OffsetSelection::From(value) => offset >= *value,
        }
    }
}
