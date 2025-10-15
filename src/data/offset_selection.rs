use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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

    pub fn narrow(&self, offset: u64) -> Option<Self> {
        match self {
            OffsetSelection::Exact(_) => None,
            OffsetSelection::From(value) => {
                Some(OffsetSelection::From(value.to_owned().max(offset + 1)))
            }
        }
    }
}
