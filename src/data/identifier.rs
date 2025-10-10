use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Identifier {
    Name(String),
    Id(u64),
}

impl Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Identifier::Name(name) => write!(f, "topic_name: {name}"),
            Identifier::Id(topic_id) => write!(f, "topic_id: {topic_id}"),
        }
    }
}
