use serde::{Deserialize, Serialize};

use crate::data::{encoding::Encoding, identifier::Identifier};

#[derive(Debug, Serialize, Deserialize)]
pub struct Fetch {
    pub topic: Identifier,
    pub partition_id: u64,
    pub offset: u64,
    pub encoding: Encoding,
}
