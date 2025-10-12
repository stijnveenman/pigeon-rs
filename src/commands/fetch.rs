use serde::{Deserialize, Serialize};

use crate::data::{encoding::Encoding, identifier::Identifier, offset_selection::OffsetSelection};

#[derive(Debug, Serialize, Deserialize)]
pub struct Fetch {
    pub topic: Identifier,
    pub partition_id: u64,
    pub offset: OffsetSelection,
    pub encoding: Encoding,
}
