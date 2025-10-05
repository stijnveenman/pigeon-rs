use super::error::{Error, Result};
use bytes::Bytes;

use crate::{
    data::{record::Record, timestamp::Timestamp},
    dur,
    meta::MetadataEntry,
};

use super::AppLock;

impl AppLock {
    pub async fn append_metadata(&mut self, entry: MetadataEntry) -> dur::error::Result<u64> {
        self.metadata
            .append(
                0,
                Record {
                    offset: 0,
                    timestamp: Timestamp::now(),
                    key: Bytes::new(),
                    value: serde_json::to_string(&entry)
                        .expect("serde_json to_string failed")
                        .into(),
                    headers: Vec::new(),
                },
            )
            .await
    }
}
