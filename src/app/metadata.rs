use super::error::{Error, Result};
use bytes::Bytes;
use tracing::{debug, warn};

use crate::{
    data::{record::Record, timestamp::Timestamp},
    dur,
    meta::MetadataEntry,
};

use super::AppLock;

impl AppLock {
    pub async fn append_metadata(&mut self, entry: MetadataEntry) -> dur::error::Result<u64> {
        let mut topic = self
            .get_topic_mut(0)
            .expect("Metadata topic id 0 does not exist");

        topic
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
            .inspect(|offset| debug!("Appended metadata entry {entry:#?} with offset {offset}"))
            .inspect_err(|e| warn!("Failed to append metadata entry {e}"))
    }
}
