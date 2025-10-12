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
    pub async fn append_metadata(&mut self, entry: MetadataEntry) -> dur::error::Result<Record> {
        let mut topic = self
            .get_topic_by_id_mut(0)
            .expect("Metadata topic id 0 does not exist");

        topic
            .append(
                0,
                Bytes::new(),
                serde_json::to_string(&entry)
                    .expect("serde_json to_string failed")
                    .into(),
                Vec::new(),
            )
            .await
            .inspect(|record| {
                debug!(
                    "Appended metadata entry {entry:#?} with offset {}",
                    record.offset
                )
            })
            .inspect_err(|e| warn!("Failed to append metadata entry {e}"))
    }
}
