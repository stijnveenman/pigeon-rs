use pigeon_core::{PError, record::Record};

use crate::{metadata::entry::MetadataEntry, systems::execution_context::ExecutionContext};

impl ExecutionContext {
    pub async fn initialise_metadata(&self) -> Result<(), PError> {
        let records = match self.read_range(".metadata", 0, 0u64..).await {
            Ok(records) => records,
            Err(PError::TopicNotFound) => vec![],
            Err(e) => panic!("Failed to read .metadata records: {e}"),
        };

        let mut metadata = self.system.metadata.write().await;

        metadata.initialise(&records);

        if !metadata.topics.contains_key(".metadata") {
            drop(metadata);
            self.create_topic(".metadata", Some(1))
                .await
                .expect("Failed to create .metadata topic");
        }

        Ok(())
    }

    pub async fn apply_metadata(&self, entry: impl Into<MetadataEntry>) -> Result<(), PError> {
        let entry = entry.into();

        let mut meta = self.system.metadata.write().await;
        meta.apply(&entry)?;
        drop(meta);

        let value = serde_json::to_string(&entry).expect("Failed to serialize metadata entry");
        self.elevate()
            .append_record(
                ".metadata",
                0,
                Record {
                    value: value.into_bytes(),
                    offset: 0,
                    key: b"metadata".to_vec(),
                },
            )
            .await?;

        Ok(())
    }
}
