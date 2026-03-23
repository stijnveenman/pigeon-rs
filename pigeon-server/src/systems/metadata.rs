use pigeon_core::{PError, record::Record};

use crate::{metadata::entry::MetadataEntry, systems::execution_context::ExecutionContext};

impl ExecutionContext {
    pub async fn initialise_metadata(&self) -> Result<(), PError> {
        let records = self.read_range(".metadata", 0, 0..).await?;
        // let records = match topics.read_range(".metadata", 0, 0u64..).await {
        //     Ok(records) => records,
        //     Err(PError::TopicNotFound) => vec![],
        //     Err(e) => panic!("Failed to read .metadata records: {e}"),
        // };

        let mut metadata = self.system.metadata.write().await;

        metadata.initialise(&records);

        // TODO: initialise if doesn't exist

        Ok(())
    }

    pub async fn apply_metadata(&self, entry: impl Into<MetadataEntry>) -> Result<(), PError> {
        let entry = entry.into();

        let mut meta = self.system.metadata.write().await;
        meta.apply(&entry)?;
        drop(meta);

        let value = serde_json::to_string(&entry).expect("Failed to serialize metadata entry");
        ExecutionContext::system(self.system.clone())
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
