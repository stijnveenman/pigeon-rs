use std::{fs::create_dir, path::Path, sync::Arc};

use pigeon_core::{PError, record::Record};
use tokio::sync::RwLock;

use crate::{
    config::ServerConfig,
    execution_context::ExecutionContext,
    metadata::{Metadata, entry::MetadataEntry},
    systems::topic_system::TopicSystem,
};

pub mod topic_system;

pub struct SystemContext {
    pub config: ServerConfig,
    metadata: RwLock<Metadata>,
    topics: TopicSystem,
}

impl SystemContext {
    pub async fn initialise(config: ServerConfig) -> Arc<Self> {
        if !Path::new(&config.data_dir).exists() {
            create_dir(&config.data_dir).unwrap();
        }

        let mut topics = TopicSystem::initialise(&config.data_dir).await;
        let records = match topics.read_range(".metadata", 0, 0u64..).await {
            Ok(records) => records,
            Err(PError::TopicNotFound) => vec![],
            Err(e) => panic!("Failed to read .metadata records: {e}"),
        };

        let metadata = Metadata::initialise(&records).await;
        topics.sync(&metadata.topics).await;

        Arc::new(SystemContext {
            topics,
            metadata: RwLock::new(metadata),
            config,
        })
    }

    pub async fn apply_metadata(&self, entry: impl Into<MetadataEntry>) -> Result<(), PError> {
        let entry = entry.into();

        let mut meta = self.metadata.write().await;
        meta.apply(&entry)?;
        drop(meta);

        let value = serde_json::to_string(&entry).expect("Failed to serialize metadata entry");
        self.append_record(
            &ExecutionContext::system(),
            ".metadata",
            0,
            Record {
                offset: 0,
                key: b"metadata".to_vec(),
                value: value.into_bytes(),
            },
        )
        .await?;

        Ok(())
    }
}
