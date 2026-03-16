use std::{fs::create_dir, path::Path, sync::Arc};

use pigeon_core::{PError, record::Record};
use tokio::sync::RwLock;

use crate::{
    config::ServerConfig,
    metadata::{Metadata, entry::MetadataEntry},
    systems::topic_system::TopicSystem,
};

mod topic_system;

pub struct SystemContext {
    pub config: ServerConfig,
    pub meta: RwLock<Metadata>,
    pub topics: TopicSystem,
}

impl SystemContext {
    pub async fn initialise(config: ServerConfig) -> Arc<Self> {
        if !Path::new(&config.data_dir).exists() {
            create_dir(&config.data_dir).unwrap();
        }

        Arc::new(SystemContext {
            topics: TopicSystem::initialise(&config.data_dir).await,
            meta: RwLock::default(),
            config,
        })
    }

    pub async fn apply_metadata(&self, entry: impl Into<MetadataEntry>) -> Result<(), PError> {
        let entry = entry.into();

        let mut meta = self.meta.write().await;
        meta.apply(&entry)?;
        drop(meta);

        let value = serde_json::to_string(&entry).expect("Failed to serialize metadata entry");
        self.topics
            .append_record(
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
