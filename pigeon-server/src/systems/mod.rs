use std::{collections::HashMap, fs::create_dir, path::Path, sync::Arc};

use pigeon_core::{PError, record::Record};
use tokio::sync::RwLock;

use crate::{
    config::ServerConfig,
    metadata::{Metadata, entry::MetadataEntry},
    systems::{execution_context::ExecutionContext, topic_system::TopicSystem, topics::TopicState},
};

pub mod execution_context;
mod metadata;
pub mod topic_system;
mod topics;

pub struct SystemContext {
    pub config: Arc<ServerConfig>,
    metadata: RwLock<Metadata>,
    topics: TopicSystem,
    /// List of open topics in memory, not necessarily all existing topics
    topic_states: RwLock<HashMap<String, TopicState>>,
}

impl SystemContext {
    pub async fn initialise(config: Arc<ServerConfig>) -> Arc<Self> {
        if !Path::new(&config.data_dir).exists() {
            create_dir(&config.data_dir).unwrap();
        }

        let mut topics = TopicSystem::initialise(config.data_dir.to_str().unwrap()).await;
        let records = match topics.read_range(".metadata", 0, 0u64..).await {
            Ok(records) => records,
            Err(PError::TopicNotFound) => vec![],
            Err(e) => panic!("Failed to read .metadata records: {e}"),
        };

        let metadata = Metadata::initialise(&records).await;
        topics.sync(&metadata.topics).await;

        let system = Arc::new(SystemContext {
            topics,
            metadata: RwLock::new(metadata),
            config,
            topic_states: Default::default(),
        });

        let _ = dbg!(
            ExecutionContext::system(system.clone())
                .read_metadata()
                .await
        );

        system
    }

    pub async fn apply_metadata(&self, entry: impl Into<MetadataEntry>) -> Result<(), PError> {
        let entry = entry.into();

        let mut meta = self.metadata.write().await;
        meta.apply(&entry)?;
        drop(meta);

        let value = serde_json::to_string(&entry).expect("Failed to serialize metadata entry");
        self.append_record(
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
