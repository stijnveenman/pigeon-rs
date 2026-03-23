use std::{collections::HashMap, fs::create_dir, path::Path, sync::Arc};

use pigeon_core::PError;
use tokio::sync::RwLock;

use crate::{
    config::ServerConfig,
    metadata::{Metadata, entry::MetadataEntry},
    systems::{execution_context::ExecutionContext, topics::TopicState},
};

pub mod execution_context;
mod metadata;
pub mod topic_system;
mod topics;

pub struct SystemContext {
    pub config: Arc<ServerConfig>,
    metadata: RwLock<Metadata>,
    /// List of open topics in memory, not necessarily all existing topics
    topic_states: RwLock<HashMap<String, TopicState>>,
}

impl SystemContext {
    pub async fn initialise(config: Arc<ServerConfig>) -> Arc<Self> {
        if !Path::new(&config.data_dir).exists() {
            create_dir(&config.data_dir).unwrap();
        }

        let system = Arc::new(SystemContext {
            metadata: RwLock::default(),
            config,
            topic_states: Default::default(),
        });

        ExecutionContext::system(system.clone())
            .initialise_metadata()
            .await
            .expect("Failed to load metadata");

        system
    }

    pub async fn apply_metadata(&self, entry: impl Into<MetadataEntry>) -> Result<(), PError> {
        let entry = entry.into();

        let mut meta = self.metadata.write().await;
        meta.apply(&entry)?;
        drop(meta);

        let value = serde_json::to_string(&entry).expect("Failed to serialize metadata entry");
        // TODO:
        // self.append_record(
        //     ".metadata",
        //     0,
        //     Record {
        //         offset: 0,
        //         key: b"metadata".to_vec(),
        //         value: value.into_bytes(),
        //     },
        // )
        // .await?;

        Ok(())
    }
}
