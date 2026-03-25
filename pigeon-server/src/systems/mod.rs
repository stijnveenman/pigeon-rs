use std::{collections::HashMap, fs::create_dir, path::Path, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::RwLock;

use crate::{
    config::ServerConfig,
    metadata::Metadata,
    systems::{
        consumer_groups::ConsumerGroup, execution_context::ExecutionContext, topics::TopicState,
    },
};

#[cfg(test)]
mod test_system;

mod consumer_groups;
pub mod execution_context;
mod metadata;
mod topics;

pub struct SystemContext {
    pub config: Arc<ServerConfig>,
    metadata: RwLock<Metadata>,
    /// List of open topics in memory, not necessarily all existing topics
    topics: RwLock<HashMap<String, TopicState>>,
    groups: Mutex<HashMap<String, ConsumerGroup>>,
}

impl SystemContext {
    pub async fn initialise(config: Arc<ServerConfig>) -> Arc<Self> {
        if !Path::new(&config.data_dir).exists() {
            create_dir(&config.data_dir).unwrap();
        }

        let system = Arc::new(SystemContext {
            metadata: RwLock::default(),
            config,
            topics: Default::default(),
            groups: Default::default(),
        });

        ExecutionContext::system(system.clone())
            .initialise_metadata()
            .await
            .expect("Failed to load metadata");

        system
    }
}
