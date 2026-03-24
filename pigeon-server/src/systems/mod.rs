use std::{collections::HashMap, fs::create_dir, path::Path, sync::Arc};

use tokio::sync::RwLock;

use crate::{
    config::ServerConfig,
    metadata::Metadata,
    systems::{execution_context::ExecutionContext, topics::TopicState},
};

#[cfg(test)]
mod test_system;

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
}
