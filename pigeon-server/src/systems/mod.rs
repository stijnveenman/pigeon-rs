use std::sync::Arc;

use crate::{config::ServerConfig, systems::topic_system::TopicSystem};

mod topic_system;

pub struct SystemContext {
    pub config: ServerConfig,
    pub topics: TopicSystem,
}

impl SystemContext {
    pub fn initialise(config: ServerConfig) -> Arc<Self> {
        Arc::new(SystemContext {
            topics: TopicSystem::initialise(&config.data_dir),
            config,
        })
    }
}
