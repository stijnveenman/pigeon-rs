use std::{fs::create_dir, path::Path, sync::Arc};

use crate::{config::ServerConfig, systems::topic_system::TopicSystem};

mod topic_system;

pub struct SystemContext {
    pub config: ServerConfig,
    pub topics: TopicSystem,
}

impl SystemContext {
    pub fn initialise(config: ServerConfig) -> Arc<Self> {
        if !Path::new(&config.data_dir).exists() {
            create_dir(&config.data_dir).unwrap();
        }

        Arc::new(SystemContext {
            topics: TopicSystem::initialise(&config.data_dir),
            config,
        })
    }
}
