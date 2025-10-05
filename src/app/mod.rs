#![allow(unused)]
pub mod error;
mod metadata;
mod topics;

use std::{collections::HashMap, sync::Arc};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::info;

use crate::{
    config::Config,
    dur::{self, topic::Topic},
    meta::Metadata,
};

pub struct App {
    app: Arc<RwLock<AppLock>>,
}

impl App {
    pub async fn load_from_disk(config: Config) -> Result<Self, dur::error::Error> {
        let config = Arc::new(config);

        // TODO: better bootstrapping of metadata topic having it itself be tracked
        let mut metadata_topic = Topic::load_from_disk(config.clone(), 0).await?;

        let metadata_messages = metadata_topic
            .read_all_from_partition(0)
            .await
            .expect("Failed to initialise metadata");

        let metadata = Metadata::from_records(metadata_messages);
        info!("Metadata loaded: {metadata:?}");

        // TODO: remove existing on disks topics if not in metadata
        let mut topics = HashMap::new();
        for (key, topic) in metadata.topics {
            let topic = Topic::load_from_disk(config.clone(), topic.topic_id)
                .await
                .expect("Failed to load topic during startup");
            topics.insert(key, topic);
        }

        Ok(Self {
            app: Arc::new(RwLock::new(AppLock {
                config,
                topics,
                metadata_topic,
            })),
        })
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, AppLock> {
        self.app.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, AppLock> {
        self.app.write().await
    }
}

impl Clone for App {
    fn clone(&self) -> Self {
        App {
            app: self.app.clone(),
        }
    }
}

pub struct AppLock {
    config: Arc<Config>,
    topics: HashMap<u64, Topic>,
    metadata_topic: Topic,
}

#[cfg(test)]
mod tests;
