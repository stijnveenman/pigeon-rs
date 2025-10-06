#![allow(unused)]
pub mod error;
mod metadata;
mod topics;

use std::{collections::HashMap, sync::Arc};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, info};

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
        debug!("Loading App with config: {:#?}", config);
        let config = Arc::new(config);

        // TODO: better bootstrapping of metadata topic having it itself be tracked
        debug!("Loading metadata topic from disk");
        let mut metadata_topic = Topic::load_from_disk(config.clone(), 0).await?;

        debug!("Loading metadata records");
        let metadata_messages = metadata_topic
            .read_all_from_partition(0)
            .await
            .expect("Failed to initialise metadata");

        debug!("Loading metadata from {} records", metadata_messages.len());
        let metadata = Metadata::from_records(metadata_messages);
        debug!("Loaded metadata: {metadata:#?}");

        // TODO: remove existing on disks topics if not in metadata
        debug!("Loading {} topics from disk", metadata.topics.len());
        let mut topics = HashMap::new();
        for (key, topic) in metadata.topics {
            let topic = Topic::load_from_disk(config.clone(), topic.topic_id)
                .await
                .expect("Failed to load topic during startup");
            topics.insert(key, topic);
        }

        debug!("Finished initialising App state from disk");
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
