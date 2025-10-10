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
        let mut metadata_topic = Topic::load_from_disk(config.clone(), 0, "foo").await?;

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
        let mut topic_ids = HashMap::new();
        for (key, topic_metadata) in metadata.topics {
            let topic = Topic::load_from_disk(
                config.clone(),
                topic_metadata.topic_id,
                &topic_metadata.name,
            )
            .await
            .expect("Failed to load topic during startup");

            topics.insert(key, topic);
            topic_ids.insert(topic_metadata.name, topic_metadata.topic_id);
        }

        let next_topic_id = *topics.keys().max().unwrap_or(&0);

        info!("Finished initialising App state from disk");
        info!("Loaded {} topics", topics.len());
        let mut app = AppLock {
            config,
            topics,
            topic_ids,
            next_topic_id,
        };

        if app.topics.is_empty() {
            info!("No metadata topic found, creating __metadata");
            app.create_topic(Some(0), "__metadata")
                .await
                .expect("Failed to initialise __metadata");
        }

        Ok(Self {
            app: Arc::new(RwLock::new(app)),
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
    next_topic_id: u64,
    topics: HashMap<u64, Topic>,
    topic_ids: HashMap<String, u64>,
}

#[cfg(test)]
mod tests;
