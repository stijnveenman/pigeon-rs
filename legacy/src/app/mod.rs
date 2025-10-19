pub mod error;
mod metadata;
mod topics;

use std::{
    collections::HashMap,
    fs::{self, remove_dir_all},
    path::Component,
    sync::Arc,
};

use tokio::sync::{broadcast, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    data::record::Record,
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

        debug!("Loading metadata topic from disk");
        let mut metadata_topic = Topic::load_from_disk(config.clone(), 0, "__metadata", 1).await?;

        debug!("Loading metadata records");
        let metadata_messages = metadata_topic
            .read_all_from_partition(0)
            .await
            .expect("Failed to initialise metadata");

        debug!("Loading metadata from {} records", metadata_messages.len());
        let metadata = Metadata::from_records(metadata_messages);
        debug!("Loaded metadata: {metadata:#?}");

        debug!("Loading {} topics from disk", metadata.topics.len());
        let mut topics = HashMap::new();
        let mut topic_ids = HashMap::new();
        for (key, topic_metadata) in metadata.topics {
            let topic = Topic::load_from_disk(
                config.clone(),
                topic_metadata.topic_id,
                &topic_metadata.name,
                topic_metadata.partitions,
            )
            .await
            .expect("Failed to load topic during startup");

            topics.insert(key, topic);
            topic_ids.insert(topic_metadata.name, topic_metadata.topic_id);
        }

        let topic_folders = fs::read_dir(config.topics_path())?;
        for folder in topic_folders {
            let folder = folder?;

            let path = folder.path();
            let component = path
                .components()
                .next_back()
                .expect("Expected at least some components in path");

            if let Component::Normal(dir_name) = component {
                let id: u64 = dir_name
                    .to_str()
                    .expect("OsStr contained unexpected characters")
                    .parse()
                    .expect("Invalid path in topics path");

                if !topics.contains_key(&id) {
                    warn!(
                        "Found path without corresponding topic, removing {}",
                        path.display()
                    );

                    remove_dir_all(path).expect("Failed to remove topic dir");
                }
            } else {
                warn!("unexpected path component {component:?}");
            }
        }

        let next_topic_id = *topics.keys().max().unwrap_or(&0);

        info!("Finished initialising App state from disk");
        info!("Loaded {} topics", topics.len());
        let mut app = AppLock {
            config,
            topics,
            topic_ids,
            next_topic_id,
            listeners: HashMap::new(),
        };

        if app.topics.is_empty() {
            info!("No metadata topic found, creating __metadata");
            app.create_topic_internal(Some(0), "__metadata", Some(1))
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
    listeners: HashMap<u64, broadcast::Sender<(u64, Arc<Record>)>>,
}

#[cfg(test)]
mod tests;
