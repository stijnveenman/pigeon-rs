#![allow(unused)]
pub mod error;
mod topics;

use std::{collections::HashMap, sync::Arc};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    config::Config,
    dur::{self, topic::Topic},
};

pub struct App {
    app: Arc<RwLock<AppLock>>,
}

impl App {
    pub async fn load_from_disk(config: Config) -> Result<Self, dur::error::Error> {
        let config = Arc::new(config);

        let metadata = Topic::load_from_disk(config.clone(), 0).await?;

        Ok(Self {
            app: Arc::new(RwLock::new(AppLock {
                config,
                topics: HashMap::new(),
                metadata,
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
    metadata: Topic,
}

#[cfg(test)]
mod tests;
