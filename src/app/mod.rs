#![allow(unused)]
pub mod error;
mod topics;

use std::{collections::HashMap, sync::Arc};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{config::Config, dur::topic::Topic};

pub struct App {
    app: Arc<RwLock<AppLock>>,
}

impl App {
    pub fn new(config: Config) -> Self {
        Self {
            app: Arc::new(RwLock::new(AppLock {
                config: Arc::new(config),
                topics: HashMap::new(),
            })),
        }
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
}

#[cfg(test)]
mod tests;
