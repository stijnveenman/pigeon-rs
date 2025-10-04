#![allow(unused)]
use std::{collections::HashMap, sync::Arc};

use tokio::sync::{RwLock, RwLockReadGuard};

use crate::dur::topic::Topic;

pub struct App {
    app: Arc<RwLock<AppLock>>,
}

impl App {
    pub fn new(app: AppLock) -> Self {
        Self {
            app: Arc::new(RwLock::new(app)),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, AppLock> {
        self.app.read().await
    }

    pub async fn write(&self) -> RwLockReadGuard<'_, AppLock> {
        self.app.read().await
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
    topics: HashMap<u64, Topic>,
}

impl AppLock {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }
}
