use std::{ops::Deref, sync::Arc};

use tempfile::{TempDir, tempdir};

use crate::{
    config::ServerConfig,
    systems::{SystemContext, execution_context::ExecutionContext},
};

pub struct TestSystem {
    pub dir: TempDir,
    pub config: Arc<ServerConfig>,
    pub system: Arc<SystemContext>,
    /// Cheap execution_context holding a System context
    execution_context: ExecutionContext,
}

impl Deref for TestSystem {
    type Target = ExecutionContext;

    fn deref(&self) -> &Self::Target {
        &self.execution_context
    }
}

impl TestSystem {
    pub async fn create() -> TestSystem {
        let dir = tempdir().expect("Failed to create tempdir for TestSystem");

        Self::from(dir).await
    }

    pub fn close(self) -> TempDir {
        self.dir
    }

    pub async fn from(dir: TempDir) -> TestSystem {
        let config = Arc::new(ServerConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        });
        let system = SystemContext::initialise(config.clone()).await;

        TestSystem {
            dir,
            config,
            execution_context: ExecutionContext::system(system.clone()),
            system,
        }
    }
}
