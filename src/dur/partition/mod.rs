use std::path::Path;

use tokio::fs::create_dir_all;

use super::error::Result;
use crate::config::Config;

pub struct Partition {
    topic_id: u64,
    partition_id: u64,
    current_offset: u64,
}

impl Partition {
    pub async fn load_from_disk(config: Config, topic_id: u64, partition_id: u64) -> Result<Self> {
        let partition_path = config.partition_path(topic_id, partition_id);

        create_dir_all(Path::new(&partition_path)).await?;

        Ok(Self {
            // FIX: load_from_disk
            current_offset: 0,
            partition_id,
            topic_id,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{fs::create_dir_all, path::Path};

    use tempfile::tempdir;

    use crate::config::{self, Config};

    fn create_config() -> (tempfile::TempDir, config::Config) {
        let dir = tempdir().expect("failed to create tempdir");

        let config = Config {
            path: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let partition_path = config.partition_path(0, 0);

        create_dir_all(Path::new(&partition_path)).expect("failed to create partition_path");

        (dir, config)
    }
}
