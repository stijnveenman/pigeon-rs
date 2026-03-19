mod http_config;
mod topic_config;

use std::path::PathBuf;

use config::Config;
use serde::{Deserialize, Serialize};

use crate::config::{http_config::HttpConfig, topic_config::TopicConfig};

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub data_dir: PathBuf,
    pub topics: TopicConfig,
    pub http: HttpConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            topics: Default::default(),
            http: Default::default(),
            data_dir: PathBuf::from("data"),
        }
    }
}

impl ServerConfig {
    pub fn load_from_file(filename: &str) -> ServerConfig {
        let defaults = Config::try_from(&ServerConfig::default()).unwrap();

        Config::builder()
            .add_source(defaults)
            .add_source(config::File::new(filename, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use tempfile::tempdir;

    use crate::config::ServerConfig;

    #[test]
    fn use_config_file_settings() {
        let dir = tempdir().unwrap();
        let config_file = dir.path().join("config.toml");

        fs::write(
            &config_file,
            "
[topics]
default_partitions = 5
",
        )
        .unwrap();

        let config = ServerConfig::load_from_file(config_file.to_str().unwrap());
        assert_eq!(config.topics.default_partitions, 5);
    }

    #[test]
    fn use_defaults() {
        let dir = tempdir().unwrap();
        let config_file = dir.path().join("config.toml");

        fs::write(
            &config_file,
            "
[topics]
",
        )
        .unwrap();

        let config = ServerConfig::load_from_file(config_file.to_str().unwrap());
        assert_eq!(config.topics.default_partitions, 1);
    }
}
