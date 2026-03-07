use config::Config;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ServerConfig {
    topics: TopicConfig,
}

impl ServerConfig {
    fn load_from_file(filename: &str) -> ServerConfig {
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
    fn use_defaults_with_overide() {
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
        assert_eq!(config.topics.foo, 5);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicConfig {
    default_partitions: u64,
    foo: u64,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            default_partitions: 1,
            foo: 5,
        }
    }
}
