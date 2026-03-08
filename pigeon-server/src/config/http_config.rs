use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    pub address: String,
    pub port: u16,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 4111,
        }
    }
}
