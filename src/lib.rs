pub mod server;

mod protocol;

mod client;
pub mod logging;
pub use client::Client;

mod api_key;
pub use api_key::ApiKey;

mod connection;
mod shutdown;

pub mod request;

pub const DEFAULT_PORT: u16 = 6394;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
