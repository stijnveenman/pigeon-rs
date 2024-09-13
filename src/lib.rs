pub mod server;

mod cursor;

mod client;
pub use client::Client;

mod connection;
mod shutdown;

mod request;

pub const DEFAULT_PORT: u16 = 6394;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
