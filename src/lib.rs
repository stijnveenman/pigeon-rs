pub mod server;

mod client;

mod cmd;

mod db;
pub use db::Message;

pub mod parse;
pub use frame::Frame;

pub mod logging;
pub use client::Client;

mod frame;
use parse::{Parse, ParseError};

mod api_key;
pub use api_key::ApiKey;

mod connection;
pub use connection::Connection;

mod shutdown;

pub const DEFAULT_PORT: u16 = 6394;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
