pub mod server;

mod client;

mod cmd;
pub use cmd::fetch;

mod db;
pub use db::Message;

pub mod logging;
pub use client::Client;

mod connection;
pub use connection::Connection;

mod shutdown;

pub const DEFAULT_PORT: u16 = 6394;
