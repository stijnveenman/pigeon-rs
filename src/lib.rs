pub mod server;

pub mod client;

mod bin_ser;
mod cmd;
pub use cmd::describe_topic;
pub use cmd::fetch;

mod db;
pub use db::Message;

pub mod logging;

mod connection;
pub use connection::Connection;

mod shutdown;

pub const DEFAULT_PORT: u16 = 6394;
