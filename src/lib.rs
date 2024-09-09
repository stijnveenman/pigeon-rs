pub mod server;

mod shutdown;
use shutdown::Shutdown;

mod connection;
use connection::Connection;

pub const DEFAULT_PORT: u16 = 6394;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
