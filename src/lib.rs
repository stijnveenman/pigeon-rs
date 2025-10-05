#![feature(btree_cursors)]

pub mod server;

pub mod client;

pub mod app;
mod cmd;
pub mod commands;
pub mod config;
mod data;
mod dur;
pub mod http;
pub use cmd::describe_topic;
pub use cmd::fetch;

mod db;
pub use db::Message;

pub mod logging;

mod connection;
pub use connection::Connection;

mod shutdown;

pub const DEFAULT_PORT: u16 = 6394;
