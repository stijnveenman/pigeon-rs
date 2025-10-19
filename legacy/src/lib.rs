#![feature(btree_cursors)]

pub mod app;
pub mod config;
pub mod data;
mod dur;
pub mod http;
mod meta;

pub mod client;
pub mod logging;

pub const DEFAULT_PORT: u16 = 6394;
