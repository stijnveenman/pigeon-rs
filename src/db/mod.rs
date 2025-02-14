mod topics;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
pub use topics::Message;
use topics::Topic;

#[derive(Debug, PartialEq, Error, Serialize, Deserialize)]
pub enum DbErr {
    #[error("Name already in use")]
    NameInUse,
    #[error("Item not found")]
    NotFound,
    #[error("Failed to receive a valid frame")]
    RecvError,
    #[error("Server is shutting down")]
    ShuttingDown,
}

pub type DbResult<T> = Result<T, DbErr>;

#[derive(Clone)]
pub(crate) struct Db {
    shared: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    topics: HashMap<String, Topic>,
    /// A broadcast for fetching cosumers, if a consuming is fetching data that does not exist yet
    /// it's added to this list. Once a matching message comes in, it is pushed to the consumer
    /// Key is (Topic, partition)
    fetches: HashMap<(String, u64), broadcast::Sender<(u64, Message)>>,
}

impl Db {
    pub(crate) fn new() -> Db {
        Db {
            shared: Arc::new(Mutex::new(State::default())),
        }
    }
}
