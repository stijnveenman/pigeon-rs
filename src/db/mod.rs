mod cluster;
mod topics;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use cluster::ClusterState;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
pub use topics::Message;
use topics::Topic;

#[derive(Debug, PartialEq, Error, Serialize, Deserialize)]
pub enum Error {
    #[error("Name already in use")]
    NameInUse,
    #[error("Topic not found")]
    TopicNotFound,
    #[error("Partition not found")]
    PartitionNotFound,
    #[error("Failed to receive a valid frame")]
    Recv,
    #[error("Server is shutting down")]
    ShuttingDown,
}

pub type DbResult<T> = Result<T, Error>;

#[derive(Clone)]
pub(crate) struct Db {
    shared: Arc<Mutex<State>>,
}

struct State {
    topics: HashMap<String, Topic>,
    /// A broadcast for fetching cosumers, if a consuming is fetching data that does not exist yet
    /// it's added to this list. Once a matching message comes in, it is pushed to the consumer
    /// Key is (Topic, partition)
    fetches: HashMap<(String, u64), broadcast::Sender<(u64, Message)>>,
    cluster_state: ClusterState,
}

impl Db {
    pub(crate) fn new(zk: zookeeper_client::Client) -> Db {
        Db {
            shared: Arc::new(Mutex::new(State {
                topics: HashMap::from([("hello".to_string(), Topic::new(1))]),
                fetches: HashMap::default(),
                cluster_state: ClusterState::new(zk),
            })),
        }
    }
}
