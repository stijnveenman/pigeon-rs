mod consumer;

use std::io;

use consumer::Consumer;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

use crate::{
    cmd::{create_topic, fetch, ping, produce, Rpc},
    connection::{self, Connection},
    db, describe_topic, Message,
};

pub struct Client {
    connection: Connection,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error in underlying IO stream")]
    Io(#[from] io::Error),
    #[error("Client Error")]
    Connection(#[from] connection::Error),
    #[error("Server did not respond")]
    NoResponse,
    #[error("Server side database error")]
    Db(#[from] db::Error),
}

/// Establish a connection with the Redis server located at `addr`.
///
/// `addr` may be any type that can be asynchronously converted to a
/// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
/// trait is the Tokio version and not the `std` version.
///
/// # Examples
///
/// ```no_run
/// use pigeon_rs::client;
///
/// #[tokio::main]
/// async fn main() {
///     let client = match client::connect("localhost:6394").await {
///         Ok(client) => client,
///         Err(_) => panic!("failed to establish connection"),
///     };
///     // drop(client);
/// }
/// ```
///
pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client, Error> {
    let socket = TcpStream::connect(addr).await?;

    let connection = Connection::new(socket);

    Ok(Client { connection })
}

pub async fn consumer(client: Client, topic: String) -> Result<Consumer, Error> {
    Consumer::consume(client, topic).await
}

impl Client {
    async fn read_response<T: DeserializeOwned + std::fmt::Debug>(&mut self) -> Result<T, Error> {
        let response = self.connection.read_frame::<Result<T, db::Error>>().await?;

        match response {
            Some(Ok(response)) => Ok(response),
            Some(Err(e)) => Err(e.into()),
            None => Err(Error::NoResponse),
        }
    }

    async fn rpc<T>(&mut self, transaction: T) -> Result<T::Response, Error>
    where
        T: Rpc,
        T::Response: DeserializeOwned + std::fmt::Debug,
    {
        let frame = transaction.to_request();
        debug!(request = ?frame);

        self.connection.write_frame(&frame).await?;

        let response = self.read_response().await;
        debug!(?response);
        response
    }

    /// Ping to the server.
    ///
    /// Returns PONG if no argument is provided, otherwise
    /// return a copy of the argument as a bulk.
    ///
    /// This command is often used to test if a connection
    /// is still alive, or to measure latency.
    ///
    /// # Examples
    /// Demonstrates basic usage.
    /// ```no_run
    /// use pigeon_rs::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    pub async fn ping(&mut self, msg: Option<Vec<u8>>) -> Result<Vec<u8>, Error> {
        self.rpc(ping::Request { msg })
            .await
            .map(|response| response.msg)
    }

    /// Create a new topic
    ///
    /// Returns OK if the topic was succesfully created
    ///
    /// # Examples
    /// Demonstrates basic usage.
    /// ```no_run
    /// use pigeon_rs::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await.unwrap();
    ///
    ///     let result = client.create_topic("topic".into(), 5).await.unwrap();
    ///     assert_eq!((), result);
    /// }
    /// ```
    pub async fn create_topic(&mut self, name: String, partitions: u64) -> Result<(), Error> {
        self.rpc(create_topic::Request { name, partitions }).await
    }

    /// Produce a message on a topic
    ///
    /// Returns a tuple of (partition_key, offset)
    /// # Examples
    /// Demonstrates basic usage.
    /// ```no_run
    /// use pigeon_rs::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await.unwrap();
    ///
    ///     let result = client.create_topic("topic".into(), 5).await.unwrap();
    ///     assert_eq!((), result);
    ///
    ///     let result = client.produce("topic".into(), "key".into(), "message".into()).await.unwrap();
    ///     assert_eq!(result, (0, 0));
    /// }
    /// ```
    pub async fn produce(
        &mut self,
        topic: String,
        key: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<(u64, u64), Error> {
        self.rpc(produce::Request { topic, key, data }).await
    }

    /// Fetch using a fetch config
    /// Can be used to fetch from multiple topics and partitions, will wait at most `timeout_ms`
    /// before returning if no message with the given offset exists yet.
    ///
    /// # Examples
    /// demonstrate basic usage
    /// ```no_run
    /// use pigeon_rs::{client, fetch};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await.unwrap();
    ///
    ///     let config = fetch::Request {
    ///         timeout_ms: 1000,
    ///         topics: vec![fetch::TopicsRequest {
    ///             topic: "test".into(),
    ///             partitions: vec![
    ///                 fetch::PartitionRequest {
    ///                     partition: 0,
    ///                     offset: 0,
    ///                 },
    ///                 fetch::PartitionRequest {
    ///                     partition: 1,
    ///                     offset: 0,
    ///                 },
    ///                 fetch::PartitionRequest {
    ///                     partition: 2,
    ///                     offset: 0,
    ///                 },
    ///             ],
    ///         }],
    ///     };
    ///     let result = client.fetch(config).await;
    /// }
    /// ```
    pub async fn fetch(&mut self, request: fetch::Request) -> Result<Option<Message>, Error> {
        self.rpc(request).await
    }

    /// Describe a topic and its partition
    ///
    /// # Examples
    /// demonstrate basic usage
    /// ```no_run
    /// use pigeon_rs::client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await.unwrap();
    ///
    ///     let result = client.describe_topic("topic".into()).await.unwrap();
    ///     println!("{:#?}", result);
    /// }
    /// ```
    pub async fn describe_topic(
        &mut self,
        topic: String,
    ) -> Result<describe_topic::TopicDescription, Error> {
        self.rpc(describe_topic::Request { topic }).await
    }
}
