use bytes::Bytes;
use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

use crate::{
    cmd::{CreateTopic, Fetch, Ping, Produce},
    connection::Connection,
    parse::Parse,
    Frame, Message,
};

pub struct Client {
    connection: Connection,
}

impl Client {
    /// Establish a connection with the Redis server located at `addr`.
    ///
    /// `addr` may be any type that can be asynchronously converted to a
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
    /// trait is the Tokio version and not the `std` version.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pigeon_rs::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = match Client::connect("localhost:6394").await {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    ///
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);

        Ok(Client { connection })
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
    ///
    /// Demonstrates basic usage.
    /// ```no_run
    /// use pigen_rs::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// Create a new topic
    ///
    /// Returns OK if the topic was succesfully created
    ///
    /// # Examples
    /// Demonstrates basic usage.
    /// ```no_run
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let result = client.create_topic("topic", 5).await.unwrap();
    ///     assert_eq!(b"OK", &result[..]);
    /// }
    /// ```
    pub async fn create_topic(&mut self, name: String, partitions: u64) -> crate::Result<Bytes> {
        let frame = CreateTopic::new(name, partitions).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// Produce a message on a topic
    ///
    /// Returns a tuple of (partition_key, offset)
    /// # Examples
    /// Demonstrates basic usage.
    /// ```no_run
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let result = client.create_topic("topic", 5).await.unwrap();
    ///     assert_eq!(b"OK", &result[..]);
    ///
    ///     let result = client.produce("topic", "key", "message").await.unwrap();
    ///     assert_eq!(result, (0, 0));
    /// }
    /// ```
    pub async fn produce(
        &mut self,
        topic: String,
        key: Bytes,
        data: Bytes,
    ) -> crate::Result<(u64, u64)> {
        let frame = Produce::new(topic, key, data).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        let response = self.read_response().await?;
        let mut parse = Parse::new(response)?;

        let partition_key = parse.next_int()?;
        let offset = parse.next_int()?;

        Ok((partition_key, offset))
    }

    /// Fetch a message from a topic
    ///
    /// Returns a message for a given offset.
    /// Errors if the topic or partition does not exist
    /// Returns Ok(None) if the offset does not exist
    /// # Examples
    /// Demonstrates basic usage.
    /// ```no_run
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let result = client.fetch("topic", 1, 10).await.unwrap();
    /// }
    /// ```
    pub async fn fetch(
        &mut self,
        topic: String,
        partition: u64,
        offset: u64,
    ) -> crate::Result<Option<Message>> {
        let frame = Fetch::new(topic, partition, offset).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        let response = self.read_response().await?;

        match response {
            Frame::Array(v) => {
                let mut parse = Parse::from_vec(v);
                let message = Message::parse_frames(&mut parse)?;
                Ok(Some(message))
            }
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(?response);

        match response {
            // Error frames are converted to `Err`
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // Receiving `None` here indicates the server has closed the
                // connection without sending a frame. This is unexpected and is
                // represented as a "connection reset by peer" error.
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");

                Err(err.into())
            }
        }
    }
}
