use std::io;

use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

use crate::{
    cmd::{ping, Command},
    connection::{self, Connection},
    db,
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
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client, Error> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    async fn read_response<T: DeserializeOwned + std::fmt::Debug>(&mut self) -> Result<T, Error> {
        let response = self.connection.read_frame::<Result<T, db::Error>>().await?;

        debug!(?response);

        match response {
            Some(Ok(response)) => Ok(response),
            Some(Err(e)) => Err(e.into()),
            None => Err(Error::NoResponse),
        }
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
    pub async fn ping(&mut self, msg: Option<Vec<u8>>) -> Result<Vec<u8>, Error> {
        let frame = Command::Ping(ping::Request::new(msg));
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        self.read_response::<ping::Response>()
            .await
            .map(|response| response.msg)
    }
}
