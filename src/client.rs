use bson::{doc, Document};
use serde::Serialize;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::connection::Connection;

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

    pub async fn test(&mut self) -> crate::Result<()> {
        let frame = doc! {
            "hello": "world"
        };

        let bytes = bson::to_vec(&frame)?;

        dbg!(&bytes);
        self.connection.write_frame(&bytes).await?;

        Ok(())
    }
}
