use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::debug;

use crate::{
    cmd::{Command, Ping, ServerResponse},
    connection::Connection,
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

    async fn read_response(&mut self) -> crate::Result<Option<ServerResponse<Ping>>> {
        let response = self.connection.read_frame().await;

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
    pub async fn ping(&mut self, msg: Option<Vec<u8>>) -> crate::Result<Vec<u8>> {
        let frame = Command::Ping(Ping::new(msg));
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Some(Ok(ping)) => Ok(ping.msg().unwrap()),
            Some(Err(e)) => Err(e.into()),
            None => Err("No Response from server`".into()),
        }
    }
}
