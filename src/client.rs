use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{
    connection::Connection,
    request::create_topics_request::{self, CreateTopicsRequest},
    response::create_topics_response::CreateTopicResponse,
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

    pub async fn create_topic(&mut self, name: &str) -> crate::Result<CreateTopicResponse> {
        self.connection
            .write_frame(CreateTopicsRequest {
                topics: vec![create_topics_request::Topic {
                    name: name.to_string(),
                    num_partitions: 1,
                }],
            })
            .await?;

        let frame = self.connection.read_frame::<CreateTopicResponse>().await?;

        match frame {
            Some(frame) => Ok(frame),
            None => Err("connection closed".into()),
        }
    }
}
