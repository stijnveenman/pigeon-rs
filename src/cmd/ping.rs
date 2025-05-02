use crate::db;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::{Rpc, Shutdown};

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    /// optional message to be returned
    msg: Option<Vec<u8>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Response {
    /// Either PONG or a copy of the message provided
    pub msg: Vec<u8>,
}

impl Rpc for Request {
    type Response = Response;

    fn to_request(self) -> super::Command {
        super::Command::Ping(self)
    }

    #[instrument(skip(self, _db))]
    async fn apply(
        self,
        _db: &mut db::Db,
        shutdown: &mut Shutdown,
    ) -> Result<Self::Response, db::Error> {
        let response = Response {
            msg: self.msg.unwrap_or(b"PONG".to_vec()),
        };

        Ok(response)
    }
}

impl Request {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<Vec<u8>>) -> Request {
        Request { msg }
    }
}
