use crate::{db, Connection};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use super::Error;

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

impl Request {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<Vec<u8>>) -> Request {
        Request { msg }
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> Result<(), Error> {
        let response: Result<_, db::Error> = Ok(Response {
            msg: self.msg.unwrap_or(b"PONG".to_vec()),
        });

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }
}
