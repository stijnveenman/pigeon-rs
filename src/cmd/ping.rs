use crate::{db, Connection};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::Error;

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<Vec<u8>>,
}

impl Ping {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<Vec<u8>>) -> Ping {
        Ping { msg }
    }

    // TODO make ping response
    pub fn msg(self) -> Option<Vec<u8>> {
        self.msg
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> Result<(), Error> {
        let response: Result<_, db::Error> = Ok(Ping::new(self.msg.or(Some(b"PONG".to_vec()))));

        dst.write_frame(&response).await?;

        Ok(())
    }
}
