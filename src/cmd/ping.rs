use crate::Connection;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::ServerResponse;

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

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response: ServerResponse<_> = Ok(self);

        dst.write_frame(&response).await?;

        Ok(())
    }
}
