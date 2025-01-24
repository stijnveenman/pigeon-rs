use crate::Connection;
use bytes::Bytes;
use tracing::{debug, instrument};

use super::Response;

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<Bytes>,
}

impl Ping {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Response::Success;

        debug!(?response);

        Ok(())
    }
}
