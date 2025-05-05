use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tracing::instrument;

use super::Rpc;

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    /// optional message to be returned
    pub msg: Option<ByteBuf>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Response {
    /// Either PONG or a copy of the message provided
    pub msg: ByteBuf,
}

impl Rpc for Request {
    type Response = Response;

    fn to_request(self) -> super::Command {
        super::Command::Ping(self)
    }

    #[instrument(skip(self, _ctx))]
    async fn apply(self, _ctx: &mut super::RpcContext) -> Result<Self::Response, crate::db::Error> {
        let response = Response {
            msg: self.msg.unwrap_or(ByteBuf::from(b"PONG")),
        };

        Ok(response)
    }
}
