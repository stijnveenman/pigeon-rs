use crate::{byte_buf::ByteBuf, db};
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

    #[instrument(skip(self, _db, _shutdown))]
    async fn apply(
        self,
        _db: &mut db::Db,
        _shutdown: &mut Shutdown,
    ) -> Result<Self::Response, db::Error> {
        let response = Response {
            msg: self.msg.unwrap_or(b"PONG".into()),
        };

        Ok(response)
    }
}
