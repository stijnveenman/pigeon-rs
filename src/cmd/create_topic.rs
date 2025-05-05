use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::Rpc;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub name: String,
    pub partitions: u64,
}

impl Rpc for Request {
    type Response = ();

    #[instrument(skip(self, ctx))]
    async fn apply(self, ctx: &mut super::RpcContext) -> Result<Self::Response, crate::db::Error> {
        ctx.db.create_topic(self.name, self.partitions)
    }

    fn to_request(self) -> super::Command {
        super::Command::CreateTopic(self)
    }
}
