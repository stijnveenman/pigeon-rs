pub mod create_topic;
pub mod describe_topic;
pub mod fetch;
pub mod ping;
pub mod produce;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    connection::{self},
    db::{self, Db},
    shutdown::Shutdown,
    Connection,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping(ping::Request),
    CreateTopic(create_topic::Request),
    Produce(produce::Request),
    Fetch(fetch::Request),
    DescribeTopic(describe_topic::Request),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error in underlying connection")]
    Connection(#[from] connection::Error),
}

pub struct RpcContext {
    pub db: Db,
    pub shutdown: Shutdown,
    pub zk: zookeeper_client::Client,
}

pub trait Rpc {
    type Response;
    fn to_request(self) -> Command;
    async fn apply(self, ctx: &mut RpcContext) -> Result<Self::Response, db::Error>;
}

impl Command {
    pub(crate) async fn apply(
        self,
        dst: &mut Connection,
        ctx: &mut RpcContext,
    ) -> Result<(), connection::Error> {
        use Command::*;

        match self {
            Ping(request) => dst.write_response(&request.apply(ctx).await).await,
            CreateTopic(request) => dst.write_response(&request.apply(ctx).await).await,
            Produce(request) => dst.write_response(&request.apply(ctx).await).await,
            Fetch(request) => dst.write_response(&request.apply(ctx).await).await,
            DescribeTopic(request) => dst.write_response(&request.apply(ctx).await).await,
        }
    }
}
