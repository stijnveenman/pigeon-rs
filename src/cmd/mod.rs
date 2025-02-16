mod ping;
pub use ping::Ping;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    connection::{self},
    db::Db,
    shutdown::Shutdown,
    Connection,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping(Ping),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error in underlying connection")]
    Connection(#[from] connection::Error),
}

impl Command {
    pub(crate) async fn apply(
        self,
        _db: &mut Db,
        dst: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> Result<(), Error> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
        }
    }
}
