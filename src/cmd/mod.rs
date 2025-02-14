mod ping;
pub use ping::Ping;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{connection::ConnectionError, db::Db, shutdown::Shutdown, Connection};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping(Ping),
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Error in underlying connection")]
    ConnectionError(#[from] ConnectionError),
}

impl Command {
    pub(crate) async fn apply(
        self,
        _db: &mut Db,
        dst: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> Result<(), CommandError> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
        }
    }
}
