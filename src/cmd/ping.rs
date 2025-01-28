use crate::Connection;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use super::Response;

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Ping {
    /// optional message to be returned
    // TODO should be bytes
    msg: Option<B>,
}

#[derive(Debug)]
struct B(Bytes);

impl Serialize for B {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = &self.0[..];
        v.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for B {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v: Vec<u8> = Vec::deserialize(deserializer)?;

        Ok(Self(Bytes::from(v)))
    }
}

impl Ping {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg: msg.map(B) }
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Response::Success;

        debug!(?response);

        Ok(())
    }
}
