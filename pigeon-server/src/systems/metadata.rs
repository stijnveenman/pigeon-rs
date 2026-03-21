use pigeon_core::PError;

use crate::{metadata::Metadata, systems::execution_context::ExecutionContext};

impl ExecutionContext {
    pub async fn read_metadata(&self) -> Result<Metadata, PError> {
        let record = self.read_record(".metadata", 0, 1).await?;

        dbg!(record);

        todo!()
    }
}
