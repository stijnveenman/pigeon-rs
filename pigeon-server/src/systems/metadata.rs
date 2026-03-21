use pigeon_core::PError;

use crate::{metadata::Metadata, systems::execution_context::ExecutionContext};

impl ExecutionContext {
    pub async fn read_metadata(&self) -> Result<Metadata, PError> {
        let records = self.read_range(".metadata", 0, 0..).await?;

        let metadata = Metadata::initialise(&records);

        Ok(metadata)
    }
}
