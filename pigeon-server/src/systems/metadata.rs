use pigeon_core::PError;

use crate::systems::execution_context::ExecutionContext;

impl ExecutionContext {
    pub async fn initialise_metadata(&self) -> Result<(), PError> {
        let records = self.read_range(".metadata", 0, 0..).await?;
        // let records = match topics.read_range(".metadata", 0, 0u64..).await {
        //     Ok(records) => records,
        //     Err(PError::TopicNotFound) => vec![],
        //     Err(e) => panic!("Failed to read .metadata records: {e}"),
        // };

        let mut metadata = self.system.metadata.write().await;

        metadata.initialise(&records);

        // TODO: initialise if doesn't exist

        Ok(())
    }
}
