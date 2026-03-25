use pigeon_core::PError;

use crate::systems::execution_context::ExecutionContext;

pub struct ConsumerGroup {
    group_id: String,
}

// TODO: permissions + testing
impl ExecutionContext {
    pub fn create_consumer_group(&self, group_id: &str) -> Result<(), PError> {
        let mut groups = self.system.groups.lock();

        if groups.contains_key(group_id) {
            return Err(PError::ConsumerGroupExists);
        }

        groups.insert(
            group_id.to_string(),
            ConsumerGroup {
                group_id: group_id.to_string(),
            },
        );

        Ok(())
    }
}
