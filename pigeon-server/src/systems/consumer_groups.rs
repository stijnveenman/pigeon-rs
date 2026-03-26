use std::{collections::HashMap, time::Instant};

use chrono::{DateTime, Utc};
use parking_lot::{MutexGuard, RawMutex, lock_api::MappedMutexGuard};
use pigeon_core::PError;

use crate::systems::execution_context::ExecutionContext;

pub struct ConsumerClient {
    last_seen: DateTime<Utc>,
}

pub enum ConsumerGroupState {
    Empty,
    Rebalancing(DateTime<Utc>),
    Stable,
}

pub struct ConsumerGroup {
    group_id: String,
    consumers: HashMap<String, ConsumerClient>,
    state: ConsumerGroupState,
    epoch: usize,
}

impl ConsumerGroup {
    fn start_epoch(&mut self) {
        self.consumers.clear();
        self.state = ConsumerGroupState::Rebalancing(Utc::now());
        self.epoch += 1;
    }

    fn add_consumer(&mut self, consumer_id: String) -> Result<usize, PError> {
        self.consumers.insert(
            consumer_id,
            ConsumerClient {
                last_seen: Utc::now(),
            },
        );

        Ok(self.epoch)
    }
}

// TODO: permissions + testing
impl ExecutionContext {
    fn get_group(
        &self,
        group_id: &str,
    ) -> Result<MappedMutexGuard<'_, RawMutex, ConsumerGroup>, PError> {
        let groups = self.system.groups.lock();

        let group = MutexGuard::try_map(groups, |groups| groups.get_mut(group_id))
            .map_err(|_| PError::ConsumerGroupNotFound)?;

        // TODO: update group, if rebalancing finished, or consumers timed out etc

        Ok(group)
    }

    pub fn create_consumer_group(&self, group_id: &str) -> Result<(), PError> {
        let mut groups = self.system.groups.lock();

        if groups.contains_key(group_id) {
            return Err(PError::ConsumerGroupExists);
        }

        groups.insert(
            group_id.to_string(),
            ConsumerGroup {
                group_id: group_id.to_string(),
                consumers: HashMap::new(),
                state: ConsumerGroupState::Empty,
                epoch: 0,
            },
        );

        Ok(())
    }

    pub fn join_consumer_group(
        &self,
        group_id: &str,
        consumer_id: &str,
        epoch: Option<usize>,
    ) -> Result<usize, PError> {
        let mut group = self.get_group(group_id)?;

        match group.state {
            ConsumerGroupState::Rebalancing(_) | ConsumerGroupState::Stable => {
                // If the consumer is not waiting for the current epoch to start
                // start a new one
                if epoch.is_none_or(|epoch| group.epoch != epoch) {
                    group.start_epoch();
                }
            }
            ConsumerGroupState::Empty => {
                group.start_epoch();
            }
        }

        group.add_consumer(consumer_id.to_string())?;

        Ok(group.epoch)
    }
}
