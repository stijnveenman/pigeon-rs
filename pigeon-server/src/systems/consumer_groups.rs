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
        self.state = ConsumerGroupState::Rebalancing(Utc::now());
        self.epoch += 1;
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

    pub fn join_consumer_group(&self, group_id: &str, consumer_id: &str) -> Result<usize, PError> {
        let mut group = self.get_group(group_id)?;

        if group.consumers.contains_key(consumer_id) {
            return Err(PError::ConsumerAlreadyInGroup);
        }

        match group.state {
            ConsumerGroupState::Rebalancing(_) => {
                group.consumers.insert(
                    consumer_id.to_string(),
                    ConsumerClient {
                        last_seen: Utc::now(),
                    },
                );
            }
            // Start a new epoch
            ConsumerGroupState::Empty | ConsumerGroupState::Stable => {
                group.start_epoch();
                group.consumers = HashMap::from([(
                    consumer_id.to_string(),
                    ConsumerClient {
                        last_seen: Utc::now(),
                    },
                )])
            }
        }

        Ok(group.epoch)
    }
}
