use tracing::debug;

use super::{Db, DbErr};

#[derive(Debug)]
pub struct Topic {
    partitions: Vec<Partition>,
}

#[derive(Debug, Default)]
pub struct Partition {}

impl Topic {
    pub(crate) fn new(partitions: u64) -> Topic {
        Topic {
            partitions: (0..partitions).map(|_| Partition::default()).collect(),
        }
    }
}

impl Db {
    pub fn create_topic(&mut self, name: String, partitions: u64) -> Result<(), DbErr> {
        let mut state = self.shared.lock().unwrap();

        if state.topics.contains_key(&name) {
            return Err(DbErr::NameInUse);
        }

        debug!("creating topic with name {}", &name);
        state.topics.insert(name, Topic::new(partitions));

        Ok(())
    }
}
