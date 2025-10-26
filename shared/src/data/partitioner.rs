use bytes::Bytes;
use murmur2::murmur64ane;

pub enum Partitioner {
    /// Partition using defauilt hash
    Default(u64),
    /// Use a static partition
    Static(u64),
}

impl Partitioner {
    pub fn select_partition(&self, key: impl Into<Bytes>) -> u64 {
        match self {
            Partitioner::Default(num_partitions) => murmur64ane(&key.into(), 0) % num_partitions,
            Partitioner::Static(partition) => *partition,
        }
    }
}
