use bytes::Bytes;

pub struct Produce {
    pub topic_id: u64,
    pub partition_id: u64,
    pub key: Bytes,
    pub value: Bytes,
    // TODO: support headers
}
