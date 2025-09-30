// TODO: can we specify multiple "test levels"
use std::sync::Arc;

use rand::{
    distr::{Alphanumeric, SampleString},
    rngs::SmallRng,
    Rng, SeedableRng,
};

use crate::{
    config::Config,
    data::{
        record::{Record, RecordHeader},
        timestamp::Timestamp,
    },
    dur::topic::Topic,
};

#[tokio::test]
async fn single_topic_random_test() {
    let count = 1000;
    let mut config = Config::default();
    config.segment.size = 1024 * 5;
    let config = Arc::new(config);

    let mut random = SmallRng::seed_from_u64(54323409);

    let mut topic = Topic::load_from_disk(config.clone(), 0)
        .await
        .expect("Failed to create topic");

    for i in 0..count {
        for p in 0..config.topic.num_partitions {
            topic
                .append(
                    p,
                    Record {
                        offset: 0,
                        timestamp: Timestamp::from(random.random::<u64>()),
                        key: format!("{}:{}", p, i).into(),
                        value: Alphanumeric.sample_string(&mut random, 16).into(),
                        headers: vec![RecordHeader {
                            key: Alphanumeric.sample_string(&mut random, 16),
                            value: Alphanumeric.sample_string(&mut random, 16).into(),
                        }],
                    },
                )
                .await
                .expect("Failed to append record to topic");
        }
    }

    for partition in &topic.partitions {
        assert!(
            partition.segments.len() > 2,
            "Test should generate at least 2 segments of messages, got: {}",
            partition.segments.len()
        );
    }
    // TODO: start asserting on generated data
}
