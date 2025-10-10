use std::sync::Arc;

use rand::{
    distr::{Alphanumeric, SampleString},
    rngs::SmallRng,
    SeedableRng,
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
#[ignore]
async fn single_topic_random_test() {
    let count = 1000;
    let mut config = Config::default();
    config.segment.size = 1024 * 5;
    let config = Arc::new(config);

    let mut random = SmallRng::seed_from_u64(54323409);

    let mut topic = Topic::load_from_disk(config.clone(), 0, "foo")
        .await
        .expect("Failed to create topic");

    let time = Timestamp::now();

    for i in 0..count {
        for p in 0..config.topic.num_partitions {
            topic
                .append(
                    p,
                    Record {
                        offset: 0,
                        timestamp: time,
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

    for i in 0..count {
        for p in 0..config.topic.num_partitions {
            let record = topic
                .read_exact(p, i)
                .await
                .expect("Failed to read message");

            assert_eq!(record.offset, i);
            assert_eq!(record.timestamp, time);
            assert_eq!(record.key, format!("{}:{}", p, i));
            assert!(record.value.len() == 16);
            assert!(record.headers.len() == 1);
        }
    }
}
