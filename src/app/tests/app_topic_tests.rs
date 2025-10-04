use crate::{
    app::{App, AppLock},
    commands::{create_topic::CreateTopic, produce::Produce},
    config::Config,
};

#[tokio::test]
async fn test_create_topics() {
    let config = Config::default();
    let app = App::new(config);

    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(CreateTopic {})
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 0);

    let topic_id = lock
        .create_topic(CreateTopic {})
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);
}

#[tokio::test]
async fn test_create_topic_and_append() {
    let config = Config::default();
    let app = App::new(config);

    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(CreateTopic {})
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 0);

    let offset = lock
        .produce(Produce {
            topic_id,
            partition_id: 1,
            key: "Hello".into(),
            value: "World".into(),
        })
        .await
        .expect("Failed to produce record");
    assert_eq!(offset, 0);
}
