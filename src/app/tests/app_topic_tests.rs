use crate::{
    app::error::Error,
    app::{App, AppLock},
    commands::create_topic::CreateTopic,
    config::Config,
    data::{record::Record, timestamp::Timestamp},
};

#[tokio::test]
async fn test_create_topics() {
    let config = Config::default();
    let app = App::load_from_disk(config)
        .await
        .expect("load_from_disk failed");

    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(Some(1), "foo")
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let topic_id = lock
        .create_topic(None, "bar")
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 2);
}

#[tokio::test]
async fn test_create_topic_and_append() {
    let config = Config::default();
    let app = App::load_from_disk(config)
        .await
        .expect("load_from_disk failed");

    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(None, "foo")
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let offset = lock
        .produce(
            topic_id,
            1,
            Record {
                offset: 0,
                timestamp: Timestamp::now(),
                headers: vec![],
                key: "Hello".into(),
                value: "World".into(),
            },
        )
        .await
        .expect("Failed to produce record");
    assert_eq!(offset, 0);
}

#[tokio::test]
async fn test_cannot_create_same_id_twice() {
    let config = Config::default();
    let app = App::load_from_disk(config)
        .await
        .expect("load_from_disk failed");

    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(Some(1), "foo")
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let result = lock.create_topic(Some(1), "bar").await;
    assert!(
        matches!(result, Err(Error::TopicIdInUse(1))),
        "Expected Error::TopicIdInUse(1) but got {result:?}"
    );
}

#[tokio::test]
async fn test_cannot_create_same_name_twice() {
    let config = Config::default();
    let app = App::load_from_disk(config)
        .await
        .expect("load_from_disk failed");

    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(None, "foo")
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let result = lock.create_topic(None, "foo").await;
    assert!(matches!(result, Err(Error::TopicNameInUse(_))));
}
