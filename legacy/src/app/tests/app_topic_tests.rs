use shared::data::identifier::Identifier;

use crate::{
    app::{error::Error, App},
    config::Config,
};

#[tokio::test]
async fn test_create_topics() {
    let config = Config::default();
    let app = App::load_from_disk(config)
        .await
        .expect("load_from_disk failed");

    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(Some(1), "foo", None)
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let topic_id = lock
        .create_topic(None, "bar", None)
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
        .create_topic(None, "foo", None)
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let offset = lock
        .produce(
            Identifier::Id(topic_id),
            1,
            "Hello".into(),
            "World".into(),
            vec![],
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
        .create_topic(Some(1), "foo", None)
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let result = lock.create_topic(Some(1), "bar", None).await;
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
        .create_topic(None, "foo", None)
        .await
        .expect("Failed to create_topic");
    assert_eq!(topic_id, 1);

    let result = lock.create_topic(None, "foo", None).await;
    assert!(matches!(result, Err(Error::TopicNameInUse(_))));
}

#[tokio::test]
async fn test_cannot_produce_on_internal_topics() {
    let config = Config::default();
    let app = App::load_from_disk(config)
        .await
        .expect("load_from_disk failed");

    let mut lock = app.write().await;

    let result = lock
        .produce(
            Identifier::Name("__metadata".to_string()),
            0,
            "Hello".into(),
            "World".into(),
            vec![],
        )
        .await;
    assert!(matches!(result, Err(Error::InternalTopicName(_))));
}
