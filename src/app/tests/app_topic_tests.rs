use crate::{
    app::{App, AppLock},
    commands::create_topic::CreateTopic,
    config::Config,
};

#[tokio::test]
async fn test_create_topics() {
    let config = Config::default();
    let app = App::new(AppLock::new(config));

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
