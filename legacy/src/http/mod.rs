mod app_error;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use app_error::{AppError, AppResult};
use axum::extract::{Path, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use shared::commands::create_topic_command::CreateTopicCommand;
use shared::commands::fetch_command::FetchCommand;
use shared::commands::produce_command::ProduceCommand;
use shared::data::encoding;
use shared::data::identifier::Identifier;
use shared::response::create_topic_response::CreateTopicResponse;
use shared::response::produce_response::ProduceResponse;
use shared::response::record_response::FetchResponse;
use shared::state::topic_state::TopicState;
use tokio::net::TcpListener;
use tokio::select;
use tokio::time::{self, Instant};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::info;

use crate::app::App;
use crate::data::record::{Record, RecordHeader};

pub struct HttpServer {
    router: Router,
    address: String,
}

async fn create_topic(
    State(app): State<App>,
    Json(create_topic): Json<CreateTopicCommand>,
) -> AppResult<CreateTopicResponse> {
    let mut lock = app.write().await;

    let topic_id = lock
        .create_topic(
            create_topic.topic_id,
            &create_topic.name,
            create_topic.partitions,
        )
        .await?;

    Ok(Json(CreateTopicResponse { topic_id }))
}

async fn produce(
    State(app): State<App>,
    Json(produce): Json<ProduceCommand>,
) -> AppResult<ProduceResponse> {
    let mut lock = app.write().await;

    let key = produce.encoding.decode(&produce.key)?;
    let value = produce.encoding.decode(&produce.value)?;
    let headers = produce
        .headers
        .unwrap_or(vec![])
        .iter()
        .map(|header| {
            Ok(RecordHeader {
                key: header.key.to_string(),
                value: produce.encoding.decode(&header.value)?,
            })
        })
        .collect::<Result<_, encoding::Error>>()?;

    let offset = lock
        .produce(produce.topic, produce.partition_id, key, value, headers)
        .await?;

    Ok(Json(ProduceResponse { offset }))
}

async fn get_topic_state(
    State(app): State<App>,
    Path(name): Path<String>,
) -> AppResult<TopicState> {
    let lock = app.read().await;

    let state = lock.get_topic_by_name(&name)?.state();

    Ok(Json(state))
}

async fn get_all_topics_state(State(app): State<App>) -> AppResult<HashMap<u64, TopicState>> {
    let lock = app.read().await;

    Ok(Json(lock.topic_states()))
}

async fn delete_topic(State(app): State<App>, Path(name): Path<String>) -> Result<(), AppError> {
    let mut lock = app.write().await;

    lock.delete_topic(&Identifier::Name(name)).await?;

    Ok(())
}

async fn fetch(
    State(app): State<App>,
    Json(fetch): Json<FetchCommand>,
) -> AppResult<FetchResponse> {
    let until = Instant::now() + Duration::from_millis(fetch.timeout_ms);
    let mut response = FetchResponse::from(&fetch);

    let lock = app.read().await;

    for topic in &fetch.topics {
        let topic_id = lock.get_topic(&topic.identifier)?.id();
        for partition in &topic.partitions {
            let mut offset = partition.offset;
            while let Some(record) = lock.read(&topic.identifier, partition.id, &offset).await? {
                let record_offset = record.offset;
                response.push(
                    record.into_response(&fetch.encoding, topic_id, partition.id)?,
                    record.size(),
                );

                match offset.narrow(record_offset) {
                    Some(next) => offset = next,
                    None => break,
                }

                if response.total_size > fetch.min_bytes {
                    return Ok(Json(response));
                }
            }
        }
    }

    drop(lock);

    let mut lock = app.write().await;
    let mut map = StreamMap::new();

    for topic in &fetch.topics {
        let topic_id = lock.get_topic(&topic.identifier)?.id();
        let mut rx = lock.subscribe(&topic.identifier)?;

        let rx = Box::pin(async_stream::stream! {
            while let Ok((partition_id, record)) = rx.recv().await {
                let Some(partition) = topic.partitions.iter().find(|p| p.id == partition_id) else {
                    continue;
                };

                if partition.offset.matches(record.offset) {
                    yield (partition_id, record);
                }
            }
        }) as Pin<Box<dyn Stream<Item = (u64, Arc<Record>)> + Send>>;

        map.insert(topic_id, rx);
    }

    drop(lock);

    loop {
        select! {
             _ = time::sleep_until(until) => return Ok(Json(response)),
            record = map.next() => {
                if let Some((topic_id, (partition_id, record))) = record {
                    response.push(record.into_response(&fetch.encoding, topic_id, partition_id)?, record.size());

                    if response.total_size > fetch.min_bytes {
                        return Ok(Json(response));
                    }
                }
            }
        };
    }
}

impl HttpServer {
    pub fn new(host: &str, port: u16, app: App) -> Self {
        let router = Router::new()
            .route("/topics", post(create_topic))
            .route("/topics", get(get_all_topics_state))
            .route("/topics/{name}/state", get(get_topic_state))
            .route("/topics/{name}", delete(delete_topic))
            .route("/topics/records", post(produce))
            .route("/topics/records", get(fetch))
            .with_state(app);

        let address = format!("{}:{}", host, port);

        HttpServer { router, address }
    }

    pub async fn serve(self) -> tokio::io::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;

        info!("Starting listener on {}", &self.address);

        axum::serve(listener, self.router)
            .await
            .expect("axum::serve failed");

        Ok(())
    }
}
