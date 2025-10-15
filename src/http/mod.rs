mod app_error;
pub mod responses;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use app_error::{AppError, AppResult};
use axum::extract::{Path, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use responses::create_topic_response::CreateTopicResponse;
use responses::produce_response::ProduceResponse;
use responses::record_response::FetchResponse;
use tokio::net::TcpListener;
use tokio::select;
use tokio::time::{self, Instant};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::info;

use crate::app::App;
use crate::commands::create_topic::CreateTopic;
use crate::commands::fetch::Fetch;
use crate::commands::produce::Produce;
use crate::data::encoding;
use crate::data::identifier::Identifier;
use crate::data::record::{Record, RecordHeader};
use crate::data::record_batch::RecordBatch;
use crate::data::state::topic_state::TopicState;

pub struct HttpServer {
    router: Router,
    address: String,
}

async fn create_topic(
    State(app): State<App>,
    Json(create_topic): Json<CreateTopic>,
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
    Json(produce): Json<Produce>,
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

async fn fetch(State(app): State<App>, Json(fetch): Json<Fetch>) -> AppResult<FetchResponse> {
    let until = Instant::now() + Duration::from_millis(fetch.timeout_ms);
    let mut batch = RecordBatch::new(fetch.min_bytes);

    let lock = app.read().await;

    for topic in &fetch.topics {
        for partition in &topic.partitions {
            let mut offset = partition.offset;
            while let Some(record) = lock.read(&topic.identifier, partition.id, &offset).await? {
                let record_offset = record.offset;
                batch.push(Arc::new(record));

                match offset.narrow(record_offset) {
                    Some(next) => offset = next,
                    None => break,
                }

                if batch.is_full() {
                    return Ok(Json(batch.into_response(&fetch.encoding)?));
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
                    yield record;
                }
            }
        }) as Pin<Box<dyn Stream<Item = Arc<Record>> + Send>>;

        map.insert(topic_id, rx);
    }

    drop(lock);

    loop {
        select! {
             _ = time::sleep_until(until) => return Ok(Json(batch.into_response(&fetch.encoding)?)),
            record = map.next() => {
                if let Some((_, record)) = record {
                    batch.push(record);

                    if batch.is_full() {
                        return Ok(Json(batch.into_response(&fetch.encoding)?));
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
