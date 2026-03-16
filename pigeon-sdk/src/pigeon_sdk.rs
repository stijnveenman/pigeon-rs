use pigeon_core::{
    PError,
    record::Record,
    rpc::{self, create_topic::CreateTopic},
};
use reqwest::{Client, ClientBuilder, Method, Request, RequestBuilder, Url};
use serde::de::DeserializeOwned;

pub struct PigeonSdk {
    pub base_url: Url,
    client: Client,
}

impl PigeonSdk {
    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.client
            .request(method, self.base_url.join(path).unwrap())
    }

    fn get(&self, path: &str) -> RequestBuilder {
        self.request(Method::GET, path)
    }

    fn post(&self, path: &str) -> RequestBuilder {
        self.request(Method::POST, path)
    }

    async fn execute<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T, PError> {
        let response = self
            .client
            .execute(request.build().map_err(|_| PError::TransportFailure)?)
            .await
            .map_err(|_| PError::TransportFailure)?;

        match response.status().is_success() {
            true => response.json().await.map_err(|_| PError::TransportFailure),
            false => Err(response
                .json::<rpc::Error>()
                .await
                .map_err(|_| PError::TransportFailure)?
                .error),
        }
    }

    pub fn new(base_url: impl Into<String>) -> Self {
        let mut base_url = base_url.into();
        if !base_url.ends_with("/") {
            base_url += "/";
        }

        Self {
            base_url: Url::parse(&base_url).unwrap(),
            client: ClientBuilder::new()
                .user_agent("PigeonSdk")
                .build()
                .unwrap(),
        }
    }

    pub async fn read_record(
        &self,
        topic_name: &str,
        partition_id: u64,
        offset: u64,
    ) -> Result<Record, PError> {
        let request = self.get(&format!("/topics/{topic_name}/{partition_id}/{offset}"));

        self.execute(request).await
    }

    pub async fn create_topic(
        &self,
        topic_name: impl Into<String>,
        num_partitions: Option<u64>,
    ) -> Result<(), PError> {
        let request = self.post("/topics").json(&CreateTopic {
            topic_name: topic_name.into(),
            num_partitions,
        });

        self.execute(request).await
    }
}
