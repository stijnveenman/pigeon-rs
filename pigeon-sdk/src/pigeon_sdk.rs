use pigeon_core::{PError, record::Record, rpc};
use reqwest::{Client, ClientBuilder, Method, Request, Url};
use serde::de::DeserializeOwned;

pub struct PigeonSdk {
    pub base_url: Url,
    client: Client,
}

impl PigeonSdk {
    fn request(&self, method: Method, path: &str) -> Request {
        Request::new(method, self.base_url.join(path).unwrap())
    }

    fn get(&self, path: &str) -> Request {
        self.request(Method::GET, path)
    }

    async fn execute<T: DeserializeOwned>(&self, request: Request) -> Result<T, PError> {
        let response = self
            .client
            .execute(request)
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
}
