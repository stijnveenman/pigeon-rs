use reqwest::{Client, IntoUrl, Url};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::data::state::topic_state::TopicState;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to parse URL")]
    UrlParseError,
    #[error("HTTP Error")]
    HttpError(#[from] reqwest::Error),
    #[error("Invalid JSON Response")]
    InvalidJsonResponse,
}

pub struct HttpClient {
    base_url: Url,
    client: Client,
}

impl HttpClient {
    pub fn new(base_url: impl IntoUrl) -> Result<Self, reqwest::Error> {
        Ok(Self {
            base_url: base_url.into_url()?,
            client: Client::new(),
        })
    }

    fn get_url(&self, url: &str) -> Result<Url, Error> {
        self.base_url.join(url).map_err(|_| Error::UrlParseError)
    }

    async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, Error> {
        let url = self.get_url(url)?;

        let response = self
            .client
            .get(url)
            .send()
            .await?
            .json()
            .await
            .map_err(|_| Error::InvalidJsonResponse)?;

        Ok(response)
    }

    pub async fn get_topic(&self, name: &str) -> Result<TopicState, Error> {
        let url = format!("/topics/{}/state", name);

        let topic = self.get_json(&url).await?;

        Ok(topic)
    }
}
