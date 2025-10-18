use std::collections::HashMap;

use reqwest::{Client, IntoUrl, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{data::state::topic_state::TopicState, http::responses::error_response::ErrorResponse};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to parse URL")]
    UrlParseError,
    #[error("HTTP Error")]
    HttpError(#[from] reqwest::Error),
    #[error("Invalid JSON Response")]
    InvalidJsonResponse,
    #[error("Invalid response {0} {1}")]
    ErrorResponse(StatusCode, String),
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

    async fn delete(&self, url: &str) -> Result<(), Error> {
        let url = self.get_url(url)?;

        let response = self.client.delete(url).send().await?;

        self.get_unit_response(response).await
    }

    async fn get<T: DeserializeOwned>(&self, url: &str) -> Result<T, Error> {
        let url = self.get_url(url)?;

        let response = self.client.get(url).send().await?;

        self.get_response(response).await
    }

    async fn get_unit_response(&self, response: Response) -> Result<(), Error> {
        match response.status() {
            StatusCode::OK => Ok(()),
            status => {
                let error_response = response
                    .json::<ErrorResponse>()
                    .await
                    .map_err(|_| Error::InvalidJsonResponse)?;

                Err(Error::ErrorResponse(status, error_response.error))
            }
        }
    }

    async fn get_response<T: DeserializeOwned>(&self, response: Response) -> Result<T, Error> {
        match response.status() {
            StatusCode::OK => Ok(response
                .json()
                .await
                .map_err(|_| Error::InvalidJsonResponse)?),
            status => {
                let error_response = response
                    .json::<ErrorResponse>()
                    .await
                    .map_err(|_| Error::InvalidJsonResponse)?;

                Err(Error::ErrorResponse(status, error_response.error))
            }
        }
    }

    pub async fn get_topic(&self, name: &str) -> Result<TopicState, Error> {
        self.get(&format!("/topics/{}/state", name)).await
    }

    pub async fn get_topics(&self) -> Result<HashMap<u64, TopicState>, Error> {
        self.get("/topics").await
    }

    pub async fn delete_topic(&self, name: &str) -> Result<(), Error> {
        self.delete(&format!("/topics/{}", name)).await
    }
}
