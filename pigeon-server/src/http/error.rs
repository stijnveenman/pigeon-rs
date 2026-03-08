use axum::{Json, http::StatusCode, response::IntoResponse};
use pigeon_core::PError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct HttpError {
    error: PError,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
    }
}

impl From<PError> for HttpError {
    fn from(value: PError) -> Self {
        HttpError { error: value }
    }
}

pub type HttpResult<T> = Result<Json<T>, HttpError>;
