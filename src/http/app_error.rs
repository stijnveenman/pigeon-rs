use axum::{http::StatusCode, response::IntoResponse, Json};

use crate::{
    app::{self},
    data::encoding,
};

use super::responses::error_response::ErrorResponse;

#[derive(Debug)]
pub struct AppError(app::error::Error);

impl From<app::error::Error> for AppError {
    fn from(value: app::error::Error) -> Self {
        AppError(value)
    }
}

impl From<encoding::Error> for AppError {
    fn from(value: encoding::Error) -> Self {
        AppError(value.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self.0 {
            app::error::Error::Durrability(error) => match error {
                crate::dur::error::Error::UnderlyingIO(error) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                }
                crate::dur::error::Error::OffsetNotFound => {
                    (StatusCode::BAD_REQUEST, error.to_string())
                }
                crate::dur::error::Error::SegmentFull => {
                    (StatusCode::BAD_REQUEST, error.to_string())
                }
                crate::dur::error::Error::PartitionNotFound => {
                    (StatusCode::BAD_REQUEST, error.to_string())
                }
                crate::dur::error::Error::InvalidLogFilename(_) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                }
            },
            app::error::Error::TopicIdNotFound(_) => (StatusCode::BAD_REQUEST, self.0.to_string()),
            app::error::Error::MaxTopicIdReached => (StatusCode::BAD_REQUEST, self.0.to_string()),
            app::error::Error::TopicIdInUse(_) => (StatusCode::BAD_REQUEST, self.0.to_string()),
            app::error::Error::TopicNameInUse(_) => (StatusCode::BAD_REQUEST, self.0.to_string()),
            app::error::Error::TopicNameNotFound(_) => {
                (StatusCode::BAD_REQUEST, self.0.to_string())
            }
            app::error::Error::InternalTopicName(_) => {
                (StatusCode::BAD_REQUEST, self.0.to_string())
            }
            app::error::Error::EncodingError(_) => (StatusCode::BAD_REQUEST, self.0.to_string()),
        };

        (
            status,
            Json(ErrorResponse {
                error: message,
                status: status.as_u16(),
            }),
        )
            .into_response()
    }
}

pub type AppResult<T> = Result<Json<T>, AppError>;
