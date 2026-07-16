//! Error types and their HTTP mappings.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Top-level error type for the server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("subscription not found")]
    NotFound,

    #[error("payload too large: {0} bytes (max {1})")]
    PayloadTooLarge(usize, usize),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("foundationdb error: {0}")]
    Fdb(#[from] foundationdb::FdbError),

    #[error("foundationdb transaction error: {0}")]
    FdbTxn(#[from] foundationdb::FdbBindingError),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::PayloadTooLarge(..) => StatusCode::PAYLOAD_TOO_LARGE,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::Fdb(_) | Error::FdbTxn(_) | Error::Serde(_) | Error::Other(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        // 5xx details are logged, not leaked to the caller.
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        }
        (status, self.to_string()).into_response()
    }
}
