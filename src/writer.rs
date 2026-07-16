//! Writer role — `POST /push/{token}`, the application-server-facing edge.
//!
//! Entirely stateless: it validates the request and runs one FDB transaction
//! ([`Store::ingest`]). It never talks to a pusher directly; the poke it leaves
//! in the target node's inbox is the only handoff, and even that is best-effort.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::AppState;
use crate::error::{Error, Result};
use crate::model::MessageHeaders;
use crate::store::Ingest;

/// Handle an incoming push message for the subscription named by `token`.
///
/// * `404` if the token has no subscription.
/// * `413` if the body exceeds the configured maximum (UnifiedPush: 4096).
/// * `201` once persisted (RFC 8030); the `TTL` header is echoed when supplied.
pub async fn push(
    State(state): State<AppState>,
    Path(token): Path<String>,
    req_headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let max = state.config.max_message_bytes;
    if body.len() > max {
        return Err(Error::PayloadTooLarge(body.len(), max));
    }

    let headers = extract_headers(&req_headers);
    let echo_ttl = headers.ttl;
    let now = now_secs();

    let outcome = state
        .store
        .ingest(&token, &body, headers, now, state.config.default_ttl_secs)
        .await?;

    match outcome {
        Ingest::NotFound => Err(Error::NotFound),
        Ingest::Accepted => {
            tracing::debug!(%token, bytes = body.len(), "accepted push message");
            let mut response = StatusCode::CREATED.into_response();
            if let Some(ttl) = echo_ttl {
                if let Ok(v) = HeaderValue::from_str(&ttl.to_string()) {
                    response.headers_mut().insert("ttl", v);
                }
            }
            Ok(response)
        }
    }
}

/// Extract the WebPush headers we forward (RFC 8030 §5). Lenient: malformed
/// values are dropped rather than rejected.
fn extract_headers(headers: &HeaderMap) -> MessageHeaders {
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    MessageHeaders {
        ttl: get("ttl").and_then(|v| v.parse().ok()),
        topic: get("topic"),
        urgency: get("urgency"),
        content_encoding: get("content-encoding"),
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
