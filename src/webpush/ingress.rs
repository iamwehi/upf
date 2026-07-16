//! `POST /push/{token}` — accept a WebPush message and forward it.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

use crate::AppState;
use crate::distributor::ws::message_frame;
use crate::error::{Error, Result};
use crate::storage::QueuedMessage;
use crate::token;
use crate::webpush::headers;

/// Handle an incoming push message for the subscription named by `token`.
///
/// * `404` if the token has no subscription.
/// * `413` if the body exceeds the configured maximum (UnifiedPush: 4096).
/// * `201` once persisted (and forwarded, if the distributor is connected).
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

    // Resolve the subscription to learn which distributor to route to.
    let sub = state
        .storage
        .get_subscription(&token)
        .await?
        .ok_or(Error::NotFound)?;

    let msg = QueuedMessage {
        msg_id: token::new_message_id(),
        body_b64: B64.encode(&body),
        headers: headers::extract(&req_headers),
        received_at_micros: now_micros(),
    };

    // Persist first (durability / offline replay), then best-effort push.
    state
        .storage
        .enqueue(&token, &sub.distributor_id, &msg)
        .await?;

    let delivered = state
        .registry
        .try_send(&sub.distributor_id, message_frame(&token, msg.clone()));

    tracing::debug!(
        %token,
        distributor = %sub.distributor_id,
        msg_id = %msg.msg_id,
        delivered,
        "accepted push message"
    );

    // RFC 8030: 201 Created (empty body). TTL is echoed when the caller set one.
    let mut response = StatusCode::CREATED.into_response();
    if let Some(ttl) = msg.headers.ttl {
        if let Ok(v) = HeaderValue::from_str(&ttl.to_string()) {
            response.headers_mut().insert("ttl", v);
        }
    }
    Ok(response)
}

fn now_micros() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0)
}
