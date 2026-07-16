//! Writer role — the ntfy publish surface (application-server-facing).
//!
//! Stateless: validate, run one `publish` transaction, return the message. It
//! never talks to a pusher directly; the poke left in the target node's inbox is
//! the only handoff, and even that is best-effort.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde::Deserialize;
use serde_json::json;

use crate::AppState;
use crate::error::{Error, Result};
use crate::ids;
use crate::model::Envelope;
use crate::protocol::NtfyMessage;

/// Publish query parameters (ntfy-compatible subset; unknown params ignored).
#[derive(Debug, Default, Deserialize)]
pub struct PublishParams {
    #[serde(default)]
    up: Option<String>,
    #[serde(default)]
    unifiedpush: Option<String>,
}

/// `POST`/`PUT /{topic}` — publish a message to a topic (ntfy semantics).
///
/// * `400` if the topic name is invalid.
/// * `413` if the body exceeds the configured maximum (4096, per ntfy/UnifiedPush).
/// * `200` with the ntfy message JSON once persisted (and poked, if subscribed).
pub async fn publish(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    Query(params): Query<PublishParams>,
    req_headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    if !ids::valid_topic(&topic) {
        return Err(Error::BadRequest("invalid topic".into()));
    }
    let max = state.config.max_message_bytes;
    if body.len() > max {
        return Err(Error::PayloadTooLarge(body.len(), max));
    }

    let up = is_unifiedpush(&params, &req_headers);
    let (message, encoding) = encode_body(&body, up);

    let now = now_secs();
    let envelope = Envelope {
        message,
        encoding,
        // In raw UnifiedPush mode ntfy ignores title/priority/tags. Outside it
        // (e.g. a manual `curl` for testing), forward them for a nicer message.
        title: if up { None } else { header_str(&req_headers, "x-title").or_else(|| header_str(&req_headers, "title")) },
        priority: if up { None } else { header_str(&req_headers, "x-priority").and_then(|v| v.parse().ok()) },
        tags: if up { Vec::new() } else { parse_tags(&req_headers) },
        received_at_secs: now,
        expiry_secs: now.saturating_add(state.config.default_ttl_secs),
    };

    state.store.publish(&topic, &envelope).await?;
    tracing::debug!(topic = short(&topic), bytes = body.len(), up, "published message");

    // ntfy returns the published message. The id here is informational — the
    // durable, resumable id is the queue versionstamp seen by subscribers.
    let msg = NtfyMessage::message(&topic, ids::ephemeral_id(), &envelope);
    Ok(Json(msg).into_response())
}

/// `GET /{topic}` — with `?up=1`, the UnifiedPush endpoint check ntfy answers
/// with `{"unifiedpush":{"version":1}}`. Without it, there's no web UI: 404.
pub async fn topic_get(
    State(_state): State<AppState>,
    Path(topic): Path<String>,
    Query(params): Query<PublishParams>,
) -> Response {
    if !ids::valid_topic(&topic) {
        return (StatusCode::BAD_REQUEST, "invalid topic").into_response();
    }
    if truthy(params.up.as_deref()) || truthy(params.unifiedpush.as_deref()) {
        return Json(json!({ "unifiedpush": { "version": 1 } })).into_response();
    }
    (StatusCode::NOT_FOUND, "no web UI; use /{topic}/ws, /json or /sse").into_response()
}

/// UnifiedPush is signalled by `?up=1`/`?unifiedpush=1`, `X-UnifiedPush: 1`, or a
/// `Content-Encoding: aes128gcm` body (the RFC 8291 encrypted WebPush encoding).
fn is_unifiedpush(params: &PublishParams, headers: &HeaderMap) -> bool {
    truthy(params.up.as_deref())
        || truthy(params.unifiedpush.as_deref())
        || truthy(header_str(headers, "x-unifiedpush").as_deref())
        || header_str(headers, "content-encoding").as_deref() == Some("aes128gcm")
}

/// ntfy's body auto-detection: UTF-8 bodies pass through as text; binary bodies
/// become base64 with `encoding = "base64"`. Non-UP text is trimmed like ntfy.
fn encode_body(body: &[u8], up: bool) -> (String, String) {
    match std::str::from_utf8(body) {
        Ok(s) if up => (s.to_string(), String::new()),
        Ok(s) => (s.trim().to_string(), String::new()),
        Err(_) => (B64.encode(body), "base64".to_string()),
    }
}

fn parse_tags(headers: &HeaderMap) -> Vec<String> {
    header_str(headers, "x-tags")
        .or_else(|| header_str(headers, "tags"))
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn truthy(v: Option<&str>) -> bool {
    matches!(
        v.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on" | "")
    )
}

fn short(topic: &str) -> &str {
    &topic[..topic.len().min(8)]
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
