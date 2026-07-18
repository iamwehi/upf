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

/// First few chars of a topic, for compact log lines. Truncates on a *char*
/// boundary — a byte-index slice would panic on a multibyte char (topics are
/// validated ASCII before publish, but this helper must not depend on that).
fn short(topic: &str) -> &str {
    match topic.char_indices().nth(8) {
        Some((idx, _)) => &topic[..idx],
        None => topic,
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use proptest::prelude::*;

    // ---- encode_body: the ntfy body auto-detection contract -----------------

    proptest! {
        /// Any valid UTF-8 body passes through as text (never base64). In raw
        /// UnifiedPush mode it is byte-for-byte; otherwise it is trimmed like ntfy.
        #[test]
        fn encode_body_utf8_passes_through(s in ".*", up: bool) {
            let (message, encoding) = encode_body(s.as_bytes(), up);
            prop_assert_eq!(&encoding, "");
            if up {
                prop_assert_eq!(message, s);
            } else {
                prop_assert_eq!(message, s.trim());
            }
        }

        /// The reverse contract: a body is base64-encoded **iff** it is not valid
        /// UTF-8, and that base64 decodes back to the exact original bytes — so a
        /// binary WebPush payload survives publish → delivery intact.
        #[test]
        fn encode_body_binary_round_trips(bytes in any::<Vec<u8>>(), up: bool) {
            let (message, encoding) = encode_body(&bytes, up);
            if std::str::from_utf8(&bytes).is_ok() {
                prop_assert_eq!(&encoding, "");
            } else {
                prop_assert_eq!(&encoding, "base64");
                prop_assert_eq!(B64.decode(&message).unwrap(), bytes);
            }
        }
    }

    // ---- UnifiedPush detection & truthiness ---------------------------------

    fn header(name: &'static str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(name, value.parse().unwrap());
        h
    }

    #[test]
    fn truthy_recognizes_ntfy_flag_values() {
        // Present-but-empty (`?up`) counts as true, as do the usual spellings,
        // case-insensitively and after trimming.
        for v in ["1", "true", "yes", "on", "", "TRUE", " Yes "] {
            assert!(truthy(Some(v)), "{v:?} should be truthy");
        }
        for v in ["0", "false", "no", "off", "nope", "2"] {
            assert!(!truthy(Some(v)), "{v:?} should be falsy");
        }
        assert!(!truthy(None));
    }

    #[test]
    fn unifiedpush_detected_from_param_header_or_encoding() {
        let no_params = PublishParams::default();

        // Nothing set → not UnifiedPush.
        assert!(!is_unifiedpush(&no_params, &HeaderMap::new()));

        // `?up=1` query param.
        let up_param = PublishParams {
            up: Some("1".into()),
            unifiedpush: None,
        };
        assert!(is_unifiedpush(&up_param, &HeaderMap::new()));

        // `X-UnifiedPush: 1` header.
        assert!(is_unifiedpush(&no_params, &header("x-unifiedpush", "1")));

        // The RFC 8291 encrypted WebPush content-encoding.
        assert!(is_unifiedpush(&no_params, &header("content-encoding", "aes128gcm")));

        // A different content-encoding is not a UnifiedPush signal.
        assert!(!is_unifiedpush(&no_params, &header("content-encoding", "gzip")));
    }

    // ---- tag parsing --------------------------------------------------------

    #[test]
    fn parse_tags_splits_trims_and_drops_empties() {
        assert_eq!(parse_tags(&header("x-tags", "a, b ,,c")), vec!["a", "b", "c"]);
        assert_eq!(parse_tags(&header("tags", "x")), vec!["x"]); // fallback header
        assert_eq!(parse_tags(&HeaderMap::new()), Vec::<String>::new());
    }

    // ---- short(): the log-prefix helper -------------------------------------

    proptest! {
        /// `short` truncates a topic for logging. It must never panic on arbitrary
        /// input — a byte-index slice would split a multibyte char and crash.
        #[test]
        fn short_never_panics(s in ".*") {
            let out = short(&s);
            prop_assert!(s.starts_with(out));
            prop_assert!(out.chars().count() <= 8);
        }
    }
}
