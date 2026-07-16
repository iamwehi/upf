//! The ntfy subscription wire format (server → subscriber).
//!
//! We speak ntfy's protocol so real UnifiedPush distributors (the ntfy Android
//! app, and anything else that subscribes to ntfy) work against UPF unmodified.
//! Field names and the `encoding` convention mirror ntfy's `model.Message`.

use serde::{Deserialize, Serialize};

use crate::model::Envelope;

pub const EVENT_OPEN: &str = "open";
pub const EVENT_KEEPALIVE: &str = "keepalive";
pub const EVENT_MESSAGE: &str = "message";

/// One line/frame delivered to a subscriber. Matches ntfy's JSON message object;
/// `open` and `keepalive` are control frames carrying no body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NtfyMessage {
    pub id: String,
    pub time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<i64>,
    pub event: String,
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// `""` (omitted) for UTF-8, `"base64"` for a binary `message`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub encoding: String,
}

impl NtfyMessage {
    /// The `open` control frame, sent once when a subscription is established.
    pub fn open(topic: &str, id: String, time: i64) -> Self {
        Self::control(EVENT_OPEN, topic, id, time)
    }

    /// A `keepalive` control frame, sent periodically to hold the connection.
    pub fn keepalive(topic: &str, id: String, time: i64) -> Self {
        Self::control(EVENT_KEEPALIVE, topic, id, time)
    }

    fn control(event: &str, topic: &str, id: String, time: i64) -> Self {
        Self {
            id,
            time,
            expires: None,
            event: event.to_string(),
            topic: topic.to_string(),
            message: None,
            title: None,
            priority: None,
            tags: Vec::new(),
            encoding: String::new(),
        }
    }

    /// A `message` frame carrying a queued envelope.
    pub fn message(topic: &str, id: String, env: &Envelope) -> Self {
        Self {
            id,
            time: env.received_at_secs as i64,
            expires: Some(env.expiry_secs as i64),
            event: EVENT_MESSAGE.to_string(),
            topic: topic.to_string(),
            message: Some(env.message.clone()),
            title: env.title.clone(),
            priority: env.priority,
            tags: env.tags.clone(),
            encoding: env.encoding.clone(),
        }
    }
}
