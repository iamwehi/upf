//! Records persisted in FoundationDB and shared across roles.

use serde::{Deserialize, Serialize};

/// A registered subscription: the durable existence of an endpoint token, plus
/// the metadata needed to authenticate pushes to it. Addressing/routing is *not*
/// stored here — that lives in the affinity (`C`) record and is rewritten every
/// time a device connects.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscription {
    pub token: String,
    /// Application instance identifier chosen by the connector (opaque to us).
    pub app_id: String,
    /// Optional VAPID public key the application server must authenticate with.
    /// Stored but not yet verified (follow-up milestone).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vapid_pubkey: Option<String>,
    pub created_at_secs: u64,
}

/// A push message persisted in the durable queue (`Q`). This is the single
/// source of truth for delivery: bells and inboxes are advisory pokes, but a
/// message is not considered lost until it is acked and cleared from here.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    /// The (already-encrypted, per RFC 8291) push body, base64-encoded for JSON.
    pub body_b64: String,
    /// Selected WebPush headers we forward verbatim (TTL, Topic, Urgency, …).
    #[serde(default)]
    pub headers: MessageHeaders,
    pub received_at_secs: u64,
    /// Absolute expiry (unix seconds); mirrored into the TTL index (`X`).
    pub expiry_secs: u64,
}

/// Subset of WebPush request headers we care about (RFC 8030 §5).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MessageHeaders {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urgency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
}
