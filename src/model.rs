//! Records persisted in FoundationDB.

use serde::{Deserialize, Serialize};

/// A published message persisted in the durable queue (`Q`). This is the single
/// source of truth for delivery; ntfy subscribers stream it and resume by offset
/// (`since=<id>`), so — unlike an ack-based queue — nothing is cleared on send.
/// The TTL index (`X`) and the janitor reclaim it.
///
/// The body is stored already in ntfy's delivery form: `message` is either a
/// UTF-8 string or a base64 blob, and `encoding` says which (`""` or `"base64"`,
/// per ntfy). This means delivery is a straight copy — no re-encoding per send.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub message: String,
    /// `""` for a UTF-8 `message`, `"base64"` for a binary one (ntfy semantics).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub encoding: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub received_at_secs: u64,
    /// Absolute expiry (unix seconds); mirrored into the TTL index (`X`).
    pub expiry_secs: u64,
}
