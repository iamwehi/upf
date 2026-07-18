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
#[cfg_attr(test, derive(PartialEq, Eq))]
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Any envelope the writer might construct. `encoding` is restricted to the
    /// two values ntfy defines (`""` / `"base64"`), but every other field is
    /// arbitrary — including non-ASCII `message`/`title`/`tags`.
    pub(crate) fn arb_envelope() -> impl Strategy<Value = Envelope> {
        (
            any::<String>(),
            prop_oneof![Just(String::new()), Just("base64".to_string())],
            any::<Option<String>>(),
            any::<Option<u8>>(),
            any::<Vec<String>>(),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(
                |(message, encoding, title, priority, tags, received_at_secs, expiry_secs)| {
                    Envelope {
                        message,
                        encoding,
                        title,
                        priority,
                        tags,
                        received_at_secs,
                        expiry_secs,
                    }
                },
            )
    }

    proptest! {
        /// An `Envelope` is the exact JSON payload stored under a `Q` key and read
        /// back on every delivery, so it must survive a serialize → deserialize
        /// round-trip unchanged. If it didn't, queued messages would corrupt
        /// silently between publish and fan-out.
        #[test]
        fn envelope_round_trips(env in arb_envelope()) {
            let bytes = serde_json::to_vec(&env).unwrap();
            let back: Envelope = serde_json::from_slice(&bytes).unwrap();
            prop_assert_eq!(back, env);
        }
    }
}
