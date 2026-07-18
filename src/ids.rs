//! Identifiers and topic validation for the ntfy-compatible surface.
//!
//! * **Message id** — the queue *versionstamp* (12 bytes) base64url-encoded. It
//!   doubles as an ntfy message `id` *and* as a resumable offset: a client that
//!   reconnects with `?since=<id>` decodes straight back to a `Q` key, so we
//!   resume the log exactly where it left off.
//! * **Ephemeral id** — a short random id for `open`/`keepalive` control frames,
//!   which have no queue entry behind them.
//! * **Topic** — the ntfy topic name, which is also our subscription key.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use foundationdb::tuple::Versionstamp;
use rand::Rng;
use rand::distributions::Alphanumeric;

use crate::error::{Error, Result};

/// Max topic length (matches ntfy's effective limit; UP topics are 14 chars).
const MAX_TOPIC_LEN: usize = 64;

/// Encode a (complete) queue versionstamp as a message id.
pub fn encode_msg_id(vs: &Versionstamp) -> String {
    URL_SAFE_NO_PAD.encode(vs.as_bytes())
}

/// Decode a message id back into a versionstamp (for `since=<id>` resume / acks).
pub fn decode_msg_id(msg_id: &str) -> Result<Versionstamp> {
    let bytes = URL_SAFE_NO_PAD
        .decode(msg_id)
        .map_err(|_| Error::BadRequest("malformed message id".into()))?;
    let arr: [u8; 12] = bytes
        .try_into()
        .map_err(|_| Error::BadRequest("message id must be 12 bytes".into()))?;
    Ok(Versionstamp::from(arr))
}

/// A short random id for control frames (`open`, `keepalive`) that have no
/// durable message behind them. ntfy ids are ~12 chars of base62; we match that.
pub fn ephemeral_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

/// Validate an ntfy topic name: non-empty, `[-_A-Za-z0-9]`, within length.
pub fn valid_topic(topic: &str) -> bool {
    !topic.is_empty()
        && topic.len() <= MAX_TOPIC_LEN
        && topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_id_round_trips_through_versionstamp() {
        let vs = Versionstamp::complete([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 7);
        let id = encode_msg_id(&vs);
        let back = decode_msg_id(&id).unwrap();
        assert_eq!(back.as_bytes(), vs.as_bytes());
        assert!(decode_msg_id("!!nope!!").is_err());
        assert!(decode_msg_id("aGVsbG8").is_err()); // wrong length
    }

    #[test]
    fn topic_validation() {
        assert!(valid_topic("upAbC123_-xyz")); // typical UnifiedPush topic
        assert!(valid_topic("my-topic"));
        assert!(!valid_topic("")); // empty
        assert!(!valid_topic("has space"));
        assert!(!valid_topic("emoji😀"));
        assert!(!valid_topic(&"x".repeat(65))); // too long
    }

    #[test]
    fn ephemeral_ids_are_unique_alnum() {
        let a = ephemeral_id();
        let b = ephemeral_id();
        assert_eq!(a.len(), 12);
        assert_ne!(a, b);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    use proptest::prelude::*;

    proptest! {
        /// Any complete versionstamp survives a round-trip through the message id.
        /// This is the contract behind `?since=<id>` resume: the id a subscriber
        /// echoes back must decode to the exact `Q` key it came from.
        #[test]
        fn msg_id_round_trips(tx_bytes: [u8; 10], user_version: u16) {
            let vs = Versionstamp::complete(tx_bytes, user_version);
            let back = decode_msg_id(&encode_msg_id(&vs))?;
            prop_assert_eq!(back.as_bytes(), vs.as_bytes());
        }

        /// `decode_msg_id` is fed straight from a client's `?since=` param, so it
        /// must never panic on arbitrary input — only ever `Ok` or `Err`.
        #[test]
        fn decode_msg_id_never_panics(s in ".*") {
            let _ = decode_msg_id(&s);
        }

        /// A string built from exactly the allowed alphabet, within the length
        /// bound, is always a valid topic.
        #[test]
        fn generated_valid_topics_are_valid(topic in "[-_A-Za-z0-9]{1,64}") {
            prop_assert!(valid_topic(&topic));
        }

        /// Oracle: `valid_topic` agrees with an independent restatement of the
        /// rule for *any* string, so it can't drift from its own spec.
        #[test]
        fn valid_topic_matches_spec(s in ".*") {
            let expected = !s.is_empty()
                && s.len() <= MAX_TOPIC_LEN
                && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
            prop_assert_eq!(valid_topic(&s), expected);
        }
    }
}
