//! Opaque tokens and message ids.
//!
//! * **Endpoint token** — the bearer capability embedded in an endpoint URL.
//!   UnifiedPush requires ≥160 bits of entropy, URL-safe. We use 160-bit
//!   (20-byte) random tokens, URL-safe base64 (no padding) → 27 chars.
//! * **Message id** — the queue *versionstamp* (12 bytes: 10-byte commit version
//!   + 2-byte user version) base64url-encoded → 16 chars. It is opaque to the
//!   device but decodes straight back to a `Q` key so an ack can clear it.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use foundationdb::tuple::Versionstamp;
use rand::RngCore;

use crate::error::{Error, Result};

/// Number of random bytes per endpoint token (160 bits, per the UnifiedPush spec).
const TOKEN_BYTES: usize = 20;

/// Generate a fresh, unguessable subscription token.
pub fn new_endpoint_token() -> String {
    let mut buf = [0u8; TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

/// Encode a (complete) queue versionstamp as the device-facing message id.
pub fn encode_msg_id(vs: &Versionstamp) -> String {
    URL_SAFE_NO_PAD.encode(vs.as_bytes())
}

/// Decode a device-supplied message id back into a versionstamp.
pub fn decode_msg_id(msg_id: &str) -> Result<Versionstamp> {
    let bytes = URL_SAFE_NO_PAD
        .decode(msg_id)
        .map_err(|_| Error::BadRequest("malformed msg_id".into()))?;
    let arr: [u8; 12] = bytes
        .try_into()
        .map_err(|_| Error::BadRequest("msg_id must be 12 bytes".into()))?;
    Ok(Versionstamp::from(arr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_unique_and_url_safe() {
        let a = new_endpoint_token();
        let b = new_endpoint_token();
        assert_ne!(a, b);
        assert!(!a.contains('+') && !a.contains('/') && !a.contains('='));
        assert_eq!(a.len(), 27);
    }

    #[test]
    fn msg_id_round_trips_through_versionstamp() {
        let vs = Versionstamp::complete([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 7);
        let id = encode_msg_id(&vs);
        let back = decode_msg_id(&id).unwrap();
        assert_eq!(back.as_bytes(), vs.as_bytes());
        assert_eq!(back.user_version(), 7);
        assert!(decode_msg_id("!!not-base64!!").is_err());
        assert!(decode_msg_id("aGVsbG8").is_err()); // wrong length
    }
}
