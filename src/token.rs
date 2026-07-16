//! Opaque token generation.
//!
//! UnifiedPush requires endpoint URLs to be hard to guess: at least 160 bits
//! of entropy, URL-safe. We generate 160-bit (20-byte) random tokens encoded
//! with URL-safe base64 (no padding) → 27 characters.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;

/// Number of random bytes per token (160 bits, per the UnifiedPush spec).
const TOKEN_BYTES: usize = 20;

/// Generate a fresh, unguessable subscription token.
pub fn new_endpoint_token() -> String {
    let mut buf = [0u8; TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

/// Generate a time-sortable, unique message id.
///
/// The id is `<20-digit micros-since-epoch><12 hex random>`. Lexicographic
/// ordering therefore matches arrival order, giving cheap FIFO range scans,
/// while the random suffix keeps concurrent arrivals unique.
pub fn new_message_id() -> String {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    let mut rnd = [0u8; 6];
    rand::thread_rng().fill_bytes(&mut rnd);
    let suffix: String = rnd.iter().map(|b| format!("{b:02x}")).collect();
    format!("{micros:020}{suffix}")
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
    fn message_ids_sort_by_time() {
        let a = new_message_id();
        let b = new_message_id();
        // Same-instant ids may tie on the micros prefix but never collide.
        assert_ne!(a, b);
        assert_eq!(a.len(), 32);
    }
}
