//! Stable token → shard hashing.
//!
//! The shard of a token must be identical on every node and across restarts and
//! Rust versions, because a *writer* computes `shard = hash(token) % K` to poke a
//! *pusher*'s inbox, and that pusher independently derived the same shard when it
//! opened its watches. Rust's `DefaultHasher` is randomly seeded and unspecified,
//! so we use a fixed FNV-1a instead.

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// 64-bit FNV-1a over the raw bytes of the token.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Shard index in `0..shard_count` for a token. `shard_count` must be >= 1.
pub fn shard_of(token: &str, shard_count: u32) -> u32 {
    debug_assert!(shard_count >= 1);
    (fnv1a(token.as_bytes()) % shard_count as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_stable_and_in_range() {
        // Golden value pins the hash so an accidental algorithm change is caught.
        assert_eq!(fnv1a(b"hello"), 0xa430_d846_80aa_bd0b);
        for tok in ["abc", "", "a-very-long-token-value-1234567890"] {
            let s = shard_of(tok, 64);
            assert!(s < 64);
            // Deterministic across calls.
            assert_eq!(s, shard_of(tok, 64));
        }
    }
}
