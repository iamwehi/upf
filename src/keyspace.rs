//! FoundationDB key layout — the single contract every role shares.
//!
//! All keys are tuple-encoded under one root subspace (`"upf"`), so they are
//! ordered and collision-free. In the ntfy model a *topic* is the addressing
//! unit: topics are implicit (no registration record), so there is no `S` map.
//!
//! ```text
//! (Q,   topic, versionstamp)         -> Envelope (JSON)          // durable queue, source of truth
//! (X,   expiry, topic, versionstamp) -> ""                       // TTL index (janitor scans this)
//! (C,   topic)                       -> node_id                  // subscriber affinity
//! (IN,  node, shard, versionstamp)   -> topic                    // per-node inbox (advisory poke)
//! (SIG, node, shard)                 -> counter (LE i64)         // watched bell, one per shard
//! (L,   node)                        -> heartbeat_secs (LE i64)  // liveness registry
//! ```
//!
//! The message id exposed to subscribers *is* the queue versionstamp (12 bytes,
//! base64url-encoded), so a `since=<id>` resume decodes straight back to a `Q`
//! key. All of a writer's versionstamped writes in one transaction share user
//! version `0`, so the `Q`, `X` and `IN` entries for a message carry one stamp.

use foundationdb::tuple::{Subspace, Versionstamp};

/// User version reused across every versionstamped write in a single writer
/// transaction, so `Q`/`X`/`IN` for one message resolve to one identical stamp.
pub const MSG_USER_VERSION: u16 = 0;

// Short, stable prefix strings for each logical map.
const Q: &str = "Q";
const X: &str = "X";
const C: &str = "C";
const IN: &str = "IN";
const SIG: &str = "SIG";
const L: &str = "L";

/// Namespaced key builder for the whole server.
#[derive(Clone)]
pub struct Keyspace {
    root: Subspace,
}

impl Default for Keyspace {
    fn default() -> Self {
        Self::new()
    }
}

impl Keyspace {
    pub fn new() -> Self {
        Self {
            root: Subspace::from("upf"),
        }
    }

    // ---- durable queue (Q) --------------------------------------------------

    /// A versionstamped queue key with an *incomplete* stamp, for use with
    /// `SetVersionstampedKey`. FDB fills in the commit version at commit time.
    pub fn queue_append(&self, topic: &str) -> Vec<u8> {
        self.root
            .pack_with_versionstamp(&(Q, topic, Versionstamp::incomplete(MSG_USER_VERSION)))
    }

    /// A concrete queue key for an already-known (complete) versionstamp.
    pub fn queue_msg(&self, topic: &str, vs: &Versionstamp) -> Vec<u8> {
        self.root.pack(&(Q, topic, vs.clone()))
    }

    /// `[begin, end)` covering a topic's whole queue.
    pub fn queue_range(&self, topic: &str) -> (Vec<u8>, Vec<u8>) {
        self.root.subspace(&(Q, topic)).range()
    }

    /// Recover the (complete) versionstamp from a queue key.
    pub fn queue_key_versionstamp(&self, key: &[u8]) -> crate::error::Result<Versionstamp> {
        let (_, _, vs) = self.root.unpack::<(String, String, Versionstamp)>(key)?;
        Ok(vs)
    }

    // ---- TTL index (X) ------------------------------------------------------

    pub fn ttl_append(&self, expiry_secs: u64, topic: &str) -> Vec<u8> {
        self.root.pack_with_versionstamp(&(
            X,
            expiry_secs as i64,
            topic,
            Versionstamp::incomplete(MSG_USER_VERSION),
        ))
    }

    /// `[begin, end)` covering every TTL entry with `expiry <= now`.
    pub fn ttl_range_due(&self, now_secs: u64) -> (Vec<u8>, Vec<u8>) {
        let begin = self.root.subspace(&(X,)).range().0;
        // Everything at expiry <= now sorts strictly before (X, now+1).
        let end = self.root.pack(&(X, now_secs as i64 + 1));
        (begin, end)
    }

    /// Decode a TTL-index key into `(expiry, topic, versionstamp)`.
    pub fn ttl_key_parts(&self, key: &[u8]) -> crate::error::Result<(u64, String, Versionstamp)> {
        let (_, expiry, topic, vs) =
            self.root.unpack::<(String, i64, String, Versionstamp)>(key)?;
        Ok((expiry.max(0) as u64, topic, vs))
    }

    // ---- affinity (C) -------------------------------------------------------

    pub fn affinity(&self, topic: &str) -> Vec<u8> {
        self.root.pack(&(C, topic))
    }

    // ---- inbox (IN) + bell (SIG) --------------------------------------------

    /// Versionstamped inbox key (poke) for `(node, shard)`; value is the topic.
    pub fn inbox_append(&self, node: &str, shard: u32) -> Vec<u8> {
        self.root.pack_with_versionstamp(&(
            IN,
            node,
            shard as i64,
            Versionstamp::incomplete(MSG_USER_VERSION),
        ))
    }

    /// `[begin, end)` covering one `(node, shard)` inbox.
    pub fn inbox_range(&self, node: &str, shard: u32) -> (Vec<u8>, Vec<u8>) {
        self.root.subspace(&(IN, node, shard as i64)).range()
    }

    pub fn bell(&self, node: &str, shard: u32) -> Vec<u8> {
        self.root.pack(&(SIG, node, shard as i64))
    }

    // ---- liveness (L) -------------------------------------------------------

    pub fn liveness(&self, node: &str) -> Vec<u8> {
        self.root.pack(&(L, node))
    }

    pub fn liveness_range(&self) -> (Vec<u8>, Vec<u8>) {
        self.root.subspace(&(L,)).range()
    }

    pub fn liveness_node(&self, key: &[u8]) -> crate::error::Result<String> {
        let (_, node) = self.root.unpack::<(String, String)>(key)?;
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Topics are validated upstream (see `ids::valid_topic`); generate from that
    // same alphabet so these properties cover exactly the keys we really pack.
    fn topic() -> impl Strategy<Value = String> {
        "[-_A-Za-z0-9]{1,64}"
    }

    proptest! {
        /// The message id a subscriber sees *is* the queue versionstamp, so a
        /// `Q` key must decode back to the exact stamp it was built from — this
        /// is what makes `since=<id>` land on the right log entry.
        #[test]
        fn queue_key_round_trips(topic in topic(), tx: [u8; 10], uv: u16) {
            let ks = Keyspace::new();
            let vs = Versionstamp::complete(tx, uv);
            let key = ks.queue_msg(&topic, &vs);
            let back = ks.queue_key_versionstamp(&key)?;
            prop_assert_eq!(back.as_bytes(), vs.as_bytes());
        }

        /// The liveness registry key decodes back to its node id.
        #[test]
        fn liveness_key_round_trips(node in topic()) {
            let ks = Keyspace::new();
            let back = ks.liveness_node(&ks.liveness(&node))?;
            prop_assert_eq!(back, node);
        }

        /// The janitor scans the TTL index in expiry order and stops at `now`,
        /// which only works if byte order matches numeric expiry order. Range is
        /// bounded to realistic epoch-seconds (< ~2106) — beyond `i64::MAX` the
        /// `as i64` cast wraps, but no real expiry approaches that.
        #[test]
        fn ttl_keys_preserve_expiry_order(
            a in 0u64..=u32::MAX as u64,
            b in 0u64..=u32::MAX as u64,
            topic in topic(),
        ) {
            let ks = Keyspace::new();
            let ka = ks.ttl_append(a, &topic);
            let kb = ks.ttl_append(b, &topic);
            prop_assert_eq!(a <= b, ka <= kb);
        }

        /// A TTL entry is "due" (inside `ttl_range_due(now)`) exactly when its
        /// expiry is `<= now` — the boundary the janitor's `now + 1` end key
        /// depends on. Uses concrete complete keys so we can byte-compare.
        #[test]
        fn ttl_range_due_includes_iff_expired(
            expiry in 0u64..=u32::MAX as u64,
            now in 0u64..=u32::MAX as u64,
            topic in topic(),
        ) {
            let ks = Keyspace::new();
            // A complete TTL key at `expiry` (same tuple shape as ttl_append,
            // but with a concrete stamp so ordering is fully determined).
            let key = ks.root.pack(&(
                X,
                expiry as i64,
                topic.as_str(),
                Versionstamp::complete([0; 10], 0),
            ));
            let (begin, end) = ks.ttl_range_due(now);
            let in_range = key.as_slice() >= begin.as_slice() && key.as_slice() < end.as_slice();
            prop_assert_eq!(in_range, expiry <= now);
        }
    }
}
