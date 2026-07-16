//! FoundationDB key layout — the single contract every role shares.
//!
//! All keys are tuple-encoded under one root subspace (`"upf"`), so they are
//! ordered and collision-free. The layout mirrors the design spec:
//!
//! ```text
//! (S,   token)                     -> Subscription (JSON)      // exists ⇒ registered
//! (Q,   token, versionstamp)       -> Envelope (JSON)          // durable queue, source of truth
//! (TI,  token, topic)              -> versionstamp             // RFC 8030 topic collapse
//! (X,   expiry, token, versionstamp) -> ""                     // TTL index (janitor scans this)
//! (C,   token)                     -> node_id                  // connection affinity
//! (IN,  node, shard, versionstamp) -> token                   // per-node inbox (advisory poke)
//! (SIG, node, shard)               -> counter (LE i64)         // watched bell, one per shard
//! (L,   node)                      -> heartbeat_secs (LE i64)  // liveness registry
//! ```
//!
//! The message id exposed to devices *is* the queue versionstamp (12 bytes,
//! base64url-encoded), so an ack round-trips straight back to a `Q` key. Because
//! all of a writer's versionstamped writes in one transaction share user version
//! `0`, the `Q`, `X` and `IN` entries for a message carry the *same* stamp.

use foundationdb::tuple::{Subspace, Versionstamp, pack_with_versionstamp};

/// User version reused across every versionstamped write in a single writer
/// transaction, so `Q`/`X`/`IN` for one message resolve to one identical stamp.
pub const MSG_USER_VERSION: u16 = 0;

// Short, stable prefix strings for each logical map.
const S: &str = "S";
const Q: &str = "Q";
const TI: &str = "TI";
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

    // ---- subscriptions (S) --------------------------------------------------

    pub fn subscription(&self, token: &str) -> Vec<u8> {
        self.root.pack(&(S, token))
    }

    // ---- durable queue (Q) --------------------------------------------------

    /// A versionstamped queue key with an *incomplete* stamp, for use with
    /// `SetVersionstampedKey`. FDB fills in the commit version at commit time.
    pub fn queue_append(&self, token: &str) -> Vec<u8> {
        self.root
            .pack_with_versionstamp(&(Q, token, Versionstamp::incomplete(MSG_USER_VERSION)))
    }

    /// A concrete queue key for an already-known (complete) versionstamp.
    pub fn queue_msg(&self, token: &str, vs: &Versionstamp) -> Vec<u8> {
        self.root.pack(&(Q, token, vs.clone()))
    }

    /// `[begin, end)` covering a subscription's whole queue.
    pub fn queue_range(&self, token: &str) -> (Vec<u8>, Vec<u8>) {
        self.root.subspace(&(Q, token)).range()
    }

    /// Recover the (complete) versionstamp from a queue key.
    pub fn queue_key_versionstamp(
        &self,
        token: &str,
        key: &[u8],
    ) -> crate::error::Result<Versionstamp> {
        let (_, _, vs) = self.root.unpack::<(String, String, Versionstamp)>(key)?;
        let _ = token;
        Ok(vs)
    }

    // ---- topic index (TI) ---------------------------------------------------

    pub fn topic_index(&self, token: &str, topic: &str) -> Vec<u8> {
        self.root.pack(&(TI, token, topic))
    }

    /// Value for a topic-index entry: a versionstamp pointing at the current
    /// message for the topic, written with `SetVersionstampedValue`.
    pub fn topic_index_value() -> Vec<u8> {
        pack_with_versionstamp(&(Versionstamp::incomplete(MSG_USER_VERSION),))
    }

    // ---- TTL index (X) ------------------------------------------------------

    pub fn ttl_append(&self, expiry_secs: u64, token: &str) -> Vec<u8> {
        self.root.pack_with_versionstamp(&(
            X,
            expiry_secs as i64,
            token,
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

    /// Decode a TTL-index key into `(expiry, token, versionstamp)`.
    pub fn ttl_key_parts(&self, key: &[u8]) -> crate::error::Result<(u64, String, Versionstamp)> {
        let (_, expiry, token, vs) = self
            .root
            .unpack::<(String, i64, String, Versionstamp)>(key)?;
        Ok((expiry.max(0) as u64, token, vs))
    }

    // ---- affinity (C) -------------------------------------------------------

    pub fn affinity(&self, token: &str) -> Vec<u8> {
        self.root.pack(&(C, token))
    }

    // ---- inbox (IN) + bell (SIG) --------------------------------------------

    /// Versionstamped inbox key (poke) for `(node, shard)`; value is the token.
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
