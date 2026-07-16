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
