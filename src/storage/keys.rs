//! FoundationDB key layout.
//!
//! All keys live under a single root subspace `("upf",)` and are tuple-encoded,
//! which keeps them ordered and collision-free:
//!
//! ```text
//! ("upf", "sub", token)                 -> Subscription (JSON)
//! ("upf", "dist_subs", dist_id, token)  -> ""            (index for listing/GC)
//! ("upf", "queue", token, msg_id)       -> QueuedMessage (JSON)
//! ("upf", "dist_ver", dist_id)          -> u64           (future FDB-watch fan-out)
//! ```

use foundationdb::tuple::Subspace;

/// Namespaced key builder for the whole server.
#[derive(Clone)]
pub struct Keys {
    root: Subspace,
}

impl Default for Keys {
    fn default() -> Self {
        Self::new()
    }
}

impl Keys {
    pub fn new() -> Self {
        Self {
            root: Subspace::from("upf"),
        }
    }

    /// Key for a subscription record, addressed by its endpoint token.
    pub fn subscription(&self, token: &str) -> Vec<u8> {
        self.root.pack(&("sub", token))
    }

    /// Index key linking a distributor to one of its subscription tokens.
    pub fn dist_sub_index(&self, distributor_id: &str, token: &str) -> Vec<u8> {
        self.root.pack(&("dist_subs", distributor_id, token))
    }

    /// Subspace covering every index entry for a distributor (for range scans).
    pub fn dist_subs_range(&self, distributor_id: &str) -> Subspace {
        self.root.subspace(&("dist_subs", distributor_id))
    }

    /// Key for a single queued message under a subscription.
    pub fn queue_msg(&self, token: &str, msg_id: &str) -> Vec<u8> {
        self.root.pack(&("queue", token, msg_id))
    }

    /// Subspace covering the whole queue for a subscription (for range scans).
    pub fn queue_range(&self, token: &str) -> Subspace {
        self.root.subspace(&("queue", token))
    }

    /// Per-distributor version counter, bumped on every enqueue. Reserved for
    /// the cross-node FDB-watch fan-out (not yet consumed).
    pub fn dist_version(&self, distributor_id: &str) -> Vec<u8> {
        self.root.pack(&("dist_ver", distributor_id))
    }

    /// Decode the trailing `token` element of a `dist_subs` index key.
    pub fn dist_sub_token(&self, distributor_id: &str, key: &[u8]) -> Option<String> {
        self.dist_subs_range(distributor_id)
            .unpack::<(String,)>(key)
            .ok()
            .map(|(t,)| t)
    }
}
