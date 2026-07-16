//! In-process registry of live distributor connections.
//!
//! Single-node for the walking skeleton: maps a distributor id to the sender
//! half of its WebSocket write channel. The cross-node story (route to the node
//! holding the connection via an FDB watch on the per-distributor version key)
//! is a follow-up milestone — see `storage::keys::dist_version`.

use dashmap::DashMap;
use tokio::sync::mpsc::UnboundedSender;

use crate::distributor::protocol::ServerFrame;

/// Maps `distributor_id` → channel that feeds that distributor's WS writer.
#[derive(Default)]
pub struct Registry {
    conns: DashMap<String, UnboundedSender<ServerFrame>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a live connection, replacing (and thereby displacing) any
    /// existing connection for the same distributor.
    pub fn insert(&self, distributor_id: String, tx: UnboundedSender<ServerFrame>) {
        self.conns.insert(distributor_id, tx);
    }

    /// Remove a connection, but only if it is still the one we recorded. This
    /// avoids a reconnecting distributor's fresh entry being torn down by the
    /// cleanup of its previous, now-defunct connection.
    pub fn remove_if(&self, distributor_id: &str, tx: &UnboundedSender<ServerFrame>) {
        self.conns
            .remove_if(distributor_id, |_, existing| existing.same_channel(tx));
    }

    /// Try to deliver a frame to a connected distributor.
    ///
    /// Returns `true` if the distributor was connected and the frame was queued
    /// to its writer (not necessarily yet sent on the wire).
    pub fn try_send(&self, distributor_id: &str, frame: ServerFrame) -> bool {
        match self.conns.get(distributor_id) {
            Some(tx) => tx.send(frame).is_ok(),
            None => false,
        }
    }
}
