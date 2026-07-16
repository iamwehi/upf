//! Per-node in-memory index of live token connections.
//!
//! This is *not* a routing table — routing lives in FDB affinity (`C`). It is
//! only the local map from a token to the socket that currently holds it on this
//! node, so a bell fire or safety poll can find the writer channel to deliver on.
//! Losing this map costs nothing durable: the messages are all still in `Q`.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::protocol::ServerFrame;

/// A token's live connection on this node.
pub struct TokenConn {
    /// Feeds the connection's WebSocket writer task.
    pub tx: UnboundedSender<ServerFrame>,
    /// Serializes drains for this token so a bell fire and the safety poll don't
    /// interleave two concurrent replays of the same queue.
    pub drain_lock: Mutex<()>,
}

/// Maps `endpoint_token` → the connection currently serving it on this node.
#[derive(Default)]
pub struct Local {
    conns: DashMap<String, Arc<TokenConn>>,
}

impl Local {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a token to a connection, displacing any previous local binding.
    pub fn attach(&self, token: String, tx: UnboundedSender<ServerFrame>) -> Arc<TokenConn> {
        let conn = Arc::new(TokenConn {
            tx,
            drain_lock: Mutex::new(()),
        });
        self.conns.insert(token, conn.clone());
        conn
    }

    /// Remove a token's binding, but only if it still points at `tx` (avoids a
    /// disconnecting connection tearing down a token another connection re-took).
    pub fn detach_if(&self, token: &str, tx: &UnboundedSender<ServerFrame>) {
        self.conns
            .remove_if(token, |_, existing| existing.tx.same_channel(tx));
    }

    /// The connection currently serving a token on this node, if any.
    pub fn get(&self, token: &str) -> Option<Arc<TokenConn>> {
        self.conns.get(token).map(|e| e.clone())
    }

    /// Snapshot of every token currently held on this node (for the safety poll).
    pub fn tokens(&self) -> Vec<String> {
        self.conns.iter().map(|e| e.key().clone()).collect()
    }
}
