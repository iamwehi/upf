//! Per-node in-memory index of live topic subscriptions.
//!
//! Not a routing table — routing lives in FDB affinity (`C`). This is only the
//! map from a topic to the subscriber socket currently serving it on this node,
//! plus that subscriber's **offset cursor** (the last queue versionstamp it has
//! been sent). Losing this map costs nothing durable: everything is still in `Q`,
//! and a reconnecting client resumes by `since=<id>`.
//!
//! One connection per topic (new subscribe displaces the old): UnifiedPush uses
//! one distributor per topic, so we don't fan a topic out to many local sockets.

use std::sync::Arc;

use dashmap::DashMap;
use foundationdb::tuple::Versionstamp;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::protocol::NtfyMessage;

/// A topic's live subscriber on this node.
pub struct TopicConn {
    /// Feeds the subscriber's transport (ws/json/sse writer).
    pub tx: UnboundedSender<NtfyMessage>,
    /// The offset already delivered to this subscriber. `None` means "from the
    /// start of the queue"; drains advance it as messages are sent. Held under a
    /// mutex so a bell fire and the safety poll can't double-deliver.
    pub cursor: Mutex<Option<Versionstamp>>,
}

/// Maps `topic` → the subscriber currently serving it on this node.
#[derive(Default)]
pub struct Local {
    conns: DashMap<String, Arc<TopicConn>>,
}

impl Local {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a topic to a subscriber with an initial cursor, displacing any prior
    /// local binding for that topic.
    pub fn attach(
        &self,
        topic: String,
        tx: UnboundedSender<NtfyMessage>,
        cursor: Option<Versionstamp>,
    ) -> Arc<TopicConn> {
        let conn = Arc::new(TopicConn {
            tx,
            cursor: Mutex::new(cursor),
        });
        self.conns.insert(topic, conn.clone());
        conn
    }

    /// Remove a topic's binding, but only if it still points at `tx`.
    pub fn detach_if(&self, topic: &str, tx: &UnboundedSender<NtfyMessage>) {
        self.conns
            .remove_if(topic, |_, existing| existing.tx.same_channel(tx));
    }

    /// The subscriber currently serving a topic on this node, if any.
    pub fn get(&self, topic: &str) -> Option<Arc<TopicConn>> {
        self.conns.get(topic).map(|e| e.clone())
    }

    /// Snapshot of every topic currently held on this node (for the safety poll).
    pub fn topics(&self) -> Vec<String> {
        self.conns.iter().map(|e| e.key().clone()).collect()
    }
}
