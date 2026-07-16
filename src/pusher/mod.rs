//! Pusher role — holds device WebSockets, watches shard bells, drains queues.
//!
//! Its whole watch budget is `K` (one per shard), independent of how many
//! connections it holds. Correctness rests on three background rules from the
//! design spec, all implemented here: bells for latency, drain-on-connect for
//! completeness (in [`ws`]), and a periodic safety poll so a lost watch or stale
//! affinity degrades to latency rather than lost messages.

pub mod local;
pub mod ws;

use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::pusher::local::Local;
use crate::store::Store;

/// Shared pusher state: the local token→socket map plus this node's identity.
pub struct Pusher {
    pub local: Arc<Local>,
    pub node_id: String,
    pub shard_count: u32,
    /// Public base URL, used to build endpoint URLs on `register`.
    public_url: String,
}

impl Pusher {
    /// Public base URL for building endpoint URLs (no trailing slash).
    pub fn public_url(&self) -> &str {
        &self.public_url
    }

    /// Create the pusher state and spawn its background tasks (bell watchers,
    /// heartbeat, safety poll). Returns the shared handle used by the WS handler.
    pub fn start(store: Arc<Store>, config: Arc<Config>) -> Arc<Pusher> {
        let pusher = Arc::new(Pusher {
            local: Arc::new(Local::new()),
            node_id: config.node_id.clone(),
            shard_count: config.shard_count,
            public_url: config.public_url.clone(),
        });

        // One watcher task per shard — the entire watch budget for this node.
        for shard in 0..pusher.shard_count {
            let store = store.clone();
            let pusher = pusher.clone();
            tokio::spawn(async move { shard_watcher(store, pusher, shard).await });
        }

        // Liveness heartbeat.
        {
            let store = store.clone();
            let node = pusher.node_id.clone();
            let period = Duration::from_secs(config.heartbeat_secs.max(1));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                loop {
                    tick.tick().await;
                    if let Err(e) = store.heartbeat(&node, now_secs()).await {
                        tracing::warn!(error = %e, "heartbeat failed");
                    }
                }
            });
        }

        // Safety poll: re-drain every live connection regardless of bells.
        {
            let store = store.clone();
            let pusher = pusher.clone();
            let period = Duration::from_secs(config.safety_poll_secs.max(1));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                loop {
                    tick.tick().await;
                    for token in pusher.local.tokens() {
                        deliver_token(&store, &pusher.local, &token).await;
                    }
                }
            });
        }

        tracing::info!(node = %pusher.node_id, shards = pusher.shard_count, "pusher started");
        pusher
    }
}

/// Watch one shard bell forever: re-arm on every fire, then drain the inbox.
async fn shard_watcher(store: Arc<Store>, pusher: Arc<Pusher>, shard: u32) {
    let node = pusher.node_id.clone();
    // Arm before the first drain so pokes arriving during startup aren't missed.
    let mut watch = match store.arm_bell(&node, shard).await {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, shard, "failed to arm shard bell");
            return;
        }
    };
    loop {
        // Wait for a writer to ring the bell (or the watch to be reset).
        let _ = watch.await;
        // Re-arm immediately so a poke landing during processing isn't lost.
        watch = match store.arm_bell(&node, shard).await {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(error = %e, shard, "failed to re-arm shard bell");
                tokio::time::sleep(Duration::from_secs(1)).await;
                match store.arm_bell(&node, shard).await {
                    Ok(w) => w,
                    Err(_) => return,
                }
            }
        };
        let tokens = match store.take_inbox(&node, shard).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, shard, "failed to read inbox");
                continue;
            }
        };
        for token in tokens {
            deliver_token(&store, &pusher.local, &token).await;
        }
    }
}

/// Drain a token's durable queue and push every message to its local socket.
/// Idempotent: unacked messages are re-sent, acked ones are already gone. Safe
/// to call from a bell fire, a fresh connect, or the safety poll.
pub async fn deliver_token(store: &Store, local: &Local, token: &str) {
    let Some(conn) = local.get(token) else {
        return; // Not held here — an orphaned poke; the message stays in Q.
    };
    let _guard = conn.drain_lock.lock().await;
    let ready = match store.drain(token).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, %token, "drain failed");
            return;
        }
    };
    for r in ready {
        let frame = crate::protocol::ServerFrame::Message {
            endpoint_token: token.to_string(),
            msg_id: r.msg_id,
            body_b64: r.envelope.body_b64,
            headers: r.envelope.headers,
        };
        if conn.tx.send(frame).is_err() {
            break; // Connection gone; nothing durable lost.
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
