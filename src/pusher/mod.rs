//! Pusher role — serves ntfy subscriptions, watches shard bells, drains queues.
//!
//! Its whole watch budget is `K` (one per shard), independent of connection
//! count. Correctness rests on three background rules: bells for latency,
//! drain-on-connect for completeness (in [`ws`]), and a periodic safety poll so a
//! lost watch or stale affinity degrades to latency, not lost messages.

pub mod local;
pub mod ws;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;

use crate::config::Config;
use crate::ids;
use crate::protocol::NtfyMessage;
use crate::pusher::local::Local;
use crate::store::Store;

/// Shared pusher state: the local topic→socket map plus this node's identity.
pub struct Pusher {
    pub local: Arc<Local>,
    pub node_id: String,
    pub shard_count: u32,
    pub keepalive_secs: u64,
}

impl Pusher {
    /// Create the pusher state and spawn its background tasks (bell watchers,
    /// heartbeat, safety poll). Returns the shared handle used by the WS handler.
    pub fn start(store: Arc<Store>, config: Arc<Config>) -> Arc<Pusher> {
        let pusher = Arc::new(Pusher {
            local: Arc::new(Local::new()),
            node_id: config.node_id.clone(),
            shard_count: config.shard_count,
            keepalive_secs: config.keepalive_secs,
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

        // Safety poll: re-drain every live subscription regardless of bells.
        {
            let store = store.clone();
            let pusher = pusher.clone();
            let period = Duration::from_secs(config.safety_poll_secs.max(1));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                loop {
                    tick.tick().await;
                    for topic in pusher.local.topics() {
                        deliver_topic(&store, &pusher.local, &topic).await;
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
    let mut watch = match store.arm_bell(&node, shard).await {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, shard, "failed to arm shard bell");
            return;
        }
    };
    loop {
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
        let topics = match store.take_inbox(&node, shard).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, shard, "failed to read inbox");
                continue;
            }
        };
        for topic in topics {
            deliver_topic(&store, &pusher.local, &topic).await;
        }
    }
}

/// Drain a topic's queue *from the subscriber's cursor* and stream the new
/// messages, advancing the cursor. Idempotent and safe to call from a bell fire,
/// a fresh connect, or the safety poll — the per-connection cursor guarantees
/// each message is sent once per connection.
pub async fn deliver_topic(store: &Store, local: &Local, topic: &str) {
    let Some(conn) = local.get(topic) else {
        return; // Not held here — an orphaned poke; the message stays in Q.
    };
    let mut cursor = conn.cursor.lock().await;
    let ready = match &*cursor {
        Some(vs) => store.drain_after(topic, vs).await,
        None => store.drain_all(topic).await,
    };
    let ready = match ready {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, %topic, "drain failed");
            return;
        }
    };
    let mut last = None;
    for r in ready {
        let msg = NtfyMessage::message(topic, r.msg_id, &r.envelope);
        if conn.tx.send(msg).is_err() {
            return; // Subscriber gone; nothing durable lost.
        }
        last = Some(r.versionstamp);
    }
    if let Some(vs) = last {
        *cursor = Some(vs);
    }
}

/// Releases a subscription's resources when its transport stream is dropped
/// (client disconnect): detaches the local binding and clears affinity if we
/// still own it. Cleanup runs on drop from any transport (ws/json/sse).
pub struct SubGuard {
    store: Arc<Store>,
    local: Arc<Local>,
    topic: String,
    node: String,
    tx: UnboundedSender<NtfyMessage>,
}

impl Drop for SubGuard {
    fn drop(&mut self) {
        self.local.detach_if(&self.topic, &self.tx);
        let store = self.store.clone();
        let topic = self.topic.clone();
        let node = self.node.clone();
        tokio::spawn(async move {
            if let Err(e) = store.release_affinity_if(&topic, &node).await {
                tracing::warn!(error = %e, %topic, "failed to release affinity");
            }
        });
    }
}

impl SubGuard {
    pub fn new(
        store: Arc<Store>,
        local: Arc<Local>,
        topic: String,
        node: String,
        tx: UnboundedSender<NtfyMessage>,
    ) -> Self {
        Self {
            store,
            local,
            topic,
            node,
            tx,
        }
    }
}

/// Emit a `keepalive` frame to a subscriber every `secs` until its channel closes.
pub fn spawn_keepalive(tx: UnboundedSender<NtfyMessage>, topic: String, secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(secs.max(1)));
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            let msg = NtfyMessage::keepalive(&topic, ids::ephemeral_id(), now_secs() as i64);
            if tx.send(msg).is_err() {
                break;
            }
        }
    });
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
