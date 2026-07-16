//! The FoundationDB access layer — every role's *only* means of communication.
//!
//! There is no service-to-service RPC anywhere in UPF: writers, pushers and the
//! janitor coordinate purely by reading and writing the keys defined in
//! [`crate::keyspace`].

use std::future::Future;
use std::pin::Pin;

use foundationdb::options::{MutationType, StreamingMode};
use foundationdb::tuple::Versionstamp;
use foundationdb::{Database, FdbBindingError, KeySelector, RangeOption};

use crate::error::Result;
use crate::hash::shard_of;
use crate::ids;
use crate::keyspace::Keyspace;
use crate::model::Envelope;

/// A pending FDB watch: resolves once the watched key changes.
pub type Watch = Pin<Box<dyn Future<Output = foundationdb::FdbResult<()>> + Send>>;

/// One drained message, ready to stream to a subscriber. Carries the versionstamp
/// so a connection can advance its offset cursor as it delivers.
#[derive(Debug, Clone)]
pub struct Ready {
    pub msg_id: String,
    pub versionstamp: Versionstamp,
    pub envelope: Envelope,
}

/// Handle to the FoundationDB cluster plus the shared key layout.
pub struct Store {
    db: Database,
    ks: Keyspace,
    shard_count: u32,
}

impl Store {
    /// Connect using the cluster file named by `FDB_CLUSTER_FILE` (or the system
    /// default). Requires the FDB network to have been booted.
    pub fn connect(shard_count: u32) -> Result<Self> {
        Ok(Self {
            db: Database::default()?,
            ks: Keyspace::new(),
            shard_count,
        })
    }

    pub fn shard_of(&self, topic: &str) -> u32 {
        shard_of(topic, self.shard_count)
    }

    // ===== writer: publish (Q + X + poke) ===================================

    /// Publish a message to a topic in one transaction: append the envelope to
    /// the durable queue, record its TTL index entry, and — if a subscriber is
    /// connected (affinity present) — poke that node's inbox and ring its shard
    /// bell. Topics are implicit (ntfy semantics): no existence check, so this
    /// never 404s. A missing/stale affinity is fine — the message rests in `Q`.
    pub async fn publish(&self, topic: &str, envelope: &Envelope) -> Result<()> {
        let envelope_bytes = serde_json::to_vec(envelope)?;
        let shard = self.shard_of(topic);
        let c_key = self.ks.affinity(topic);
        let q_key = self.ks.queue_append(topic);
        let x_key = self.ks.ttl_append(envelope.expiry_secs, topic);

        self.db
            .run(|trx, _| {
                let c_key = c_key.clone();
                let q_key = q_key.clone();
                let x_key = x_key.clone();
                let envelope_bytes = envelope_bytes.clone();
                let topic = topic.to_string();
                let ks = self.ks.clone();
                async move {
                    // Append to the durable queue (source of truth) + TTL index.
                    trx.atomic_op(&q_key, &envelope_bytes, MutationType::SetVersionstampedKey);
                    trx.atomic_op(&x_key, b"", MutationType::SetVersionstampedKey);

                    // If a subscriber is connected, poke its node. Stale affinity
                    // is acceptable — never verified, never retried.
                    if let Some(node_bytes) = trx.get(&c_key, false).await? {
                        let node = String::from_utf8_lossy(&node_bytes).into_owned();
                        let in_key = ks.inbox_append(&node, shard);
                        trx.atomic_op(&in_key, topic.as_bytes(), MutationType::SetVersionstampedKey);
                        trx.atomic_op(
                            &ks.bell(&node, shard),
                            &1i64.to_le_bytes(),
                            MutationType::Add,
                        );
                    }
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    // ===== pusher: affinity (C) =============================================

    /// Claim this topic for `node` (a subscriber connected here).
    pub async fn claim_affinity(&self, topic: &str, node: &str) -> Result<()> {
        let key = self.ks.affinity(topic);
        let node = node.as_bytes().to_vec();
        self.db
            .run(|trx, _| {
                let key = key.clone();
                let node = node.clone();
                async move {
                    trx.set(&key, &node);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    /// Compare-and-clear affinity: release the topic only if it still points at
    /// `node`. Prevents a disconnecting node from stomping a newer subscriber's
    /// claim.
    pub async fn release_affinity_if(&self, topic: &str, node: &str) -> Result<()> {
        let key = self.ks.affinity(topic);
        let node = node.as_bytes().to_vec();
        self.db
            .run(|trx, _| {
                let key = key.clone();
                let node = node.clone();
                async move {
                    if let Some(cur) = trx.get(&key, false).await? {
                        if cur.as_ref() == node.as_slice() {
                            trx.clear(&key);
                        }
                    }
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    // ===== pusher: drain (Q) ================================================

    /// Drain a topic's whole queue in order (paged). Used for `since=all`.
    pub async fn drain_all(&self, topic: &str) -> Result<Vec<Ready>> {
        let (begin, end) = self.ks.queue_range(topic);
        self.drain_range(begin, end).await
    }

    /// Drain messages strictly after `vs` (offset resume / live streaming).
    pub async fn drain_after(&self, topic: &str, vs: &Versionstamp) -> Result<Vec<Ready>> {
        let (_, end) = self.ks.queue_range(topic);
        let begin = self.ks.queue_msg(topic, vs); // exclusive lower bound below
        self.drain_range_after(begin, end).await
    }

    /// The most recent versionstamp in a topic's queue, if any (for live-only
    /// subscriptions that should skip history).
    pub async fn latest(&self, topic: &str) -> Result<Option<Versionstamp>> {
        let (begin, end) = self.ks.queue_range(topic);
        let last = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut opt = RangeOption::from(begin..end);
                    opt.reverse = true;
                    opt.limit = Some(1);
                    let kvs = trx.get_range(&opt, 1, false).await?;
                    Ok(kvs.iter().next().map(|kv| kv.key().to_vec()))
                }
            })
            .await?;
        match last {
            Some(key) => Ok(Some(self.ks.queue_key_versionstamp(&key)?)),
            None => Ok(None),
        }
    }

    // ===== pusher: bells (SIG) + inbox (IN) =================================

    /// Arm a watch on a shard bell. Resolves when any writer rings it. The watch
    /// is established at commit and outlives the transaction; drop to cancel.
    pub async fn arm_bell(&self, node: &str, shard: u32) -> Result<Watch> {
        let key = self.ks.bell(node, shard);
        let trx = self.db.create_trx()?;
        let watch = trx.watch(&key);
        trx.commit().await.map_err(foundationdb::FdbError::from)?;
        Ok(Box::pin(watch))
    }

    /// Read and clear a `(node, shard)` inbox, returning the poked topics. Only
    /// the entries actually read are cleared, so a poke racing this drain is left
    /// for the next bell rather than dropped.
    pub async fn take_inbox(&self, node: &str, shard: u32) -> Result<Vec<String>> {
        let (begin, end) = self.ks.inbox_range(node, shard);
        let topics = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut opt = RangeOption::from(begin..end);
                    opt.mode = StreamingMode::WantAll;
                    let mut topics = Vec::new();
                    let mut iteration = 1;
                    loop {
                        let kvs = trx.get_range(&opt, iteration, false).await?;
                        let last = kvs.iter().last().map(|kv| kv.key().to_vec());
                        for kv in kvs.iter() {
                            topics.push(String::from_utf8_lossy(kv.value()).into_owned());
                            trx.clear(kv.key());
                        }
                        if !kvs.more() {
                            break;
                        }
                        match last {
                            Some(k) => opt.begin = KeySelector::first_greater_than(k),
                            None => break,
                        }
                        iteration += 1;
                    }
                    Ok(topics)
                }
            })
            .await?;
        Ok(topics)
    }

    // ===== liveness (L) =====================================================

    pub async fn heartbeat(&self, node: &str, now_secs: u64) -> Result<()> {
        let key = self.ks.liveness(node);
        let value = (now_secs as i64).to_le_bytes().to_vec();
        self.db
            .run(|trx, _| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    trx.set(&key, &value);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    // ===== janitor: expiry (X → Q) ==========================================

    /// Delete up to `limit` expired messages: for each due `X` entry, clear the
    /// referenced `Q` entry and the `X` entry itself. Returns how many were swept.
    pub async fn expire(&self, now_secs: u64, limit: usize) -> Result<usize> {
        let (begin, end) = self.ks.ttl_range_due(now_secs);
        let ks = self.ks.clone();
        let swept = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                let ks = ks.clone();
                async move {
                    let mut opt = RangeOption::from(begin..end);
                    opt.mode = StreamingMode::WantAll;
                    opt.limit = Some(limit);
                    let kvs = trx.get_range(&opt, 1, false).await?;
                    let mut n = 0usize;
                    for kv in kvs.iter() {
                        let (_, topic, vs) = ks
                            .ttl_key_parts(kv.key())
                            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
                        trx.clear(&ks.queue_msg(&topic, &vs));
                        trx.clear(kv.key());
                        n += 1;
                    }
                    Ok(n)
                }
            })
            .await?;
        Ok(swept)
    }

    /// Sweep records for nodes that stopped heartbeating: clear their liveness
    /// entry plus every inbox and bell key for their shards. Pokes to a dead node
    /// are orphaned; the messages themselves survive in `Q`.
    pub async fn sweep_dead_nodes(&self, cutoff_secs: u64) -> Result<usize> {
        let (begin, end) = self.ks.liveness_range();
        let kvs = self.read_range(begin, end).await?;
        let shard_count = self.shard_count;
        let mut swept = 0usize;
        for (key, value) in kvs {
            let node = self.ks.liveness_node(&key)?;
            let ts = read_le_i64(&value).unwrap_or(0);
            if ts >= cutoff_secs as i64 {
                continue;
            }
            let l_key = self.ks.liveness(&node);
            let mut ranges = Vec::new();
            let mut bells = Vec::new();
            for shard in 0..shard_count {
                ranges.push(self.ks.inbox_range(&node, shard));
                bells.push(self.ks.bell(&node, shard));
            }
            self.db
                .run(|trx, _| {
                    let l_key = l_key.clone();
                    let ranges = ranges.clone();
                    let bells = bells.clone();
                    async move {
                        trx.clear(&l_key);
                        for (b, e) in &ranges {
                            trx.clear_range(b, e);
                        }
                        for bell in &bells {
                            trx.clear(bell);
                        }
                        Ok(())
                    }
                })
                .await?;
            swept += 1;
        }
        Ok(swept)
    }

    // ===== shared range readers =============================================

    /// Read all messages in `[begin, end)` and decode them into `Ready`.
    async fn drain_range(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<Ready>> {
        let kvs = self.read_range(begin, end).await?;
        self.decode_ready(kvs)
    }

    /// Like [`drain_range`] but excludes the lower bound key itself (`> begin`).
    async fn drain_range_after(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<Ready>> {
        let kvs = self.read_range_after(begin, end).await?;
        self.decode_ready(kvs)
    }

    fn decode_ready(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Vec<Ready>> {
        let mut out = Vec::with_capacity(kvs.len());
        for (key, value) in kvs {
            let vs = self.ks.queue_key_versionstamp(&key)?;
            let envelope: Envelope = serde_json::from_slice(&value)?;
            out.push(Ready {
                msg_id: ids::encode_msg_id(&vs),
                versionstamp: vs,
                envelope,
            });
        }
        Ok(out)
    }

    async fn read_range(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.read_range_inner(KeySelector::first_greater_or_equal(begin), end)
            .await
    }

    async fn read_range_after(
        &self,
        begin: Vec<u8>,
        end: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.read_range_inner(KeySelector::first_greater_than(begin), end)
            .await
    }

    /// Read every key/value from `begin` (a selector) up to `end`, paged.
    async fn read_range_inner(
        &self,
        begin: KeySelector<'static>,
        end: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let out = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut opt = RangeOption::from((begin, KeySelector::first_greater_or_equal(end)));
                    opt.mode = StreamingMode::WantAll;
                    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                    let mut iteration = 1;
                    loop {
                        let kvs = trx.get_range(&opt, iteration, false).await?;
                        let last = kvs.iter().last().map(|kv| kv.key().to_vec());
                        for kv in kvs.iter() {
                            out.push((kv.key().to_vec(), kv.value().to_vec()));
                        }
                        if !kvs.more() {
                            break;
                        }
                        match last {
                            Some(k) => opt.begin = KeySelector::first_greater_than(k),
                            None => break,
                        }
                        iteration += 1;
                    }
                    Ok(out)
                }
            })
            .await?;
        Ok(out)
    }
}

fn read_le_i64(bytes: &[u8]) -> Option<i64> {
    let arr: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
    Some(i64::from_le_bytes(arr))
}
