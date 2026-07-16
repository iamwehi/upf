//! The FoundationDB access layer — every role's *only* means of communication.
//!
//! There is no service-to-service RPC anywhere in UPF: writers, pushers and the
//! janitor coordinate purely by reading and writing the keys defined in
//! [`crate::keyspace`]. This module holds the transactions that implement the
//! three algorithms from the design spec.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use foundationdb::options::{MutationType, StreamingMode};
use foundationdb::tuple::{Subspace, Versionstamp};
use foundationdb::{Database, FdbBindingError, KeySelector, RangeOption};

use crate::error::Result;
use crate::hash::shard_of;
use crate::keyspace::Keyspace;
use crate::model::{Envelope, MessageHeaders, Subscription};
use crate::token;

/// A pending FDB watch: resolves once the watched key changes.
pub type Watch = Pin<Box<dyn Future<Output = foundationdb::FdbResult<()>> + Send>>;

/// Outcome of accepting a push at the ingest edge.
#[derive(Debug, PartialEq, Eq)]
pub enum Ingest {
    /// No subscription exists for the token → HTTP 404.
    NotFound,
    /// Persisted to the queue (and poked, if a device was connected).
    Accepted,
}

/// One drained, ready-to-deliver message.
#[derive(Debug, Clone)]
pub struct Ready {
    pub msg_id: String,
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

    pub fn shard_of(&self, token: &str) -> u32 {
        shard_of(token, self.shard_count)
    }

    // ===== registration (S) ==================================================

    /// Mint a brand-new subscription and persist its `S` record.
    pub async fn create_subscription(
        &self,
        app_id: String,
        vapid_pubkey: Option<String>,
        now_secs: u64,
    ) -> Result<Subscription> {
        let sub = Subscription {
            token: token::new_endpoint_token(),
            app_id,
            vapid_pubkey,
            created_at_secs: now_secs,
        };
        let key = self.ks.subscription(&sub.token);
        let value = serde_json::to_vec(&sub)?;
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
        Ok(sub)
    }

    /// Fetch a subscription by endpoint token.
    pub async fn get_subscription(&self, token: &str) -> Result<Option<Subscription>> {
        let key = self.ks.subscription(token);
        let raw = self
            .db
            .run(|trx, _| {
                let key = key.clone();
                async move { Ok(trx.get(&key, false).await?.map(|v| v.to_vec())) }
            })
            .await?;
        match raw {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Delete a subscription and everything addressed by its token. Stale `X`
    /// (TTL) entries are left to the janitor — clearing an already-gone `Q` entry
    /// is a no-op, so they are harmless.
    pub async fn delete_subscription(&self, token: &str) -> Result<()> {
        let s_key = self.ks.subscription(token);
        let c_key = self.ks.affinity(token);
        let (q_begin, q_end) = self.ks.queue_range(token);
        // Topic-index entries live at (TI, token, *).
        let (ti_begin, ti_end) = Subspace::from("upf").subspace(&("TI", token)).range();
        self.db
            .run(|trx, _| {
                let s_key = s_key.clone();
                let c_key = c_key.clone();
                let (q_begin, q_end) = (q_begin.clone(), q_end.clone());
                let (ti_begin, ti_end) = (ti_begin.clone(), ti_end.clone());
                async move {
                    trx.clear(&s_key);
                    trx.clear(&c_key);
                    trx.clear_range(&q_begin, &q_end);
                    trx.clear_range(&ti_begin, &ti_end);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    // ===== writer: ingest (Q + TI + X + poke) ================================

    /// The writer's single transaction (design spec §"Writer algorithm").
    ///
    /// Verifies the subscription, appends the envelope to the durable queue,
    /// applies topic collapse, records the TTL index entry, and — if a device is
    /// currently connected (affinity present) — pokes that node's inbox and rings
    /// its shard bell. A missing/stale affinity is fine: the message rests in `Q`
    /// and is picked up on connect or by the safety poll.
    pub async fn ingest(
        &self,
        token: &str,
        body: &[u8],
        headers: MessageHeaders,
        now_secs: u64,
        default_ttl_secs: u64,
    ) -> Result<Ingest> {
        let ttl = headers.ttl.unwrap_or(default_ttl_secs);
        let expiry_secs = now_secs.saturating_add(ttl);
        let envelope = Envelope {
            body_b64: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, body),
            headers: headers.clone(),
            received_at_secs: now_secs,
            expiry_secs,
        };
        let envelope_bytes = serde_json::to_vec(&envelope)?;
        let shard = self.shard_of(token);

        let s_key = self.ks.subscription(token);
        let c_key = self.ks.affinity(token);
        let q_key = self.ks.queue_append(token);
        let x_key = self.ks.ttl_append(expiry_secs, token);
        let topic = headers.topic.clone();

        let accepted = self
            .db
            .run(|trx, _| {
                let s_key = s_key.clone();
                let c_key = c_key.clone();
                let q_key = q_key.clone();
                let x_key = x_key.clone();
                let envelope_bytes = envelope_bytes.clone();
                let topic = topic.clone();
                let token = token.to_string();
                let ks = self.ks.clone();
                async move {
                    // 1. Authenticate: the subscription must exist.
                    if trx.get(&s_key, false).await?.is_none() {
                        return Ok(false);
                    }

                    // 2. Topic collapse (RFC 8030 §5.4): a new message on a topic
                    //    supersedes the previous undelivered one.
                    if let Some(topic) = &topic {
                        let ti_key = ks.topic_index(&token, topic);
                        if let Some(prev) = trx.get(&ti_key, false).await? {
                            if let Ok((old_vs,)) =
                                foundationdb::tuple::unpack::<(Versionstamp,)>(&prev)
                            {
                                trx.clear(&ks.queue_msg(&token, &old_vs));
                            }
                        }
                        trx.atomic_op(
                            &ti_key,
                            &Keyspace::topic_index_value(),
                            MutationType::SetVersionstampedValue,
                        );
                    }

                    // 3. Append to the durable queue (source of truth).
                    trx.atomic_op(&q_key, &envelope_bytes, MutationType::SetVersionstampedKey);

                    // 4. TTL index entry (same stamp as the queue entry).
                    trx.atomic_op(&x_key, b"", MutationType::SetVersionstampedKey);

                    // 5. If a device is connected, poke its node. Stale affinity
                    //    is acceptable — never verified, never retried.
                    if let Some(node_bytes) = trx.get(&c_key, false).await? {
                        let node = String::from_utf8_lossy(&node_bytes).into_owned();
                        let in_key = ks.inbox_append(&node, shard);
                        trx.atomic_op(
                            &in_key,
                            token.as_bytes(),
                            MutationType::SetVersionstampedKey,
                        );
                        trx.atomic_op(
                            &ks.bell(&node, shard),
                            &1i64.to_le_bytes(),
                            MutationType::Add,
                        );
                    }
                    Ok(true)
                }
            })
            .await?;

        Ok(if accepted {
            Ingest::Accepted
        } else {
            Ingest::NotFound
        })
    }

    // ===== pusher: affinity (C) =============================================

    /// Claim this token for `node` (device connected here).
    pub async fn claim_affinity(&self, token: &str, node: &str) -> Result<()> {
        let key = self.ks.affinity(token);
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

    /// Compare-and-clear affinity: release the token only if it still points at
    /// `node`. Prevents a disconnecting node from stomping a newer owner's claim
    /// after the device migrated.
    pub async fn release_affinity_if(&self, token: &str, node: &str) -> Result<()> {
        let key = self.ks.affinity(token);
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

    // ===== pusher: drain (Q) + ack ==========================================

    /// Read a subscription's queue in order, ready for delivery. Paged to respect
    /// FDB's 5s / 10MB transaction limits.
    pub async fn drain(&self, token: &str) -> Result<Vec<Ready>> {
        let (begin, end) = self.ks.queue_range(token);
        let kvs = self.read_range(begin, end).await?;
        let mut out = Vec::with_capacity(kvs.len());
        for (key, value) in kvs {
            let vs = self.ks.queue_key_versionstamp(token, &key)?;
            let envelope: Envelope = serde_json::from_slice(&value)?;
            out.push(Ready {
                msg_id: token::encode_msg_id(&vs),
                envelope,
            });
        }
        Ok(out)
    }

    /// Clear one message from the queue in response to a device ack.
    pub async fn ack(&self, token: &str, msg_id: &str) -> Result<()> {
        let vs = token::decode_msg_id(msg_id)?;
        let key = self.ks.queue_msg(token, &vs);
        self.db
            .run(|trx, _| {
                let key = key.clone();
                async move {
                    trx.clear(&key);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    // ===== pusher: bells (SIG) + inbox (IN) =================================

    /// Arm a watch on a shard bell. The returned future resolves when any writer
    /// rings the bell. The watch is established at commit and outlives the
    /// transaction; drop the future to cancel it.
    pub async fn arm_bell(&self, node: &str, shard: u32) -> Result<Watch> {
        let key = self.ks.bell(node, shard);
        let trx = self.db.create_trx()?;
        let watch = trx.watch(&key);
        trx.commit().await.map_err(foundationdb::FdbError::from)?;
        Ok(Box::pin(watch))
    }

    /// Read and clear a `(node, shard)` inbox, returning the poked tokens. Only
    /// the entries actually read are cleared, so a poke racing this drain is left
    /// for the next bell rather than silently dropped.
    pub async fn take_inbox(&self, node: &str, shard: u32) -> Result<Vec<String>> {
        let (begin, end) = self.ks.inbox_range(node, shard);
        let tokens = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut opt = RangeOption::from(begin..end);
                    opt.mode = StreamingMode::WantAll;
                    let mut tokens = Vec::new();
                    let mut iteration = 1;
                    loop {
                        let kvs = trx.get_range(&opt, iteration, false).await?;
                        let last = kvs.iter().last().map(|kv| kv.key().to_vec());
                        for kv in kvs.iter() {
                            tokens.push(String::from_utf8_lossy(kv.value()).into_owned());
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
                    Ok(tokens)
                }
            })
            .await?;
        Ok(tokens)
    }

    // ===== liveness (L) =====================================================

    /// Refresh this node's heartbeat.
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
                        let (_, token, vs) = ks
                            .ttl_key_parts(kv.key())
                            .map_err(|e| FdbBindingError::new_custom_error(Box::new(e)))?;
                        trx.clear(&ks.queue_msg(&token, &vs));
                        trx.clear(kv.key());
                        n += 1;
                    }
                    Ok(n)
                }
            })
            .await?;
        Ok(swept)
    }

    /// Node ids seen alive at or after `cutoff_secs`.
    pub async fn live_nodes(&self, cutoff_secs: u64) -> Result<HashSet<String>> {
        let (begin, end) = self.ks.liveness_range();
        let kvs = self.read_range(begin, end).await?;
        let mut live = HashSet::new();
        for (key, value) in kvs {
            let node = self.ks.liveness_node(&key)?;
            let ts = read_le_i64(&value).unwrap_or(0);
            if ts >= cutoff_secs as i64 {
                live.insert(node);
            }
        }
        Ok(live)
    }

    /// Sweep records for nodes that have stopped heartbeating: clear their
    /// liveness entry plus every inbox and bell key for their shards. Pokes to a
    /// dead node are orphaned; the messages themselves survive in `Q`.
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

    // ===== shared range reader ==============================================

    /// Read every key/value in `[begin, end)`, paged, in key order.
    async fn read_range(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let out = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut opt = RangeOption::from(begin..end);
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
