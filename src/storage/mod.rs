//! FoundationDB-backed persistence: subscriptions and per-subscription queues.

pub mod keys;

use foundationdb::options::{MutationType, StreamingMode};
use foundationdb::{Database, KeySelector, RangeOption};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::storage::keys::Keys;

/// A registered subscription: the mapping from an opaque endpoint token to the
/// distributor that should receive its messages.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscription {
    pub token: String,
    pub distributor_id: String,
    /// Application instance identifier chosen by the connector (opaque to us).
    pub app_id: String,
    /// Optional VAPID public key the application server must authenticate with.
    /// Stored but not yet verified (follow-up milestone).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vapid_pubkey: Option<String>,
    pub created_at_micros: u128,
}

/// A push message persisted for delivery. Kept until the distributor acks it,
/// providing at-least-once delivery and offline replay.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuedMessage {
    pub msg_id: String,
    /// The (already-encrypted, per RFC 8291) push body, base64-encoded for JSON.
    pub body_b64: String,
    /// Selected WebPush headers we forward verbatim (TTL, Topic, Urgency, …).
    #[serde(default)]
    pub headers: MessageHeaders,
    pub received_at_micros: u128,
}

/// Subset of WebPush request headers we care about (RFC 8030).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MessageHeaders {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urgency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
}

/// Handle to the FoundationDB cluster plus our key layout.
pub struct Storage {
    db: Database,
    keys: Keys,
}

impl Storage {
    /// Connect using the cluster file named by `FDB_CLUSTER_FILE` (or the
    /// system default). Requires the FDB network to have been booted.
    pub fn connect() -> Result<Self> {
        let db = Database::default()?;
        Ok(Self {
            db,
            keys: Keys::new(),
        })
    }

    #[cfg(test)]
    pub fn with_database(db: Database) -> Self {
        Self {
            db,
            keys: Keys::new(),
        }
    }

    // ---- subscriptions ------------------------------------------------------

    /// Create (or overwrite) a subscription and index it under its distributor.
    pub async fn put_subscription(&self, sub: &Subscription) -> Result<()> {
        let sub_key = self.keys.subscription(&sub.token);
        let idx_key = self.keys.dist_sub_index(&sub.distributor_id, &sub.token);
        let value = serde_json::to_vec(sub)?;
        self.db
            .run(|trx, _| {
                let sub_key = sub_key.clone();
                let idx_key = idx_key.clone();
                let value = value.clone();
                async move {
                    trx.set(&sub_key, &value);
                    trx.set(&idx_key, b"");
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    /// Look up a subscription by its endpoint token.
    pub async fn get_subscription(&self, token: &str) -> Result<Option<Subscription>> {
        let key = self.keys.subscription(token);
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

    /// Delete a subscription, its index entry, and any queued messages.
    pub async fn delete_subscription(&self, token: &str, distributor_id: &str) -> Result<()> {
        let sub_key = self.keys.subscription(token);
        let idx_key = self.keys.dist_sub_index(distributor_id, token);
        let (qbegin, qend) = self.keys.queue_range(token).range();
        self.db
            .run(|trx, _| {
                let sub_key = sub_key.clone();
                let idx_key = idx_key.clone();
                let qbegin = qbegin.clone();
                let qend = qend.clone();
                async move {
                    trx.clear(&sub_key);
                    trx.clear(&idx_key);
                    trx.clear_range(&qbegin, &qend);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    /// List every subscription token belonging to a distributor.
    pub async fn list_tokens_for_distributor(&self, distributor_id: &str) -> Result<Vec<String>> {
        let subspace = self.keys.dist_subs_range(distributor_id);
        let (begin, end) = subspace.range();
        let keys = self.range_keys(begin, end).await?;
        Ok(keys
            .iter()
            .filter_map(|k| self.keys.dist_sub_token(distributor_id, k))
            .collect())
    }

    // ---- message queue ------------------------------------------------------

    /// Persist a message for a subscription and bump the distributor version.
    pub async fn enqueue(
        &self,
        token: &str,
        distributor_id: &str,
        msg: &QueuedMessage,
    ) -> Result<()> {
        let msg_key = self.keys.queue_msg(token, &msg.msg_id);
        let ver_key = self.keys.dist_version(distributor_id);
        let value = serde_json::to_vec(msg)?;
        self.db
            .run(|trx, _| {
                let msg_key = msg_key.clone();
                let ver_key = ver_key.clone();
                let value = value.clone();
                async move {
                    trx.set(&msg_key, &value);
                    // Atomic +1, little-endian — reserved for FDB-watch fan-out.
                    trx.atomic_op(&ver_key, &1u64.to_le_bytes(), MutationType::Add);
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    /// Return all un-acked messages for a subscription, in arrival order.
    pub async fn list_queue(&self, token: &str) -> Result<Vec<QueuedMessage>> {
        let subspace = self.keys.queue_range(token);
        let (begin, end) = subspace.range();
        let values = self.range_values(begin, end).await?;
        values
            .iter()
            .map(|v| serde_json::from_slice(v).map_err(Into::into))
            .collect()
    }

    /// Remove a single message from a subscription's queue (on distributor ack).
    pub async fn ack(&self, token: &str, msg_id: &str) -> Result<()> {
        let key = self.keys.queue_msg(token, msg_id);
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

    // ---- range helpers ------------------------------------------------------

    /// Read every value in `[begin, end)` (paginated, in key order).
    async fn range_values(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<Vec<u8>>> {
        let out = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut opt = RangeOption::from(begin..end);
                    opt.mode = StreamingMode::WantAll;
                    let mut out = Vec::new();
                    let mut iteration = 1;
                    loop {
                        let values = trx.get_range(&opt, iteration, false).await?;
                        let last_key = values.iter().last().map(|kv| kv.key().to_vec());
                        for kv in values.iter() {
                            out.push(kv.value().to_vec());
                        }
                        if !values.more() {
                            break;
                        }
                        match last_key {
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

    /// Read every key in `[begin, end)` (paginated, in key order).
    async fn range_keys(&self, begin: Vec<u8>, end: Vec<u8>) -> Result<Vec<Vec<u8>>> {
        let out = self
            .db
            .run(|trx, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    let mut opt = RangeOption::from(begin..end);
                    opt.mode = StreamingMode::WantAll;
                    let mut out: Vec<Vec<u8>> = Vec::new();
                    let mut iteration = 1;
                    loop {
                        let values = trx.get_range(&opt, iteration, false).await?;
                        let last_key = values.iter().last().map(|kv| kv.key().to_vec());
                        for kv in values.iter() {
                            out.push(kv.key().to_vec());
                        }
                        if !values.more() {
                            break;
                        }
                        match last_key {
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
