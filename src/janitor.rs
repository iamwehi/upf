//! Janitor role — the only background reclaimer.
//!
//! It never delivers anything; it just keeps the keyspace bounded:
//!  1. **TTL expiry** — scan the `X` index for due entries and delete the
//!     referenced `Q` messages. This is the load-bearing job.
//!  2. **Dead-node sweep** — clear inbox/bell/liveness records for nodes that
//!     stopped heartbeating. Purely hygiene: those pokes are advisory and the
//!     messages they point at are safe in `Q`.

use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::store::Store;

/// Max expired messages to reap per transaction, keeping each pass well within
/// FDB's 5s / 10MB limits. Successive passes drain any backlog.
const EXPIRE_BATCH: usize = 1000;

/// Consider a node dead after this many missed heartbeat intervals.
const DEAD_NODE_MISSES: u64 = 6;

/// Run the janitor loop until the process exits.
pub fn start(store: Arc<Store>, config: Arc<Config>) {
    let period = Duration::from_secs(config.janitor_interval_secs.max(1));
    let dead_after = config
        .heartbeat_secs
        .saturating_mul(DEAD_NODE_MISSES)
        .max(1);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tracing::info!(
            interval_secs = config.janitor_interval_secs,
            "janitor started"
        );
        loop {
            tick.tick().await;
            let now = now_secs();

            // 1. Expire due messages, in bounded batches until the pass is clear.
            let mut total = 0usize;
            loop {
                match store.expire(now, EXPIRE_BATCH).await {
                    Ok(n) => {
                        total += n;
                        if n < EXPIRE_BATCH {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "expire pass failed");
                        break;
                    }
                }
            }
            if total > 0 {
                tracing::debug!(expired = total, "janitor reaped expired messages");
            }

            // 2. Sweep records for nodes that stopped heartbeating.
            let cutoff = now.saturating_sub(dead_after);
            match store.sweep_dead_nodes(cutoff).await {
                Ok(n) if n > 0 => tracing::info!(nodes = n, "swept dead node records"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "dead-node sweep failed"),
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
