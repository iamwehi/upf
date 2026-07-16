//! ntfy-compatible subscription transports: WebSocket, JSON stream, and SSE.
//!
//! All three share one setup ([`open_feed`]): resolve the `since` offset, register
//! the subscriber locally, claim affinity, emit an `open` frame, drain history,
//! and then stream live messages fed by the shard bells. The transports differ
//! only in how they serialize [`NtfyMessage`]s onto the wire.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use foundationdb::tuple::Versionstamp;
use futures_util::{SinkExt, StreamExt, stream};
use serde::Deserialize;
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::AppState;
use crate::error::Result;
use crate::ids;
use crate::protocol::NtfyMessage;
use crate::pusher::{Pusher, SubGuard, deliver_topic, spawn_keepalive};
use crate::store::Store;

/// Subscription query parameters (ntfy-compatible; unknown params are ignored).
#[derive(Debug, Default, Deserialize)]
pub struct SubParams {
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    poll: Option<String>,
}

/// Where a subscription should start reading from.
enum Since {
    /// Only messages published after this connection (skip history).
    Live,
    /// The entire cached queue.
    All,
    /// Strictly after a known message id (offset resume).
    After(Versionstamp),
    /// Everything received at/after a unix-seconds threshold.
    SinceTime(u64),
}

/// A subscription's message source: a live stream (with its cleanup guard) or a
/// one-shot poll snapshot.
enum Feed {
    Stream(UnboundedReceiver<NtfyMessage>, SubGuard),
    Poll(Vec<NtfyMessage>),
}

// ===== transport handlers ===================================================

/// `GET /{topic}/ws` — the default ntfy transport (JSON frames over WebSocket).
pub async fn ws(
    ws: WebSocketUpgrade,
    Path(topic): Path<String>,
    Query(params): Query<SubParams>,
    State(state): State<AppState>,
) -> Response {
    let ctx = match SubCtx::prepare(&state, topic) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    ws.on_upgrade(move |socket| serve_ws(socket, ctx, params))
}

/// `GET /{topic}/json` — newline-delimited JSON stream.
pub async fn json(
    Path(topic): Path<String>,
    Query(params): Query<SubParams>,
    State(state): State<AppState>,
) -> Response {
    let ctx = match SubCtx::prepare(&state, topic) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let feed = match ctx.open(&params).await {
        Ok(f) => f,
        Err(e) => return e.into_response(),
    };
    let body = Body::from_stream(feed.into_line_stream());
    ([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response()
}

/// `GET /{topic}/sse` — Server-Sent Events stream.
pub async fn sse(
    Path(topic): Path<String>,
    Query(params): Query<SubParams>,
    State(state): State<AppState>,
) -> Response {
    let ctx = match SubCtx::prepare(&state, topic) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let feed = match ctx.open(&params).await {
        Ok(f) => f,
        Err(e) => return e.into_response(),
    };
    let events = feed
        .into_message_stream()
        .map(|msg| Ok::<_, std::convert::Infallible>(Event::default().data(to_json(&msg))));
    Sse::new(events).into_response()
}

// ===== shared subscription context ==========================================

/// Validated per-request context: the store, pusher, and topic.
struct SubCtx {
    store: Arc<Store>,
    pusher: Arc<Pusher>,
    topic: String,
}

impl SubCtx {
    /// Validate the role and topic, or produce the error response to return.
    fn prepare(state: &AppState, topic: String) -> std::result::Result<SubCtx, Response> {
        let Some(pusher) = state.pusher.clone() else {
            return Err((StatusCode::NOT_FOUND, "pusher role not enabled here").into_response());
        };
        if !ids::valid_topic(&topic) {
            return Err((StatusCode::BAD_REQUEST, "invalid topic").into_response());
        }
        Ok(SubCtx {
            store: state.store.clone(),
            pusher,
            topic,
        })
    }

    /// Build the message feed for this subscription (streaming or one-shot poll).
    async fn open(&self, params: &SubParams) -> Result<Feed> {
        let since = parse_since(params.since.as_deref(), now_secs());
        if truthy(params.poll.as_deref()) {
            return Ok(Feed::Poll(self.collect_poll(since).await?));
        }
        self.open_stream(since).await
    }

    /// Register the live subscription and return its channel + cleanup guard.
    async fn open_stream(&self, since: Since) -> Result<Feed> {
        let (tx, rx) = mpsc::unbounded_channel::<NtfyMessage>();

        // Resolve the starting cursor. For SinceTime we stream history manually
        // below and then continue live after the newest existing message.
        let cursor = match &since {
            Since::All => None,
            Since::Live | Since::SinceTime(_) => self.store.latest(&self.topic).await?,
            Since::After(vs) => Some(vs.clone()),
        };

        self.pusher
            .local
            .attach(self.topic.clone(), tx.clone(), cursor);
        self.store
            .claim_affinity(&self.topic, &self.pusher.node_id)
            .await?;

        let _ = tx.send(NtfyMessage::open(
            &self.topic,
            ids::ephemeral_id(),
            now_secs() as i64,
        ));

        if let Since::SinceTime(t) = since {
            for r in self.store.drain_all(&self.topic).await? {
                if r.envelope.received_at_secs >= t {
                    let _ = tx.send(NtfyMessage::message(&self.topic, r.msg_id, &r.envelope));
                }
            }
        }

        // Deliver anything the cursor covers plus messages poked during setup.
        deliver_topic(&self.store, &self.pusher.local, &self.topic).await;

        spawn_keepalive(tx.clone(), self.topic.clone(), self.pusher.keepalive_secs);

        let guard = SubGuard::new(
            self.store.clone(),
            self.pusher.local.clone(),
            self.topic.clone(),
            self.pusher.node_id.clone(),
            tx,
        );
        Ok(Feed::Stream(rx, guard))
    }

    /// One-shot poll: return the matching cached messages, no live subscription.
    async fn collect_poll(&self, since: Since) -> Result<Vec<NtfyMessage>> {
        let floor = if let Since::SinceTime(t) = &since { *t } else { 0 };
        let ready = match since {
            Since::All | Since::SinceTime(_) => self.store.drain_all(&self.topic).await?,
            Since::After(vs) => self.store.drain_after(&self.topic, &vs).await?,
            Since::Live => Vec::new(),
        };
        Ok(ready
            .into_iter()
            .filter(|r| r.envelope.received_at_secs >= floor)
            .map(|r| NtfyMessage::message(&self.topic, r.msg_id, &r.envelope))
            .collect())
    }
}

/// Drive a WebSocket subscription: forward feed frames as JSON text, and watch
/// the socket for close. The `SubGuard` (held in the feed) cleans up on return.
async fn serve_ws(socket: WebSocket, ctx: SubCtx, params: SubParams) {
    let feed = match ctx.open(&params).await {
        Ok(f) => f,
        Err(e) => {
            // Best-effort: tell the client, then close.
            let mut socket = socket;
            let _ = socket
                .send(Message::Text(format!("{{\"error\":\"{e}\"}}").into()))
                .await;
            return;
        }
    };

    let (mut sink, mut stream) = socket.split();
    match feed {
        Feed::Poll(msgs) => {
            for msg in msgs {
                if sink.send(Message::Text(to_json(&msg).into())).await.is_err() {
                    return;
                }
            }
            let _ = sink.send(Message::Close(None)).await;
        }
        Feed::Stream(mut rx, _guard) => loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(msg) => {
                        if sink.send(Message::Text(to_json(&msg).into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
                incoming = stream.next() => match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => continue, // ignore client frames (pings handled by axum)
                    Some(Err(_)) => break,
                },
            }
        },
    }
}

// ===== feed → wire helpers ==================================================

impl Feed {
    /// A stream of newline-terminated JSON lines (for the JSON transport).
    fn into_line_stream(
        self,
    ) -> impl futures_util::Stream<Item = std::result::Result<String, std::convert::Infallible>> {
        self.into_message_stream()
            .map(|msg| Ok(to_json(&msg) + "\n"))
    }

    /// A stream of `NtfyMessage`s, keeping any cleanup guard alive until drained.
    fn into_message_stream(self) -> impl futures_util::Stream<Item = NtfyMessage> {
        match self {
            Feed::Poll(msgs) => stream::iter(msgs).left_stream(),
            // The guard rides in the unfold state, so it drops (→ cleanup) exactly
            // when the response stream is dropped on client disconnect.
            Feed::Stream(rx, guard) => stream::unfold((rx, guard), |(mut rx, guard)| async move {
                rx.recv().await.map(|msg| (msg, (rx, guard)))
            })
            .right_stream(),
        }
    }
}

// ===== since parsing ========================================================

fn parse_since(raw: Option<&str>, now: u64) -> Since {
    let Some(v) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Since::Live;
    };
    if v.eq_ignore_ascii_case("all") {
        return Since::All;
    }
    if let Ok(vs) = ids::decode_msg_id(v) {
        return Since::After(vs);
    }
    if let Ok(secs) = v.parse::<u64>() {
        return Since::SinceTime(secs);
    }
    if let Some(dur) = parse_duration(v) {
        return Since::SinceTime(now.saturating_sub(dur));
    }
    Since::All // Unknown form: over-deliver rather than silently drop history.
}

/// Parse a simple `<n><unit>` duration (s/m/h/d) into seconds.
fn parse_duration(s: &str) -> Option<u64> {
    let (num, unit) = s.split_at(s.len().checked_sub(1)?);
    let n: u64 = num.parse().ok()?;
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => return None,
    };
    Some(n.saturating_mul(mult))
}

fn truthy(v: Option<&str>) -> bool {
    matches!(
        v.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on" | "")
    )
}

fn to_json(msg: &NtfyMessage) -> String {
    serde_json::to_string(msg).unwrap_or_else(|_| "{}".to_string())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
