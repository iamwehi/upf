//! End-to-end tests of the ntfy-compatible surface against a real FoundationDB.
//!
//! Exercises: ws subscribe → publish reaches `Q` → writer pokes the pusher's
//! inbox/bell → pusher streams the message; offline publish rests in `Q` and is
//! replayed via `since=all`; binary bodies ride as base64; and the UnifiedPush
//! endpoint check. Run with `scripts/cargo.sh test`.

use std::sync::{Arc, OnceLock};

use base64::Engine;
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use upf::AppState;
use upf::config::Config;
use upf::protocol::NtfyMessage;
use upf::pusher::Pusher;
use upf::store::Store;

fn ensure_boot() {
    static BOOT: OnceLock<()> = OnceLock::new();
    BOOT.get_or_init(|| {
        // SAFETY: called once; guard leaked so the network stays up for the test.
        let network = unsafe { foundationdb::boot() };
        std::mem::forget(network);
    });
}

/// Start an all-roles server on an ephemeral port; returns its base URL.
async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");

    let config = Arc::new(Config {
        public_url: base.clone(),
        node_id: format!("test-{}", uuid::Uuid::new_v4()),
        shard_count: 8,
        safety_poll_secs: 1,
        keepalive_secs: 3600, // keepalives out of the way for deterministic reads
        ..Config::default()
    });
    let store = Arc::new(Store::connect(config.shard_count).unwrap());
    let pusher = Some(Pusher::start(store.clone(), config.clone()));
    let state = AppState {
        store,
        config: config.clone(),
        pusher,
    };
    tokio::spawn(async move {
        axum::serve(listener, upf::router(state)).await.unwrap();
    });
    base
}

/// A fresh, valid, unique topic name.
fn fresh_topic() -> String {
    format!("uptest{}", uuid::Uuid::new_v4().simple())
}

async fn subscribe(base: &str, topic: &str, query: &str) -> WsRead {
    let ws_url = base.replace("http://", "ws://") + &format!("/{topic}/ws{query}");
    let (ws, _) = connect_async(&ws_url).await.unwrap();
    let (_write, read) = ws.split();
    WsRead { read }
}

struct WsRead {
    read: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
}

impl WsRead {
    async fn next(&mut self) -> NtfyMessage {
        loop {
            let msg = self.read.next().await.expect("stream ended").expect("ws error");
            if let Message::Text(t) = msg {
                return serde_json::from_str(t.as_str()).expect("bad ntfy message");
            }
        }
    }

    /// Skip control frames; return the next `message` event.
    async fn next_message(&mut self) -> NtfyMessage {
        loop {
            let m = self.next().await;
            if m.event == "message" {
                return m;
            }
        }
    }
}

#[tokio::test]
async fn subscribe_receives_published_message() {
    ensure_boot();
    let base = start_server().await;
    let topic = fresh_topic();
    let http = reqwest::Client::new();

    let mut sub = subscribe(&base, &topic, "").await;
    assert_eq!(sub.next().await.event, "open");

    let resp = http
        .post(format!("{base}/{topic}?up=1"))
        .body("hello-world")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let msg = sub.next_message().await;
    assert_eq!(msg.topic, topic);
    assert_eq!(msg.message.as_deref(), Some("hello-world"));
    assert_eq!(msg.encoding, "");
}

#[tokio::test]
async fn offline_publish_is_replayed_with_since_all() {
    ensure_boot();
    let base = start_server().await;
    let topic = fresh_topic();
    let http = reqwest::Client::new();

    // Publish while nobody is subscribed — it rests in Q.
    let resp = http
        .post(format!("{base}/{topic}?up=1"))
        .body("second")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Subscribe with since=all and expect the cached message replayed.
    let mut sub = subscribe(&base, &topic, "?since=all").await;
    let msg = sub.next_message().await;
    assert_eq!(msg.message.as_deref(), Some("second"));
}

#[tokio::test]
async fn binary_payload_is_base64() {
    ensure_boot();
    let base = start_server().await;
    let topic = fresh_topic();
    let http = reqwest::Client::new();

    let payload: &[u8] = &[0xff, 0xfe, 0x00, 0x01, 0x80];
    let resp = http
        .post(format!("{base}/{topic}?up=1"))
        .body(payload.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let mut sub = subscribe(&base, &topic, "?since=all").await;
    let msg = sub.next_message().await;
    assert_eq!(msg.encoding, "base64");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(msg.message.unwrap())
        .unwrap();
    assert_eq!(decoded, payload);
}

#[tokio::test]
async fn unifiedpush_endpoint_check() {
    ensure_boot();
    let base = start_server().await;
    let topic = fresh_topic();
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("{base}/{topic}?up=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["unifiedpush"]["version"], 1);

    // Without ?up=1 there is no web UI.
    let resp = http.get(format!("{base}/{topic}")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}
