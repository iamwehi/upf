//! End-to-end walking-skeleton test against a real FoundationDB.
//!
//! Requires the FDB network (booted once here) and a reachable cluster via
//! `FDB_CLUSTER_FILE`. Run with `scripts/cargo.sh test`.

use std::sync::{Arc, OnceLock};

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use upf::AppState;
use upf::config::Config;
use upf::distributor::protocol::{ClientFrame, ServerFrame};
use upf::distributor::registry::Registry;
use upf::storage::Storage;

/// Boot the FDB client network exactly once for the whole test binary.
fn ensure_boot() {
    static BOOT: OnceLock<()> = OnceLock::new();
    BOOT.get_or_init(|| {
        // SAFETY: called once; guard is intentionally leaked so the network
        // stays up for the lifetime of the test process.
        let network = unsafe { foundationdb::boot() };
        std::mem::forget(network);
    });
}

/// Start the server on an ephemeral port; returns its base URL.
async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");

    let config = Config {
        public_url: base.clone(),
        ..Config::default()
    };
    let state = AppState {
        storage: Arc::new(Storage::connect().unwrap()),
        registry: Arc::new(Registry::new()),
        config: Arc::new(config),
    };
    tokio::spawn(async move {
        axum::serve(listener, upf::router(state)).await.unwrap();
    });
    base
}

async fn send<S>(write: &mut S, frame: &ClientFrame)
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let txt = serde_json::to_string(frame).unwrap();
    write.send(Message::Text(txt.into())).await.unwrap();
}

async fn next_frame<S>(read: &mut S) -> ServerFrame
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let msg = read.next().await.expect("stream ended").expect("ws error");
        if let Message::Text(t) = msg {
            return serde_json::from_str(t.as_str()).expect("bad server frame");
        }
    }
}

#[tokio::test]
async fn register_deliver_and_replay() {
    ensure_boot();
    let base = start_server().await;
    let ws_url = base.replace("http://", "ws://") + "/distributor/ws";
    let dist_id = format!("test-dist-{}", uuid::Uuid::new_v4());
    let http = reqwest::Client::new();

    // ---- connect, register, capture endpoint --------------------------------
    let (ws, _) = connect_async(&ws_url).await.unwrap();
    let (mut write, mut read) = ws.split();
    send(&mut write, &ClientFrame::Hello {
        distributor_id: Some(dist_id.clone()),
    })
    .await;
    matches_welcome(next_frame(&mut read).await, &dist_id);

    send(&mut write, &ClientFrame::Register {
        app_id: "app-1".into(),
        vapid: None,
    })
    .await;
    let (endpoint, endpoint_token) = match next_frame(&mut read).await {
        ServerFrame::Registered {
            endpoint,
            endpoint_token,
            ..
        } => (endpoint, endpoint_token),
        other => panic!("expected registered, got {other:?}"),
    };

    // ---- online delivery ----------------------------------------------------
    let resp = http
        .post(&endpoint)
        .header("TTL", "60")
        .body("hello-world")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let msg_id = match next_frame(&mut read).await {
        ServerFrame::Message {
            body_b64,
            msg_id,
            endpoint_token: t,
            ..
        } => {
            assert_eq!(t, endpoint_token);
            assert_eq!(decode(&body_b64), b"hello-world");
            msg_id
        }
        other => panic!("expected message, got {other:?}"),
    };
    send(&mut write, &ClientFrame::Ack {
        endpoint_token: endpoint_token.clone(),
        msg_id,
    })
    .await;

    // ---- offline replay -----------------------------------------------------
    // Drop the connection, push while offline, reconnect and expect replay.
    drop(write);
    drop(read);
    // Give the server a moment to process the ack and the disconnect.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let resp = http.post(&endpoint).body("second").send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let (ws, _) = connect_async(&ws_url).await.unwrap();
    let (mut write, mut read) = ws.split();
    send(&mut write, &ClientFrame::Hello {
        distributor_id: Some(dist_id.clone()),
    })
    .await;
    matches_welcome(next_frame(&mut read).await, &dist_id);

    // The replayed queue must contain "second" (and only unacked messages).
    let replayed = match next_frame(&mut read).await {
        ServerFrame::Message { body_b64, .. } => decode(&body_b64),
        other => panic!("expected replayed message, got {other:?}"),
    };
    assert_eq!(replayed, b"second");
}

#[tokio::test]
async fn unknown_endpoint_is_404() {
    ensure_boot();
    let base = start_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/push/does-not-exist"))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

fn matches_welcome(frame: ServerFrame, expected: &str) {
    match frame {
        ServerFrame::Welcome { distributor_id } => assert_eq!(distributor_id, expected),
        other => panic!("expected welcome, got {other:?}"),
    }
}

fn decode(b64: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(b64).unwrap()
}
