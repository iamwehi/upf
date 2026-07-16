//! A minimal ntfy subscriber, for manual testing without the real ntfy app.
//!
//! Connects to `<UPF_WS_URL>` (an ntfy `/{topic}/ws` endpoint), prints the
//! `open`/`keepalive`/`message` frames, and base64-decodes binary payloads.
//!
//! ```text
//! # subscribe to a topic:
//! UPF_TOPIC=upDEMO0001 cargo run --example mock_distributor
//! # then publish to it (UnifiedPush raw mode):
//! curl -X POST --data 'hi there' 'http://localhost:8080/upDEMO0001?up=1'
//! ```
//!
//! Env: UPF_WS_BASE (default ws://localhost:8080), UPF_TOPIC (default upDEMO0001),
//!      UPF_SINCE (optional ntfy `since=` value, e.g. `all`).

use base64::Engine;
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use upf::protocol::NtfyMessage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let base = std::env::var("UPF_WS_BASE").unwrap_or_else(|_| "ws://localhost:8080".to_string());
    let topic = std::env::var("UPF_TOPIC").unwrap_or_else(|_| "upDEMO0001".to_string());
    let since = std::env::var("UPF_SINCE").ok();

    let mut url = format!("{base}/{topic}/ws");
    if let Some(since) = &since {
        url.push_str(&format!("?since={since}"));
    }
    println!("subscribing: {url}");

    let (ws, _resp) = connect_async(&url).await?;
    let (_write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        let text = match msg? {
            Message::Text(t) => t.to_string(),
            Message::Close(_) => break,
            _ => continue,
        };
        let m: NtfyMessage = serde_json::from_str(&text)?;
        match m.event.as_str() {
            "open" => println!("✓ subscribed to {}", m.topic),
            "keepalive" => println!("· keepalive"),
            "message" => {
                let body = m.message.unwrap_or_default();
                let shown = if m.encoding == "base64" {
                    let raw = base64::engine::general_purpose::STANDARD
                        .decode(&body)
                        .unwrap_or_default();
                    format!("{:?} (base64, {} bytes)", String::from_utf8_lossy(&raw), raw.len())
                } else {
                    format!("{body:?}")
                };
                println!("→ message {}: {shown}", m.id);
            }
            other => println!("frame {other}: {text}"),
        }
    }
    Ok(())
}
