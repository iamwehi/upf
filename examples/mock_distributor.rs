//! A minimal UnifiedPush distributor for manual testing.
//!
//! Connects over WebSocket, then either registers a fresh application instance
//! (printing the endpoint URL + token) or re-subscribes an existing token, and
//! prints (auto-acking) every push message it receives.
//!
//! ```text
//! cargo run --example mock_distributor
//! # then POST to the printed endpoint, e.g.:
//! curl -X POST --data 'hi there' <endpoint>
//! #
//! # to resume an existing subscription (e.g. after a restart):
//! UPF_SUB_TOKEN=<token> cargo run --example mock_distributor
//! ```
//!
//! Env: UPF_WS_URL (default ws://localhost:8080/distributor/ws),
//!      UPF_DIST_ID (default "mock-distributor"),
//!      UPF_APP_ID  (default "demo-app"),
//!      UPF_SUB_TOKEN (optional: subscribe this token instead of registering).

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use upf::protocol::{ClientFrame, ServerFrame};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("UPF_WS_URL")
        .unwrap_or_else(|_| "ws://localhost:8080/distributor/ws".to_string());
    let dist_id = std::env::var("UPF_DIST_ID").unwrap_or_else(|_| "mock-distributor".to_string());
    let app_id = std::env::var("UPF_APP_ID").unwrap_or_else(|_| "demo-app".to_string());
    let sub_token = std::env::var("UPF_SUB_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());

    let (ws, _resp) = connect_async(&url).await?;
    let (mut write, mut read) = ws.split();

    let send = |frame: &ClientFrame| serde_json::to_string(frame);
    write
        .send(Message::Text(
            send(&ClientFrame::Hello {
                distributor_id: Some(dist_id),
            })?
            .into(),
        ))
        .await?;

    match &sub_token {
        Some(token) => {
            println!("re-subscribing existing token {token}");
            write
                .send(Message::Text(
                    send(&ClientFrame::Subscribe {
                        endpoint_token: token.clone(),
                    })?
                    .into(),
                ))
                .await?;
        }
        None => {
            write
                .send(Message::Text(
                    send(&ClientFrame::Register {
                        app_id,
                        vapid: None,
                    })?
                    .into(),
                ))
                .await?;
        }
    }

    println!("listening for push messages (Ctrl-C to quit)...");
    while let Some(msg) = read.next().await {
        let text = match msg? {
            Message::Text(t) => t.to_string(),
            Message::Close(_) => break,
            _ => continue,
        };
        match serde_json::from_str::<ServerFrame>(&text)? {
            ServerFrame::Welcome { distributor_id } => {
                println!("✓ connected as {distributor_id}");
            }
            ServerFrame::Registered {
                endpoint,
                endpoint_token,
                ..
            } => {
                println!("✓ endpoint: {endpoint}");
                println!("  (token: {endpoint_token} — set UPF_SUB_TOKEN to resume it)");
            }
            ServerFrame::Subscribed { endpoint_token } => {
                println!("✓ subscribed {endpoint_token}");
            }
            ServerFrame::Message {
                endpoint_token,
                msg_id,
                body_b64,
                headers,
            } => {
                let body = base64::engine::general_purpose::STANDARD
                    .decode(&body_b64)
                    .unwrap_or_default();
                println!(
                    "→ message {msg_id}: {:?}  headers={headers:?}",
                    String::from_utf8_lossy(&body)
                );
                // Auto-ack so the server can drop it from the queue.
                write
                    .send(Message::Text(
                        send(&ClientFrame::Ack {
                            endpoint_token,
                            msg_id,
                        })?
                        .into(),
                    ))
                    .await?;
            }
            ServerFrame::Error { reason } => eprintln!("! server error: {reason}"),
            other => println!("frame: {other:?}"),
        }
    }
    Ok(())
}
