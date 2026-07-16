//! WebSocket upgrade + per-connection state machine.

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::AppState;
use crate::distributor::protocol::{ClientFrame, ServerFrame};
use crate::storage::{QueuedMessage, Subscription};
use crate::token;

/// Axum handler: upgrade the HTTP request to a WebSocket.
pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| connection(socket, state))
}

/// Drive one distributor connection to completion.
async fn connection(socket: WebSocket, state: AppState) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerFrame>();

    // Writer task: serialize outbound frames onto the wire.
    let writer = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            let Ok(txt) = serde_json::to_string(&frame) else {
                continue;
            };
            if ws_tx.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    let mut distributor_id: Option<String> = None;

    while let Some(Ok(msg)) = ws_rx.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            // Binary / Ping / Pong: not part of our protocol; ignore.
            _ => continue,
        };

        let frame: ClientFrame = match serde_json::from_str(text.as_str()) {
            Ok(f) => f,
            Err(e) => {
                send(&out_tx, ServerFrame::Error {
                    reason: format!("invalid frame: {e}"),
                });
                continue;
            }
        };

        if let Err(e) = handle_frame(&state, &out_tx, &mut distributor_id, frame).await {
            tracing::warn!(error = %e, "frame handling failed");
            send(&out_tx, ServerFrame::Error {
                reason: e.to_string(),
            });
        }
    }

    // Teardown.
    if let Some(id) = &distributor_id {
        state.registry.remove_if(id, &out_tx);
    }
    drop(out_tx);
    writer.abort();
}

async fn handle_frame(
    state: &AppState,
    out_tx: &UnboundedSender<ServerFrame>,
    distributor_id: &mut Option<String>,
    frame: ClientFrame,
) -> crate::error::Result<()> {
    match frame {
        ClientFrame::Hello { distributor_id: id } => {
            let id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            state.registry.insert(id.clone(), out_tx.clone());
            *distributor_id = Some(id.clone());
            send(out_tx, ServerFrame::Welcome {
                distributor_id: id.clone(),
            });
            replay_queued(state, out_tx, &id).await?;
        }

        ClientFrame::Register { app_id, vapid } => {
            let id = require_hello(distributor_id)?;
            let token = token::new_endpoint_token();
            let sub = Subscription {
                token: token.clone(),
                distributor_id: id.clone(),
                app_id: app_id.clone(),
                vapid_pubkey: vapid,
                created_at_micros: now_micros(),
            };
            state.storage.put_subscription(&sub).await?;
            send(out_tx, ServerFrame::Registered {
                app_id,
                endpoint: state.config.endpoint_for(&token),
                endpoint_token: token,
            });
        }

        ClientFrame::Unregister { endpoint_token } => {
            let id = require_hello(distributor_id)?;
            state
                .storage
                .delete_subscription(&endpoint_token, &id)
                .await?;
            send(out_tx, ServerFrame::Unregistered { endpoint_token });
        }

        ClientFrame::Ack {
            endpoint_token,
            msg_id,
        } => {
            require_hello(distributor_id)?;
            state.storage.ack(&endpoint_token, &msg_id).await?;
        }

        ClientFrame::Ping => send(out_tx, ServerFrame::Pong),
    }
    Ok(())
}

/// On (re)connect, resend every un-acked message across all of the
/// distributor's subscriptions.
async fn replay_queued(
    state: &AppState,
    out_tx: &UnboundedSender<ServerFrame>,
    distributor_id: &str,
) -> crate::error::Result<()> {
    let tokens = state
        .storage
        .list_tokens_for_distributor(distributor_id)
        .await?;
    for endpoint_token in tokens {
        let queued = state.storage.list_queue(&endpoint_token).await?;
        for msg in queued {
            send(out_tx, message_frame(&endpoint_token, msg));
        }
    }
    Ok(())
}

/// Build a `Message` server frame from a queued message.
pub fn message_frame(endpoint_token: &str, msg: QueuedMessage) -> ServerFrame {
    ServerFrame::Message {
        endpoint_token: endpoint_token.to_string(),
        msg_id: msg.msg_id,
        body_b64: msg.body_b64,
        headers: msg.headers,
    }
}

fn require_hello(distributor_id: &Option<String>) -> crate::error::Result<String> {
    distributor_id
        .clone()
        .ok_or_else(|| crate::error::Error::BadRequest("send `hello` first".into()))
}

fn send(out_tx: &UnboundedSender<ServerFrame>, frame: ServerFrame) {
    // A closed channel just means the connection is going away; ignore.
    let _ = out_tx.send(frame);
}

fn now_micros() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0)
}
