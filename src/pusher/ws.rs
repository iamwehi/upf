//! Distributor WebSocket upgrade + per-connection state machine.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::AppState;
use crate::error::{Error, Result};
use crate::protocol::{ClientFrame, ServerFrame};
use crate::pusher::{Pusher, deliver_token};
use crate::store::Store;

/// Axum handler: upgrade the HTTP request to a WebSocket. Rejects if this process
/// does not run the pusher role.
pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let Some(pusher) = state.pusher.clone() else {
        return (StatusCode::NOT_FOUND, "pusher role not enabled here").into_response();
    };
    ws.on_upgrade(move |socket| connection(socket, state.store.clone(), pusher))
}

/// Drive one distributor connection to completion.
async fn connection(socket: WebSocket, store: Arc<Store>, pusher: Arc<Pusher>) {
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
    // Tokens this connection currently holds, so we can release them on teardown.
    let mut owned: HashSet<String> = HashSet::new();

    while let Some(Ok(msg)) = ws_rx.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue, // Binary / Ping / Pong: not part of our protocol.
        };

        let frame: ClientFrame = match serde_json::from_str(text.as_str()) {
            Ok(f) => f,
            Err(e) => {
                send(
                    &out_tx,
                    ServerFrame::Error {
                        reason: format!("invalid frame: {e}"),
                    },
                );
                continue;
            }
        };

        if let Err(e) = handle_frame(
            &store,
            &pusher,
            &out_tx,
            &mut distributor_id,
            &mut owned,
            frame,
        )
        .await
        {
            tracing::warn!(error = %e, "frame handling failed");
            send(
                &out_tx,
                ServerFrame::Error {
                    reason: e.to_string(),
                },
            );
        }
    }

    // Teardown: drop local bindings and release affinity we still own.
    for token in &owned {
        pusher.local.detach_if(token, &out_tx);
        if let Err(e) = store.release_affinity_if(token, &pusher.node_id).await {
            tracing::warn!(error = %e, %token, "failed to release affinity");
        }
    }
    drop(out_tx);
    writer.abort();
}

async fn handle_frame(
    store: &Arc<Store>,
    pusher: &Arc<Pusher>,
    out_tx: &UnboundedSender<ServerFrame>,
    distributor_id: &mut Option<String>,
    owned: &mut HashSet<String>,
    frame: ClientFrame,
) -> Result<()> {
    match frame {
        ClientFrame::Hello { distributor_id: id } => {
            let id = id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            *distributor_id = Some(id.clone());
            send(out_tx, ServerFrame::Welcome { distributor_id: id });
        }

        ClientFrame::Register { app_id, vapid } => {
            require_hello(distributor_id)?;
            let sub = store
                .create_subscription(app_id.clone(), vapid, now_secs())
                .await?;
            attach(store, pusher, out_tx, owned, &sub.token).await?;
            send(
                out_tx,
                ServerFrame::Registered {
                    app_id,
                    endpoint: pusher_endpoint(pusher, &sub.token),
                    endpoint_token: sub.token,
                },
            );
        }

        ClientFrame::Subscribe { endpoint_token } => {
            require_hello(distributor_id)?;
            if store.get_subscription(&endpoint_token).await?.is_none() {
                return Err(Error::NotFound);
            }
            attach(store, pusher, out_tx, owned, &endpoint_token).await?;
            send(out_tx, ServerFrame::Subscribed { endpoint_token });
        }

        ClientFrame::Unregister { endpoint_token } => {
            require_hello(distributor_id)?;
            store.delete_subscription(&endpoint_token).await?;
            owned.remove(&endpoint_token);
            pusher.local.detach_if(&endpoint_token, out_tx);
            send(out_tx, ServerFrame::Unregistered { endpoint_token });
        }

        ClientFrame::Ack {
            endpoint_token,
            msg_id,
        } => {
            require_hello(distributor_id)?;
            store.ack(&endpoint_token, &msg_id).await?;
        }

        ClientFrame::Ping => send(out_tx, ServerFrame::Pong),
    }
    Ok(())
}

/// Attach a token to this connection: record it locally, claim affinity so
/// future pushes route here, and drain-on-connect (the completeness rule).
async fn attach(
    store: &Arc<Store>,
    pusher: &Arc<Pusher>,
    out_tx: &UnboundedSender<ServerFrame>,
    owned: &mut HashSet<String>,
    token: &str,
) -> Result<()> {
    pusher.local.attach(token.to_string(), out_tx.clone());
    store.claim_affinity(token, &pusher.node_id).await?;
    owned.insert(token.to_string());
    deliver_token(store, &pusher.local, token).await;
    Ok(())
}

/// The public endpoint URL. The pusher doesn't hold `Config`, so we rebuild it
/// from the same public-URL contract the writer uses (carried on the pusher).
fn pusher_endpoint(pusher: &Pusher, token: &str) -> String {
    format!("{}/push/{}", pusher.public_url(), token)
}

fn require_hello(distributor_id: &Option<String>) -> Result<()> {
    if distributor_id.is_none() {
        return Err(Error::BadRequest("send `hello` first".into()));
    }
    Ok(())
}

fn send(out_tx: &UnboundedSender<ServerFrame>, frame: ServerFrame) {
    let _ = out_tx.send(frame); // Closed channel just means the conn is going away.
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
