//! JSON control frames exchanged over the distributor WebSocket.
//!
//! Every frame is a JSON object with a `"type"` discriminator. Client frames
//! flow distributor → server; server frames flow server → distributor.
//!
//! A distributor multiplexes many subscriptions over one socket. Each session it
//! `subscribe`s the tokens it already holds (re-establishing affinity so pushes
//! route to this node) and `register`s to obtain new ones. Delivery, affinity and
//! draining are all *per token* — the "device" of the design spec is really a
//! `(connection, token)` pair.

use serde::{Deserialize, Serialize};

use crate::model::MessageHeaders;

/// Frames sent by the distributor to the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// First frame on a connection: identifies the distributor. If no
    /// `distributor_id` is supplied the server mints one and returns it. The id
    /// is informational (for logs); routing is per-token, not per-distributor.
    Hello {
        #[serde(default)]
        distributor_id: Option<String>,
    },
    /// Register a new application instance; the server mints an endpoint token
    /// and immediately attaches it to this connection.
    Register {
        app_id: String,
        #[serde(default)]
        vapid: Option<String>,
    },
    /// Attach an already-registered token to this connection: claim affinity and
    /// replay any queued messages. Sent for every persisted token on reconnect.
    Subscribe { endpoint_token: String },
    /// Remove a subscription by its endpoint token.
    Unregister { endpoint_token: String },
    /// Acknowledge receipt of a delivered message so it can be dropped from `Q`.
    Ack {
        endpoint_token: String,
        msg_id: String,
    },
    /// Liveness check.
    Ping,
}

/// Frames sent by the server to the distributor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Response to `hello`, echoing the (possibly newly minted) distributor id.
    Welcome { distributor_id: String },
    /// Response to `register`: the public endpoint URL and its token (now attached).
    Registered {
        app_id: String,
        endpoint: String,
        endpoint_token: String,
    },
    /// Response to `subscribe`: the token is now bound to this connection.
    Subscribed { endpoint_token: String },
    /// Response to `unregister`.
    Unregistered { endpoint_token: String },
    /// A forwarded push message. `body_b64` is the raw (encrypted) WebPush body.
    Message {
        endpoint_token: String,
        msg_id: String,
        body_b64: String,
        headers: MessageHeaders,
    },
    /// Response to `ping`.
    Pong,
    /// An error the distributor should surface/log; non-fatal to the connection.
    Error { reason: String },
}
