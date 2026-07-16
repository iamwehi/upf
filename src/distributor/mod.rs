//! Distributor-facing gateway (WebSocket).
//!
//! The push-server ↔ distributor link is intentionally unspecified by
//! UnifiedPush, so we define our own: a single persistent WebSocket per
//! distributor carrying JSON control frames (see [`protocol`]).

pub mod protocol;
pub mod registry;
pub mod ws;
