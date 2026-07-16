//! WebPush ingress (application-server-facing, RFC 8030).
//!
//! Application servers `POST` encrypted push messages to the opaque endpoint
//! URL we handed the distributor. For the walking skeleton we accept and
//! forward the body verbatim; deeper RFC semantics (VAPID verification, TTL
//! expiry, Topic replacement, Urgency) are follow-up milestones.

pub mod headers;
pub mod ingress;
