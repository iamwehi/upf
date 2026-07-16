//! Environment-driven configuration.
//!
//! Every process runs one or more *roles* (writer / pusher / janitor) but shares
//! the same config surface; unused fields are simply ignored by disabled roles.

use std::fmt;

/// The three deployable roles. A single binary can run any subset; they only
/// ever communicate through FoundationDB, never directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// HTTP ingest: `POST /push/{token}` from application servers.
    Writer,
    /// WebSocket delivery: holds device connections, watches shard bells, drains.
    Pusher,
    /// Background TTL expiry + orphan sweeps.
    Janitor,
}

impl Role {
    fn parse(s: &str) -> Option<Role> {
        match s.trim().to_ascii_lowercase().as_str() {
            "writer" => Some(Role::Writer),
            "pusher" => Some(Role::Pusher),
            "janitor" => Some(Role::Janitor),
            _ => None,
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Role::Writer => "writer",
            Role::Pusher => "pusher",
            Role::Janitor => "janitor",
        };
        f.write_str(s)
    }
}

/// Runtime configuration, populated from environment variables.
#[derive(Clone, Debug)]
pub struct Config {
    /// Socket address the HTTP/WS server binds to (e.g. `0.0.0.0:8080`).
    pub bind: String,
    /// Public base URL used to build endpoint URLs handed to distributors.
    /// No trailing slash (e.g. `http://localhost:8080`).
    pub public_url: String,
    /// Maximum accepted WebPush body size, in bytes (RFC 8030 / UnifiedPush: 4096).
    pub max_message_bytes: usize,
    /// Roles this process runs.
    pub roles: Vec<Role>,
    /// Stable identity for this pusher node — the value written into affinity
    /// (`C`) records and inbox/bell (`IN`/`SIG`) keys. Defaults to a random id.
    pub node_id: String,
    /// Number of inbox shards per node (`K`). A pusher opens exactly this many
    /// watches, independent of connection count. Must match across the fleet.
    pub shard_count: u32,
    /// Default message lifetime when the caller sends no `TTL` header, in seconds.
    pub default_ttl_secs: u64,
    /// How often each pusher re-drains every live connection regardless of bells.
    /// This backstop turns lost watches / stale affinity into latency, not loss.
    pub safety_poll_secs: u64,
    /// How often a subscriber connection is sent an ntfy `keepalive` frame.
    pub keepalive_secs: u64,
    /// How often a pusher refreshes its liveness heartbeat (`L`).
    pub heartbeat_secs: u64,
    /// How often the janitor runs its expiry + sweep pass.
    pub janitor_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".to_string(),
            public_url: "http://localhost:8080".to_string(),
            max_message_bytes: 4096,
            roles: vec![Role::Writer, Role::Pusher, Role::Janitor],
            node_id: format!("node-{}", uuid::Uuid::new_v4()),
            shard_count: 64,
            default_ttl_secs: 4 * 7 * 24 * 60 * 60, // 4 weeks
            safety_poll_secs: 60,
            keepalive_secs: 45, // ntfy's default
            heartbeat_secs: 10,
            janitor_interval_secs: 30,
        }
    }
}

impl Config {
    /// Build a `Config`, overriding defaults from `UPF_*` environment variables.
    pub fn from_env() -> Self {
        let mut cfg = Config::default();
        if let Ok(v) = std::env::var("UPF_BIND") {
            cfg.bind = v;
        }
        if let Ok(v) = std::env::var("UPF_PUBLIC_URL") {
            cfg.public_url = v.trim_end_matches('/').to_string();
        }
        if let Ok(n) = env_parse("UPF_MAX_MESSAGE_BYTES") {
            cfg.max_message_bytes = n;
        }
        if let Ok(v) = std::env::var("UPF_ROLES") {
            let roles: Vec<Role> = v.split(',').filter_map(Role::parse).collect();
            if !roles.is_empty() {
                cfg.roles = roles;
            }
        }
        if let Ok(v) = std::env::var("UPF_NODE_ID") {
            if !v.is_empty() {
                cfg.node_id = v;
            }
        }
        if let Ok(n) = env_parse("UPF_SHARD_COUNT") {
            if n >= 1 {
                cfg.shard_count = n;
            }
        }
        if let Ok(n) = env_parse("UPF_DEFAULT_TTL_SECS") {
            cfg.default_ttl_secs = n;
        }
        if let Ok(n) = env_parse("UPF_SAFETY_POLL_SECS") {
            cfg.safety_poll_secs = n;
        }
        if let Ok(n) = env_parse("UPF_KEEPALIVE_SECS") {
            cfg.keepalive_secs = n;
        }
        if let Ok(n) = env_parse("UPF_HEARTBEAT_SECS") {
            cfg.heartbeat_secs = n;
        }
        if let Ok(n) = env_parse("UPF_JANITOR_INTERVAL_SECS") {
            cfg.janitor_interval_secs = n;
        }
        cfg
    }

    pub fn has_role(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }
}

/// Parse an environment variable into any `FromStr`, erroring if unset/invalid.
fn env_parse<T: std::str::FromStr>(name: &str) -> std::result::Result<T, ()> {
    std::env::var(name).map_err(|_| ())?.parse().map_err(|_| ())
}
