//! Environment-driven configuration.

/// Runtime configuration, populated from environment variables.
#[derive(Clone, Debug)]
pub struct Config {
    /// Socket address the HTTP server binds to (e.g. `0.0.0.0:8080`).
    pub bind: String,
    /// Public base URL used to build endpoint URLs handed to distributors.
    /// No trailing slash (e.g. `http://localhost:8080`).
    pub public_url: String,
    /// Maximum accepted WebPush body size, in bytes (RFC 8030 / UnifiedPush: 4096).
    pub max_message_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".to_string(),
            public_url: "http://localhost:8080".to_string(),
            max_message_bytes: 4096,
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
        if let Ok(v) = std::env::var("UPF_MAX_MESSAGE_BYTES") {
            if let Ok(n) = v.parse() {
                cfg.max_message_bytes = n;
            }
        }
        cfg
    }

    /// Build the public endpoint URL for a subscription token.
    pub fn endpoint_for(&self, token: &str) -> String {
        format!("{}/push/{}", self.public_url, token)
    }
}
