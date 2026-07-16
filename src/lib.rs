//! UPF — a scalable UnifiedPush push server backed by FoundationDB.
//!
//! Architecture (see `docs`/plan): three roles share one FDB cluster.
//!  * WebPush ingress (RFC 8030) — application servers `POST` here.
//!  * Distributor gateway — distributors connect over WebSocket.
//!  * Storage — subscriptions + per-subscription message queue in FDB.

pub mod config;
pub mod distributor;
pub mod error;
pub mod storage;
pub mod token;
pub mod webpush;

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use crate::config::Config;
use crate::distributor::registry::Registry;
use crate::storage::Storage;

/// Shared application state handed to every request handler.
///
/// Cheaply cloneable: everything behind an `Arc`.
#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Storage>,
    pub registry: Arc<Registry>,
    pub config: Arc<Config>,
}

/// Build the axum router for the whole server.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/push/{token}", post(webpush::ingress::push))
        .route("/distributor/ws", get(distributor::ws::handler))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

/// Boot FoundationDB, wire up state, and serve until shutdown.
pub async fn run() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env();
    tracing::info!(bind = %config.bind, public_url = %config.public_url, "starting upf");

    // The FDB network must be booted exactly once and the guard kept alive for
    // the lifetime of the process. Dropping it cleanly shuts the client down.
    // SAFETY: called once, before any other FDB API use, and `_network` is held
    // until `run` returns (i.e. until the server stops).
    let _network = unsafe { foundationdb::boot() };

    let storage = Arc::new(Storage::connect()?);
    let registry = Arc::new(Registry::new());
    let state = AppState {
        storage,
        registry,
        config: Arc::new(config.clone()),
    };

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");

    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("upf=debug,info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
