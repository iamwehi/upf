//! UPF — a scalable UnifiedPush push server backed by FoundationDB.
//!
//! Three roles share one FDB cluster and communicate *only* through it — there
//! is no service-to-service RPC:
//!  * **Writer** ([`writer`]) — application servers `POST` here (RFC 8030 WebPush).
//!  * **Pusher** ([`pusher`]) — distributors connect over WebSocket; it watches
//!    shard bells and drains durable queues.
//!  * **Janitor** ([`janitor`]) — expires TTL'd messages and sweeps dead nodes.
//!
//! A single binary can run any subset of roles (`UPF_ROLES`); the walking-skeleton
//! default runs all three in one process, still coordinating purely via FDB.

pub mod config;
pub mod error;
pub mod hash;
pub mod ids;
pub mod janitor;
pub mod keyspace;
pub mod model;
pub mod protocol;
pub mod pusher;
pub mod store;
pub mod writer;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;

use crate::config::{Config, Role};
use crate::pusher::Pusher;
use crate::store::Store;

/// Shared application state handed to every request handler.
///
/// Cheaply cloneable: everything behind an `Arc`. `pusher` is present only when
/// this process runs the pusher role.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub config: Arc<Config>,
    pub pusher: Option<Arc<Pusher>>,
}

/// Build the axum router, mounting only the routes for this process's roles.
///
/// The surface is ntfy-compatible: publish to `/{topic}` (writer), subscribe on
/// `/{topic}/ws|json|sse` (pusher). Running both roles in one process serves the
/// whole ntfy surface at one base URL — the simplest target for a real
/// distributor. In a split deployment a single ingress must front both roles.
pub fn router(state: AppState) -> Router {
    let mut router = Router::new().route("/healthz", get(healthz));
    if state.config.has_role(Role::Writer) {
        router = router.route(
            "/{topic}",
            get(writer::topic_get)
                .post(writer::publish)
                .put(writer::publish),
        );
    }
    if state.config.has_role(Role::Pusher) {
        router = router
            .route("/{topic}/ws", get(pusher::ws::ws))
            .route("/{topic}/json", get(pusher::ws::json))
            .route("/{topic}/sse", get(pusher::ws::sse));
    }
    router.with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

/// Boot FoundationDB, wire up the enabled roles, and serve until shutdown.
pub async fn run() -> anyhow::Result<()> {
    init_tracing();

    let config = Arc::new(Config::from_env());
    let roles: Vec<String> = config.roles.iter().map(|r| r.to_string()).collect();
    tracing::info!(
        bind = %config.bind,
        public_url = %config.public_url,
        node = %config.node_id,
        roles = %roles.join(","),
        "starting upf"
    );

    // The FDB network must be booted exactly once and the guard kept alive for
    // the lifetime of the process. SAFETY: called once, before any other FDB API
    // use, and `_network` is held until `run` returns.
    let _network = unsafe { foundationdb::boot() };

    let store = Arc::new(Store::connect(config.shard_count)?);

    // Start background roles.
    let pusher = if config.has_role(Role::Pusher) {
        Some(Pusher::start(store.clone(), config.clone()))
    } else {
        None
    };
    if config.has_role(Role::Janitor) {
        janitor::start(store.clone(), config.clone());
    }

    let state = AppState {
        store,
        config: config.clone(),
        pusher,
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
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("upf=debug,info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
