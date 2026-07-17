//! `loadgen` — a load generator *and* correctness checker for the ntfy surface.
//!
//! It impersonates a whole fleet at once: it opens many WebSocket subscribers
//! (fake devices) and fires many concurrent publishers (fake application
//! servers), then proves the system delivered everything correctly and reports
//! end-to-end latency.
//!
//! How correctness is checked: each topic has exactly ONE publisher, which sends
//! a strictly increasing sequence number `0,1,2,…`, awaiting each publish so the
//! commit order (= the queue versionstamp order = the delivery order) matches the
//! sequence order. Each subscriber therefore expects to receive `0,1,2,…` in
//! order, with no gaps and no duplicates. The message body also carries a
//! send-timestamp (microseconds on a shared monotonic clock), so the subscriber
//! measures true send→receive latency when the frame comes back.
//!
//! Bodies are published with `?up=1` (UnifiedPush raw mode), so ntfy echoes them
//! back verbatim as the `message` field — no trimming, no re-encoding.
//!
//! ```text
//! # against the local dev pod (scripts/dev-up.sh), from inside the dev image:
//! scripts/cargo.sh run --release --example loadgen
//!
//! # dial it in via env (all optional; defaults below aim big):
//! LOADGEN_TOPICS=1000 LOADGEN_SUBS_PER_TOPIC=2 LOADGEN_RATE=2000 \
//! LOADGEN_DURATION_SECS=30 scripts/cargo.sh run --release --example loadgen
//! ```
//!
//! Knobs — pass as `key=value` args (the loadgen container runs against the
//! roles by service name; `just bench` fills in `http_base`/`ws_base`):
//!
//! ```text
//! just up                                      # bring up FDB + roles
//! just bench topics=1000 subs=1 rate=2000 duration=30
//! ```
//!
//!   http_base   publish target base       (default http://localhost:8080)
//!   ws_base     subscribe base            (default ws://localhost:8080)
//!   topics      distinct topics           (default 1000)  = number of devices
//!   subs        subscribers per topic     (default 1)     see note below
//!   rate        total publishes/sec across all topics (default 2000)
//!   duration    publish duration, seconds (default 30)
//!   grace       drain window after publishing stops, seconds (default 5)
//!   payload     extra body padding, bytes (default 0; capped to fit 4096)
//!   connect_concurrency   parallel ws handshakes (default 200)
//!   connect_timeout       max seconds to wait for all subs to open (default 60)
//!
//! NOTE on `subs`: this server serves ONE subscriber per topic (the UnifiedPush
//! model — one distributor per topic). A newer subscribe *displaces* the older
//! in the node's local map, so with `subs>1` only the last connection per topic
//! receives messages and the run will report apparent loss BY DESIGN — that is
//! the server's documented behavior, not a defect. Leave `subs=1` for a real
//! delivery test; raise `topics` to add scale.
//!
//! Total ws connections = topics * subs; opening thousands needs a high
//! `ulimit -n`. Numbers against the single-node in-memory FDB from `dev-up.sh`
//! prove CORRECTNESS (loss/order/dup), but are NOT representative throughput —
//! that needs a real multi-process cluster.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use upf::protocol::NtfyMessage;

/// Process-wide monotonic origin, so a timestamp stamped by a publisher task and
/// read by a subscriber task share one clock (both live in this process).
static START: OnceLock<Instant> = OnceLock::new();
fn now_us() -> u64 {
    START.get().expect("START set in main").elapsed().as_micros() as u64
}

// ===== configuration ========================================================

struct Cfg {
    http_base: String,
    ws_base: String,
    topics: usize,
    subs_per_topic: usize,
    rate: f64,
    duration_secs: u64,
    grace_secs: u64,
    payload_bytes: usize,
    connect_concurrency: usize,
    connect_timeout_secs: u64,
}

impl Cfg {
    /// Defaults, then overridden by any `key=value` args (see module docs).
    fn from_args() -> Self {
        // Collect `key=value` tokens from the command line into a lookup.
        let overrides: std::collections::HashMap<String, String> = std::env::args()
            .skip(1)
            .filter_map(|a| a.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
            .collect();
        let get_str = |k: &str, d: &str| overrides.get(k).cloned().unwrap_or_else(|| d.to_string());
        let get_num = |k: &str, d: u64| overrides.get(k).and_then(|v| v.parse().ok()).unwrap_or(d);

        Cfg {
            http_base: get_str("http_base", "http://localhost:8080"),
            ws_base: get_str("ws_base", "ws://localhost:8080"),
            topics: get_num("topics", 1000) as usize,
            subs_per_topic: get_num("subs", 1) as usize,
            rate: get_num("rate", 2000) as f64,
            duration_secs: get_num("duration", 30),
            grace_secs: get_num("grace", 5),
            payload_bytes: get_num("payload", 0) as usize,
            connect_concurrency: get_num("connect_concurrency", 200) as usize,
            connect_timeout_secs: get_num("connect_timeout", 60),
        }
    }
}

// ===== shared state =========================================================

/// Everything the subscriber/publisher tasks share, behind one `Arc`.
struct Shared {
    // throughput
    published: AtomicU64,
    publish_errors: AtomicU64,
    delivered: AtomicU64,
    // correctness
    in_order: AtomicU64,
    gaps: AtomicU64, // count of messages a subscriber skipped over (seq jumped forward)
    reordered_or_dup: AtomicU64, // a seq at/below what we already passed
    ws_connect_errors: AtomicU64,
    ws_read_errors: AtomicU64,
    // liveness
    connected: AtomicUsize,
    // per-topic: how many its single publisher successfully sent, and how many
    // subscribers actually opened on it (for the expected-deliveries math).
    sent: Vec<AtomicU64>,
    subs_connected: Vec<AtomicU64>,
    // latency
    hist: Histogram,
    // config echoed for tasks
    topics: Vec<String>,
    http_base: String,
    ws_base: String,
    payload_bytes: usize,
}

// ===== latency histogram (bounded, atomic) ==================================

/// Upper bucket edges in microseconds. A sample lands in the first bucket whose
/// edge it does not exceed; anything larger lands in the overflow bucket.
const EDGES_US: &[u64] = &[
    100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000,
    1_000_000, 2_000_000, 5_000_000, 10_000_000, 30_000_000, 60_000_000, 120_000_000,
];

struct Histogram {
    buckets: Vec<AtomicU64>, // len == EDGES_US.len() + 1 (last = overflow)
    max_us: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        Histogram {
            buckets: (0..=EDGES_US.len()).map(|_| AtomicU64::new(0)).collect(),
            max_us: AtomicU64::new(0),
        }
    }

    fn record(&self, us: u64) {
        let idx = EDGES_US.iter().position(|&e| us <= e).unwrap_or(EDGES_US.len());
        self.buckets[idx].fetch_add(1, Relaxed);
        let mut cur = self.max_us.load(Relaxed);
        while us > cur {
            match self.max_us.compare_exchange_weak(cur, us, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Approximate percentile, reported as the bucket's upper edge (e.g. `≤5.0ms`).
    fn percentile(&self, p: f64) -> String {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Relaxed)).collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return "-".to_string();
        }
        let target = ((p * total as f64).ceil() as u64).max(1);
        let mut cum = 0u64;
        for (i, c) in counts.iter().enumerate() {
            cum += c;
            if cum >= target {
                return if i < EDGES_US.len() {
                    format!("≤{}", fmt_us(EDGES_US[i]))
                } else {
                    format!(">{}", fmt_us(*EDGES_US.last().unwrap()))
                };
            }
        }
        "-".to_string()
    }

    fn max(&self) -> String {
        let m = self.max_us.load(Relaxed);
        if m == 0 { "-".to_string() } else { fmt_us(m) }
    }
}

fn fmt_us(us: u64) -> String {
    if us < 1_000 {
        format!("{us}us")
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.1}s", us as f64 / 1_000_000.0)
    }
}

// ===== message body =========================================================

/// The publish body: a compact JSON carrying the sequence number and the
/// send-timestamp (micros on the shared clock), plus optional padding.
#[derive(Deserialize)]
struct Payload {
    s: u64,
    u: u64,
}

fn build_body(seq: u64, pad: usize) -> String {
    let u = now_us();
    let mut body = format!("{{\"s\":{seq},\"u\":{u},\"p\":\"");
    body.reserve(pad + 2);
    for _ in 0..pad {
        body.push('a');
    }
    body.push_str("\"}");
    body
}

// ===== subscriber ===========================================================

/// One fake device: connect, wait for `open`, then verify the ordered stream.
async fn run_subscriber(
    shared: Arc<Shared>,
    topic_idx: usize,
    sem: Arc<Semaphore>,
    mut stop_rx: watch::Receiver<bool>,
) {
    // Hold a connect permit only through the handshake + first `open`, so the
    // ramp-up doesn't open thousands of sockets in one thundering herd.
    let mut permit = Some(sem.acquire_owned().await.expect("semaphore open"));

    let topic = &shared.topics[topic_idx];
    let url = format!("{}/{}/ws", shared.ws_base, topic);
    let (ws, _resp) = match connect_async(&url).await {
        Ok(ok) => ok,
        Err(_) => {
            shared.ws_connect_errors.fetch_add(1, Relaxed);
            return;
        }
    };
    let (mut sink, mut stream) = ws.split();

    // Local cursor: the next sequence number we expect to see, in order.
    let mut expected_next: u64 = 0;
    let mut opened = false;

    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() { break; }
            }
            item = stream.next() => match item {
                Some(Ok(Message::Text(t))) => {
                    let txt = t.to_string();
                    let Ok(frame) = serde_json::from_str::<NtfyMessage>(&txt) else { continue };
                    match frame.event.as_str() {
                        "open" => {
                            if !opened {
                                opened = true;
                                shared.subs_connected[topic_idx].fetch_add(1, Relaxed);
                                shared.connected.fetch_add(1, Relaxed);
                                permit.take(); // release: this sub is live
                            }
                        }
                        "message" => {
                            if let Some(body) = &frame.message {
                                if let Ok(p) = serde_json::from_str::<Payload>(body) {
                                    shared.delivered.fetch_add(1, Relaxed);
                                    shared.hist.record(now_us().saturating_sub(p.u));
                                    if p.s == expected_next {
                                        shared.in_order.fetch_add(1, Relaxed);
                                        expected_next += 1;
                                    } else if p.s > expected_next {
                                        // Skipped ahead: (p.s - expected_next) went missing.
                                        shared.gaps.fetch_add(p.s - expected_next, Relaxed);
                                        expected_next = p.s + 1;
                                    } else {
                                        // At/below the cursor: a duplicate or a reorder.
                                        shared.reordered_or_dup.fetch_add(1, Relaxed);
                                    }
                                }
                            }
                        }
                        _ => {} // keepalive et al.
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sink.send(Message::Pong(p)).await;
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(_)) => {
                    shared.ws_read_errors.fetch_add(1, Relaxed);
                    break;
                }
            }
        }
    }
}

// ===== publisher ============================================================

/// One fake application server: publish `0,1,2,…` to a single topic, awaiting
/// each send so the sequence order is the durable commit order.
async fn run_publisher(
    shared: Arc<Shared>,
    topic_idx: usize,
    client: reqwest::Client,
    deadline: Instant,
    interval: Duration,
    initial_delay: Duration,
) {
    tokio::time::sleep(initial_delay).await; // stagger starts across topics

    let url = format!("{}/{}?up=1", shared.http_base, shared.topics[topic_idx]);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut seq: u64 = 0;

    loop {
        if Instant::now() >= deadline {
            break;
        }
        ticker.tick().await;
        if Instant::now() >= deadline {
            break;
        }
        let body = build_body(seq, shared.payload_bytes);
        match client.post(&url).body(body).send().await {
            Ok(r) if r.status().is_success() => {
                shared.sent[topic_idx].fetch_add(1, Relaxed);
                shared.published.fetch_add(1, Relaxed);
                seq += 1; // advance only on success -> the stream stays contiguous
            }
            _ => {
                shared.publish_errors.fetch_add(1, Relaxed);
            }
        }
    }
}

// ===== live progress reporter ===============================================

async fn run_reporter(shared: Arc<Shared>, desired: usize, mut stop_rx: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut secs = 0u64;
    let mut last_pub = 0u64;
    let mut last_del = 0u64;
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() { break; }
            }
            _ = tick.tick() => {
                let pubd = shared.published.load(Relaxed);
                let deld = shared.delivered.load(Relaxed);
                let dp = pubd - last_pub;
                let dd = deld - last_del;
                last_pub = pubd;
                last_del = deld;
                println!(
                    "[{secs:>3}s] conn {:>5}/{desired}  pub {pubd:>8} (+{dp:>5}/s)  \
                     deliv {deld:>9} (+{dd:>6}/s)  p50 {} p99 {}  perr {} rerr {}",
                    shared.connected.load(Relaxed),
                    shared.hist.percentile(0.50),
                    shared.hist.percentile(0.99),
                    shared.publish_errors.load(Relaxed),
                    shared.ws_read_errors.load(Relaxed),
                );
                secs += 1;
            }
        }
    }
}

// ===== main =================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    START.set(Instant::now()).ok();
    let cfg = Cfg::from_args();

    // Unique topic prefix per run so keys don't collide with a previous run.
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let nonce = &nonce[..8];
    let topics: Vec<String> = (0..cfg.topics).map(|i| format!("lg{nonce}{i:06}")).collect();
    let desired = cfg.topics * cfg.subs_per_topic;

    // Body size sanity: keep it under the server's 4096-byte cap.
    let payload_bytes = cfg.payload_bytes.min(3800);

    println!("================ loadgen ================");
    println!("http_base:        {}", cfg.http_base);
    println!("ws_base:          {}", cfg.ws_base);
    println!("topics:           {}", cfg.topics);
    println!("subs/topic:       {}  -> {desired} websocket connections", cfg.subs_per_topic);
    println!("target rate:      {:.0} publishes/sec (across all topics)", cfg.rate);
    println!("duration:         {}s  (+{}s drain)", cfg.duration_secs, cfg.grace_secs);
    println!("payload padding:  {payload_bytes} bytes");
    println!("========================================");
    if desired > 512 {
        println!("note: {desired} connections -> ensure `ulimit -n` is high enough.");
    }
    if cfg.subs_per_topic > 1 {
        println!(
            "note: subs={} but this server serves ONE subscriber per topic; the extra\n\
             \x20     connections receive nothing, so expect ~{:.0}% apparent loss BY DESIGN.",
            cfg.subs_per_topic,
            (1.0 - 1.0 / cfg.subs_per_topic as f64) * 100.0
        );
    }

    let shared = Arc::new(Shared {
        published: AtomicU64::new(0),
        publish_errors: AtomicU64::new(0),
        delivered: AtomicU64::new(0),
        in_order: AtomicU64::new(0),
        gaps: AtomicU64::new(0),
        reordered_or_dup: AtomicU64::new(0),
        ws_connect_errors: AtomicU64::new(0),
        ws_read_errors: AtomicU64::new(0),
        connected: AtomicUsize::new(0),
        sent: (0..cfg.topics).map(|_| AtomicU64::new(0)).collect(),
        subs_connected: (0..cfg.topics).map(|_| AtomicU64::new(0)).collect(),
        hist: Histogram::new(),
        topics,
        http_base: cfg.http_base.clone(),
        ws_base: cfg.ws_base.clone(),
        payload_bytes,
    });

    let (stop_tx, stop_rx) = watch::channel(false);

    // Reporter.
    let reporter = tokio::spawn(run_reporter(shared.clone(), desired, stop_rx.clone()));

    // Ramp up subscribers (bounded handshake concurrency).
    println!("connecting {desired} subscribers...");
    let sem = Arc::new(Semaphore::new(cfg.connect_concurrency.max(1)));
    let mut sub_handles: Vec<JoinHandle<()>> = Vec::with_capacity(desired);
    for t in 0..cfg.topics {
        for _ in 0..cfg.subs_per_topic {
            sub_handles.push(tokio::spawn(run_subscriber(
                shared.clone(),
                t,
                sem.clone(),
                stop_rx.clone(),
            )));
        }
    }

    // Wait until every subscriber has its `open` (or the connect timeout fires).
    // This barrier matters: a `since=live` sub only receives messages published
    // AFTER it connects, so publishing early would look like loss.
    let connect_deadline = Instant::now() + Duration::from_secs(cfg.connect_timeout_secs);
    loop {
        let c = shared.connected.load(Relaxed);
        if c >= desired {
            break;
        }
        if Instant::now() >= connect_deadline {
            println!("warning: only {c}/{desired} subscribers connected; proceeding anyway.");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let connected_now = shared.connected.load(Relaxed);
    println!("subscribers ready: {connected_now}/{desired}. publishing...");

    // Launch publishers. One per topic; a per-topic interval spreads the target
    // aggregate rate, and a staggered start avoids a synchronized burst.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let per_topic_interval = Duration::from_secs_f64((cfg.topics as f64 / cfg.rate).max(0.0001));
    let test_start = Instant::now();
    let deadline = test_start + Duration::from_secs(cfg.duration_secs);
    let mut pub_handles: Vec<JoinHandle<()>> = Vec::with_capacity(cfg.topics);
    for t in 0..cfg.topics {
        let stagger = per_topic_interval.mul_f64(t as f64 / cfg.topics.max(1) as f64);
        pub_handles.push(tokio::spawn(run_publisher(
            shared.clone(),
            t,
            client.clone(),
            deadline,
            per_topic_interval,
            stagger,
        )));
    }

    // Run the publish window, then a drain window for in-flight messages.
    tokio::time::sleep(Duration::from_secs(cfg.duration_secs)).await;
    for h in pub_handles {
        let _ = h.await;
    }
    println!("publishing done; draining for {}s...", cfg.grace_secs);
    tokio::time::sleep(Duration::from_secs(cfg.grace_secs)).await;

    // Stop subscribers + reporter and let them wind down.
    let _ = stop_tx.send(true);
    for h in sub_handles {
        let _ = h.await;
    }
    let _ = reporter.await;

    print_report(&shared, &cfg, connected_now, desired);
    Ok(())
}

// ===== final report =========================================================

fn print_report(shared: &Shared, cfg: &Cfg, connected: usize, desired: usize) {
    let published = shared.published.load(Relaxed);
    let delivered = shared.delivered.load(Relaxed);
    let publish_errors = shared.publish_errors.load(Relaxed);
    let gaps = shared.gaps.load(Relaxed);
    let reord = shared.reordered_or_dup.load(Relaxed);
    let connect_errs = shared.ws_connect_errors.load(Relaxed);
    let read_errs = shared.ws_read_errors.load(Relaxed);

    // Expected deliveries = for each topic, (messages sent) * (subs that opened).
    let expected: u64 = (0..cfg.topics)
        .map(|t| shared.sent[t].load(Relaxed) * shared.subs_connected[t].load(Relaxed))
        .sum();

    let dur = cfg.duration_secs.max(1) as f64;
    let drain = (cfg.duration_secs + cfg.grace_secs).max(1) as f64;
    let pub_rate = published as f64 / dur;
    let del_rate = delivered as f64 / drain;
    let loss = expected as i64 - delivered as i64;
    let loss_pct = if expected > 0 { (loss as f64 / expected as f64) * 100.0 } else { 0.0 };

    println!();
    println!("================ loadgen report ================");
    println!("subscribers:        {connected}/{desired} connected");
    println!("publishers:         {} (1 per topic)", cfg.topics);
    println!();
    println!("throughput");
    println!("  published:        {published}   ({pub_rate:.0}/s)");
    println!("  publish errors:   {publish_errors}");
    println!(
        "  delivered:        {delivered}   ({del_rate:.0}/s)   [fanout x{}]",
        cfg.subs_per_topic
    );
    println!();
    println!("end-to-end latency (send -> receive)");
    println!(
        "  p50 {}   p90 {}   p95 {}   p99 {}   max {}",
        shared.hist.percentile(0.50),
        shared.hist.percentile(0.90),
        shared.hist.percentile(0.95),
        shared.hist.percentile(0.99),
        shared.hist.max(),
    );
    println!();
    println!("correctness");
    println!("  expected deliveries: {expected}");
    println!("  actual deliveries:   {delivered}");
    if loss >= 0 {
        println!("  loss:                {loss}  ({loss_pct:.4}%)");
    } else {
        println!("  surplus (dupes):     {}  (received more than sent)", -loss);
    }
    println!("  out-of-order/dupes:  {reord}");
    println!("  gaps (skipped):      {gaps}");
    println!("  ws connect errors:   {connect_errs}");
    println!("  ws read errors:      {read_errs}");
    println!();

    let clean = loss == 0
        && reord == 0
        && gaps == 0
        && publish_errors == 0
        && connect_errs == 0
        && read_errs == 0;
    if clean {
        println!("VERDICT: PASS — every message delivered, in order, exactly once.");
    } else {
        println!("VERDICT: FAIL — see the anomalies above.");
    }
    println!("================================================");
}
