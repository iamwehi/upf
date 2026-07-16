# UPF — a scalable UnifiedPush push server on FoundationDB

UPF is a [UnifiedPush](https://unifiedpush.org/) push server written in Rust. It
scales horizontally by keeping **all state and all coordination in FoundationDB**
— there is no message bus, no service mesh, and no service-to-service RPC. Every
role reads and writes the same keyspace; FDB is simultaneously the database, the
queue, and the message bus.

```
app servers ──POST──▶ [LB] ──▶ writer × W ──txn──▶ ┌─────┐
                                                    │ FDB │ ◀─watch/drain─ pusher × P ◀─WS─ distributors
                             janitor × 1 ──txn────▶ └─────┘
```

---

## Where this sits in UnifiedPush

UnifiedPush splits the world in two, and only one half is standardized:

| Link | Spec | This server |
| --- | --- | --- |
| **App server → push server** | WebPush — [RFC 8030](https://www.rfc-editor.org/rfc/rfc8030) (HTTP delivery), [8291](https://www.rfc-editor.org/rfc/rfc8291) (encryption), [8292](https://www.rfc-editor.org/rfc/rfc8292) (VAPID) | `POST /push/{token}` |
| **Push server → distributor** | *deliberately unspecified* | our **WebSocket** protocol |

The push body is opaque, end-to-end-encrypted bytes (RFC 8291); UPF stores and
forwards it verbatim and never decrypts it. Endpoint tokens carry ≥160 bits of
entropy and are URL-safe (UnifiedPush requirement).

---

## The three roles

A role is a deployable. One binary runs any subset (`UPF_ROLES`); by default it
runs all three in-process — but they *still* only talk through FDB, so the
single-process build and a sharded fleet exercise identical code paths.

- **Writer** — stateless HTTP ingest. Handles `POST /push/{token}`, runs one FDB
  transaction, returns `201`. Scales with push rate behind an L4 load balancer.
- **Pusher** — stateful WebSocket delivery. Holds distributor connections,
  watches shard "bells", drains durable queues. Scales with connection count;
  every node is identical.
- **Janitor** — background worker. Expires TTL'd messages and sweeps records left
  by dead nodes. One instance is plenty for a long time.

---

## Key concepts introduced by this design

If you already knew the walking-skeleton version (an in-process registry mapping
distributor → socket), these are what changed and why.

### 1. FDB-only coordination — no bus, no RPC

A writer never calls a pusher. When a message arrives it lands in FoundationDB,
and the pusher holding the relevant connection *finds out through FoundationDB*.
This is what lets writers and pushers scale on independent axes and lets any node
die without a failover protocol: state was never in a process.

### 2. `Q` is the only source of truth

Every push is appended to a durable per-token queue `Q` **before** anything else
happens. Everything else in the keyspace — bells, inboxes, the TTL index — is
*advisory*. Losing any of it costs latency, never a message. A message is not
considered delivered until the distributor **acks** it and it is cleared from `Q`.
Delivery is therefore **at-least-once**; a distributor should dedupe by `msg_id`.

### 3. Versionstamps as message identity

Queue entries are keyed by an FDB **versionstamp** — a 12-byte value FDB assigns
at commit time that is globally ordered by commit sequence. This gives us, for
free:

- **FIFO ordering** without trusting any clock (versionstamps are assigned by the
  cluster, not the writer).
- **A stable message id.** The versionstamp *is* the `msg_id` we hand the
  distributor (base64url-encoded). When the distributor acks it, we decode it
  straight back into the `Q` key and clear it.
- **Consistency across indexes.** All of a writer's versionstamped writes in one
  transaction share user-version `0`, so the `Q` entry, its TTL-index (`X`) entry,
  and the inbox poke all carry the *same* stamp — one message, one id.

### 4. Affinity — routing that lives in the database

`(C, token) → node_id` records which pusher node currently holds a token's
connection. A writer reads it to decide whom to poke. It is **soft state**: a
stale affinity just means a poke goes to the wrong (or a dead) node — the message
is safe in `Q` and gets picked up on the next connect or safety poll. Affinity is
never verified and pokes are never retried.

### 5. Bells + inboxes — an O(K) wakeup mechanism

The naïve way to wake a pusher is one FDB `watch` per connection — but watches are
capped (~10k/connection) and that ties your connection density to your watch
budget. Instead:

- Each node has **K shards** (default 64). A token maps to a shard by a stable
  hash (`shard = fnv1a(token) % K`).
- A node opens exactly **K watches** — one per `(SIG, node, shard)` **bell** —
  *regardless of how many connections it holds*. Watch budget is O(K), not
  O(connections).
- To poke, a writer appends the token to that node's **inbox**
  `(IN, node, shard, versionstamp) → token` and bumps the bell counter. The
  bell's change wakes the node's watch; it reads the inbox, and for each token it
  actually holds, drains that token's `Q`.

### 6. Three rules that make it correct

The whole system's correctness reduces to three invariants:

1. **`Q` is the only source of truth** — bells/inboxes are advisory.
2. **Every connect full-drains `Q`** (drain-on-connect) — this one rule absorbs
   node death, connection migration, and missed watches.
3. **A periodic safety poll** re-drains every live connection regardless of bells
   — so a lost watch or a stale affinity is a *latency* bug, never a *loss* bug.

---

## FoundationDB keyspace

All keys are tuple-encoded under one root subspace (`"upf"`).

```
(S,   token)                       -> Subscription (JSON)       exists ⇒ registered
(Q,   token, versionstamp)         -> Envelope (JSON)           durable queue — source of truth
(TI,  token, topic)                -> versionstamp              RFC 8030 topic collapse
(X,   expiry, token, versionstamp) -> ""                        TTL index (janitor scans this)
(C,   token)                       -> node_id                   connection affinity (soft)
(IN,  node, shard, versionstamp)   -> token                     per-node inbox (advisory poke)
(SIG, node, shard)                 -> counter (LE i64)          watched bell, one per shard
(L,   node)                        -> heartbeat_secs (LE i64)   liveness registry
```

## Algorithms (one transaction each)

**Writer** (`POST /push/{token}`): verify `S` exists → append `Q` → if `Topic`,
clear the previous topic message and repoint `TI` → write `X` → read affinity
`C`; if present, append to that node's `IN` and ring its `SIG`. Commit. A missing
affinity just means the device is offline; the message waits in `Q`.

**Pusher**:
- *Boot*: open K watches, one per `(SIG, self, shard)`.
- *On connect (`subscribe`/`register`)*: authenticate against `S`, write
  `(C, token) → self`, register the socket locally, **full-drain `Q`**.
- *On bell fire*: re-arm the watch immediately, read+clear the inbox, drain `Q`
  for each held token.
- *Safety poll* (~every `UPF_SAFETY_POLL_SECS`): drain every held connection.
- *On disconnect*: compare-and-clear affinity (only if it still equals self).

**Janitor**: scan `(X, 0..now)` in bounded batches, clearing each due `Q` message
and its `X` entry; then clear inbox/bell/liveness records for nodes that stopped
heartbeating.

---

## Distributor WebSocket protocol

JSON frames, each tagged with a `"type"`. A distributor multiplexes many
subscriptions over one socket. Each session it `subscribe`s the tokens it already
holds (re-establishing affinity) and `register`s to obtain new ones.

| Client → server | Server → client |
| --- | --- |
| `hello {distributor_id?}` | `welcome {distributor_id}` |
| `register {app_id, vapid?}` | `registered {app_id, endpoint, endpoint_token}` |
| `subscribe {endpoint_token}` | `subscribed {endpoint_token}` |
| `unregister {endpoint_token}` | `unregistered {endpoint_token}` |
| `ack {endpoint_token, msg_id}` | `message {endpoint_token, msg_id, body_b64, headers}` |
| `ping` | `pong` / `error {reason}` |

The "device" in the delivery algorithm is really a `(connection, token)` pair:
affinity, inbox, and draining are all per **token**, not per distributor.

---

## Running locally

FoundationDB has no native macOS-arm64 client library, so the build and tests run
inside a Linux container that links `libfdb_c` copied from the matching FDB image.
Everything is driven by scripts (podman).

```sh
scripts/dev-up.sh          # start FDB in a pod, init the DB, build the dev image
scripts/cargo.sh build     # build inside the dev container
scripts/cargo.sh test      # unit + e2e tests against the live FDB
scripts/cargo.sh run       # run all roles on the pod's :8080
scripts/dev-down.sh         # tear the pod down
```

Manual smoke test:

```sh
# terminal A — run the server
scripts/cargo.sh run

# terminal B — a mock distributor: registers, prints its endpoint, auto-acks
scripts/cargo.sh run --example mock_distributor

# terminal C — push to the printed endpoint
curl -X POST --data 'hi there' <endpoint>
```

Kill the mock distributor, `curl` again while it's offline, then restart it with
`UPF_SUB_TOKEN=<token>` — the queued message replays on reconnect, proving `Q`
durability.

---

## Running as separate containers (`compose.yaml`)

`scripts/cargo.sh run` is a single all-roles process. `compose.yaml` runs the
real deployment shape instead: FoundationDB plus **writer, pusher and janitor as
three separate containers** — each the *same* image (`Containerfile`) with a
different `UPF_ROLES`. They share nothing but the database.

```sh
docker compose up --build      # or: podman compose up --build
```

- **writer** — published on `localhost:8080`; app servers `POST` here.
- **pusher** — published on `localhost:8081`; distributors connect to
  `ws://localhost:8081/distributor/ws`. It mints endpoint URLs pointing at the
  writer's public port (`UPF_PUBLIC_URL=http://localhost:8080`).
- **janitor** — no ports; runs TTL expiry + sweeps.
- **fdb** + **fdb-init** — the database and a one-shot that configures it; the
  role containers wait on `fdb-init` completing.

Two invariants the file encodes: writer and pusher share an identical
`UPF_SHARD_COUNT` (the poke addresses a shard both sides must agree on), and the
pusher has a stable `UPF_NODE_ID` so its affinity/bell ownership survives
restarts. Scale a role with `docker compose up --scale pusher=3` — because
coordination is entirely in FDB, added pushers are immediately live.

---

## Configuration (`UPF_*` environment variables)

| Variable | Default | Meaning |
| --- | --- | --- |
| `UPF_BIND` | `0.0.0.0:8080` | HTTP/WS listen address |
| `UPF_PUBLIC_URL` | `http://localhost:8080` | base URL used to build endpoints |
| `UPF_ROLES` | `writer,pusher,janitor` | roles this process runs |
| `UPF_NODE_ID` | random | this node's identity (affinity/inbox/bell owner) |
| `UPF_SHARD_COUNT` | `64` | inbox shards per node (`K`); must match fleet-wide |
| `UPF_MAX_MESSAGE_BYTES` | `4096` | max WebPush body (UnifiedPush limit) |
| `UPF_DEFAULT_TTL_SECS` | `2419200` (4w) | message lifetime when no `TTL` header |
| `UPF_SAFETY_POLL_SECS` | `60` | how often pushers re-drain live connections |
| `UPF_HEARTBEAT_SECS` | `10` | pusher liveness heartbeat interval |
| `UPF_JANITOR_INTERVAL_SECS` | `30` | janitor pass interval |
| `FDB_CLUSTER_FILE` | system default | FoundationDB cluster file |

---

## Deployment shape

Writers scale with push rate; pushers scale with concurrent connections (a tuned
node holds 100k+); FDB scales commit throughput via proxies/logs and read
throughput via storage servers. All pushers are identical — add nodes to add
capacity. Multi-region: one cluster per region, region-prefixed into the token,
routed by prefix.

## Roadmap

Deliberately deferred (the delivery core above is complete and tested):

- VAPID (RFC 8292) verification — the `S` record already carries the public key.
- RFC 8030 `Urgency`, richer `Topic`/receipt semantics.
- Rate limiting (`429`).
- Distributor authentication hardening.
- Metrics / OpenTelemetry.
