# UPF — a scalable, ntfy-compatible UnifiedPush server on FoundationDB

UPF is a [UnifiedPush](https://unifiedpush.org/) push server written in Rust. It
scales horizontally by keeping **all state and all coordination in FoundationDB**
— there is no message bus, no service mesh, and no service-to-service RPC. Every
role reads and writes the same keyspace; FDB is simultaneously the database, the
queue, and the message bus.

It speaks **[ntfy](https://ntfy.sh)'s protocol**, so real UnifiedPush
distributors — notably the ntfy Android app, one of the most widely installed —
work against it unmodified. You can point a real phone at UPF to test it.

```
app servers ──POST /{topic}──▶ [LB] ──▶ writer × W ──txn──▶ ┌─────┐
                                                            │ FDB │ ◀─watch/drain─ pusher × P ◀─ntfy ws/json/sse─ distributors
                                     janitor × 1 ──txn────▶ └─────┘
```

---

## Where this sits in UnifiedPush

UnifiedPush splits the world in two, and only one half is standardized:

| Link | Spec | This server |
| --- | --- | --- |
| **App server → push server** | WebPush — [RFC 8030](https://www.rfc-editor.org/rfc/rfc8030)/[8291](https://www.rfc-editor.org/rfc/rfc8291)/[8292](https://www.rfc-editor.org/rfc/rfc8292) | ntfy-style `POST /{topic}?up=1` |
| **Push server → distributor** | *deliberately unspecified* | **ntfy's protocol** (`/{topic}/ws`,`/json`,`/sse`) |

We adopt ntfy's protocol on **both** halves so the whole ecosystem — app servers
*and* distributors — interoperates. The push body is opaque, end-to-end-encrypted
bytes (RFC 8291); UPF stores and forwards it verbatim and never decrypts it.
Binary bodies are base64-encoded on the wire exactly as ntfy does it.

---

## The three roles

A role is a deployable. One binary runs any subset (`UPF_ROLES`); by default it
runs all three in-process — but they *still* only talk through FDB, so the
single-process build and a sharded fleet exercise identical code paths.

- **Writer** — stateless HTTP ingest. Handles `POST`/`PUT /{topic}`, runs one FDB
  transaction, returns the message JSON. Scales with push rate behind an L4 LB.
- **Pusher** — stateful subscription delivery. Holds `/{topic}/ws|json|sse`
  connections, watches shard "bells", streams durable queues. Scales with
  connection count; every node is identical.
- **Janitor** — background worker. Expires TTL'd messages and sweeps records left
  by dead nodes. One instance is plenty for a long time.

---

## Key concepts

### 1. FDB-only coordination — no bus, no RPC

A writer never calls a pusher. When a message arrives it lands in FoundationDB,
and the pusher holding the relevant connection *finds out through FoundationDB*.
This lets writers and pushers scale on independent axes and lets any node die
without a failover protocol: state was never in a process.

### 2. `Q` is the only source of truth; delivery is offset-based

Every publish is appended to a durable per-topic queue `Q` **before** anything
else. Everything else in the keyspace — bells, inboxes, the TTL index — is
*advisory*; losing it costs latency, never a message.

ntfy has **no per-message ack**: a subscriber tracks an offset and resumes with
`since=<id>`. That maps perfectly onto `Q` (see §3), so UPF is **cursor-based**,
not ack-based — nothing is cleared on send. Each connection remembers the last
versionstamp it was sent and only receives newer ones. Messages persist until
their TTL; the **janitor** reclaims them. A client that reconnects with
`since=<last-id>` gets exactly what it missed.

### 3. Versionstamps as message identity *and* offset

Queue entries are keyed by an FDB **versionstamp** — a 12-byte value FDB assigns
at commit time, globally ordered by commit sequence. This gives us, for free:

- **FIFO ordering** without trusting any clock (the cluster assigns it).
- **A resumable id.** The versionstamp *is* the ntfy message `id` (base64url).
  `since=<id>` decodes straight back into a `Q` key, so resuming a subscription
  is a range scan "everything after this key" — exactly ntfy's `since` semantics.
- **Consistency across indexes.** All of a writer's versionstamped writes in one
  transaction share user-version `0`, so the `Q` entry, its TTL-index (`X`) entry,
  and the inbox poke carry the *same* stamp — one message, one id.

### 4. Affinity — routing that lives in the database

`(C, topic) → node_id` records which pusher node currently holds a topic's
subscriber. A writer reads it to decide whom to poke. It is **soft state**: a
stale affinity just sends a poke to the wrong (or a dead) node — the message is
safe in `Q` and is picked up on the next connect or safety poll. Affinity is
never verified and pokes are never retried.

### 5. Bells + inboxes — an O(K) wakeup mechanism

The naïve way to wake a pusher is one FDB `watch` per connection — but watches are
capped (~10k/connection), tying connection density to your watch budget. Instead:

- Each node has **K shards** (default 64). A topic maps to a shard by a stable
  hash (`shard = fnv1a(topic) % K`).
- A node opens exactly **K watches** — one per `(SIG, node, shard)` **bell** —
  *regardless of how many connections it holds*. Watch budget is O(K), not
  O(connections).
- To poke, a writer appends the topic to that node's **inbox**
  `(IN, node, shard, versionstamp) → topic` and bumps the bell counter. The
  bell's change wakes the node's watch; it reads the inbox and, for each topic it
  holds, streams new `Q` entries from that subscriber's cursor.

### 6. Three rules that make it correct

1. **`Q` is the only source of truth** — bells/inboxes are advisory.
2. **Every connect drains `Q` from the requested offset** — this absorbs node
   death, connection migration, and missed watches.
3. **A periodic safety poll** re-drains every live connection regardless of bells
   — so a lost watch or a stale affinity is a *latency* bug, never a *loss* bug.

---

## FoundationDB keyspace

All keys are tuple-encoded under one root subspace (`"upf"`). Topics are implicit
(ntfy semantics) — there is no registration record.

```
(Q,   topic, versionstamp)         -> Envelope (JSON)           durable queue — source of truth
(X,   expiry, topic, versionstamp) -> ""                        TTL index (janitor scans this)
(C,   topic)                       -> node_id                   subscriber affinity (soft)
(IN,  node, shard, versionstamp)   -> topic                     per-node inbox (advisory poke)
(SIG, node, shard)                 -> counter (LE i64)          watched bell, one per shard
(L,   node)                        -> heartbeat_secs (LE i64)   liveness registry
```

## Algorithms (one transaction each)

**Writer** (`POST /{topic}`): append `Q` → write `X` → read affinity `C`; if
present, append to that node's `IN` and ring its `SIG`. Commit. No topic
existence check (topics are implicit); a missing affinity just means nobody is
subscribed, and the message waits in `Q`.

**Pusher**:
- *Boot*: open K watches, one per `(SIG, self, shard)`.
- *On subscribe*: resolve the `since` offset, write `(C, topic) → self`, register
  the socket locally with that cursor, emit `open`, then stream from the cursor.
- *On bell fire*: re-arm the watch immediately, read+clear the inbox, stream new
  `Q` entries for each held topic (advancing its cursor).
- *Safety poll* (~every `UPF_SAFETY_POLL_SECS`): re-drain every held connection.
- *On disconnect*: compare-and-clear affinity (only if it still equals self).

**Janitor**: scan `(X, 0..now)` in bounded batches, clearing each due `Q` message
and its `X` entry; then clear inbox/bell/liveness records for dead nodes.

---

## The ntfy protocol UPF speaks

### Publish (app server → UPF)

```
POST /{topic}?up=1        # or PUT; ?unifiedpush=1, X-UnifiedPush: 1, or
                          # Content-Encoding: aes128gcm also enable UnifiedPush mode
```

The request body is the message, verbatim. UTF-8 bodies pass through as text;
binary bodies become `"message": "<base64>"` with `"encoding": "base64"`. Response
is `200` with the ntfy message JSON. Max body 4096 bytes (`413` if exceeded);
invalid topic names give `400`.

`GET /{topic}?up=1` is the UnifiedPush endpoint check and returns
`{"unifiedpush":{"version":1}}`.

### Subscribe (distributor → UPF)

```
GET /{topic}/ws          # WebSocket (ntfy's default), JSON frames
GET /{topic}/json        # newline-delimited JSON stream
GET /{topic}/sse         # Server-Sent Events
```

Query parameters: `since=` (`all`, a message `id`, a unix timestamp, or a duration
like `10m`/`1h`/`1d`) resumes from an offset; `poll=1` returns the cached messages
and closes. The server sends an `open` frame, periodic `keepalive` frames, and
`message` frames. Each frame is an ntfy message object:

```json
{"id":"AAAAAcXoizIAAAAA","time":1784214948,"event":"message",
 "topic":"upDEMO0001","message":"hello","encoding":""}
```

Because topics are implicit and ids are offsets, reliable delivery is: subscribe,
remember the last `id`, and on reconnect pass `since=<id>`.

---

## Running locally

The [`Justfile`](./Justfile) (`just --list`) is the one interface, and it
dispatches per OS:

- **Linux** — FoundationDB has a native client library, so `cargo` builds
  directly and FDB runs natively under systemd. No container.
- **macOS** — no native arm64 FDB client lib, so builds run in a Linux dev
  container (linking `libfdb_c` from the FDB image) and FDB runs in a podman pod.
  The same recipes transparently call the `scripts/` (podman) under the hood.

```sh
just setup            # deps: apt on Linux; verifies podman on macOS
just install-fdb      # FoundationDB (Linux only; a no-op note on macOS)
just fdb-single       # a FoundationDB to develop against
just build            # release build of the server + loadgen
just test             # unit + e2e tests
just run              # all roles on :8080   (just run pusher — a single role)
just fdb-down         # tear FDB down
```

On macOS you can still call the scripts directly (`scripts/dev-up.sh`,
`scripts/cargo.sh …`, `scripts/dev-down.sh`) — that's what `just` invokes there.
For benchmarking against a multi-process FDB, see **[BENCHING.md](./BENCHING.md)**.

Manual smoke test with `curl` (talk to the all-roles server on `:8080`):

```sh
T=upDEMO0001
# subscribe (prints open + messages); or use the bundled mock:
#   UPF_TOPIC=$T cargo run --example mock_distributor
curl -N "http://localhost:8080/$T/json" &
# publish (UnifiedPush raw mode):
curl -X POST --data 'hi there' "http://localhost:8080/$T?up=1"
# offline replay: publish with nobody listening, then resume from the start:
curl -X POST --data 'while offline' "http://localhost:8080/$T?up=1"
curl "http://localhost:8080/$T/json?since=all&poll=1"
```

### With the real ntfy Android app

Because UPF speaks ntfy, you can test against a real device: in the ntfy app,
**Settings → set the server URL** to your all-roles UPF instance, then use it as
your UnifiedPush distributor. Registrations, the endpoint check, and delivery all
work. (Point it at a single base URL that serves both publish and subscribe — the
all-roles process does; see the split-deployment note below.)

### Load testing (`examples/loadgen.rs`)

`loadgen` is a load generator *and* correctness checker: it opens many WebSocket
subscribers (fake devices) and many concurrent publishers (fake app servers),
then proves delivery is correct and measures end-to-end latency. Each topic gets
one publisher sending a strictly increasing sequence number `0,1,2,…` (awaited,
so commit order = queue-versionstamp order = delivery order) plus a send
timestamp; every subscriber then asserts it received that stream **in order, with
no gaps and no duplicates**, and records send→receive latency.

```sh
# Linux: run the server (`just run`) in one shell, then drive it:
just bench                                    # loadgen defaults (1000 topics, 2000/s, 30s)
just bench topics=2000 rate=6000 duration=20  # args pass straight to loadgen
just bench-sweep 2000 20                       # sweep rates to find the knee

# macOS dev container equivalent:
scripts/cargo.sh run --release --example loadgen -- topics=2000 rate=6000 duration=20
```

For benchmarking against a multi-process FDB on a real Linux host, see
**[BENCHING.md](./BENCHING.md)**.

It prints a live per-second line and a final report with throughput, a bounded
latency histogram (p50/p90/p95/p99/max), and a correctness verdict (loss /
out-of-order / duplicates / gaps).

Two things to know when reading the output:

- **One subscriber per topic.** This server serves a single subscriber per topic
  (the UnifiedPush model — a newer subscribe displaces the older locally). Set
  `subs=1`; a higher value makes the extra connections receive nothing and the
  run report apparent loss *by design*. Add scale with `topics`, not `subs`.
- **Overload degrades to latency, not loss.** Past the delivery rate the node can
  sustain, ingest outruns delivery and a backlog builds in `Q`, so latency climbs
  — but nothing is dropped: the backlog drains once publishing stops and every
  message still arrives exactly once, in order. That's the durable-queue design
  working as intended.

> Correctness results (loss/order/dup) from the single-node **in-memory** FDB in
> `dev-up.sh` are fully valid. The **throughput/latency numbers are not** capacity
> figures — a real multi-process `ssd` cluster (and pushers on their own nodes)
> moves the knee substantially.

---

## Running as separate containers (`compose.yaml`)

`scripts/cargo.sh run` is a single all-roles process. `compose.yaml` runs the
real deployment shape: FoundationDB plus **writer, pusher and janitor as three
separate containers** — each the *same* image (`Containerfile`) with a different
`UPF_ROLES`. They share nothing but the database.

```sh
docker compose up --build      # or: podman compose up --build
```

- **writer** — published on `localhost:8080`; app servers `POST /{topic}` here.
- **pusher** — published on `localhost:8081`; distributors subscribe at
  `/{topic}/ws`.
- **janitor** — no ports; runs TTL expiry + sweeps.
- **fdb** + **fdb-init** — the database and a one-shot that configures it; the
  role containers wait on `fdb-init` completing.

Invariants the file encodes: writer and pusher share an identical
`UPF_SHARD_COUNT` (the poke addresses a shard both sides must agree on), and the
pusher has a stable `UPF_NODE_ID` so affinity/bell ownership survives restarts.
Scale a role with `docker compose up --scale pusher=3` — because coordination is
entirely in FDB, added pushers are immediately live.

> **Single base URL.** ntfy uses one origin for both publish and subscribe, so a
> distributor configured with base URL `X` will publish endpoints of the form
> `X/{topic}` *and* subscribe at `X/{topic}/ws`. In the split deployment, put an
> ingress in front that routes `/{topic}/ws|json|sse` → pusher and everything else
> → writer. The all-roles process already serves both, which is why it's the
> simplest target for a real distributor.

---

## Configuration (`UPF_*` environment variables)

| Variable | Default | Meaning |
| --- | --- | --- |
| `UPF_BIND` | `0.0.0.0:8080` | HTTP/WS listen address |
| `UPF_ROLES` | `writer,pusher,janitor` | roles this process runs |
| `UPF_NODE_ID` | random | this node's identity (affinity/inbox/bell owner) |
| `UPF_SHARD_COUNT` | `64` | inbox shards per node (`K`); must match fleet-wide |
| `UPF_MAX_MESSAGE_BYTES` | `4096` | max message body (ntfy/UnifiedPush limit) |
| `UPF_DEFAULT_TTL_SECS` | `2419200` (4w) | message retention / cache window |
| `UPF_SAFETY_POLL_SECS` | `60` | how often pushers re-drain live connections |
| `UPF_KEEPALIVE_SECS` | `45` | interval between `keepalive` frames |
| `UPF_HEARTBEAT_SECS` | `10` | pusher liveness heartbeat interval |
| `UPF_JANITOR_INTERVAL_SECS` | `30` | janitor pass interval |
| `UPF_PUBLIC_URL` | `http://localhost:8080` | informational (logged) |
| `FDB_CLUSTER_FILE` | system default | FoundationDB cluster file |

---

## Deployment shape

Writers scale with push rate; pushers scale with concurrent connections (a tuned
node holds 100k+); FDB scales commit throughput via proxies/logs and read
throughput via storage servers. All pushers are identical — add nodes to add
capacity. Multi-region: one cluster per region, region-prefixed into the topic,
routed by prefix.

## Roadmap

Deliberately deferred (the delivery core above is complete and tested):

- Topic access control / auth (ntfy tokens); today topics are open.
- Rate limiting (`429`) — including ntfy's subscribe-before-publish rule.
- Message attachments and the richer ntfy publish features (title/priority/tags
  are forwarded; attachments, actions, and scheduling are not).
- Metrics / OpenTelemetry.
