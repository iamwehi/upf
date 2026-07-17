# Benchmarking UPF

The whole stack runs in containers via [`compose.yaml`](./compose.yaml) — FDB,
the three roles, and the load generator — so benching is the same everywhere. On
a **native Linux host** the containers are just namespaced host processes (no
VM), so FDB, the roles, and loadgen each use real cores; that's where throughput
numbers mean something. On macOS everything runs inside the podman VM (a few
shared cores), so treat those numbers as correctness-only.

Everything is driven by the [`Justfile`](./Justfile) (`just --list`).

## 0. Prerequisites

- `podman` and a compose provider: `podman-compose` (`brew install podman-compose`
  on macOS, or `pip install podman-compose`) or `docker compose` (override with
  `COMPOSE="docker compose"`).
- `just` (`cargo install just`, `apt install just`, or `snap install --classic just`).

Clone the repo and `cd` in. `just setup` verifies the above.

## 1. Build the images

```sh
just build      # server + loadgen image, FDB image, dev image
```

Re-run this after any code change (the bench runs the built image, not source).

## 2. Bring up a multi-process FDB + the roles

```sh
just up 8 ssd   # 8 fdbserver processes on the ssd engine, + writer/pusher/janitor
```

`up N ssd` runs N host-networked `fdbserver` processes in the `fdb` container,
creates the DB on `ssd`, and sizes logs/proxies to N (the rest serve storage).
Pick N by the box — roughly one process per core, leaving a couple for the roles
and loadgen. (`just up` with no args is 1 process / in-memory — the fast
correctness default.) Confirm the process count and roles:

```sh
just fdb-status
```

## 3. Drive it

```sh
just bench topics=2000 rate=20000 duration=20    # one run
just bench-sweep 2000 20                          # sweep rate = 2k…80k, 20s each
```

`bench` runs the `loadgen` container against the roles by service name; args pass
straight through as `key=value` (`topics`, `subs`, `rate`, `duration`, `payload`,
…; see the top of [`examples/loadgen.rs`](./examples/loadgen.rs)). It prints a
live per-second line and a final report: throughput, a latency histogram
(p50/p90/p95/p99/max), and a correctness verdict (loss / order / dup / gaps).

## 4. Watch where the load actually lands

The point of a bench is knowing *what* saturates. While a run is in flight:

```sh
watch -n1 'just fdb-status | sed -n "/Workload:/,/Backup/p"'   # FDB txn/read/write rates
just fdb-status | grep :450                                    # per-process CPU/disk
podman stats --no-stream                                       # per-container CPU/mem
```

If FDB shows low CPU and headroom while delivery lags publish, the limit is the
app's delivery path, not the database — scaling FDB won't help, and the pusher is
where to look. (That was the finding on the small dev VM: FDB was never the
bottleneck there.)

## 5. Scaling and splitting

- **More pushers:** `podman compose up -d --scale pusher=3` — because coordination
  is entirely in FDB, added pushers are immediately live. (Put an ingress in front
  routing `/{topic}/ws|json|sse` → pushers and everything else → writer.)
- **Across machines (the only true capacity test).** Run FDB on its own host(s),
  the roles on another, and loadgen from a third, so nothing cannibalizes anyone
  else's cores. Point the roles at FDB via the shared cluster file and loadgen at
  the roles via `http_base`/`ws_base`.

## 6. Teardown

```sh
just down       # stop everything and wipe the volumes
```
