# Benchmarking UPF on a native Linux host

On Linux, FoundationDB has a native client library, so UPF builds and runs
**without any container** — `cargo` links against the locally-installed
`libfdb_c.so`, and the FDB server runs natively under systemd. This removes the
build-container indirection the macOS dev flow needs (`scripts/`), and — more
importantly for benching — lets FDB, the app, and the load generator each use
real cores instead of fighting over a small shared VM.

Everything below is driven by the [`Justfile`](./Justfile) (`just --list`).

## 0. Prerequisites

A Debian/Ubuntu host with `sudo`, plus:

```sh
# Rust toolchain (if not already present)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# just (the task runner)
cargo install just         # or: apt install just / snap install --classic just
```

Clone the repo and `cd` into it. All `just` recipes run from the repo root.

## 1. Install dependencies and FoundationDB

```sh
just setup          # clang/libclang, build-essential, curl
just install-fdb    # FoundationDB client + server .debs (matches the pinned version)
```

`install-fdb` leaves a working single-process DB running (the server package
auto-configures one). Confirm:

```sh
just fdb-status
```

## 2. Build and (optionally) test

```sh
just build          # release build of the server + loadgen

just fdb-single     # single-process in-memory DB — the correctness engine
just test           # unit + e2e tests against it
```

## 3. Stand up a multi-process FDB for benching

```sh
just fdb-bench 8    # 8 fdbserver processes, ssd engine, logs/proxies sized to 8
```

This regenerates `/etc/foundationdb/foundationdb.conf` from
[`fdb/foundationdb.conf`](./fdb/foundationdb.conf) with N process sections,
restarts FDB, and sizes the transaction subsystem. Pick N based on the box —
roughly one process per core, leaving a couple of cores for the app and the load
generator (which also run on this host unless you split across machines; see
§6). It **wipes** the DB; bench data is disposable.

Verify the process count and roles:

```sh
just fdb-status
```

## 4. Run the server, then drive it

Two terminals (or `tmux`):

```sh
# terminal 1 — the all-roles server on :8080
just run

# terminal 2 — the load generator
just bench topics=2000 rate=20000 duration=20
```

`bench` passes its args straight to the load generator as `key=value`
(`topics`, `subs`, `rate`, `duration`, `payload`, `http_base`, `ws_base`, …);
with no args, loadgen's own defaults apply (1000 topics, 2000/s, 30s). See the
top of [`examples/loadgen.rs`](./examples/loadgen.rs) for every knob.

To find the throughput knee in one shot:

```sh
just bench-sweep 2000 20     # sweeps rate = 2k…80k against 2000 topics, 20s each
```

## 5. Watch where the load actually lands

The point of a bench is knowing *what* saturates. While a run is in flight:

```sh
watch -n1 'fdbcli --exec "status" | sed -n "/Workload:/,/Backup/p"'   # FDB txn/read/write/conflict rates
fdbcli --exec "status details" | grep 127.0.0.1                        # per-process CPU/disk
top    # or htop — is it the app, loadgen, or fdb eating cores?
```

If FDB shows low CPU and headroom while delivery lags publish, the limit is the
app's delivery path, not the database — bump `just fdb-bench` won't help, and the
pusher is where to look. (That was exactly the finding on the small dev VM.)

## 6. Splitting roles / machines (closer to production)

**Split roles on one box** — give each its own port, point loadgen at both:

```sh
UPF_BIND=0.0.0.0:8080 just run-writer      # terminal 1
UPF_BIND=0.0.0.0:8081 just run-pusher      # terminal 2
UPF_BIND=0.0.0.0:8082 just run-janitor     # terminal 3 (no HTTP surface, but binds)
just bench http_base=http://localhost:8080 ws_base=ws://localhost:8081 \
           topics=2000 rate=20000 duration=20
```

**Across machines (rung 4 — the only true capacity test).** Run FDB on its own
host(s), copy `/etc/foundationdb/fdb.cluster` to the app host(s) (or set
`FDB_CLUSTER_FILE`), run writer/pusher there, and run loadgen from a *third*
host pointed at their addresses via `http_base`/`ws_base`. Now nothing
cannibalizes anyone else's cores, and the numbers mean something.

## 7. Teardown

```sh
just fdb-single     # back to a single in-memory process
# or stop FDB entirely:
sudo systemctl stop foundationdb
```
