# UPF task runner — the whole stack in containers, one way on every OS.
#
# Everything (FoundationDB, the three roles, the load generator, tests) runs in
# containers via compose, so there is no host toolchain and no OS branching.
#
#   just setup            # verify podman + a compose provider
#   just build            # build the images
#   just up               # FDB + writer + pusher + janitor  (1 proc, memory)
#   just up 4 ssd         # multi-process ssd FDB for benching
#   just bench topics=2000 rate=20000 duration=20
#   just test             # cargo test in a container against FDB
#   just down             # stop + wipe
#
# Change a code file → `just build` again (image rebuild). See BENCHING.md.

set shell := ["bash", "-uc"]

# Compose provider — override with COMPOSE="docker compose" if you prefer docker.
compose := env_var_or_default("COMPOSE", "podman compose")

# Inside the compose network the roles listen on :8080; loadgen targets them by
# service name (the host-published 8081 for the pusher is external-only).
targets := "http_base=http://writer:8080 ws_base=ws://pusher:8080"

# List recipes.
default:
    @just --list

# Verify podman + a compose provider are available.
setup:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v podman >/dev/null || { echo "install podman first"; exit 1; }
    {{compose}} version >/dev/null 2>&1 || { echo "install a compose provider: podman-compose (pip install podman-compose) or docker compose"; exit 1; }
    echo "ok: $({{compose}} version 2>/dev/null | head -1)"

# Build the images (server+loadgen, FDB, dev). Re-run after code changes.
build:
    {{compose}} build

# Bring up FDB + the three roles. procs = fdbserver processes, engine = memory|ssd.
up procs="1" engine="memory":
    FDB_PROCS={{procs}} FDB_ENGINE={{engine}} {{compose}} up -d fdb fdb-init writer pusher janitor
    @echo "up: writer -> localhost:8080   pusher -> localhost:8081   (FDB {{procs}} proc / {{engine}})"

# Stop everything and wipe the volumes (bench data is disposable).
down:
    {{compose}} down -v

# Tail logs (optionally for one service: `just logs pusher`).
logs *svc="":
    {{compose}} logs -f {{svc}}

ps:
    {{compose}} ps

# Full FoundationDB status.
fdb-status:
    {{compose}} exec fdb fdbcli --exec "status details"

# Unit + e2e tests: cargo test in the dev container against the running FDB.
test:
    {{compose}} run --rm dev cargo test

# ---- benchmarking ---------------------------------------------------------

# Drive the running roles with loadgen; args pass through as key=value (e.g. topics=2000 rate=20000).
bench *args="":
    {{compose}} run --rm loadgen {{targets}} {{args}}

# Sweep publish rates against the running roles to find the knee (positional: topics duration).
bench-sweep topics="2000" duration="20" *extra="":
    #!/usr/bin/env bash
    set -euo pipefail
    for r in 2000 5000 10000 20000 40000 80000; do
      echo "======== rate=$r ========"
      {{compose}} run --rm loadgen {{targets}} \
        topics={{topics}} rate=$r duration={{duration}} {{extra}} | \
        grep -E "throughput|published:|delivered:|p50 |loss:|VERDICT"
    done
