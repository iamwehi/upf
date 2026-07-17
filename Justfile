# UPF task runner — one interface, two platforms.
#
# On Linux, FoundationDB has a native client library: `cargo` runs directly and
# FDB runs natively under systemd. On macOS there is no native FDB client lib, so
# builds run in the dev container and FDB runs in a podman pod (scripts/). This
# Justfile dispatches to the right path per OS, so the same commands work on both:
#
#   just setup            # deps (apt on Linux; checks podman on macOS)
#   just fdb-single       # a FoundationDB to develop against
#   just build && just test
#   just run              # terminal 1: the all-roles server
#   just bench            # terminal 2: drive it with the load generator
#
# Benchmarking against a multi-process FDB: `just fdb-bench N`. See BENCHING.md.

set shell := ["bash", "-uc"]

# FoundationDB version — must match the libfdb_c the binary links against.
fdb_version := "7.3.78"

# Default number of fdbserver processes for `fdb-bench` (Linux).
procs := "4"

# Debian package arch for the FDB downloads, derived from the host.
deb_arch := if arch() == "aarch64" { "arm64" } else { "amd64" }

# THE OS SWITCH: on Linux, cargo runs natively; on macOS it runs in the dev
# container (scripts/cargo.sh forwards args + UPF_* env to podman). Every build
# recipe goes through this, so they are identical on both platforms.
cargo := if os() == "macos" { "scripts/cargo.sh" } else { "cargo" }

# List recipes.
default:
    @just --list

# ---- build / test (native on Linux, container on macOS) -------------------

build:
    {{cargo}} build --release --bin upf --example loadgen

check:
    {{cargo}} check --all-targets

# Unit + e2e tests (needs a running FDB — e.g. `just fdb-single`).
test:
    {{cargo}} test

fmt:
    {{cargo}} fmt

clippy:
    {{cargo}} clippy --all-targets -- -D warnings

clean:
    {{cargo}} clean

# ---- run the server -------------------------------------------------------

# Run the given roles (default: all three in one process).
run roles="writer,pusher,janitor":
    UPF_ROLES={{roles}} {{cargo}} run --release --bin upf

# Run one role (macOS pod publishes only :8080, so one at a time; real split → Linux/compose).
run-writer:  (run "writer")
run-pusher:  (run "pusher")
run-janitor: (run "janitor")

# ---- benchmarking ---------------------------------------------------------

# Drive the running server with loadgen; args pass through as key=value (e.g. topics=2000 rate=20000).
bench *args="":
    {{cargo}} run --release --example loadgen -- {{args}}

# Sweep publish rates against the running server to find the knee (positional: topics duration).
bench-sweep topics="2000" duration="20" *extra="":
    #!/usr/bin/env bash
    set -euo pipefail
    for r in 2000 5000 10000 20000 40000 80000; do
      echo "======== rate=$r ========"
      {{cargo}} run --release --example loadgen -- \
        topics={{topics}} rate=$r duration={{duration}} {{extra}} | \
        grep -E "throughput|published:|delivered:|p50 |loss:|VERDICT"
    done

# ---- one-time setup -------------------------------------------------------

# Install build dependencies (apt on Linux; verifies podman on macOS).
setup:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname)" = Darwin ]; then
      command -v podman >/dev/null || { echo "install podman first (brew install podman && podman machine init && podman machine start)"; exit 1; }
      echo "macOS: builds + FDB run in containers. Bring FDB up with: just fdb-single"
    else
      sudo apt-get update
      sudo apt-get install -y --no-install-recommends clang libclang-dev build-essential curl ca-certificates
      echo "Linux deps installed. Next: just install-fdb"
    fi

# Install the FoundationDB client + server packages (Linux; no-op on macOS).
install-fdb version=fdb_version:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname)" = Darwin ]; then
      echo "macOS: FoundationDB runs from a container image — no host install."
      echo "Use: just fdb-single  (single-process)  or  just fdb-bench  (multi-process)"
      exit 0
    fi
    cd /tmp
    base="https://github.com/apple/foundationdb/releases/download/{{version}}"
    curl -fLO "$base/foundationdb-clients_{{version}}-1_{{deb_arch}}.deb"
    curl -fLO "$base/foundationdb-server_{{version}}-1_{{deb_arch}}.deb"
    sudo dpkg -i "foundationdb-clients_{{version}}-1_{{deb_arch}}.deb" \
                 "foundationdb-server_{{version}}-1_{{deb_arch}}.deb"
    fdbcli --exec "status minimal"

# ---- FoundationDB management ----------------------------------------------

# Show full cluster status.
fdb-status:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname)" = Darwin ]; then
      if podman container exists fdb-mp; then c=fdb-mp
      elif podman container exists fdb; then c=fdb
      else echo "no fdb container running; try: just fdb-single"; exit 1; fi
      exec podman exec "$c" fdbcli --exec "status details"
    fi
    fdbcli --exec "status details"

# Single-process, in-memory DB — fast correctness loop (used by `just test`).
fdb-single:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname)" = Darwin ]; then exec scripts/dev-up.sh; fi
    sudo cp fdb/foundationdb.conf /etc/foundationdb/foundationdb.conf
    sudo systemctl restart foundationdb
    for _ in $(seq 1 30); do fdbcli --exec "status minimal" >/dev/null 2>&1 && break; sleep 1; done
    fdbcli --exec "configure new single memory" || true
    fdbcli --exec "status minimal"

# Multi-process ssd DB for benching: N processes, logs/proxies sized to N. WIPES data.
fdb-bench procs=procs:
    #!/usr/bin/env bash
    set -euo pipefail
    # macOS uses the fixed 4-process container variant; Linux scales to `procs`.
    if [ "$(uname)" = Darwin ]; then echo "macOS: multi-process FDB via scripts/dev-up-mp.sh (4 procs)"; exec scripts/dev-up-mp.sh; fi
    P={{procs}}
    tmp=$(mktemp)
    cp fdb/foundationdb.conf "$tmp"
    for i in $(seq 1 $((P-1))); do printf '[fdbserver.%d]\n' $((4500+i)) >> "$tmp"; done
    sudo cp "$tmp" /etc/foundationdb/foundationdb.conf
    rm -f "$tmp"
    sudo systemctl restart foundationdb
    echo "waiting for $P processes..."
    for _ in $(seq 1 30); do fdbcli --exec "status minimal" >/dev/null 2>&1 && break; sleep 1; done
    fdbcli --exec "configure new single ssd" || true
    L=$(( P/4>0 ? P/4 : 1 )); C=$(( P/4>0 ? P/4 : 1 )); G=$(( P/8>0 ? P/8 : 1 ))
    fdbcli --exec "configure ssd logs=$L commit_proxies=$C grv_proxies=$G resolvers=1" || true
    fdbcli --exec "status details" | grep -E "FoundationDB processes|Redundancy|Storage engine|Desired|Fault Tolerance"

# Stop / tear down FoundationDB.
fdb-down:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(uname)" = Darwin ]; then exec scripts/dev-down.sh; fi
    sudo systemctl stop foundationdb
