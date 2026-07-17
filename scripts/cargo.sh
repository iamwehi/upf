#!/usr/bin/env bash
# Run cargo inside the dev container, joined to the `upf` pod so it can reach
# FoundationDB (127.0.0.1:4500) and, for `run`, serve on the pod's port 8080.
#
#   scripts/cargo.sh build
#   scripts/cargo.sh test
#   scripts/cargo.sh run
set -euo pipefail

POD=upf
DEV_IMAGE=upf-dev
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [ -t 0 ]; then TTY=(-it); else TTY=(-i); fi

exec podman run --rm "${TTY[@]}" --pod "$POD" \
  -v "$ROOT":/work:Z \
  -v "$ROOT/.fdb/fdb.cluster":/etc/foundationdb/fdb.cluster:ro,Z \
  -e FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster \
  -e UPF_PUBLIC_URL="${UPF_PUBLIC_URL:-http://localhost:8080}" \
  -e CARGO_HOME=/work/.cargo-home \
  -e RUST_LOG="${RUST_LOG:-upf=debug,info}" \
  ${UPF_ROLES:+-e UPF_ROLES="$UPF_ROLES"} \
  ${UPF_BIND:+-e UPF_BIND="$UPF_BIND"} \
  "$DEV_IMAGE" cargo "$@"
