#!/usr/bin/env bash
# Bring up the local dev environment: a podman pod running FoundationDB, an
# initialized database, an exported cluster file, and the built dev image.
set -euo pipefail

POD=upf
FDB_IMAGE=docker.io/foundationdb/foundationdb:7.3.78
DEV_IMAGE=upf-dev
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Pod: shared network namespace for fdb + app; publish the HTTP port.
podman pod exists "$POD" || podman pod create --name "$POD" -p 8080:8080

# FoundationDB in host networking mode → coordinator on 127.0.0.1:4500,
# reachable by the app container in the same pod.
if ! podman container exists fdb; then
  podman run -d --pod "$POD" --name fdb -e FDB_NETWORKING_MODE=host "$FDB_IMAGE"
fi

echo "waiting for fdbserver..."
for _ in $(seq 1 30); do
  if podman exec fdb fdbcli --exec "status minimal" >/dev/null 2>&1; then break; fi
  sleep 1
done

# Initialize the database (idempotent: errors if already configured).
podman exec fdb fdbcli --exec "configure new single memory" >/dev/null 2>&1 || true
podman exec fdb fdbcli --exec "status minimal"

# Export the cluster file for the app/tests to mount.
mkdir -p "$ROOT/.fdb"
podman exec fdb cat /var/fdb/fdb.cluster > "$ROOT/.fdb/fdb.cluster"
echo "cluster file: $(cat "$ROOT/.fdb/fdb.cluster")"

# Build the dev image (rust + matching libfdb_c).
podman build -t "$DEV_IMAGE" -f "$ROOT/Containerfile.dev" "$ROOT"

echo "dev environment ready. Use scripts/cargo.sh to build/run/test."
