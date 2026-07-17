#!/usr/bin/env bash
# Bring up a MULTI-PROCESS FoundationDB in the dev pod, to test UPF against an
# FDB that isn't bottlenecked on a single process (the default `dev-up.sh` runs
# one process doing everything). One container runs four `fdbserver` processes,
# roles separated by class:
#
#   4500  stateless  — proxies, resolver, master, cluster controller (commit path)
#   4501  log        — transaction log (durably records commits)
#   4502  storage    — holds data, serves reads (drains)
#   4503  storage    — "
#
# Sized for the small dev VM (FDB shares its cores with the app + loadgen). Data
# is disposable (no volume); the DB uses the real `ssd` storage engine. Processes
# are launched directly (not via fdbmonitor) and supervised by `wait`.
set -euo pipefail

POD=upf
FDB_IMAGE=docker.io/foundationdb/foundationdb:7.3.78
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Pod: shared network namespace so the app container reaches FDB on 127.0.0.1.
podman pod exists "$POD" || podman pod create --name "$POD" -p 8080:8080

# Replace any existing fdb container with the multi-process one.
podman rm -f fdb fdb-mp >/dev/null 2>&1 || true

# Launch four fdbserver processes (host networking → all on 127.0.0.1). The
# cluster file lives at the image's default path so fdbcli finds it unaided.
podman run -d --pod "$POD" --name fdb-mp --entrypoint bash "$FDB_IMAGE" -c '
  set -e
  CF=/var/fdb/fdb.cluster
  [ -s "$CF" ] || echo "docker:docker@127.0.0.1:4500" > "$CF"
  mkdir -p /var/fdb/logs
  start() { # port class
    mkdir -p "/var/fdb/data/$1"
    /usr/bin/fdbserver --cluster-file "$CF" \
      --public-address "auto:$1" --listen-address public \
      --datadir "/var/fdb/data/$1" --logdir /var/fdb/logs \
      --storage-memory 512MiB \
      --locality-zoneid mp --locality-machineid "m$1" --class "$2" &
  }
  start 4500 stateless
  start 4501 log
  start 4502 storage
  start 4503 storage
  wait
'

echo "waiting for fdbserver..."
for _ in $(seq 1 30); do
  if podman exec fdb-mp fdbcli --exec "status minimal" >/dev/null 2>&1; then break; fi
  sleep 1
done

# Create the database on the ssd engine (idempotent).
podman exec fdb-mp fdbcli --exec "configure new single ssd" >/dev/null 2>&1 || true
echo "=== status details ==="
podman exec fdb-mp fdbcli --exec "status details"

# Export the cluster file for the app/tests to mount (same path dev-up.sh uses).
mkdir -p "$ROOT/.fdb"
podman exec fdb-mp cat /var/fdb/fdb.cluster > "$ROOT/.fdb/fdb.cluster"
echo "cluster file: $(cat "$ROOT/.fdb/fdb.cluster")"
echo "multi-process FDB ready. Use scripts/cargo.sh to build/run/test."
