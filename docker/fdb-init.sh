#!/usr/bin/env bash
# One-shot: wait for the FDB processes, create the database on the requested
# engine, and size the transaction subsystem to the process count. Completing
# successfully is the gate the role containers wait on.
set -e

export FDB_CLUSTER_FILE=/var/fdb/fdb.cluster
echo "waiting for fdbserver..."
for _ in $(seq 1 60); do
  fdbcli --exec 'status minimal' >/dev/null 2>&1 && break
  sleep 1
done

P="${FDB_PROCS:-1}"
E="${FDB_ENGINE:-memory}"
# Create if new (harmless error if already configured), then size logs/proxies.
fdbcli --exec "configure new single $E" || true
L=$(( P/4>0 ? P/4 : 1 )); C=$(( P/4>0 ? P/4 : 1 )); G=$(( P/8>0 ? P/8 : 1 ))
fdbcli --exec "configure $E logs=$L commit_proxies=$C grv_proxies=$G resolvers=1" || true
fdbcli --exec 'status minimal'
echo "foundationdb ready ($P procs, $E engine)."
