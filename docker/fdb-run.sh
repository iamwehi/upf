#!/usr/bin/env bash
# Entrypoint for the FoundationDB service: launch FDB_PROCS fdbserver processes
# in one container, all on this container's IP (so peers on the compose network
# reach them via the shared cluster file). Unclassed — FDB auto-recruits roles
# and fdb-init sizes logs/proxies. Supervised by `wait`. Data is disposable.
set -e

IP=$(hostname -i | awk '{print $1}')
CF=/var/fdb/fdb.cluster
echo "docker:docker@$IP:4500" > "$CF"
mkdir -p /var/fdb/logs

P="${FDB_PROCS:-1}"
echo "starting $P fdbserver process(es) on $IP:4500..$((4500+P-1))"
for i in $(seq 0 $((P-1))); do
  port=$((4500+i))
  mkdir -p "/var/fdb/data/$port"
  /usr/bin/fdbserver --cluster-file "$CF" \
    --public-address "$IP:$port" --listen-address public \
    --datadir "/var/fdb/data/$port" --logdir /var/fdb/logs \
    --storage-memory 512MiB \
    --locality-zoneid z --locality-machineid "m$port" &
done
wait
