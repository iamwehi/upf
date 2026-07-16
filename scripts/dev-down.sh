#!/usr/bin/env bash
# Tear down the local dev environment (pod + FoundationDB container).
set -euo pipefail
podman pod rm -f upf 2>/dev/null || true
echo "torn down. (FDB data was in-memory; the exported .fdb/ cluster file remains.)"
