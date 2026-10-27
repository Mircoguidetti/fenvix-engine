#!/usr/bin/env bash
# Restart the two engines of the gap tests with the current release binary: 27018 (single-node replica set rs0, local store) and
# 27019 (standalone, local store). usage: gaps/restart_engines.sh [fresh]
cd "$(dirname "$0")/.."
[ "${1:-}" = fresh ] && rm -rf /tmp/gaps-store /tmp/gaps-store-sa
SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT% --store local --local-dir /tmp/gaps-store" ./server.sh 27018 600
SERVER_EXTRA="--ttl-sweep-secs 1 --store local --local-dir /tmp/gaps-store-sa" ./server.sh 27019 600
