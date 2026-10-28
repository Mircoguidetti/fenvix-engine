#!/usr/bin/env bash
# Start the engine for the gap tests on 27018 (local object store, single-node replica set rs0 like the reference on 27020).
# usage: gaps/engine.sh [fresh] [extra args...]   -- "fresh" wipes the store first
cd "$(dirname "$0")/../.."
if [ "${1:-}" = fresh ]; then shift; rm -rf /tmp/gaps-store; fi
export SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT% --store local --local-dir /tmp/gaps-store --no-leases $*"
exec mongo-tests/server.sh 27018 600
