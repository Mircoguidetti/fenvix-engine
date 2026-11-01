#!/usr/bin/env bash
# Runs every area of the unified spec tests.
# TESTS = the test/ folder of mongo-python-driver at the tag of the PyMongo version installed (here 4.19.0):
# the JSON copies of github.com/mongodb/specifications that the PyMongo team runs for that version, so that what the
# tests expect from the driver matches the driver used.
set -u
cd "$(dirname "$0")"
T=${TESTS:?path of mongo-python-driver/test}
sv() { grep -l '"schemaVersion"' $(find "$@" -name '*.json' 2>/dev/null) 2>/dev/null; }
./run_area.sh crud $(sv $T/crud)
./run_area.sh indici $(sv $T/index_management)
./run_area.sh collezioni $(sv $T/collection_management)
./run_area.sh sessioni $(sv $T/sessions)
./run_area.sh transazioni $(sv $T/transactions $T/transactions-convenient-api)
./run_area.sh change-streams $(sv $T/change_streams)
./run_area.sh retryable-writes $(sv $T/retryable_writes)
./run_area.sh retryable-reads $(sv $T/retryable_reads)
./run_area.sh monitoraggio-comandi $(sv $T/command_monitoring $T/command_logging)
./run_area.sh run-command $(sv $T/run_command)
./run_area.sh read-write-concern $(sv $T/read_write_concern)
./run_area.sh versioned-api $(sv $T/versioned-api)
./run_area.sh gridfs $(sv $T/gridfs)
./run_area.sh causal-consistency $(sv $T/causal_consistency)
