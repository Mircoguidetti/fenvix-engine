#!/usr/bin/env bash
# The production build of the node and the writer must refuse the test-only options (exit code 2), and the writer
# must refuse an object store that is not on this machine. Built in its own target directory, without features.
set -u
cd "$(dirname "$0")/../p2"
T=$(mktemp -d)
cargo build -q --release -p j3z-node -p j3z-writer --target-dir "$T" || exit 1
N=$T/release/j3z-node; W=$T/release/j3z-writer
base="--holder w --index 0 --nwriters 1 --me 1 --nodes 127.0.0.1:1,127.0.0.1:2,127.0.0.1:3 --dbs 1 --client-listen 127.0.0.1:0 --bucket b --s3-key k --s3-secret s"
fail=0
expect2() { "$@" >/dev/null 2>&1; rc=$?; if [ $rc -ne 2 ]; then echo "FAIL (exit $rc, want 2): $*"; fail=1; else echo "ok (exit 2): ${*: -2}"; fi; }
expect2 $N --dir "$T/n" --listen 127.0.0.1:0 --mutation no_fencing
expect2 $N --dir "$T/n" --listen 127.0.0.1:0 --faults-file /nonexistent
expect2 $W $base --s3-endpoint http://127.0.0.1:9 --mutation no_fencing
expect2 $W $base --s3-endpoint http://127.0.0.1:9 --mutation lease_no_margin
expect2 $W $base --s3-endpoint http://127.0.0.1:9 --faults-file /nonexistent
expect2 $W $base --s3-endpoint https://s3.amazonaws.com
rm -rf "$T"
exit $fail
