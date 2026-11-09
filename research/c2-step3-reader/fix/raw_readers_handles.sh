#!/usr/bin/env bash
# Fix round of step 3, defect 1: the raw-reader check must fail when a file other than src/maps.rs calls
# lifecycle::open_writers() or open_readers() (their Db handles read SlateDB directly, which a grep cannot follow).
# Each case adds one stray caller to a copy of the tree and runs judge/raw_readers.sh there. Exit 1 when a case passes.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
miss=0
run_case() {
  local name=$1 file=$2 body=$3
  local tmp; tmp=$(mktemp -d)
  mkdir -p "$tmp/judge" "$tmp/mongo-server"
  cp "$ROOT/judge/raw_readers.sh" "$tmp/judge/"; cp -r "$ROOT/src" "$tmp/src"; cp -r "$ROOT/mongo-server/src" "$tmp/mongo-server/src"
  printf '%s\n' "$body" > "$tmp/$file"
  if "$tmp/judge/raw_readers.sh" > "$tmp/out.txt" 2>&1; then echo "MISSED ($name)"; miss=$((miss+1)); else echo "caught ($name)"; fi
  rm -rf "$tmp"
}
run_case "open_writers() in mongo-server" mongo-server/src/stray_handles.rs 'pub fn stray() -> usize { tenant_bench::lifecycle::open_writers().len() }'
run_case "open_readers() in src" src/stray_handles.rs 'pub fn stray() -> usize { crate::lifecycle::open_readers().len() }'
if [ $miss -gt 0 ]; then echo "handle check: FAIL ($miss missed)"; exit 1; fi
echo "handle check: every case caught"
