#!/usr/bin/env bash
# Review of compaction 2 step 3: the judge's raw-reader check (judge/raw_readers.sh) must fail when a direct SlateDB
# reader appears outside src/fx/ and src/lifecycle.rs (plan round 2, fix 3). It looks only for `DbReader::builder`,
# `SstReader::new`, `DbSnapshot` and `.snapshot().await`, so slatedb 0.17's other ways to read rows pass it:
# `DbReader::open` (db_reader.rs:910) and a `Db` opened with `Db::open` or `Db::builder` and read with get/scan.
# Each case adds one stray reader to a copy of the tree and runs the check. Exit 1 when the check misses any case.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
miss=0
run_case() {
  local name=$1 body=$2
  local tmp; tmp=$(mktemp -d)
  mkdir -p "$tmp/judge" "$tmp/mongo-server"
  cp "$ROOT/judge/raw_readers.sh" "$tmp/judge/"
  cp -r "$ROOT/src" "$tmp/src"
  cp -r "$ROOT/mongo-server/src" "$tmp/mongo-server/src"
  printf '%s\n' "$body" > "$tmp/mongo-server/src/stray_reader.rs"
  if "$tmp/judge/raw_readers.sh" > "$tmp/out.txt" 2>&1; then
    echo "MISSED ($name): the check passes with a direct SlateDB reader in mongo-server/src/stray_reader.rs"
    miss=$((miss+1))
  else
    echo "caught ($name)"
  fi
  rm -rf "$tmp"
}
run_case "DbReader::open" 'pub async fn stray(p: String, s: std::sync::Arc<dyn object_store::ObjectStore>) -> anyhow::Result<Option<bytes::Bytes>> {
    let r = slatedb::DbReader::open(p, s, slatedb::DbReaderMode::FollowLatest, Default::default()).await?;
    Ok(r.get(b"doc/k").await?)
}'
run_case "Db::open" 'pub async fn stray(p: String, s: std::sync::Arc<dyn object_store::ObjectStore>) -> anyhow::Result<Option<bytes::Bytes>> {
    let db = slatedb::Db::open(p, s).await?;
    Ok(db.get(b"doc/k").await?)
}'
run_case "Db::builder" 'pub async fn stray(p: String, s: std::sync::Arc<dyn object_store::ObjectStore>) -> anyhow::Result<Vec<slatedb::KeyValue>> {
    let db = slatedb::Db::builder(p, s).build().await?;
    let mut it = db.scan::<Vec<u8>, _>(..).await?;
    let mut out = Vec::new();
    while let Some(kv) = it.next().await? { out.push(kv); }
    Ok(out)
}'
run_case "DbReader::builder (control: must be caught)" 'pub async fn stray(p: String, s: std::sync::Arc<dyn object_store::ObjectStore>) -> anyhow::Result<()> {
    let _r = slatedb::DbReader::builder(p, s).build().await?;
    Ok(())
}'
if [ $miss -gt 0 ]; then echo "raw-reader check: FAIL ($miss direct readers missed)"; exit 1; fi
echo "raw-reader check: every case caught"
