#!/bin/bash
# Copia SlateDB 0.17.0 dal registro di cargo in vendor/slatedb e rende regolabile il livello di zstd
# (M29_ZSTD_LEVEL, di serie 3 come SlateDB). Unica modifica alla libreria, solo per questa prova.
set -e
cd "$(dirname "$0")"
S=$(ls -d ~/.cargo/registry/src/*/slatedb-0.17.0)
rm -rf vendor/slatedb && mkdir -p vendor && cp -r "$S" vendor/slatedb
sed -i 's|zstd::bulk::compress(&data, 3)|zstd::bulk::compress(\&data, std::env::var("M29_ZSTD_LEVEL").ok().and_then(\|v\| v.parse().ok()).unwrap_or(3))|' vendor/slatedb/src/format/sst.rs
grep -q M29_ZSTD_LEVEL vendor/slatedb/src/format/sst.rs
