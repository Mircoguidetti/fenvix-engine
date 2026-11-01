#!/bin/bash
# Rifà tutte le misure di N3 (misura-lettore). Tutto su 1 core (CPU 0).
set -e
cd "$(dirname "$0")"
cargo build --release
B=./target/release/misura-lettore
mkdir -p risultati
c() { n=$1; shift; RUST_BACKTRACE=0 taskset -c 0 $B "$@" --out risultati/$n.json > risultati/$n.log 2>&1; }
c grande-1run-b4k --docs 200000
c grande-1run-b16k --docs 200000 --blocco 16384
c grande-5run-b4k --docs 200000 --run 5
c piccolo-1run-b4k --docs 2000
c grande-1run-b16k-slate16k --docs 200000 --blocco 16384 --slate-blocco 16384
