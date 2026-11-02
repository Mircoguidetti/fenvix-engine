#!/usr/bin/env bash
# (ri)avvia il proxy in sfondo; il pid va in risultati/proxy.pid
cd "$(dirname "$0")/.."
[ -f risultati/proxy.pid ] && kill "$(cat risultati/proxy.pid)" 2>/dev/null && sleep 0.5
setsid nohup ./target/release/proxy --modello "${1:-zero}" > risultati/proxy.log 2>&1 < /dev/null &
echo $! > risultati/proxy.pid
sleep 1
