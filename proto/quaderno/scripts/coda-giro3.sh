#!/usr/bin/env bash
cd "$(dirname "$0")/.."
until grep -q FINITO /root/pq-work/b6.log 2>/dev/null; do sleep 10; done
scripts/banco2-10min.sh results/banco2-scarico-10min 2000 2048 > /root/pq-work/b2-10.log 2>&1; echo "banco2-10min $?" >> /root/pq-work/coda3.log
echo FINITO >> /root/pq-work/coda3.log
