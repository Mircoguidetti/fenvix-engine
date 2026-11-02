#!/usr/bin/env bash
cd "$(dirname "$0")/.."
scripts/banco6.sh results/banco6 600 2560 2500 3000 3500 > /root/pq-work/b6.log 2>&1; echo "banco6 $?" >> /root/pq-work/coda3.log
scripts/banco2-10min.sh results/banco2-scarico-10min 2000 2048 > /root/pq-work/b2-10.log 2>&1; echo "banco2-10min $?" >> /root/pq-work/coda3.log
scripts/ab-banco1.sh results/ab-banco1 > /root/pq-work/ab.log 2>&1; echo "ab $?" >> /root/pq-work/coda3.log
echo FINITO >> /root/pq-work/coda3.log
