#!/usr/bin/env bash
# Giro 2: tutti i banchi in fila dopo il banco 2.
cd "$(dirname "$0")/.."
until grep -q FINITO /root/pq-work/banco2.log 2>/dev/null; do sleep 10; done
scripts/banco1.sh results/banco1 a b c > /root/pq-work/b1.log 2>&1; echo "banco1 $?" >> /root/pq-work/coda.log
scripts/banco3.sh results/banco3 60 > /root/pq-work/b3.log 2>&1; echo "banco3 $?" >> /root/pq-work/coda.log
scripts/banco4.sh results/banco4 600 > /root/pq-work/b4.log 2>&1; echo "banco4 $?" >> /root/pq-work/coda.log
scripts/banco6.sh results/banco6 600 768 > /root/pq-work/b6.log 2>&1; echo "banco6 $?" >> /root/pq-work/coda.log
echo FINITO >> /root/pq-work/coda.log
