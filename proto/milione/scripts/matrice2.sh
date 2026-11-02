#!/bin/bash
# Seconda serie: letture solo dai database già aperti (costo di conoscerne N, senza risvegli)
cd "$(dirname "$0")/.."
R=risultati/p1.jsonl; B=./target/release
for N in 1000 100000 1000000; do
  echo "== solo-aperti $N" >&2
  $B/pm-cameriere --n $N --capienza 100000 --dati ram --scalda-s 20 --durata-s 60 --solo-aperti --etichetta "solo-aperti-lru-prima-$N" --out $R
done
echo FINE >&2
