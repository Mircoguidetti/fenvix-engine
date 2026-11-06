#!/bin/bash
# Simulatore: scala delle taglie (L0 8 MB come oggi) e 20 TB con L0 da 64 e 256 MB, ritmo 7,6 MB/s (4.000 scritture/s x 1,9 KB).
OUT=risultati/sim.jsonl
s() { nice -n 10 python3 scripts/sim.py --ovh 1.33 "$@" >> $OUT; }
for t in 0.25 0.5 1 2 10 50 100 200; do
  for c in 0 2 4 6; do s --taglia-gb $t --w-mbs 5 --l0-mb 8 --consolida $c; done
done
for l0 in 64 256; do
  for c in 0 2 4 6; do s --taglia-gb 20480 --w-mbs 7.6 --l0-mb $l0 --consolida $c; done
  for k in 2 3 4; do s --taglia-gb 20480 --w-mbs 7.6 --l0-mb $l0 --consolida $k --modo grandi --soglia-grandi 64; done
done
