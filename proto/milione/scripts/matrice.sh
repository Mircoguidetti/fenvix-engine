#!/bin/bash
# Le corse della prova P1 nell'ordine del rapporto. Il finto S3 su disco deve girare sulla porta 9000.
set -u
cd "$(dirname "$0")/.."
R=risultati/p1.jsonl
B=./target/release
c() { echo "== $*" >&2; "$@" --out $R; }
for N in 1000 100000 1000000; do
  c $B/pm-cameriere --n $N --capienza 100000 --dati ram --scalda-s 20 --durata-s 60 --etichetta "letture-$N"
done
# tempesta: dopo il riavvio, 1.000 freddi al secondo per 30 s, senza e con 30 ms per GET (S3 Standard)
c $B/pm-cameriere --n 1000000 --capienza 100000 --dati ram --scalda-s 5 --durata-s 10 --tempesta 1000 --tempesta-s 30 --etichetta tempesta-0ms
c $B/pm-cameriere --n 1000000 --capienza 100000 --dati ram --scalda-s 5 --durata-s 10 --tempesta 1000 --tempesta-s 30 --ritardo-get-ms 30 --etichetta tempesta-30ms
# RAM per aperto senza la scatola (scatola nella cache su disco locale)
c $B/pm-cameriere --n 1000000 --capienza 100000 --dati disco --scalda-s 5 --durata-s 30 --etichetta disco-1000000
echo FINE >&2
