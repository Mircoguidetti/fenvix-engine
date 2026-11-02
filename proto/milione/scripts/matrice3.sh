#!/bin/bash
# Terza serie: scatole nella cache su disco locale, LRU grande (500.000 aperti su un milione: risvegli sotto l'1% atteso)
cd "$(dirname "$0")/.."
R=risultati/p1.jsonl; B=./target/release
$B/pm-cameriere --n 1000 --capienza 100000 --dati disco --scalda-s 20 --durata-s 60 --etichetta disco-1000 --out $R
$B/pm-cameriere --n 1000000 --capienza 500000 --dati disco --scalda-s 30 --durata-s 60 --etichetta disco-lru500k-1000000 --out $R
echo FINE >&2
