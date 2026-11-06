#!/bin/bash
# aspetta la fine della prova coi segmenti a 0,25 GB, ferma la prima matrice e lancia la seconda
while ! grep -q '"etichetta":"cliente-c4-s16-l1-0.25gb"' risultati/carichi.jsonl; do sleep 10; done
while ! grep -q '"etichetta":"cliente-c4-s16-l1-0.25gb"' risultati/misure.jsonl; do sleep 5; done
pkill -f scripts/matrice.sh; sleep 2; pkill -f "lavaggio-per-cliente carica"; sleep 5
rm -rf data/*
echo "$(date +%T) prima matrice fermata"
scripts/matrice2.sh
