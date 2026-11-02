#!/usr/bin/env bash
# Corsa completa del banco M10. Serve: moto su 127.0.0.1:9000 con il bucket prova-kn, il proxy su 127.0.0.1:9100,
# gli oggetti già caricati (`banco carica`). Uso: scripts/corsa.sh <modello> <file di uscita> [taglie]
set -euo pipefail
cd "$(dirname "$0")/.."
B=./target/release/banco
MOD=$1
OUT=$2
TAGLIE=${3:-"4096 65536 1048576 16777216"}
P=http://127.0.0.1:9100

# riscaldamento: ogni (oggetto, intervallo) passa una volta da moto, senza ritardo
curl -s -X POST $P/__modello/zero >/dev/null
for s in $TAGLIE; do
  for st in normale pezzi4 kn:4+1 kn:8+2; do
    $B leggi --strategia $st --taglia $s --letture 400 --conc 8 >/dev/null
  done
done
curl -s -X POST $P/__modello/$MOD >/dev/null

for s in $TAGLIE; do
  if [ "$s" -ge 8388608 ]; then N=1000; C=2; else N=10000; C=4; fi
  # prima la normale: il suo p95 è la soglia della richiesta doppia
  $B leggi --strategia normale --taglia $s --letture $N --conc $C --uscita $OUT --etichetta $MOD
  P95=$(tail -1 $OUT | python3 -c 'import json,sys; print(round(json.load(sys.stdin)["p95_ms"]))')
  for st in pezzi4 doppia:$P95 copie:2 copie:5 copie:10 kn:4+1 kn:8+2; do
    $B leggi --strategia $st --taglia $s --letture $N --conc $C --uscita $OUT --etichetta $MOD
  done
done
