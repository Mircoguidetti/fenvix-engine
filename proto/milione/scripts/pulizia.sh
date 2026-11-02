#!/bin/bash
# Pulizia delle scatole orfane su un milione di database: orfani messi apposta, inventario, giro, verifica; poi la
# pulizia di oggi (LIST per database) su un campione di 10.000 per contarne le richieste
cd "$(dirname "$0")/.."
R=risultati/p1.jsonl; B=./target/release
$B/pm-pulizia --out $R sporca --n 1000000
$B/pm-pulizia --out $R inventario
$B/pm-pulizia --out $R giro
$B/pm-pulizia --out $R verifica --n 1000000
$B/pm-pulizia --out $R vecchio --da 500000 --quanti 10000
echo FINE >&2
