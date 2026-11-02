#!/bin/bash
# Prova "1.000 creazioni al secondo" sul finto S3 (HTTP), senza e con il calcolo SCRAM del server
cd "$(dirname "$0")/.."
R=risultati/p1.jsonl; B=./target/release
$B/pm-crea --out $R cpu --n 200
$B/pm-crea --out $R http --da 2000000 --ritmo 1000 --secondi 30 --iter 0
$B/pm-crea --out $R http --da 2100000 --ritmo 1000 --secondi 30 --iter 15000
$B/pm-crea --out $R http --da 2200000 --ritmo 500 --secondi 30 --iter 0
echo FINE >&2
