#!/bin/bash
# 4 GB, pile a gradini T=4, scrittore a 2 MB/s, da sola sulla macchina (la prima a 4 GB girava insieme ad altre prove)
ET=cliente-c0-s0-l1-v4-r2-g1-solo-4gb
B=./target/release/lavaggio-per-cliente
$B carica --disposizione cliente --livelli 4 --ritmo-mbs 2 --taglia-gb 4 --regime 1 --dir data/$ET --etichetta $ET > logs/$ET.log 2>&1
$B misura --disposizione cliente --dir data/$ET --etichetta $ET --n 200 >> logs/$ET.log 2>&1
rm -rf "data/cliente-c0-s0-l1-v4-r2-g1-solo-4gb"
echo fatto >> logs/$ET.log
