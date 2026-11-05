#!/bin/bash
# M31 in locale: il carico di P1 (10 GB, 5.120 clienti da circa 2 MB, 30% dormienti fino al 60%) e il carico da 2 KB.
set -u
cd "$(dirname "$0")"
B=./target/release/m31
mkdir -p logs risultati data
$B carica --disposizione cliente --livelli 4 --lavaggio 1 --taglia-gb ${GB:-10} --regime 0 --clienti ${CLIENTI:-5120} --dormienti 0.3 --fine-dormienti 0.6 \
  --blocco-kb 64 --compressione lz4 --documenti veri --attesa-fine 0 --ritmo-mbs ${RITMO:-4} --doc-byte 310 --dir data/p1 --etichetta p1-${GB:-10}gb --out risultati/carichi.jsonl > logs/carica-p1.log 2>&1
$B carica --disposizione cliente --livelli 4 --lavaggio 1 --taglia-gb ${GB2:-2} --regime 0 --clienti ${CLIENTI2:-1024} --dormienti 0.3 --fine-dormienti 0.6 \
  --blocco-kb 64 --compressione lz4 --documenti veri --attesa-fine 0 --ritmo-mbs ${RITMO:-4} --doc-byte 2000 --dir data/d2k --etichetta d2k-${GB2:-2}gb --out risultati/carichi.jsonl > logs/carica-d2k.log 2>&1
echo fatto
