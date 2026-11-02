#!/usr/bin/env bash
# Giro finale con pochi minuti rimasti: a = due dischi Alibaba con riuso (79, 177), d = Meta 2024. Griglia snella.
set -uo pipefail
cd proto/bancone/sim && cargo build --release -q 2>&1 | tail -3; cd - >/dev/null
SIM=proto/bancone/sim/target/release/bancone-sim
J=$OUT/risultati.jsonl
SCARICATI=$OUT/byte_scaricati.txt
export POLITICHE=LRU,S3FIFO,ARC,WTinyLFU
leggi() { rm -f /tmp/t.zst
  aws s3api get-object --no-sign-request --bucket cache-datasets --key "$1" --range "bytes=0-$(( $2 - 1 ))" /tmp/t.zst >/dev/null 2>>$OUT/err.txt
  echo "$1 $(stat -c %s /tmp/t.zst 2>/dev/null || echo 0)" >> $SCARICATI; }
dec() { zstd -dc /tmp/t.zst 2>/dev/null; }
verifica() {
  caps=$(python3 -c "import json; d=[json.loads(l) for l in open('$J') if '\"verifica\"' in l and '\"$2\"' in l]; print(' '.join(str(x['cap']) for x in d if x['policy']=='lru'))")
  timeout 6m python3 proto/bancone/verifica_lcs.py "$1" $caps | sed "s/^{/{\"trace\": \"$2\", /" >> $OUT/libcachesim.jsonl
}
case "$PARTE" in
  a)
    for n in 79 177; do
      leggi cache_dataset_lcs/alibaba/$n.lcs.zst 300000000
      if [ $n = 79 ]; then
        dec | timeout 4m $SIM --kind lcs --mode block --input - --name alibaba-$n --max 12000000 --grid verifica --out $J --export /tmp/v.og 2>>$OUT/err.txt
        verifica /tmp/v.og alibaba-$n; rm -f /tmp/v.og
      fi
      dec | timeout 7m $SIM --kind lcs --mode block --input - --name alibaba-$n --max 12000000 --grid snello --out $J 2>>$OUT/err.txt
    done ;;
  d)
    k=cache_dataset_lcs/metaKV/202401_kv_traces_all_sort.csv.lcs.sample0.1.zst
    leggi $k 300000000
    dec | timeout 4m $SIM --kind lcs --mode kv --input - --name meta-202401 --max 15000000 --grid verifica --out $J --export /tmp/v.og 2>>$OUT/err.txt
    verifica /tmp/v.og meta-202401; rm -f /tmp/v.og
    dec | timeout 9m $SIM --kind lcs --mode kv --input - --name meta-202401 --max 15000000 --grid snello --out $J 2>>$OUT/err.txt ;;
esac
awk '{s+=$2} END {print "byte compressi scaricati", s}' $SCARICATI
tail -5 $OUT/err.txt
