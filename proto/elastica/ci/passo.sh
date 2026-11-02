#!/usr/bin/env bash
# M17: scarica (bucket pubblico cache-datasets, lettura anonima) e conta le richieste per minuto.
# LIMITE DURO del prompt: 30 GB in tutto. Ogni parte ha il suo tetto (somma 26 GB) e si ferma PRIMA di
# superarlo: la dimensione di ogni file si legge dall'elenco prima di scaricarlo, e si scarica il file intero
# con get-object (niente flussi con tee: errore di M15). Byte scaricati scritti in $OUT/byte_scaricati.txt.
set -uo pipefail
C=proto/elastica/ci/conta.py
B=cache-datasets
SC=$OUT/byte_scaricati.txt; : > $SC
tot() { awk '{s+=$2} END {print s+0}' $SC; }
prendi() { # chiave dimensione formato nome
  local k=$1 sz=$2 f=$3 nm=$4
  if [ $(( $(tot) + sz )) -gt $TETTO ]; then echo "TETTO: salto $k ($sz)"; return 1; fi
  rm -f /tmp/f.zst
  aws s3api get-object --no-sign-request --bucket $B --key "$k" /tmp/f.zst >/dev/null 2>>$OUT/err.txt || { echo "errore $k"; return 0; }
  echo "$k $(stat -c %s /tmp/f.zst)" >> $SC
  zstd -dc /tmp/f.zst 2>>$OUT/err.txt | python3 $C $f $nm $OUT/$nm.npz >> $OUT/conteggi.txt 2>>$OUT/err.txt
  rm -f /tmp/f.zst
}
elenco() { aws s3 ls --no-sign-request --recursive "s3://$B/$1" | awk '{print $4, $3}'; }
scegli() { # da "chiave dimensione" sceglie in ordine pseudo-casuale fisso i file tra MIN e MAX byte, fino a BUDGET
  python3 - "$1" "$2" "$3" <<'EOF'
import sys, hashlib
mn, mx, bud = map(int, sys.argv[1:4]); out = []; s = 0
rows = [l.split() for l in open('/tmp/elenco.txt') if l.strip()]
rows = [(k, int(z)) for k, z in rows if k.endswith('.zst') and mn <= int(z) <= mx]
rows.sort(key=lambda r: hashlib.md5(r[0].encode()).hexdigest())
for k, z in rows:
    if s + z > bud: continue
    out.append((k, z)); s += z
for k, z in out: print(k, z)
EOF
}
BUD=4000000000; GRAPPOLI="26 10 50 53 45 3 35 49 7 13 8 24"
if [ "${GIRO:-}" = prova ]; then BUD=300000000; GRAPPOLI="26 10"; fi
case "$PARTE" in
  tw) TETTO=7000000000
      # grappoli Twitter (oracleGeneral, 10% delle chiavi, settimana intera): i piccoli più il 24 di M15
      for c in $GRAPPOLI; do
        k=cache_dataset_oracleGeneral/2020_twitter/cluster$c.oracleGeneral.sample10.zst
        sz=$(aws s3 ls --no-sign-request s3://$B/$k | awk '{print $3}')
        prendi $k ${sz:-0} og tw$c || break
      done ;;
  al) TETTO=4500000000
      elenco cache_dataset_lcs/alibaba/ > /tmp/elenco.txt; wc -l /tmp/elenco.txt
      scegli 20000000 200000000 $BUD > $OUT/scelti.txt
      while read k sz; do n=$(basename $k .lcs.zst); prendi $k $sz lcs al$n || break; done < $OUT/scelti.txt ;;
  te) TETTO=4500000000
      elenco cache_dataset_lcs/tencentBlock/ > /tmp/elenco.txt; wc -l /tmp/elenco.txt
      scegli 10000000 200000000 $BUD > $OUT/scelti.txt
      while read k sz; do n=$(basename $k .lcs.zst); prendi $k $sz lcs te$n || break; done < $OUT/scelti.txt ;;
  me) TETTO=5000000000
      for k in cache_dataset_lcs/metaKV/202401_kv_traces_all_sort.csv.lcs.sample0.01.zst \
               cache_dataset_lcs/metaKV/202206_kv_traces_all.csv.lcs.sample0.01.zst \
               cache_dataset_lcs/metaKV/202210_kv_traces_all_sort.csv.lcs.sample0.01.zst \
               cache_dataset_lcs/metaKV/202312_kv_traces_all.csv.lcs.sample0.01.zst; do
        sz=$(aws s3 ls --no-sign-request s3://$B/$k | awk '{print $3}')
        n=me$(basename $k | cut -c1-6); prendi $k ${sz:-0} lcs $n || break
      done ;;
esac
echo "byte compressi scaricati $(tot)"
tail -5 $OUT/err.txt 2>/dev/null
