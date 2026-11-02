#!/usr/bin/env bash
# Simulazione completa. Parti: a Alibaba, b Tencent, c Twitter (oracleGeneral, campione 10% delle chiavi), d Meta 2024.
# Le tracce si leggono in streaming dal bucket pubblico: si scarica solo l'inizio che serve (--max richieste).
set -uo pipefail
cd proto/bancone/sim && cargo build --release -q 2>&1 | tail -3; cd - >/dev/null
SIM=proto/bancone/sim/target/release/bancone-sim
J=$OUT/risultati.jsonl
SCARICATI=$OUT/byte_scaricati.txt
leggi() { # leggi chiave -> flusso decompresso su stdout, conta i byte compressi scaricati
  aws s3 cp --no-sign-request --quiet "s3://cache-datasets/$1" - | tee >(wc -c | sed "s|^|$1 |" >> $SCARICATI) | zstd -dc 2>/dev/null
}
scegli_dischi() { # $1 cartella lcs, $2 quanti candidati, $3 quanti tenere
  aws s3 ls --no-sign-request --recursive "s3://cache-datasets/cache_dataset_lcs/$1/" | awk '$4 ~ /\.lcs\.zst$/ && $3 > 3e8 && $3 < 8e9' | sort -k3 -n -r | head -$2 | awk '{print $4}' > $OUT/candidati.txt
  while read k; do
    n=$(basename $k .lcs.zst)
    leggi $k | $SIM --kind lcs --mode block --input - --name "$1-$n" --max 10000000 --grid info --out $OUT/candidati.jsonl 2>>$OUT/err.txt
  done < $OUT/candidati.txt
  # i dischi con più letture (in pagine da 4 KB) nei primi 10 milioni di richieste
  python3 - "$OUT/candidati.jsonl" "$3" > $OUT/scelti.txt <<'PY'
import json, sys
rows=[json.loads(l) for l in open(sys.argv[1])]
rows.sort(key=lambda d: -d["read_records"])
for d in rows[:int(sys.argv[2])]: print(d["trace"])
PY
}
verifica() { # $1 file og esportato, $2 json info del flusso: confronto con libCacheSim alle stesse taglie
  caps=$(python3 -c "import json,sys; d=[json.loads(l) for l in open('$J') if '\"verifica\"' in l and '\"$3\"' in l]; print(' '.join(str(x['cap']) for x in d if x['policy']=='lru'))")
  python3 -I proto/bancone/verifica_lcs.py "$1" $caps | sed "s/^{/{\"trace\": \"$3\", /" >> $OUT/libcachesim.jsonl
}
case "$PARTE" in
  a|b)
    if [ "$PARTE" = a ]; then D=alibaba; else D=tencentBlock; fi
    scegli_dischi $D 24 3
    first=1
    while read name; do
      n=${name#$D-}
      k=cache_dataset_lcs/$D/$n.lcs.zst
      if [ $first = 1 ]; then
        leggi $k | $SIM --kind lcs --mode block --input - --name "$name" --max 30000000 --grid verifica --out $J --export /tmp/v.og 2>>$OUT/err.txt
        verifica /tmp/v.og x "$name"; rm -f /tmp/v.og; first=0
      fi
      leggi $k | $SIM --kind lcs --mode block --input - --name "$name" --max 30000000 --out $J 2>>$OUT/err.txt
    done < $OUT/scelti.txt
    ;;
  c)
    for c in 24 25 11; do
      k=cache_dataset_oracleGeneral/2020_twitter/cluster$c.oracleGeneral.sample10.zst
      if [ $c = 24 ]; then
        leggi $k | $SIM --kind og --mode kv --input - --name "twitter-c$c" --max 40000000 --grid verifica --out $J --export /tmp/v.og 2>>$OUT/err.txt
        verifica /tmp/v.og x "twitter-c$c"; rm -f /tmp/v.og
      fi
      leggi $k | $SIM --kind og --mode kv --input - --name "twitter-c$c" --max 40000000 --out $J 2>>$OUT/err.txt
    done
    ;;
  d)
    k=cache_dataset_lcs/metaKV/202401_kv_traces_all_sort.csv.lcs.sample0.1.zst
    leggi $k | $SIM --kind lcs --mode kv --input - --name "meta-202401" --max 40000000 --grid verifica --out $J --export /tmp/v.og 2>>$OUT/err.txt
    verifica /tmp/v.og x "meta-202401"; rm -f /tmp/v.og
    leggi $k | $SIM --kind lcs --mode kv --input - --name "meta-202401" --max 40000000 --out $J 2>>$OUT/err.txt
    ;;
esac
awk '{s+=$2} END {print "byte compressi scaricati", s}' $SCARICATI
tail -5 $OUT/err.txt
