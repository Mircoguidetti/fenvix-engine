#!/bin/bash
# Giro 6 (M16): il capo veloce. UN database, finestra vera (24 s e 60 s), scaricatore acceso (moto come S3),
# ciclo chiuso con 512 record in volo (16 lavoratori × lotti da 32, come il generatore di mongod), zstd fuori dal thread
# principale, cronometri fini. Capo sul core 0; repliche di riserva, generatore, scaricatore e moto sul core 1.
# Confronto: albero (binario del giro 5 con lo stesso generatore, /root/pq-albero) contro hash (questo branch).
# Finestra di 5 s nel nodo: con 24 s le 3 repliche superano gli 8 GB del contenitore (MISURATO: replica uccisa per
# memoria con tetto 2.300 MB) e con 10 s parte lo sfratto e il kernel uccide repliche durante le prove (MISURATO,
# results/g6-w10-oom). La crescita del costo con la finestra di 24 e 60 s si misura con examples/finestra.rs.
# Uso: coda-giro6.sh [PARTE]   PARTE: a (B1) b (inserimenti) e (inserimenti a 5.000/s, ciclo aperto) c (B1 24 s con tetto di RAM: sfratto) d (letture accese)
cd "$(dirname "$0")/.."
export PQ_FLUSHER=1 PQ_GEN_NICE=-10 PQ_ZSTD=1 PQ_SW_FINE=1 PQ_SAMPLE=20 PQ_DBS=1 PQ_MEMCAP=${PQ_MEMCAP:-2048}
R=results/g6
mkdir -p $R
ALBERO=/root/pq-albero
HASH=$(pwd)/target/release/pq
run() { # nome bin real keys window_s batch [reads] [rate: 0 = ciclo chiuso]
  local nome=$1 bin=$2 real=$3 k=$4 w=$5 b=$6 rd=${7:-0} rate=${8:-0}
  PQ_BIN=$bin PQ_REAL=$real PQ_WINDOW_MS=$((w*1000)) PQ_WARM=$((w+10)) PQ_READS=$rd \
    python3 scripts/lotti.py $R/$nome $rate $b 512 1 $k >> $R/$nome.log 2>&1
}
for P in ${1:-a b c d}; do
  for rep in 1 2; do
    case $P in
      a) for k in 8 5; do run b1-w5-k$k-albero $ALBERO 2 $k 5 32; run b1-w5-k$k-hash $HASH 2 $k 5 32; done ;;
      b) for k in 8 5; do run ins-w5-k$k-albero $ALBERO 1 $k 5 32; run ins-w5-k$k-hash $HASH 1 $k 5 32; done ;;
      c) PQ_MEMCAP=1500 run b1-w24-k8-tetto-albero $ALBERO 2 8 24 32; PQ_MEMCAP=1500 run b1-w24-k8-tetto-hash $HASH 2 8 24 32 ;;
      e) for k in 8 5; do run ins5000-w5-k$k-albero $ALBERO 1 $k 5 32 0 5000; run ins5000-w5-k$k-hash $HASH 1 $k 5 32 0 5000; done ;;
      d) run b1-w5-k8-letture-albero $ALBERO 2 8 5 32 5000; run b1-w5-k8-letture-hash $HASH 2 8 5 32 5000 ;;
    esac
  done
done
python3 scripts/cluster.py down >/dev/null 2>&1
echo FINE >> $R/FINE
