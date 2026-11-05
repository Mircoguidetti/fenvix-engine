#!/bin/bash
# Cucina minima (K2) · i pacchi per le macchine e la CA della giornata (sul runner, niente AWS). Lo usa il workflow e la
# prova locale. Variabili: C (cartella cucina), PACCHI (uscita), CA_DIR, COPIE, GB, BUCKET_DATI, BIN (cartella con
# mongo-server-x86_64/ e mongo-server-aarch64/, ognuna con mongo-server e motore-commit.txt).
# VENDOR_LOCALE=1 (solo la prova locale): librerie per il Python di questa macchina invece che per Ubuntu 24.04.
set -euo pipefail
mkdir -p "$PACCHI"
vendor() { # vendor CARTELLA CPU PACCHETTI...: librerie Python per Ubuntu 24.04 (Python 3.12) della macchina
  local d=$1 cpu=$2; shift 2
  if [ -n "${VENDOR_LOCALE:-}" ]; then python3 -m pip install -q --target "$d/vendor" "$@"; return; fi
  /usr/bin/python3 -m pip install -q --target "$d/vendor" --only-binary=:all: --implementation cp --python-version 3.12 \
    --platform "manylinux_2_28_$cpu" --platform "manylinux_2_17_$cpu" --platform "manylinux2014_$cpu" "$@"
}
# cliente (x86_64): lo script e i suoi aiutanti, come l'Irlanda
d=$(mktemp -d)
vendor "$d" x86_64 'pymongo>=4.6,<5' backports.zstd boto3
cp "$C"/prova_infinite.py "$C"/esegui-cliente.sh "$C"/s3io.py "$C"/attendi_db.py "$C"/opzioni.txt "$C"/massimo.txt \
  "$C"/carica.txt "$C"/cpu_macchina.py "$C"/somma_massimo.py "$C"/controlla_parallelo.py "$C"/prepara_stato.py "$d"/
bash "$C"/ci/certificati.sh ca "$CA_DIR"; cp "$CA_DIR"/ca.pem "$d"/
{ echo "COPIE=$COPIE"; echo "GB=$GB"; } > "$d"/cliente.env
tar czf "$PACCHI"/cliente.tar.gz -C "$d" .
echo "cliente: script md5 $(md5sum "$d"/prova_infinite.py | cut -d' ' -f1), copie $COPIE, GB $GB"
# server: uno per CPU compilata
for cpu in x86_64 aarch64; do
  b=$BIN/mongo-server-$cpu
  [ -f "$b/mongo-server" ] || continue
  d=$(mktemp -d)
  vendor "$d" "$cpu" 'pymongo>=4.6,<5' boto3
  cp "$b"/mongo-server "$b"/motore-commit.txt "$C"/esegui-server.sh "$C"/s3io.py "$C"/crea_utente.py "$C"/cpu_macchina.py "$C"/server.txt "$d"/
  chmod +x "$d"/mongo-server "$d"/esegui-server.sh
  echo "BUCKET_DATI=$BUCKET_DATI" > "$d"/server.env
  tar czf "$PACCHI"/server-$cpu.tar.gz -C "$d" .
  echo "server $cpu: motore $(cat "$d"/motore-commit.txt), sha256 $(sha256sum "$d"/mongo-server | cut -d' ' -f1)"
done
ls -la "$PACCHI"
