#!/bin/bash
# Prova locale M31 · il freno sulle richieste a S3 (revisore M31, C7 / R2). esegui.sh in piccolo su moto, con il tetto
# delle GET abbassato a 300: `tempi p1` deve fermarsi con il codice 97, esegui.sh deve scrivere FRENO e saltare alla fine
# (niente carico d2k, niente scritture). 0 $.
set -u
source "$(dirname "$0")/comune.sh"
BASE=${1:-/tmp/m31-freno}; mkdir -p "$BASE"; export SANDBOX_BASE=$BASE
nuova_sandbox "$BASE/freno"; FALLITE=0
aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=eu-west-1 > /dev/null
D=$SANDBOX/macchina; mkdir -p "$D"; cp "$M_DIR/target/release/m31" "$G_DIR/esegui.sh" "$D/"
( cd "$D" && PROVA_LOCALE=1 M31_FRENO_GET=300 GB_P1=0.1 CLIENTI_P1=100 RITMO=20 N=10 RISCALDO=20 INSIEME=10 \
  FINTO=25:0.6:90 SALI_S=5 bash ./esegui.sh "$BUCKET" > esegui.log 2>&1 )
P=$D/out/passi.log
controlla "tempi p1 fermato dal freno (codice 97)" grep -q "FRENO: richieste a S3 oltre il tetto in tempi p1" "$P"
controlla "riga FRENO del programma nel suo log" grep -q "^FRENO: 301 richieste GET" "$D/out/tempi-p1.log"
controlla "poi subito la fine" bash -c "tail -1 '$P' | grep -q ' fine$'"
controlla "niente carico d2k né scritture dopo il freno" bash -c "! grep -q 'carica d2k\|scrivi' '$P'"
controlla "fine.json nel bucket (il workflow vede FINITA)" aws s3 ls "s3://$BUCKET/risultati/fine.json"
echo "   passi: $(tr '\n' '|' < "$P")"
echo "   programma: $(grep FRENO "$D/out/tempi-p1.log")"
echo "== RISULTATO: $FALLITE controlli falliti"
[ "$FALLITE" = 0 ]
