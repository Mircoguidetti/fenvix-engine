#!/bin/bash
# M31 · recupero dei risultati con le credenziali fresche della pulizia, PRIMA di cancellare il bucket (revisore M31, C3).
# Se il rinnovo delle credenziali durante l'attesa è fallito, la copia di giornata-ci.sh fallisce in silenzio e la
# pulizia cancellerebbe il bucket con dentro i risultati. Qui si copia di nuovo (copia = sovrascrive, non perde nulla)
# e si toglie il numero dell'account. Non fallisce mai il job: se non c'è niente da copiare lo dice e basta.
set -u
OUT=${M31_OUT:-/tmp/m31-out}; mkdir -p "$OUT"
if aws s3 cp "s3://$BUCKET/risultati/" "$OUT/" --recursive --only-show-errors; then
  echo "RECUPERO: $(find "$OUT" -type f | wc -l) file in $OUT"
else
  echo "RECUPERO: niente da copiare da s3://$BUCKET/risultati/ (bucket mai creato o vuoto)"
fi
if [ -n "${M31_ACCT:-}" ]; then
  grep -rl "$M31_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$M31_ACCT/<ACCOUNT_ID>/g"
fi
exit 0
