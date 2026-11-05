#!/bin/bash
# Q8: ricopia i risultati dal bucket con credenziali fresche, prima della pulizia (come M31, revisore C3).
set -u
OUT=${Q8_OUT:-/tmp/q8-out}; mkdir -p "$OUT"
aws s3 cp "s3://$BUCKET/risultati/" "$OUT/" --recursive --only-show-errors || echo "recupero dei risultati non riuscito"
