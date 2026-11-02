#!/usr/bin/env bash
# le due corse decisive, una dopo l'altra
cd "$(dirname "$0")/.."
scripts/corsa.sh base risultati/base.jsonl > risultati/corsa-base.log 2>&1
scripts/corsa.sh uno-su-cento risultati/uno-su-cento.jsonl > risultati/corsa-uno-su-cento.log 2>&1
echo FINITO > risultati/FINITO
