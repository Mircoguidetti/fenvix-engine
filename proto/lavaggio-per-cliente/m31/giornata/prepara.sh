#!/bin/bash
# Costruisce m31-giornata.tar.gz: il programma (Linux x86_64, glibc 2.39: gira su Ubuntu 24.04) e esegui.sh.
set -e
cd "$(dirname "$0")/.."
T=${CARGO_TARGET_DIR:-target}
cargo build --release
rm -rf /tmp/m31-pacco && mkdir -p /tmp/m31-pacco && cp $T/release/m31 giornata/esegui.sh /tmp/m31-pacco/
strip /tmp/m31-pacco/m31 || true
tar czf giornata/m31-giornata.tar.gz -C /tmp/m31-pacco m31 esegui.sh

# il pacco unico da caricare in CloudShell: giornata.sh e il programma
rm -rf /tmp/m31-cs && mkdir -p /tmp/m31-cs && cp giornata/giornata.sh giornata/m31-giornata.tar.gz /tmp/m31-cs/
tar czf giornata/m31-cloudshell.tar.gz -C /tmp/m31-cs giornata.sh m31-giornata.tar.gz
rm giornata/m31-giornata.tar.gz
sha256sum giornata/m31-cloudshell.tar.gz | tee giornata/m31-cloudshell.sha256
