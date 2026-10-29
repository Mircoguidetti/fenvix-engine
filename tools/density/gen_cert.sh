#!/usr/bin/env bash
# Self-signed test certificate for the engine's requireTLS (CN localhost, SAN 127.0.0.1). Written to the work directory, never committed.
set -eu
D=${DENSITY_WORK:-/home/user/density-work}/tls
mkdir -p $D
[ -f $D/server.pem ] && exit 0
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout $D/key.pem -out $D/ca.pem 2>/dev/null
cat $D/ca.pem $D/key.pem > $D/server.pem
echo "certificate in $D"
