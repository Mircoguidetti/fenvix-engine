#!/bin/bash
# Kitchen (K2) · the TLS certificates of the run, on the runner (like aws-test3: a self-signed CA of the run, the server
# certificate signed by it). The CA key never leaves the runner; the server key goes into the private run bucket (only
# the server machine reads it) and disappears with the bucket at the end.
#   certs.sh ca FOLDER                -> FOLDER/ca.key, FOLDER/ca.pem (2 days)
#   certs.sh server FOLDER IP OUTPUT  -> OUTPUT: chain + server key, valid for IP and 127.0.0.1
set -euo pipefail
case "$1" in
  ca)
    mkdir -p "$2"
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=kitchen-ca" -keyout "$2/ca.key" -out "$2/ca.pem" 2>/dev/null ;;
  server)
    d=$2; ip=$3; out=$4; t=$(mktemp -d)
    [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "bad IP: $ip" >&2; exit 1; }
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$ip" -keyout "$t/s.key" -out "$t/s.csr" 2>/dev/null
    printf "subjectAltName=IP:%s,IP:127.0.0.1,DNS:localhost\n" "$ip" > "$t/san.ext"
    openssl x509 -req -in "$t/s.csr" -CA "$d/ca.pem" -CAkey "$d/ca.key" -CAcreateserial -days 2 -extfile "$t/san.ext" -out "$t/s.crt" 2>/dev/null
    cat "$t/s.crt" "$t/s.key" > "$out"; rm -rf "$t" ;;
  *) echo "usage: certs.sh ca|server ..." >&2; exit 2 ;;
esac
