#!/bin/bash
# Cucina minima (K2) · i certificati TLS della giornata, sul runner (come aws-test3: CA della giornata firmata da se',
# certificato del server firmato dalla CA). La chiave della CA non lascia mai il runner; la chiave del server va nel
# bucket privato della giornata (la legge solo la macchina server) e sparisce con il bucket alla fine.
#   certificati.sh ca CARTELLA                 -> CARTELLA/ca.key, CARTELLA/ca.pem (2 giorni)
#   certificati.sh server CARTELLA IP USCITA   -> USCITA: catena + chiave del server, valido per IP e 127.0.0.1
set -euo pipefail
case "$1" in
  ca)
    mkdir -p "$2"
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=cucina-ca" -keyout "$2/ca.key" -out "$2/ca.pem" 2>/dev/null ;;
  server)
    d=$2; ip=$3; out=$4; t=$(mktemp -d)
    [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "IP non valido: $ip" >&2; exit 1; }
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$ip" -keyout "$t/s.key" -out "$t/s.csr" 2>/dev/null
    printf "subjectAltName=IP:%s,IP:127.0.0.1,DNS:localhost\n" "$ip" > "$t/san.ext"
    openssl x509 -req -in "$t/s.csr" -CA "$d/ca.pem" -CAkey "$d/ca.key" -CAcreateserial -days 2 -extfile "$t/san.ext" -out "$t/s.crt" 2>/dev/null
    cat "$t/s.crt" "$t/s.key" > "$out"; rm -rf "$t" ;;
  *) echo "uso: certificati.sh ca|server ..." >&2; exit 2 ;;
esac
