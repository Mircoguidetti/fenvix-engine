#!/usr/bin/env bash
# Svuota il cestino interno di MinIO (oggetti già cancellati) per le voci più vecchie di 2 minuti.
while true; do
  find /tmp/minio-data/.minio.sys/tmp/.trash -mindepth 1 -maxdepth 1 -mmin +1 -exec rm -rf {} + 2>/dev/null
  sleep 30
done
