#!/usr/bin/env bash
# Svuota il cestino interno di MinIO (oggetti già cancellati) per le voci più vecchie di 20 secondi.
while true; do
  find /tmp/minio-data/.minio.sys/tmp/.trash -mindepth 1 -maxdepth 1 ! -newermt "-20 seconds" -exec rm -rf {} + 2>/dev/null
  sleep 10
done
