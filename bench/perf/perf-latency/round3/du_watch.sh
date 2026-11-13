#!/bin/bash
# every 20 s while run $1 is going: MinIO disk by area and the log replicas' disk (MB)
R=$1; OUT=$R-du.txt
until docker ps --format '{{.Names}}' | grep -q '^minio$'; do sleep 2; done
while docker ps --format '{{.Names}}' | grep -q '^minio$'; do
  T=$(date +%s)
  M=$(docker exec minio sh -c 'cd /data 2>/dev/null && for d in fxbench/tenants/*/fx fxbench/tenants/*/compacted fxbench/tenants/*/wal .minio.sys/multipart .minio.sys/tmp .minio.sys; do [ -e "$d" ] && echo "$(du -sm $d 2>/dev/null | cut -f1):$d"; done | tr "\n" " "')
  L=$(for i in 0 1 2; do docker exec log-$i du -sm /data/log 2>/dev/null | cut -f1; done | tr "\n" " ")
  echo "$T $M logs=$L" >> $OUT
  sleep 20
done
