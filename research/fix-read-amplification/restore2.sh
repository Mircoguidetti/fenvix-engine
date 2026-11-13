#!/bin/bash
# restore bucket $1 from /home/user/snap/$1 (MinIO stopped meanwhile); "save" as $2 snapshots it instead
B=$1
pkill -f "minio server"; sleep 2
if [ "${2:-}" = save ]; then rm -rf /home/user/snap/$B; cp -a /home/user/minio-data/$B /home/user/snap/$B
else rm -rf /home/user/minio-data/$B && cp -a /home/user/snap/$B /home/user/minio-data/$B; fi
nohup $HOME/.fenvix-judge/env/bin/minio server /home/user/minio-data --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 >> /home/user/minio.log 2>&1 &
for i in $(seq 60); do curl -s -o /dev/null http://127.0.0.1:9000/minio/health/live && exit 0; sleep 1; done; exit 1
