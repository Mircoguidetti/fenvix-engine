#!/bin/bash
# restore the generated 3 GiB bucket from the snapshot (MinIO stopped meanwhile)
pkill -f "minio server"; sleep 2
rm -rf /home/user/minio-data/gen3gib && cp -a /home/user/snap/gen3gib /home/user/minio-data/gen3gib
nohup $HOME/.fenvix-judge/env/bin/minio server /home/user/minio-data --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 >> /home/user/minio.log 2>&1 &
for i in $(seq 60); do curl -s -o /dev/null http://127.0.0.1:9000/minio/health/live && exit 0; sleep 1; done; exit 1
