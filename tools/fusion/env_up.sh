#!/usr/bin/env bash
# Bring the test environment back after a machine restart (idempotent): dockerd, the reference mongod containers
# (mongo7 :27017 standalone, mongors :27020 replica set rs0) and MinIO :9000 (built from source, data in /tmp/minio-data).
# MINIO_ONLY=1 brings up only MinIO (the WAL-cost runs need no docker and no reference mongod; docker takes minutes after a restart).
set -u
if [ -z "${MINIO_ONLY:-}" ]; then
  if ! docker info > /dev/null 2>&1; then
    (nohup dockerd > /tmp/dockerd.log 2>&1 &)
    for _ in $(seq 1 60); do docker info > /dev/null 2>&1 && break; sleep 1; done
  fi
  for c in mongo7 mongors; do
    # the first start after a machine restart can fail with a stale shim ("container with given ID already exists")
    for _ in 1 2 3; do
      [ "$(docker inspect -f '{{.State.Running}}' $c 2>/dev/null)" = true ] && break
      docker start $c > /dev/null 2>&1; sleep 3
    done
  done
fi
if ! (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then
  mkdir -p /tmp/minio-data/bench
  MINIO_PROMETHEUS_AUTH_TYPE=public MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup "${MINIO_BIN:-/tmp/gopath/bin/minio}" server /tmp/minio-data --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > /tmp/claude-0/minio.log 2>&1 &
fi
if [ -z "${MINIO_ONLY:-}" ]; then
  PY=${PY:-/home/user/test5-compat-indexes/mongo-tests/.venv/bin/python}
  $PY - <<'P'
import time, pymongo, sys
def wait(uri, need_primary, secs=120):
    t = time.time()
    while time.time() - t < secs:
        try:
            c = pymongo.MongoClient(uri, serverSelectionTimeoutMS=1500)
            h = c.admin.command("hello")
            if (not need_primary) or h.get("isWritablePrimary"):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False
ok1 = wait("mongodb://localhost:27017/?directConnection=true", False)
ok2 = wait("mongodb://localhost:27020/?directConnection=true", True)
print("mongo7", ok1, "mongors-primary", ok2)
sys.exit(0 if ok1 and ok2 else 1)
P
fi
for _ in $(seq 1 30); do (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null && { echo "minio up"; exit 0; }; sleep 1; done
echo "minio DOWN"; exit 1
