#!/usr/bin/env bash
# A local sharded cluster of mongod 7.0 (judge/setup.sh's binaries): one config server, one shard, mongos on 28100.
# The reference for "like MongoDB sharded collections" (founder, 2026-11-07). $0, local only.
set -euo pipefail
B=${JUDGE_HOME:-$HOME/.fenvix-judge}/env/bin; S=${1:-/tmp/fenvix-sharded}; mkdir -p "$S/cfg" "$S/sh"
"$B/mongod" --configsvr --replSet cfg --dbpath "$S/cfg" --port 28101 --bind_ip 127.0.0.1 --fork --logpath "$S/cfg.log" > /dev/null
"$B/mongod" --shardsvr --replSet sh --dbpath "$S/sh" --port 28102 --bind_ip 127.0.0.1 --fork --logpath "$S/sh.log" > /dev/null
"$B/python" - <<'P'
import pymongo, time
pymongo.MongoClient("mongodb://127.0.0.1:28101/?directConnection=true").admin.command("replSetInitiate", {"_id": "cfg", "configsvr": True, "members": [{"_id": 0, "host": "127.0.0.1:28101"}]})
pymongo.MongoClient("mongodb://127.0.0.1:28102/?directConnection=true").admin.command("replSetInitiate", {"_id": "sh", "members": [{"_id": 0, "host": "127.0.0.1:28102"}]})
time.sleep(8)
P
"$B/mongos" --configdb cfg/127.0.0.1:28101 --port 28100 --bind_ip 127.0.0.1 --fork --logpath "$S/mongos.log" > /dev/null
"$B/python" -c "
import pymongo, time
c = pymongo.MongoClient('mongodb://127.0.0.1:28100')
for _ in range(30):
    try: c.admin.command('addShard', 'sh/127.0.0.1:28102'); break
    except Exception: time.sleep(1)
print('mongos up on 28100')"
