"""Proposal 4 assumes the compactor can cut each message out of the Mongo saver's stored msgpack bytes as a
byte range, identical everywhere the message appears, so the dedup we measured is reachable without
re-encoding. Check: for every message in every checkpoint document (and every write document) of the
first threads, its standalone msgpack encoding occurs verbatim inside the stored bytes."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pymongo import MongoClient
from langgraph.checkpoint.mongodb import MongoDBSaver
db_name = sys.argv[1]; T = int(sys.argv[2])
mc = MongoClient("mongodb://127.0.0.1:27017/"); db = mc[db_name]
serde = MongoDBSaver(mc, db_name=db_name).serde
tids = sorted(db.checkpoints.distinct("thread_id"))[:T]
found = missing = 0
for t in tids:
    for d in db.checkpoints.find({"thread_id": t}):
        cp = serde.loads_typed((d["type"], d["checkpoint"]))
        for m in cp["channel_values"].get("messages", []):
            b = serde.dumps_typed(m)[1]
            if b in d["checkpoint"]: found += 1
            else: missing += 1
    for d in db.checkpoint_writes.find({"thread_id": t}):
        v = serde.loads_typed((d["type"], d["value"]))
        for m in (v if isinstance(v, list) else [v]):
            if hasattr(m, "content"):
                b = serde.dumps_typed(m)[1]
                if b in d["value"]: found += 1
                else: missing += 1
print(db_name, "messages found verbatim", found, "missing", missing)
