"""R-rolling (second reviewer), fix-box-first-trip round 2: `_id` stays unique per customer with the in-box check.

usage: python3 rr2_bft.py <mongo-server binary> [extra server args...]   (--store local; no MinIO here)
Every case must answer the same with `--id-check-in-box true` (head) and `false` (the locator read, the base's check).
Prints one line per case and "OUTCOMES <json>"; exits 1 if a duplicate `_id` of one customer is stored.
"""
import json, os, socket, subprocess, sys, tempfile, time

import pymongo
from bson import Int64
from pymongo.errors import DuplicateKeyError, OperationFailure

BIN, EXTRA = sys.argv[1], sys.argv[2:]
D = tempfile.mkdtemp(prefix="rr2bft-")
os.makedirs(os.path.join(D, "store"))


def port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


P = port()


def start():
    log = open(os.path.join(D, "server.log"), "ab")
    proc = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{P}", "--store", "local", "--local-dir", os.path.join(D, "store"), "--holder", "rr2",
                             "--no-ttl"] + EXTRA, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(300):
        try:
            socket.create_connection(("127.0.0.1", P), 0.2).close(); break
        except OSError:
            if proc.poll() is not None:
                raise SystemExit(open(os.path.join(D, "server.log")).read()[-3000:])
            time.sleep(0.1)
    return proc, pymongo.MongoClient(f"mongodb://127.0.0.1:{P}/?directConnection=true", serverSelectionTimeoutMS=20000)


out = {}


def case(name, fn):
    try:
        fn()
        out[name] = "accepted"
    except DuplicateKeyError:
        out[name] = "duplicate"
    except OperationFailure as e:
        out[name] = f"error {e.code}"
    print(f"{name}: {out[name]}")


proc, c = start()
t = c.app.checkpoints
# customer-keyed like test_79/test_87: the unique (thread_id, checkpoint_ns, checkpoint_id) index
t.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1)], unique=True)
for s in range(20):
    t.update_one({"thread_id": "A", "checkpoint_ns": "", "checkpoint_id": f"c{s:03d}"}, {"$set": {"v": s}, "$setOnInsert": {"_id": s}}, upsert=True)
t.insert_one({"_id": "s1", "thread_id": "A", "checkpoint_ns": "", "checkpoint_id": "str"})


def ups(cust, cid, _id):
    return lambda: t.update_one({"thread_id": cust, "checkpoint_ns": "", "checkpoint_id": cid}, {"$set": {"x": 1}, "$setOnInsert": {"_id": _id}}, upsert=True)


def rounds(tag):
    case(f"{tag}: upsert same customer, _id 3 (int32)", ups("A", f"{tag}-d1", 3))
    case(f"{tag}: upsert same customer, _id 3.0 (double)", ups("A", f"{tag}-d2", 3.0))
    case(f"{tag}: upsert same customer, _id Int64(3)", ups("A", f"{tag}-d3", Int64(3)))
    case(f"{tag}: upsert same customer, _id 's1'", ups("A", f"{tag}-d4", "s1"))
    case(f"{tag}: insert same customer, _id 4.0", lambda: t.insert_one({"_id": 4.0, "thread_id": "A", "checkpoint_ns": "", "checkpoint_id": f"{tag}-d5"}))
    case(f"{tag}: upsert other customer, _id 5", ups(f"B{tag}", f"{tag}-o1", 5))
    case(f"{tag}: move that document to A (holds _id 5)", lambda: t.update_one({"_id": 5, "thread_id": f"B{tag}"}, {"$set": {"thread_id": "A"}}))


rounds("memtable")
proc.terminate(); proc.wait(60)
proc, c = start()
t = c.app.checkpoints
c.app.other.insert_one({"x": 1})
rounds("restart")
ids = {}
for d in t.find({"thread_id": "A"}, {"_id": 1}):
    k = float(d["_id"]) if isinstance(d["_id"], (int, float)) else d["_id"]
    ids[k] = ids.get(k, 0) + 1
dups = {str(k): n for k, n in ids.items() if n > 1}
# every original document of customer A is still there, unchanged (an accepted move onto a held `_id` overwrites one)
lost = [s for s in range(20) if t.find_one({"thread_id": "A", "checkpoint_ns": "", "checkpoint_id": f"c{s:03d}", "v": s, "_id": s}) is None]
print(f"customer A: original documents missing or overwritten: {lost}")
dups = dups or ({"overwritten": lost} if lost else {})
print(f"customer A: {sum(ids.values())} documents, duplicate _ids {dups}")
print("OUTCOMES " + json.dumps(out, sort_keys=True))
proc.terminate(); proc.wait(60)
sys.exit(1 if dups else 0)
