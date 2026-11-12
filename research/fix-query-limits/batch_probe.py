#!/usr/bin/env python3
# The probe that measured mongod 7.0's batch byte rule (cmd.rs BATCH_BYTES): for k small documents before a big one,
# the largest total that still fits in one batch, for find and getMore. Usage: batch_probe.py mongodb://<mongod>
# The byte threshold as a function of the number of small documents before the big one (find and getMore).
import sys, pymongo, bson
c = pymongo.MongoClient(sys.argv[1]); db = c.probe
M = 16 * 1024 * 1024
def fits(k, extra, getmore):
    db.c.drop()
    small = [{"_id": i, "p": "a" * 50} for i in range(k)]
    ts = sum(len(bson.encode(d)) for d in small)
    b = {"_id": 10**6, "p": ""}
    b["p"] = "b" * (M - ts + extra - len(bson.encode(b)))
    pre = [{"_id": -1}] if getmore else []
    db.c.insert_many(pre + small + [b, {"_id": 10**7}])
    if getmore:
        r = db.command("find", "c", batchSize=1)
        r = db.command("getMore", r["cursor"]["id"], collection="c")
        return len(r["cursor"]["nextBatch"]) >= k + 1
    r = db.command("find", "c", batchSize=100000)
    return len(r["cursor"]["firstBatch"]) >= k + 1
for gm in (False, True):
    for k in (1, 2, 3, 10, 11, 12):
        lo, hi = -100000, 0
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if fits(k, mid, gm): lo = mid
            else: hi = mid
        print("getMore" if gm else "find", "k", k, "largest sum = 16MiB", lo)
