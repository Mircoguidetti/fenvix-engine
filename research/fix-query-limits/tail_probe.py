#!/usr/bin/env python3
# (copied from the review of fix-query-limits, fenvix-brain sessions/2026-11-11-review-fix-query-limits/probes/)
"""Tailable cursor on a capped collection with big documents (a cursor path the fix leaves as it was): every getMore's
batch length and reply bytes, mongod and ours. Usage: tail_probe.py URI DBNAME"""
import sys, time
from bson import SON
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

RAW = CodecOptions(document_class=RawBSONDocument)
c = MongoClient(sys.argv[1]); d = c[sys.argv[2]]
d.drop_collection("cap"); d.create_collection("cap", capped=True, size=200 * 1024 * 1024)
d.cap.insert_one({"_id": -1})
r = d.command(SON([("find", "cap"), ("filter", {}), ("tailable", True), ("awaitData", True)]), codec_options=RAW)
cid = r["cursor"]["id"]
for i in range(12):
    d.cap.insert_one({"_id": i, "p": "x" * (5 * 1024 * 1024)})
n, log, err = 0, [], None
t0 = time.time()
while n < 12 and time.time() - t0 < 30:
    try:
        g = d.command(SON([("getMore", cid), ("collection", "cap"), ("maxTimeMS", 500)]), codec_options=RAW)
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)[:160]}"; break
    nb = list(g["cursor"]["nextBatch"])
    if nb:
        log.append((len(nb), len(g.raw)))
    n += len(nb)
print(f"documents {n} of 12; batches {[x[0] for x in log]}; reply bytes {[x[1] for x in log]}; largest {max([x[1] for x in log] or [0])}; error {err}")
