#!/usr/bin/env python3
# (copied from the review of fix-query-limits, fenvix-brain sessions/2026-11-11-review-fix-query-limits/probes/)
"""Change stream with big events: every getMore's batch length and reply bytes, until all events arrive or 90 s pass.
Usage: cs_probe.py URI [n_events] [doc_mb]"""
import sys, time
import bson
from bson import SON
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

uri = sys.argv[1]; N = int(sys.argv[2]) if len(sys.argv) > 2 else 12; MB = float(sys.argv[3]) if len(sys.argv) > 3 else 3
RAW = CodecOptions(document_class=RawBSONDocument)
c = MongoClient(uri); d = c[sys.argv[4] if len(sys.argv) > 4 else "app_cs_review"]
d.drop_collection("cs"); d.create_collection("cs")
r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]), codec_options=RAW)
cid = r["cursor"]["id"]
size = int(MB * 1024 * 1024)
for i in range(N):
    d.cs.insert_one({"_id": i, "p": "x" * size})
t0 = time.time(); n = 0; log = []
while n < N and time.time() - t0 < 90:
    g = d.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 1000)]), codec_options=RAW)
    nb = list(g["cursor"]["nextBatch"])
    log.append((round(time.time() - t0, 1), len(nb), len(g.raw)))
    n += len(nb)
    if g["cursor"]["id"] == 0:
        log.append("cursor closed"); break
print("documents in the collection:", d.cs.count_documents({}), end="; ")
print(f"events {n} of {N}; getMores (seconds, batch, bytes): {[x for x in log if x == 'cursor closed' or x[1]]}; empty getMores {sum(1 for x in log if x != 'cursor closed' and not x[1])}")
