#!/usr/bin/env python3
"""Cursor batches near the 16 MiB limit: the same batch boundaries as mongod 7.0 and every document read once.

Usage: batch_diff.py --real mongodb://... --ours mongodb://...
For each case the collection is filled the same way on both servers; the raw commands (find / aggregate, then getMore
until the cursor is closed) give the list of batch lengths, which must be equal, and every document (raw BSON) must
come back once, in the same order. Also prints the largest reply (bytes) each server sent."""
import argparse, random, sys
import bson
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
from bson.codec_options import CodecOptions
RAW = CodecOptions(document_class=RawBSONDocument)

ap = argparse.ArgumentParser()
ap.add_argument("--real", required=True)
ap.add_argument("--ours", required=True)
ap.add_argument("--only", default="")
a = ap.parse_args()
OPTS = dict(document_class=RawBSONDocument)
real, ours = MongoClient(a.real, **OPTS), MongoClient(a.ours, **OPTS)
DB = "app_batch_check"
M = 16 * 1024 * 1024
MAXDOC = M  # mongod's largest document a client may insert


def doc_of(i, size):
    d = {"_id": i, "p": ""}
    d["p"] = "x" * (size - len(bson.encode(d)))
    assert len(bson.encode(d)) == size
    return d


def fill(cl, docs):
    c = cl[DB].c
    c.drop()
    # insert a few at a time: a message holds at most 48 MB
    chunk, size = [], 0
    for d in docs:
        n = len(bson.encode(d))
        if chunk and size + n > 40 * 1024 * 1024:
            c.insert_many(chunk); chunk, size = [], 0
        chunk.append(d); size += n
    if chunk:
        c.insert_many(chunk)


def batches(cl, first_cmd, more_kw, coll="c"):
    db = cl.get_database(DB, codec_options=RAW)
    r = db.command(first_cmd, codec_options=RAW)
    lens, docs, biggest = [], [], len(r.raw)
    cur = r["cursor"]
    b = cur["firstBatch"]
    lens.append(len(b)); docs += [x.raw for x in b]
    cid = cur["id"]
    while cid:
        r = db.command({"getMore": bson.Int64(cid), "collection": coll, **more_kw}, codec_options=RAW)
        biggest = max(biggest, len(r.raw))
        cur = r["cursor"]
        b = cur["nextBatch"]
        lens.append(len(b)); docs += [x.raw for x in b]
        cid = cur["id"]
    return lens, docs, biggest


def pair(extra, k_small=1):
    """k small documents, then one whose size puts the first k+1 at 16 MiB + extra (the boundary of the probe)."""
    small = [doc_of(i, 70) for i in range(k_small)]
    big = doc_of(10 ** 6, M + extra - 70 * k_small)
    return small + [big, doc_of(10 ** 7, 50)]


rng = random.Random(11)
CASES = [
    ("6 docs of 16 MiB", [doc_of(i, MAXDOC) for i in range(6)], {}, {}),
    ("6 docs of 4 MB", [doc_of(i, 4_000_000) for i in range(6)], {}, {}),
    ("10 docs of 5 MB, getMore", [doc_of(i, 5_000_000) for i in range(10)], {"batchSize": 1}, {}),
    ("first batch at the edge (fits)", pair(-56), {"batchSize": 1000}, {}),
    ("first batch at the edge (does not fit)", pair(-55), {"batchSize": 1000}, {}),
    ("first batch at the edge, 12 small docs (fits)", pair(-91, 12), {"batchSize": 1000}, {}),
    ("first batch at the edge, 12 small docs (does not)", pair(-90, 12), {"batchSize": 1000}, {}),
    ("getMore at the edge (fits)", [doc_of(-1, 60)] + pair(-60), {"batchSize": 1}, {}),
    ("getMore at the edge (does not fit)", [doc_of(-1, 60)] + pair(-59), {"batchSize": 1}, {}),
    ("getMore at the edge, 12 small docs (fits)", [doc_of(-1, 60)] + pair(-95, 12), {"batchSize": 1}, {}),
    ("getMore at the edge, 12 small docs (does not)", [doc_of(-1, 60)] + pair(-94, 12), {"batchSize": 1}, {}),
    ("100,000 docs of 1 KB", [doc_of(i, 1024) for i in range(100_000)], {}, {}),
    ("100,000 docs of 1 KB, getMore batchSize 20,000", [doc_of(i, 1024) for i in range(100_000)], {}, {"batchSize": 20_000}),
    ("60 docs of random size up to 8 MB", [doc_of(i, rng.randrange(100, 8_000_000)) for i in range(60)], {}, {}),
    ("singleBatch with 4 MB docs", [doc_of(i, 4_000_000) for i in range(8)], {"singleBatch": True, "batchSize": 100}, {}),
    ("limit 6 with 4 MB docs", [doc_of(i, 4_000_000) for i in range(8)], {"limit": 6}, {}),
    ("find on a view, 6 docs of 4 MB", [doc_of(i, 4_000_000) for i in range(6)], {"view": True}, {}),
    ("aggregate on a view, 6 docs of 4 MB", [doc_of(i, 4_000_000) for i in range(6)], {"view": True, "aggregate": True}, {}),
    ("aggregate, 6 docs of 4 MB", [doc_of(i, 4_000_000) for i in range(6)], {"aggregate": True}, {}),
    ("aggregate at the edge (fits)", pair(-56), {"aggregate": True, "batchSize": 1000}, {}),
    ("aggregate at the edge (does not fit)", pair(-55), {"aggregate": True, "batchSize": 1000}, {}),
]
bad = 0
largest = {"real": 0, "ours": 0}
for name, docs, first_kw, more_kw in CASES:
    if a.only and a.only not in name:
        continue
    first_kw = dict(first_kw)
    coll = "v" if first_kw.pop("view", False) else "c"
    if first_kw.pop("aggregate", False):
        cur = {"batchSize": first_kw.pop("batchSize")} if "batchSize" in first_kw else {}
        cmd = {"aggregate": coll, "pipeline": [{"$match": {}}], "cursor": cur}
    else:
        cmd = {"find": coll, **first_kw}
    res = {}
    for label, cl in (("real", real), ("ours", ours)):
        fill(cl, docs)
        if coll == "v" and "v" not in cl[DB].list_collection_names():
            cl[DB].command("create", "v", viewOn="c", pipeline=[{"$match": {}}])
        res[label] = batches(cl, cmd, more_kw, coll)
        largest[label] = max(largest[label], res[label][2])
    (lr, dr, br), (lo, do, bo) = res["real"], res["ours"]
    ok = lr == lo and dr == do and len(set(do)) == len(do)
    bad += not ok
    show = lambda l: str(l) if len(l) <= 12 else f"{l[:6]} ... {l[-3:]} ({len(l)} batches)"
    print(f"{'PASS' if ok else 'FAIL'}  {name}: mongod {show(lr)}, ours {show(lo)}; largest reply mongod {br:,} B, ours {bo:,} B")
# mapReduce reads its input through an aggregation of the server's own: 6 documents of 4 MB, all counted
mr = {}
for label, cl in (("real", real), ("ours", ours)):
    fill(cl, [doc_of(i, 4_000_000) for i in range(6)])
    r = cl[DB].command("mapReduce", "c", map="function() { emit(1, this.p.length); }", reduce="function(k, v) { return Array.sum(v); }", out={"inline": 1})
    mr[label] = [bson.decode(x.raw) if hasattr(x, "raw") else x for x in r["results"]]
ok = mr["real"] == mr["ours"]
bad += not ok
print(f"{'PASS' if ok else 'FAIL'}  mapReduce over 6 docs of 4 MB: mongod {mr['real']}, ours {mr['ours']}")
for cl in (real, ours):
    cl.drop_database(DB)
print(f"largest reply over all cases: mongod {largest['real']:,} B, ours {largest['ours']:,} B")
print("RESULT", "PASS" if bad == 0 else f"FAIL ({bad})")
sys.exit(1 if bad else 0)
