#!/usr/bin/env python3
"""Tailable cursors and change streams near 16 MiB: the same batches as mongod 7.0, every document or event once.

Usage: tail_cs_diff.py --real mongodb://<mongod replica set> --ours mongodb://<ours, --change-log-max-mb 1024>
Tailable: batch lengths, reply bytes and raw documents must be identical. Change streams: batch lengths and the events'
content must be identical (each event without its resume token, cluster time and wall time: the token is a different,
shorter format on ours; drivers treat it as opaque), and resuming from the token of a batch cut at 16 MiB must give the
next event, nothing skipped or read twice. Ours needs a change log big enough for the events (older defect P1 of the
review: a stream that falls behind the default 32 MB log skips events)."""
import argparse, sys, time
import bson
from bson import SON
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

ap = argparse.ArgumentParser(); ap.add_argument("--real", required=True); ap.add_argument("--ours", required=True)
a = ap.parse_args()
RAW = CodecOptions(document_class=RawBSONDocument)
MB = 1024 * 1024
clients = {"real": MongoClient(a.real), "ours": MongoClient(a.ours)}
DB = "app_tail_cs_check"


def doc_of(i, size):
    x = {"_id": i, "p": ""}
    x["p"] = "x" * (size - len(bson.encode(x)))
    return x


def tail_run(c, sizes, first_kw, more_kw, filt=None):
    d = c[DB]
    d.drop_collection("cap"); d.create_collection("cap", capped=True, size=400 * MB)
    # one small document first: on an empty capped collection mongod closes a tailable cursor at once (ours keeps it)
    d.cap.insert_one({"_id": "seed"})
    pre = [s for s in sizes if s < 0]  # negative: inserted before the find (in the first batch)
    for i, s in enumerate(pre):
        d.cap.insert_one(doc_of(-100 - i, -s))
    cmd = SON([("find", "cap"), ("filter", filt or {}), ("tailable", True), ("awaitData", True), *first_kw.items()])
    r = d.command(cmd, codec_options=RAW)
    cid = r["cursor"]["id"]
    batches = [(len(r["cursor"]["firstBatch"]), len(r.raw))]
    docs = [x.raw for x in r["cursor"]["firstBatch"]]
    post = [s for s in sizes if s >= 0]
    for i, s in enumerate(post):
        d.cap.insert_one(doc_of(i, s))
    want = 1 + len(pre) + len(post) if not filt else None
    t0 = time.time()
    while time.time() - t0 < 20 and cid:
        g = d.command(SON([("getMore", cid), ("collection", "cap"), ("maxTimeMS", 300), *more_kw.items()]), codec_options=RAW)
        nb = list(g["cursor"]["nextBatch"])
        if nb:
            batches.append((len(nb), len(g.raw)))
            docs += [x.raw for x in nb]
        elif want is not None and len(docs) >= want:
            break
        elif want is None and time.time() - t0 > 3:
            break
        cid = g["cursor"]["id"]
    return batches, docs


def strip(ev):
    e = bson.decode(ev)
    for k in ("_id", "clusterTime", "wallTime", "collectionUUID"):
        e.pop(k, None)
    return bson.encode(e)


def cs_run(c, ops, pipeline=(), opts=None, more_kw=None):
    d = c[DB]
    d.drop_collection("cs"); d.create_collection("cs")
    d.cs.insert_one({"_id": "seed", "v": 0})
    r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": opts or {}}, *pipeline]), ("cursor", {})]), codec_options=RAW)
    cid = r["cursor"]["id"]
    for op in ops:
        op(d.cs)
    batches, events, tokens = [], [], []
    t0 = time.time()
    empty = 0
    while time.time() - t0 < 30 and cid and empty < 3:
        g = d.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 500), *(more_kw or {}).items()]), codec_options=RAW)
        nb = list(g["cursor"]["nextBatch"])
        if nb:
            batches.append(len(nb))
            events += [x.raw for x in nb]
            tokens.append(bson.decode(g["cursor"]["postBatchResumeToken"].raw))
            empty = 0
        else:
            empty += 1
        cid = g["cursor"]["id"]
    return batches, events, tokens


def resume_check(c, ops):
    """Resume after the first batch from its postBatchResumeToken: the events then must be the rest, once each."""
    b, ev, tok = cs_run(c, ops)
    d = c[DB]
    with d.cs.watch(resume_after=tok[0], max_await_time_ms=500) as s:
        rest = []
        t0 = time.time()
        while time.time() - t0 < 20 and len(rest) < len(ev) - b[0]:
            x = s.try_next()
            if x is not None:
                rest.append(x["documentKey"]["_id"])
    first = [bson.decode(e)["documentKey"]["_id"] for e in ev]
    return first[b[0]:] == rest, b, len(rest)


big = lambda n, mb: [lambda col, i=i: col.insert_one(doc_of(i, int(mb * MB))) for i in range(n)]
TAIL = [
    ("12 docs of 5 MB, getMore", [5 * MB] * 12, {}, {}, None),
    ("12 docs of 2 MB", [2 * MB] * 12, {}, {}, None),
    ("first batch: 6 docs of 4 MB already there", [-4_000_000] * 6, {}, {}, None),
    ("first batch at the edge (fits)", [-200, -(16 * MB - 56 - 200)], {"batchSize": 1000}, {}, None),
    ("first batch at the edge (does not fit)", [-200, -(16 * MB - 55 - 200)], {"batchSize": 1000}, {}, None),
    ("getMore at the edge (fits)", [200, 16 * MB - 60 - 200, 50], {}, {}, None),
    ("getMore at the edge (does not fit)", [200, 16 * MB - 59 - 200, 50], {}, {}, None),
    ("getMore batchSize 2, 5 MB docs", [5 * MB] * 7, {}, {"batchSize": 2}, None),
    ("filter keeps every other 5 MB doc", [5 * MB] * 12, {}, {}, {"_id": {"$in": [0, 2, 4, 6, 8, 10]}}),
]
CS = [
    ("12 inserts of 5 MB", big(12, 5), (), None, None),
    ("12 inserts of 2 MB", big(12, 2), (), None, None),
    ("30 inserts of 1 MB", big(30, 1), (), None, None),
    ("$match keeps even ids, 12 of 5 MB", big(12, 5), ({"$match": {"documentKey._id": {"$in": [0, 2, 4, 6, 8, 10]}}},), None, None),
    ("$project drops fullDocument, 12 of 5 MB", big(12, 5), ({"$project": {"fullDocument": 0}},), None, None),
    ("updates with updateLookup, 8 of 4 MB", big(8, 4) + [lambda col, i=i: col.update_one({"_id": i}, {"$set": {"u": 1}}) for i in range(8)], (), {"fullDocument": "updateLookup"}, None),
    ("getMore batchSize 2, 12 of 5 MB", big(12, 5), (), None, {"batchSize": 2}),
]
bad = 0
for name, sizes, fk, mk, filt in TAIL:
    r = {k: tail_run(c, sizes, fk, mk, filt) for k, c in clients.items()}
    ok = r["real"] == r["ours"] and len(set(r["ours"][1])) == len(r["ours"][1])
    bad += not ok
    print(f"{'PASS' if ok else 'FAIL'}  tailable {name}: mongod {r['real'][0]}, ours {r['ours'][0]} (batch, bytes); {len(r['ours'][1])} documents")
for name, ops, pipe, opts, mk in CS:
    r = {k: cs_run(c, ops, pipe, opts, mk) for k, c in clients.items()}
    se = {k: [strip(e) for e in v[1]] for k, v in r.items()}
    keys = [(bson.decode(e)["operationType"], bson.decode(e)["documentKey"]["_id"]) for e in r["ours"][1]]
    ok = r["real"][0] == r["ours"][0] and se["real"] == se["ours"] and len(keys) == len(set(keys)) == len(r["real"][1])
    bad += not ok
    if se["real"] != se["ours"]:
        diff = [(bson.decode(x), bson.decode(y)) for x, y in zip(se["real"], se["ours"]) if x != y][:1]
        print("   first different event (mongod, ours):", [(sorted(x.keys()), sorted(y.keys())) for x, y in diff])
    print(f"{'PASS' if ok else 'FAIL'}  change stream {name}: mongod {r['real'][0]}, ours {r['ours'][0]}; events mongod {len(r['real'][1])}, ours {len(r['ours'][1])}")
for k, c in clients.items():
    ok, b, n = resume_check(c, big(12, 5))
    bad += not ok
    print(f"{'PASS' if ok else 'FAIL'}  change stream resume after a 16 MiB batch on {k}: first batch {b[0]} events, then {n} more, nothing skipped or repeated")
for c in clients.values():
    c.drop_database(DB)
print("RESULT", "PASS" if not bad else f"FAIL ({bad})")
sys.exit(1 if bad else 0)
