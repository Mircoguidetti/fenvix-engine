#!/usr/bin/env python3
"""The 16 MiB edge of a change stream's getMore, on one server: a small event then a big one whose two events (as this
server writes them) total 16 MiB + extra; the largest extra for which both come in one batch. Then the same for a
tailable cursor on a capped collection. Usage: cs_edge.py URI"""
import sys, bson
from bson import SON
from pymongo import MongoClient
c = MongoClient(sys.argv[1]); d = c.app_cs_edge
M = 16 * 1024 * 1024

def event_overhead():
    d.drop_collection("cs"); d.create_collection("cs")
    r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]))
    doc = {"_id": 1, "p": "x" * 1000}
    d.cs.insert_one(doc)
    g = d.command(SON([("getMore", r["cursor"]["id"]), ("collection", "cs"), ("maxTimeMS", 2000)]))
    e = g["cursor"]["nextBatch"][0]
    return len(bson.encode(e)) - len(bson.encode(doc))

def doc_of(i, size):
    x = {"_id": i, "p": ""}
    x["p"] = "x" * (size - len(bson.encode(x)))
    return x

def cs_fits(extra, ov):
    d.drop_collection("cs"); d.create_collection("cs")
    r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]))
    a = doc_of(1, 200); b = doc_of(2, M + extra - (200 + ov) - ov)
    d.cs.insert_one(a); d.cs.insert_one(b); d.cs.insert_one({"_id": 3})
    got = 0
    for _ in range(10):
        g = d.command(SON([("getMore", r["cursor"]["id"]), ("collection", "cs"), ("maxTimeMS", 1000)]))
        n = len(g["cursor"]["nextBatch"])
        if n:
            return n >= 2
    raise RuntimeError("no events")

def tail_fits(extra):
    d.drop_collection("cap"); d.create_collection("cap", capped=True, size=100 * 1024 * 1024)
    d.cap.insert_one({"_id": -1})
    r = d.command(SON([("find", "cap"), ("tailable", True), ("awaitData", True), ("batchSize", 1)]))
    d.cap.insert_one(doc_of(1, 200)); d.cap.insert_one(doc_of(2, M + extra - 200)); d.cap.insert_one({"_id": 3})
    for _ in range(10):
        g = d.command(SON([("getMore", r["cursor"]["id"]), ("collection", "cap"), ("maxTimeMS", 1000)]))
        n = len(g["cursor"]["nextBatch"])
        if n:
            return n >= 2
    raise RuntimeError("no documents")

def edge(f):
    lo, hi = -2000, 0
    assert f(lo) and not f(hi)
    while hi - lo > 1:
        m = (lo + hi) // 2
        if f(m): lo = m
        else: hi = m
    return lo

ov = event_overhead()
print(f"event overhead {ov} bytes; change stream: two events fit up to 16 MiB {edge(lambda x: cs_fits(x, ov))}; tailable: two documents fit up to 16 MiB {edge(tail_fits)}")
