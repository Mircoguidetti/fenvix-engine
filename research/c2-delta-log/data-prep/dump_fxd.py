"""Dump every document the MongoDBSaver wrote in DEFAULT mode, per cell, as the raw BSON mongod stores.

File <cell>.fxd (all integers little endian):
  magic b"FXD1", then records until EOF;
  record = coll u8 (0 = checkpoints, 1 = checkpoint_writes), thread_id length u16, thread_id utf-8,
           doc length u32, doc bytes (raw BSON as returned by mongod, RawBSONDocument.raw).

Order = the order the saver wrote them. MongoDBSaver (langgraph-checkpoint-mongodb 0.5.1) never sets _id:
put() is update_one(filter, {"$set": doc}, upsert=True) and put_writes() is bulk_write of
UpdateOne(filter, {"$set"|"$setOnInsert": doc}, upsert=True), so mongod generates _id (ObjectId) at the
upsert, from one process-wide counter shared by both collections. Sorting both collections by _id
therefore gives the insertion order. check_order() verifies it on every cell: within a thread the
checkpoint ids (uuid6, increasing in time) come in increasing order, every write comes after the
checkpoint it belongs to, and threads (run one after the other) are contiguous.

Also writes <cell>.json: docs and bytes per collection, threads, total BSON bytes, and the field names and
BSON types of one checkpoints doc and one checkpoint_writes doc (recursive into embedded documents).

Usage: dump_fxd.py [CELL ...]   (default: every cell of cells.py)
"""
import hashlib
import heapq
import json
import os
import struct
import sys

import bson
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

import cells as C

COLLS = ("checkpoints", "checkpoint_writes")
RAW = CodecOptions(document_class=RawBSONDocument)

BSON_TYPES = {
    0x01: "double", 0x02: "string", 0x03: "document", 0x04: "array", 0x05: "binary", 0x07: "objectId",
    0x08: "bool", 0x09: "date", 0x0A: "null", 0x10: "int32", 0x11: "timestamp", 0x12: "int64",
    0x13: "decimal128",
}


def layout(raw, depth=0):
    """Field names, BSON types and sizes of a raw BSON document, in stored order."""
    out = []
    raw = bytes(raw)
    bson.decode(raw)  # validates the document; the walk below reads the bytes directly
    i, end = 4, len(raw) - 1
    while i < end:
        t = raw[i]
        j = raw.index(b"\x00", i + 1)
        name = raw[i + 1:j].decode()
        p = j + 1
        f = {"name": name, "type": BSON_TYPES.get(t, hex(t))}
        if t in (0x03, 0x04):
            ln = struct.unpack_from("<i", raw, p)[0]
            f["bytes"] = ln
            f["fields"] = layout(raw[p:p + ln], depth + 1)
            p += ln
        elif t == 0x05:
            ln = struct.unpack_from("<i", raw, p)[0]
            f["subtype"] = raw[p + 4]
            f["bytes"] = ln
            p += 5 + ln
        elif t == 0x02:
            ln = struct.unpack_from("<i", raw, p)[0]
            f["bytes"] = ln - 1
            f["value_example"] = raw[p + 4:p + 3 + ln].decode()[:60]
            p += 4 + ln
        elif t == 0x07:
            p += 12
        elif t in (0x01, 0x09, 0x11, 0x12):
            p += 8
        elif t == 0x10:
            f["value_example"] = struct.unpack_from("<i", raw, p)[0]
            p += 4
        elif t == 0x08:
            p += 1
        elif t == 0x0A:
            pass
        elif t == 0x13:
            p += 16
        else:
            raise ValueError(f"unhandled BSON type {t:#x} in field {name}")
        out.append(f)
        i = p
    return out


def stream(db, ci):
    for d in db[COLLS[ci]].with_options(codec_options=RAW).find({}, sort=[("_id", 1)]):
        raw = d.raw
        yield bytes(d["_id"].binary), ci, d["thread_id"], d["checkpoint_id"], raw


def check_order(recs):
    """recs: (oid, coll, thread, checkpoint_id) in dump order.

    Hard invariants (a violation means _id order is not insertion order): threads are contiguous (run one
    after the other, graph.invoke waits for its writes) and, within a thread, checkpoint ids (uuid6, made in
    time order, puts chained one after the other) come in increasing order.
    Informational: writes that reach mongod before the checkpoint they belong to. LangGraph's default
    durability is "async": put and put_writes run on a background executor, so a task's writes can be
    upserted before the checkpoint doc they reference. That is the real arrival order, not an error.
    """
    problems = []
    seen_threads, cur = set(), None
    last_ck, cks = {}, {}
    early_writes = 0
    for oid, ci, tid, ckid in recs:
        if tid != cur:
            if tid in seen_threads:
                problems.append(f"thread {tid} not contiguous")
            seen_threads.add(tid)
            cur = tid
        if ci == 0:
            if tid in last_ck and ckid <= last_ck[tid]:
                problems.append(f"thread {tid}: checkpoint {ckid} after {last_ck[tid]}")
            last_ck[tid] = ckid
            cks.setdefault(tid, set()).add(ckid)
        elif ckid not in cks.get(tid, ()):
            early_writes += 1
    return problems[:20], early_writes


def dump(cell, mc):
    db = mc[cell["mongo_db"]]
    os.makedirs(C.DUMPS, exist_ok=True)
    path = os.path.join(C.DUMPS, cell["cell"] + ".fxd")
    counts, sizes = [0, 0], [0, 0]
    sha = [hashlib.sha256(), hashlib.sha256()]
    threads = set()
    order = []
    sample = [None, None]
    with open(path + ".tmp", "wb") as f:
        f.write(b"FXD1")
        for oid, ci, tid, ckid, raw in heapq.merge(stream(db, 0), stream(db, 1), key=lambda r: r[0]):
            tb = tid.encode()
            f.write(struct.pack("<BH", ci, len(tb)) + tb + struct.pack("<I", len(raw)) + raw)
            counts[ci] += 1
            sizes[ci] += len(raw)
            sha[ci].update(raw)
            threads.add(tid)
            order.append((oid, ci, tid, ckid))
            if sample[ci] is None and counts[ci] > 3:  # a doc from inside the first thread, not the very first
                sample[ci] = raw
    os.replace(path + ".tmp", path)
    problems, early_writes = check_order(order)
    meta = {
        "cell": cell["cell"],
        "mongo_db": cell["mongo_db"],
        "file": path,
        "file_bytes": os.path.getsize(path),
        "threads": len(threads),
        "docs": {COLLS[0]: counts[0], COLLS[1]: counts[1]},
        "bson_bytes": {COLLS[0]: sizes[0], COLLS[1]: sizes[1]},
        "total_bson_bytes": sum(sizes),
        "sha256_in_id_order": {COLLS[0]: sha[0].hexdigest(), COLLS[1]: sha[1].hexdigest()},
        "order": "both collections merged by _id (ObjectId generated by mongod at upsert)",
        "order_check_problems": problems,
        "writes_before_their_checkpoint": early_writes,
        "layout": {COLLS[0]: layout(sample[0]), COLLS[1]: layout(sample[1])},
    }
    json.dump(meta, open(os.path.join(C.DUMPS, cell["cell"] + ".json"), "w"), indent=1)
    print(cell["cell"], meta["threads"], meta["docs"], meta["total_bson_bytes"], "order problems:", len(problems), "early writes:", early_writes, flush=True)
    return meta


def main():
    want = set(sys.argv[1:])
    mc = MongoClient(C.MONGO)
    for cell in C.cells():
        if want and cell["cell"] not in want:
            continue
        if cell["mongo_db"] not in mc.list_database_names():
            print(cell["cell"], "SKIPPED: no database", cell["mongo_db"], flush=True)
            continue
        dump(cell, mc)


if __name__ == "__main__":
    main()
