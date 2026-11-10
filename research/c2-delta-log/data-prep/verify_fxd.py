"""Re-read every <cell>.fxd and check it against the Mongo collections it was dumped from.

Per cell and collection: document count (file vs collStats count vs count_documents), total BSON bytes
(file vs collStats size, which is the sum of the stored BSON sizes, vs an $bsonSize aggregate), and
sha256 of the docs in _id order (file vs a fresh read). Also: the file parses to EOF exactly, and each
record's thread_id equals the thread_id field inside its document.

Usage: verify_fxd.py [CELL ...]  -> prints one line per cell, writes results/verify.json, exits 1 on any mismatch
"""
import hashlib
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
HERE = os.path.dirname(os.path.abspath(__file__))


def read_fxd(path):
    counts, sizes = [0, 0], [0, 0]
    sha = [hashlib.sha256(), hashlib.sha256()]
    threads = set()
    tid_mismatch = 0
    with open(path, "rb") as f:
        data = f.read()
    assert data[:4] == b"FXD1", "bad magic"
    p = 4
    while p < len(data):
        ci, tl = struct.unpack_from("<BH", data, p)
        p += 3
        tid = data[p:p + tl].decode()
        p += tl
        (dl,) = struct.unpack_from("<I", data, p)
        p += 4
        doc = data[p:p + dl]
        p += dl
        assert len(doc) == dl and struct.unpack_from("<i", doc, 0)[0] == dl, "truncated or bad doc length"
        assert ci in (0, 1)
        counts[ci] += 1
        sizes[ci] += dl
        sha[ci].update(doc)
        threads.add(tid)
        tid_mismatch += RawBSONDocument(doc)["thread_id"] != tid
    assert p == len(data)
    return counts, sizes, [s.hexdigest() for s in sha], len(threads), tid_mismatch


def main():
    want = set(sys.argv[1:])
    mc = MongoClient(C.MONGO)
    out, bad = {}, 0
    for cell in C.cells():
        if want and cell["cell"] not in want:
            continue
        path = os.path.join(C.DUMPS, cell["cell"] + ".fxd")
        if not os.path.exists(path):
            print(cell["cell"], "no dump")
            continue
        db = mc[cell["mongo_db"]]
        counts, sizes, shas, nthreads, tid_mismatch = read_fxd(path)
        r = {"threads_in_file": nthreads, "thread_id_mismatches": tid_mismatch}
        ok = tid_mismatch == 0
        for ci, coll in enumerate(COLLS):
            st = db.command("collStats", coll)
            agg = list(db[coll].aggregate([{"$group": {"_id": None, "n": {"$sum": 1}, "b": {"$sum": {"$bsonSize": "$$ROOT"}}}}]))[0]
            h = hashlib.sha256()
            for d in db[coll].with_options(codec_options=CodecOptions(document_class=RawBSONDocument)).find({}, sort=[("_id", 1)]):
                h.update(d.raw)
            c = {"file_docs": counts[ci], "mongo_count": st["count"], "mongo_count_documents": agg["n"],
                 "file_bytes": sizes[ci], "mongo_size": st["size"], "mongo_bsonsize_sum": agg["b"],
                 "sha_match": h.hexdigest() == shas[ci]}
            c["ok"] = (counts[ci] == st["count"] == agg["n"] and sizes[ci] == st["size"] == agg["b"] and c["sha_match"])
            ok &= c["ok"]
            r[coll] = c
        r["ok"] = ok
        bad += not ok
        out[cell["cell"]] = r
        print(cell["cell"], "OK" if ok else "MISMATCH",
              {k: (r[k]["file_docs"], r[k]["file_bytes"]) for k in COLLS}, flush=True)
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    json.dump(out, open(os.path.join(HERE, "results", "verify.json"), "w"), indent=1)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
