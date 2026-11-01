#!/usr/bin/env python3
"""The customer's exit door: mongodump from the engine, mongorestore back, compare everything.

Steps
1. fill database `uscita_src` with varied data: every common BSON type, 20 000 documents, nested arrays, indexes
   (unique, compound, TTL, partial, sparse, text, 2dsphere, hashed, wildcard, collation), a validator, a capped
   collection, a view and a GridFS file;
2. mongodump --db uscita_src (folder) and mongodump --archive --gzip;
3. mongorestore both dumps into new databases (`--nsFrom/--nsTo`) of the same engine;
4. compare: documents (same BSON bytes, collection by collection, in _id order), index definitions, collection
   options (validator, capped), the view's pipeline, the GridFS file content.
5. the dump files themselves are checked as a mongod would read them: every .bson file decodes, and its count
   matches.

Usage: porta_uscita.py --uri mongodb://... --bin <folder with mongodump/mongorestore> --work <scratch folder>
"""
import argparse
import datetime
import gzip
import hashlib
import io
import json
import os
import random
import shutil
import subprocess
import uuid

import bson
import gridfs
from bson import Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from pymongo import MongoClient


def fill(db):
    for c in db.list_collection_names():
        db.drop_collection(c)
    rnd = random.Random(42)
    docs = []
    for i in range(20000):
        docs.append({
            "_id": i, "s": f"utente-{i}", "i32": i, "i64": Int64(i * 10_000_000_000), "dbl": i / 7,
            "dec": Decimal128(f"{i}.{i % 100:02d}"), "b": i % 2 == 0, "nil": None,
            "date": datetime.datetime(2026, 1, 1) + datetime.timedelta(seconds=i),
            "oid": ObjectId(), "bin": Binary(rnd.randbytes(16)), "uuid": Binary.from_uuid(uuid.UUID(int=i)),
            "arr": [i, str(i), {"k": i}], "sub": {"a": {"b": {"c": i}}},
            "re": Regex("^a.*", "i"), "ts": Timestamp(1700000000 + i, 1), "code": Code("function(){return 1}"),
            "mn": MinKey(), "mx": MaxKey(), "loc": {"type": "Point", "coordinates": [rnd.uniform(-10, 10),
                                                                                    rnd.uniform(-10, 10)]},
            "txt": rnd.choice(["il gatto dorme", "la casa è rossa", "fenvix su s3", "mare e montagna"]),
            "ttl": datetime.datetime(2100, 1, 1), "opt": i if i % 3 == 0 else None,
        })
    db.dati.insert_many(docs)
    db.dati.create_index("s", unique=True)
    db.dati.create_index([("i32", 1), ("dbl", -1)], name="comp")
    db.dati.create_index("ttl", expireAfterSeconds=3600)
    db.dati.create_index("opt", partialFilterExpression={"opt": {"$exists": True}}, name="part")
    db.dati.create_index("nil", sparse=True)
    db.dati.create_index([("txt", "text")], default_language="italian")
    db.dati.create_index([("loc", "2dsphere")])
    db.dati.create_index([("s", "hashed")], name="hash")
    db.dati.create_index([("sub.$**", 1)], name="wild")
    db.dati.create_index("s", name="s_coll", collation={"locale": "it", "strength": 2})
    db.create_collection("validata", validator={"$jsonSchema": {"required": ["nome"]}}, validationLevel="strict")
    db.validata.insert_many([{"nome": f"n{i}"} for i in range(10)])
    db.create_collection("limitata", capped=True, size=1 << 20, max=100)
    db.limitata.insert_many([{"n": i} for i in range(150)])
    db.create_collection("vista", viewOn="dati", pipeline=[{"$match": {"b": True}}, {"$project": {"s": 1}}])
    fs = gridfs.GridFSBucket(db)
    payload = rnd.randbytes(3 * 1024 * 1024 + 123)
    fs.upload_from_stream("file.bin", io.BytesIO(payload), metadata={"tipo": "prova"})
    return hashlib.sha256(payload).hexdigest()


def snapshot(db):
    out = {}
    for info in db.list_collections():
        name = info["name"]
        o = {"type": info.get("type"), "options": info.get("options", {})}
        if info.get("type") != "view":
            h = hashlib.sha256()
            n = 0
            for d in db[name].find({}, sort=[("_id", 1)]) if not info["options"].get("capped") else db[name].find():
                h.update(bson.encode(d))
                n += 1
            o["count"] = n
            o["sha256"] = h.hexdigest()
            o["indexes"] = sorted((json.dumps({k: v for k, v in ix.items() if k not in ("v", "ns")}, sort_keys=True,
                                              default=str) for ix in db[name].list_indexes()))
        out[name] = o
    return out


def gridfs_sha(db):
    out = io.BytesIO()
    gridfs.GridFSBucket(db).download_to_stream_by_name("file.bin", out)
    return hashlib.sha256(out.getvalue()).hexdigest()


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, (p.stdout + p.stderr)[-3000:]


def check_dump_files(folder, src):
    problems = []
    for f in sorted(os.listdir(folder)):
        if f.endswith(".bson"):
            data = open(os.path.join(folder, f), "rb").read()
            n = len(bson.decode_all(data))
            coll = f[:-5]
            exp = src.get(coll, {}).get("count")
            if exp is not None and exp != n:
                problems.append(f"{f}: {n} documents, the database has {exp}")
    return problems


def compare(a, b):
    diffs = []
    for name in sorted(set(a) | set(b)):
        if name not in b:
            diffs.append(f"{name}: missing after restore")
            continue
        if name not in a:
            diffs.append(f"{name}: extra after restore")
            continue
        for k in ("type", "options", "count", "sha256", "indexes"):
            if name == "system.views" and k == "sha256":
                continue  # its _id carries the database name, which changes with --nsTo (view options are compared)
            if a[name].get(k) != b[name].get(k):
                va, vb = a[name].get(k), b[name].get(k)
                if k == "indexes":
                    va = sorted(set(va) - set(vb))
                    vb = sorted(set(vb) - set(a[name][k]))
                diffs.append(f"{name}.{k}: before {str(va)[:400]} / after {str(vb)[:400]}")
    return diffs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work", required=True)
    a = ap.parse_args()
    c = MongoClient(a.uri)
    res = {"steps": {}}
    payload_sha = fill(c.uscita_src)
    src = snapshot(c.uscita_src)
    res["source"] = {k: {kk: v.get(kk) for kk in ("count", "type")} for k, v in src.items()}
    shutil.rmtree(a.work, ignore_errors=True)
    os.makedirs(a.work)
    dump = os.path.join(a.bin, "mongodump")
    restore = os.path.join(a.bin, "mongorestore")
    rc, log = run([dump, "--uri", a.uri, "--db", "uscita_src", "--out", os.path.join(a.work, "cartella")])
    res["steps"]["mongodump cartella"] = {"rc": rc, "log": log[-1200:]}
    rc, log = run([dump, "--uri", a.uri, "--db", "uscita_src", "--gzip", "--archive=" + os.path.join(a.work, "a.gz")])
    res["steps"]["mongodump archivio gzip"] = {"rc": rc, "log": log[-600:]}
    res["dump_file_problems"] = check_dump_files(os.path.join(a.work, "cartella", "uscita_src"), src)
    for target, args in (("uscita_dst1", ["--dir", os.path.join(a.work, "cartella")]),
                         ("uscita_dst2", ["--gzip", "--archive=" + os.path.join(a.work, "a.gz")])):
        c.drop_database(target)
        for coll in c[target].list_collection_names():
            c[target].drop_collection(coll)
        rc, log = run([restore, "--uri", a.uri, "--nsFrom", "uscita_src.*", "--nsTo", target + ".*",
                       "--drop", "--stopOnError"] + args)
        step = {"rc": rc, "log": log[-1500:]}
        if rc == 0:
            dst = snapshot(c[target])
            step["differences"] = compare(src, dst)
            try:
                step["gridfs_same_bytes"] = gridfs_sha(c[target]) == payload_sha
            except Exception as e:
                step["gridfs_same_bytes"] = f"error {e}"
        res["steps"]["mongorestore -> " + target] = step
    print(json.dumps(res, indent=1, default=str))


if __name__ == "__main__":
    main()
