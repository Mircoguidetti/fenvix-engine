#!/usr/bin/env python3
"""Show the replies of one script case (scripts/*.json) on the reference mongod and on the engine, step by step.
usage: script_show.py <case name> [--steps 2,3]   (std/sa cases: real 27017 / engine 27018 (27019 for sa); rs cases: real 27020 / engine 27018)"""
import json, os, sys, glob
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE); sys.path.insert(0, os.path.join(HERE, ".."))
import g10_scripts as g
from pymongo import MongoClient
want = sys.argv[1]
only = None
if "--steps" in sys.argv:
    only = {int(x) for x in sys.argv[sys.argv.index("--steps") + 1].split(",")}
c = None
for f in glob.glob(os.path.join(HERE, "scripts", "*.json")):
    for x in json.load(open(f)):
        if x["name"] == want:
            c = x
if not c:
    sys.exit("no such case")
mode = c.get("mode", "std")
real = MongoClient("localhost", 27020 if mode == "rs" else 27017, **({"replicaSet": "rs0"} if mode == "rs" else {"directConnection": True}))
ours = MongoClient("localhost", 27019 if mode == "sa" else 27018, directConnection=True)
import uuid
res = []
for cl in (real, ours):
    db = cl["ss_" + uuid.uuid4().hex[:8]]
    res.append(g.make(c)(cl, db))
    cl.drop_database(db.name)
keys = list(res[0].keys())
for k in keys:
    if only is not None and k[1:].isdigit() and int(k[1:]) not in only:
        continue
    a, b = json.dumps(res[0][k], default=str), json.dumps(res[1].get(k), default=str)
    print(("= " if a == b else "! ") + k + " " + (a if len(a) < 1500 else a[:1500] + "..."))
    if a != b:
        print("  ours " + (b if len(b) < 1500 else b[:1500] + "..."))
