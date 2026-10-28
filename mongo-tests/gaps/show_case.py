#!/usr/bin/env python3
"""Print the result of one gap case on the reference mongod and on the engine, side by side (for debugging a failing case).
usage: show_case.py <case-name-substring> [--rs]"""
import glob, importlib, json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
import gaps_py
from pymongo import MongoClient

want = sys.argv[1]
for f in sorted(glob.glob(os.path.join(HERE, want.split(".")[0] + "_*.py"))):
    importlib.import_module(os.path.basename(f)[:-3])
kw = dict(serverSelectionTimeoutMS=15000, socketTimeoutMS=60000)
for c in gaps_py.CASES:
    if want not in c["name"]:
        continue
    rs = c["rs"]
    real = MongoClient("localhost", 27020 if rs else 27017, **({"replicaSet": "rs0"} if rs else {}), **kw)
    ours = MongoClient("localhost", 27018, directConnection=True, **kw)
    print("==", c["name"])
    outs = []
    for name, cl in (("real", real), ("ours", ours)):
        r, _ = gaps_py.run_side(cl, c)
        outs.append(r)
        try:
            print(name, json.dumps(json.loads(r), indent=None, ensure_ascii=False)[:int(os.environ.get("SHOW", "3000"))])
        except Exception:
            print(name, r[:int(os.environ.get("SHOW", "3000"))])
    a, b = outs
    if a != b:
        i = next((k for k in range(min(len(a), len(b))) if a[k] != b[k]), min(len(a), len(b)))
        print("first difference at", i, "\n  real:", a[max(0, i - 80):i + 120], "\n  ours:", b[max(0, i - 80):i + 120])
