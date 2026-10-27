#!/usr/bin/env python3
"""Differential tests for the compat-gaps work (pymongo). Same method as probe_diff / run_py: a case is a function f(client, db) run on
real mongod 7.0 and on the engine, each on a fresh database; results and errors (code, codeName) are canonicalised with run_py.canon
and must be identical.

    gaps_py.py [--only substr] [--real URI] [--real-rs URI] [--ours URI] [--item g01]

Case modules are gaps/g*_*.py; every case is registered with @case(name, rs=False, ...). A case may return a dict/list/scalar, or raise;
the error code and codeName are compared. Cases marked rs=True run on the replica-set mongod (27020) and on the engine started with
--replica-set rs0."""
import argparse, importlib, json, os, sys, time, uuid, glob
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import run_py
sys.modules.setdefault("gaps_py", sys.modules[__name__])
from pymongo import MongoClient

CASES = []


def case(name, rs=False, sort_keys=False, uri=None):
    def deco(f):
        CASES.append(dict(name=name, rs=rs, f=f, sort_keys=sort_keys, uri=uri))
        return f
    return deco


def canon(x, sort_keys=False):
    return run_py.canon(x, {"sort_keys": True} if sort_keys else {})


def run_side(client, c):
    dbname = "gp_" + uuid.uuid4().hex[:10]
    try:
        try:
            return canon(c["f"](client, client[dbname]), c["sort_keys"]), False
        except Exception as e:  # noqa
            return canon(run_py.err_result(e, False), c["sort_keys"]), True
    finally:
        try:
            client.drop_database(dbname)
        except Exception:  # noqa
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", default="mongodb://localhost:27017")
    ap.add_argument("--real-rs", default="mongodb://localhost:27020/?replicaSet=rs0")
    ap.add_argument("--ours", default="mongodb://localhost:27018")
    ap.add_argument("--ours-rs", default=None, help="engine URI for rs cases (default: --ours)")
    ap.add_argument("--only", default="")
    ap.add_argument("--item", default="")
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    for f in sorted(glob.glob(os.path.join(HERE, "g[0-9]*.py"))):
        m = os.path.basename(f)[:-3]
        if a.item and not m.startswith(a.item):
            continue
        importlib.import_module(m)
    kw = dict(serverSelectionTimeoutMS=15000, socketTimeoutMS=60000)
    real, real_rs, ours = MongoClient(a.real, **kw), MongoClient(a.real_rs, **kw), MongoClient(a.ours, **kw)
    ours_rs = MongoClient(a.ours_rs, **kw) if a.ours_rs else ours
    res, bad = [], 0
    for c in CASES:
        if a.only and a.only not in c["name"]:
            continue
        r, _ = run_side(real_rs if c["rs"] else real, c)
        o, _ = run_side(ours_rs if c["rs"] else ours, c)
        ok = r == o
        bad += not ok
        res.append(dict(name=c["name"], status="pass" if ok else "fail", real=r[:500], ours=o[:500]))
        print("." if ok else "F", c["name"], "" if ok else f"\n    real: {r[:400]}\n    ours: {o[:400]}", flush=True)
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1)
    print(f"gaps_py: {len(res) - bad}/{len(res)} identical to mongod 7.0.45")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
