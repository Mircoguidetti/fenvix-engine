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


CERTS = os.path.join(os.path.dirname(HERE), "..", ".tools", "certs")
PAIRS = {"A": (27040, 27048), "B": (27041, 27049), "C": (27042, 27050), "D": (27043, 27051), "E": (27044, 27052)}


def case(name, rs=False, sort_keys=False, uri=None, auth=False, pair=None, opts=None):
    """auth=True: run on the servers started with --auth (see auth_env.sh), through a client logged in as the root user.
    pair="A".."E": run on the TLS server pair of tls_env.py; the client is built with `opts` (TLS options) and carries .gaps_addr."""
    def deco(f):
        CASES.append(dict(name=name, rs=rs, f=f, sort_keys=sort_keys, uri=uri, auth=auth, pair=pair, opts=opts or {}))
        return f
    return deco


def canon(x, sort_keys=False):
    return run_py.canon(x, {"sort_keys": True} if sort_keys else {})


_COUNTER = [0]


def uniq(prefix="n"):
    """Deterministic unique names within one side of a case (the same on both servers)."""
    _COUNTER[0] += 1
    return f"{prefix}{_COUNTER[0]}"


def deep_diff(a, b, path=""):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a: out.append(f"{path}/{k}: only ours = {json.dumps(b[k])[:160]}")
            elif k not in b: out.append(f"{path}/{k}: only real = {json.dumps(a[k])[:160]}")
            else: out += deep_diff(a[k], b[k], f"{path}/{k}")
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)): out += deep_diff(x, y, f"{path}[{i}]")
    elif a != b:
        out.append(f"{path}: real={json.dumps(a)[:160]} ours={json.dumps(b)[:160]}")
    return out


def cleanup_auth(client, dbname):
    """Remove the users and roles a case created (everything except the root user)."""
    try:
        for d in (dbname, dbname + "_o"):
            client[d].command("dropAllUsersFromDatabase")
            client[d].command("dropAllRolesFromDatabase")
        for u in client.admin.command("usersInfo", 1)["users"]:
            if u["user"] != "root":
                client.admin.command("dropUser", u["user"])
        client.admin.command("dropAllRolesFromDatabase")
    except Exception as e:  # noqa
        print("cleanup failed:", e)


def run_side(client, c):
    dbname = "gp_" + uuid.uuid4().hex[:10]
    _COUNTER[0] = 0
    try:
        try:
            return canon(c["f"](client, client[dbname]), c["sort_keys"]).replace(dbname, "$DB"), False
        except Exception as e:  # noqa
            return canon(run_py.err_result(e, False), c["sort_keys"]).replace(dbname, "$DB"), True
    finally:
        try:
            client.drop_database(dbname)
        except Exception:  # noqa
            pass
        if c.get("auth"):
            cleanup_auth(client, dbname)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", default="mongodb://localhost:27017")
    ap.add_argument("--real-rs", default="mongodb://localhost:27020/?replicaSet=rs0")
    ap.add_argument("--ours", default="mongodb://localhost:27018")
    ap.add_argument("--ours-rs", default=None, help="engine URI for rs cases (default: --ours)")
    ap.add_argument("--real-auth", default="mongodb://root:rootpw@localhost:27030/?authSource=admin")
    ap.add_argument("--ours-auth", default="mongodb://root:rootpw@localhost:27028/?authSource=admin")
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
    real_a = ours_a = None
    res, bad = [], 0
    for c in CASES:
        if a.only and a.only not in c["name"]:
            continue
        if c.get("pair"):
            rp, op = PAIRS[c["pair"]]
            opts = {k: (os.path.join(CERTS, v) if isinstance(v, str) and v.endswith(".pem") else v) for k, v in c["opts"].items()}
            outs = []
            for port in (rp, op):
                cl = MongoClient("localhost", port, connect=False, serverSelectionTimeoutMS=4000, **opts)
                cl.gaps_addr = ("localhost", port)
                outs.append(run_side(cl, c)[0])
                cl.close()
            r, o = outs
        elif c["auth"]:
            real_a = real_a or MongoClient(a.real_auth, **kw)
            ours_a = ours_a or MongoClient(a.ours_auth, **kw)
            r, _ = run_side(real_a, c)
            o, _ = run_side(ours_a, c)
        else:
            r, _ = run_side(real_rs if c["rs"] else real, c)
            o, _ = run_side(ours_rs if c["rs"] else ours, c)
        ok = r == o
        bad += not ok
        res.append(dict(name=c["name"], status="pass" if ok else "fail", real=r[:500], ours=o[:500]))
        print("." if ok else "F", c["name"], flush=True)
        if not ok:
            try:
                for line in deep_diff(json.loads(r), json.loads(o))[:12]:
                    print("    diff", line)
            except Exception:  # noqa
                print(f"    real: {r[:400]}\n    ours: {o[:400]}")
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1)
    print(f"gaps_py: {len(res) - bad}/{len(res)} identical to mongod 7.0.45")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
