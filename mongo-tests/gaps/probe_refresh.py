"""Copy of compat/tools/probe_diff.py for the compat-gaps work: the probes that send explicit txnNumbers (rs probes) use a client of their own
on the engine, so the pooled implicit sessions of the other probes keep their numbers (the audit ran against an engine that ignored them).
usage: as probe_diff.py; --out defaults to /tmp."""
#!/usr/bin/env python3
"""Extra differential probes written for this audit (the suite in mongo-tests has no tests for these features).

Each probe is a function f(client, db) registered with @case(name, ids, ...). It is run twice, once against real mongo:7.0 and once
against the engine, each on a fresh database; the results (or the error code/codeName) are canonicalised with the suite's own
comparison code (run_py.canon / err_result) and compared. A probe passes when both sides are identical. A failing probe is
classified 'reject' (the engine errors where mongo succeeds) or 'differ' (both succeed with different results, e.g. an ignored option).

    probe_diff.py [--real mongodb://localhost:27017] [--real-rs "mongodb://localhost:27020/?replicaSet=rs0"] [--ours mongodb://localhost:27018] [--only substr]
writes compat/probe_diff_results.json  {cases: {name: {...}}, by_id: {inventory id: [{name, status, kind}]}}"""
import argparse, importlib, json, os, sys, time, uuid
HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "compat", "tools")
sys.path.insert(0, os.path.join(HERE, "..", "..", "mongo-tests"))
sys.path.insert(0, os.path.join(HERE, "..", "probes"))
import run_py
sys.modules.setdefault("probe_diff", sys.modules[__name__])
from pymongo import MongoClient

CASES = []


def case(name, ids, rs=False, fail="partial", expect_error=False, control=None, sort_keys=False):
    """fail: status a failing probe gives its rows when both sides succeed but differ ('missing' where the engine silently ignores the feature)."""
    def deco(f):
        CASES.append(dict(name=name, ids=list(ids), rs=rs, f=f, fail=fail, expect_error=expect_error, control=control, sort_keys=sort_keys))
        return f
    return deco


def canon(x, sort_keys=False):
    return run_py.canon(x, {"sort_keys": True} if sort_keys else {})


def deep_diff(a, b, path=""):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a: out.append(f"{path}/{k}: only ours = {json.dumps(b[k])[:120]}")
            elif k not in b: out.append(f"{path}/{k}: only real = {json.dumps(a[k])[:120]}")
            else: out += deep_diff(a[k], b[k], f"{path}/{k}")
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)): out += deep_diff(x, y, f"{path}[{i}]")
    elif a != b:
        out.append(f"{path}: real={json.dumps(a)[:120]} ours={json.dumps(b)[:120]}")
    return out


def run_side(client, case_, which="f"):
    dbname = "cp_" + uuid.uuid4().hex[:10]
    try:
        try:
            res = case_[which](client, client[dbname])
            return canon(res, case_.get("sort_keys", False)), False
        except Exception as e:  # noqa
            r = run_py.err_result(e, False)
            return canon(r, case_.get("sort_keys", False)), True
    finally:
        try:
            client.drop_database(dbname)
        except Exception:  # noqa
            pass


MODULES = ["p_bson", "p_collation", "p_concerns", "p_options", "p_changestreams", "p_txn", "p_misc", "p_gridfs", "p_driver"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", default="mongodb://localhost:27017")
    ap.add_argument("--real-rs", default="mongodb://localhost:27020/?replicaSet=rs0")
    ap.add_argument("--ours", default="mongodb://localhost:27018")
    ap.add_argument("--only", default="")
    ap.add_argument("--diff", action="store_true", help="print a structured diff for each failing probe")
    ap.add_argument("--repeat", type=int, default=2, help="run every probe this many times; a probe whose verdict changes between runs is reported as 'flaky' and ignored as evidence")
    ap.add_argument("--out", default="/tmp/probe_refresh_results.json")
    a = ap.parse_args()
    for m in MODULES:
        if os.path.exists(os.path.join(HERE, "..", "probes", m + ".py")):
            importlib.import_module(m)
    kw = dict(serverSelectionTimeoutMS=15000, socketTimeoutMS=60000)
    real, real_rs, ours, ours_rs = MongoClient(a.real, **kw), MongoClient(a.real_rs, **kw), MongoClient(a.ours, **kw), MongoClient(a.ours, **kw)
    out, by_id = {}, {}
    for c in CASES:
        if a.only and a.only not in c["name"]:
            continue
        t0 = time.time()
        r, rerr = run_side(real_rs if c["rs"] else real, c)
        o, oerr = run_side(ours_rs if c["rs"] else ours, c)
        ok = r == o
        kind = "" if ok else ("reject" if (oerr and not rerr) else "differ")
        if not ok and kind == "differ" and c.get("control"):
            cc = dict(c, f=c["control"])
            rc, _ = run_side(real_rs if c["rs"] else real, cc); oc, _ = run_side(ours_rs if c["rs"] else ours, cc)
            if oc == o and rc != r:
                kind = "ignored"      # the engine accepts the option but its result is identical to the request without it
        if ok and rerr and oerr and not c["expect_error"]:
            r, rerr = run_side(real_rs if c["rs"] else real, c)       # one retry: transient errors on both sides compare equal
            o, oerr = run_side(ours_rs if c["rs"] else ours, c)
            ok = r == o
            kind = "" if ok else ("reject" if (oerr and not rerr) else "differ")
            if ok and rerr and oerr:
                kind = "invalid"      # both sides raised the same error: the probe itself is broken, not evidence
        status = "invalid" if kind == "invalid" else ("pass" if ok else "fail")
        for _ in range(a.repeat - 1):
            r2, rerr2 = run_side(real_rs if c["rs"] else real, c)
            o2, oerr2 = run_side(ours_rs if c["rs"] else ours, c)
            if (r2 == o2) != ok:
                status, kind = "flaky", "flaky"
                break
        out[c["name"]] = dict(ids=c["ids"], status=status, kind=kind, fail=c["fail"], real=r[:400], ours=o[:400], secs=round(time.time() - t0, 2))
        for i in c["ids"]:
            by_id.setdefault(i, []).append(dict(name=c["name"], status=status, kind=kind, fail=c["fail"]))
        print(("I" if status == "invalid" else "?" if status == "flaky" else "." if status == "pass" else "F"), c["name"], ("" if status == "pass" else f"\n    real: {r[:300]}\n    ours: {o[:300]}"), flush=True)
        if a.diff and status == "fail":
            for line in deep_diff(json.loads(r), json.loads(o))[:12]: print("      diff", line)
    if not a.only:
        json.dump(dict(cases=out, by_id=by_id), open(a.out, "w"), indent=1, sort_keys=True)
    n = sum(v["status"] == "pass" for v in out.values()); inv = sum(v["status"] == "invalid" for v in out.values())
    print(f"{n}/{len(out)} probes identical on both servers ({inv} invalid: both sides errored identically)")


if __name__ == "__main__":
    main()
