#!/usr/bin/env python3
"""T1 bench: cold reads of never-read clients, one at a time, against mongo-server.

The queries are built by `prova_infinite.py`'s own `compito()` (imported, not copied, not modified), on a database
loaded by `prova_infinite.py carica`, so Q2, Q1, Q1d, Q3 and Q4 are the same shapes as on Atlas.

For every query: wait until the server is quiet (no object-store read in flight, no maps warm-up, no read-ahead
running), read the server's trip meter (serverStatus fenvix.waiter), run the find, read the meter again. Reported per
query: client-side milliseconds, trips (waves of object-store reads waited one after the other), reads (requests).
Then the same query again at once ("hot"), and, for Q1, the next page back of the same client after the read-ahead had
time to run ("next": the client keeps scrolling).

Usage: cold_reads.py --uri URI --state stato.json --script prova_infinite.py --label before --out before.json
"""
import argparse
import importlib.util
import json
import os
import random
import statistics
import sys
import time

from bson import ObjectId
from pymongo import MongoClient


def load_script(path):
    spec = importlib.util.spec_from_file_location("prova_infinite", path)
    mod = importlib.util.module_from_spec(spec)
    sys.argv = [path]  # the script parses nothing at import
    spec.loader.exec_module(mod)
    return mod


def waiter(admin):
    s = admin.command("serverStatus")
    w = s.get("fenvix", {}).get("waiter", {})
    return w


def quiet(admin, timeout=30.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        w = waiter(admin)
        busy = w.get("readsInFlight", 0) + w.get("readAhead", {}).get("busy", 0) + w.get("maps", {}).get("busy", 0)
        if busy == 0:
            return w
        time.sleep(0.02)
    return waiter(admin)


def run_find(db, spec):
    c = db[spec["coll"]].find(spec["filtro"])
    if spec.get("sort"):
        c = c.sort(spec["sort"])
    if spec.get("limit"):
        c = c.limit(spec["limit"])
    t0 = time.perf_counter()
    docs = list(c)
    return (time.perf_counter() - t0) * 1000.0, docs


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(p / 100.0 * (len(xs) - 1)))))
    return xs[k]


def summary(rows, key):
    xs = [r[key] for r in rows if r.get(key) is not None]
    if not xs:
        return None
    return {"n": len(xs), "p50": pct(xs, 50), "p95": pct(xs, 95), "p99": pct(xs, 99), "max": max(xs), "mean": statistics.mean(xs)}


def rss_mb(pid):
    try:
        for line in open(f"/proc/{pid}/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    except OSError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--db", default="app_demo")
    ap.add_argument("--state", required=True)
    ap.add_argument("--script", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=40, help="clients per query and group")
    ap.add_argument("--warmup", type=int, default=0, help="reads of other clients before measuring (steady state)")
    ap.add_argument("--pid", type=int, default=0, help="server pid, for its RSS")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--queries", default="Q2,Q1,Q1d,Q3,Q4")
    a = ap.parse_args()

    pi = load_script(a.script)
    st = json.load(open(a.state))
    cl = st["clienti"]
    n = cl["n"]
    dormant = set(cl["riservati"])
    q2 = [ObjectId(x) if x else None for x in cl["q2_id"]]
    rng = random.Random(a.seed)
    act = [i for i in range(n) if i not in dormant and q2[i] is not None]
    dor = [i for i in sorted(dormant) if q2[i] is not None]
    rng.shuffle(act)
    rng.shuffle(dor)

    client = MongoClient(a.uri, directConnection=True)
    db = client[a.db]
    admin = client.admin
    # open the database (one tiny command) and let the waiter settle (maps warm-up)
    t0 = time.time()
    db.command("ping")
    db["ordini"].find_one({"_id": ObjectId()})
    time.sleep(1.0)
    w0 = quiet(admin, timeout=600)
    open_s = time.time() - t0
    out = {"label": a.label, "open_and_maps_s": open_s, "after_open": w0, "rss_mb_after_open": rss_mb(a.pid) if a.pid else None, "queries": {}}

    used = set()
    def take(lst, k):
        r = []
        for i in lst:
            if i not in used and len(r) < k:
                used.add(i)
                r.append(i)
        return r

    if a.warmup:
        wa = take(act, a.warmup // 2) + take(dor, a.warmup - a.warmup // 2)
        for j, i in enumerate(wa):
            qq = ["Q2", "Q1", "Q3", "Q1d", "Q4"][j % 5]
            sp = pi.compito(qq, i, "warmup", "t1-warmup", q2_id=q2[i], riservato=i in dormant, byte_cliente=cl["byte"][i])["spec"]
            run_find(db, sp)
        out["after_warmup"] = quiet(admin)

    for q in a.queries.split(","):
        rows = []
        for group, lst in (("dormant", dor), ("active", act)):
            for i in take(lst, a.n):
                sp = pi.compito(q, i, group, "t1-" + q, q2_id=q2[i], riservato=i in dormant, byte_cliente=cl["byte"][i])["spec"]
                w1 = quiet(admin)
                time.sleep(0.03)  # longer than the chain gap (10 ms): this query starts a new chain
                ms, docs = run_find(db, sp)
                w2 = waiter(admin)
                hot_ms, _ = run_find(db, sp)
                row = {"q": q, "group": group, "client": i, "ms": ms, "docs": len(docs),
                       "trips": w2.get("trips", 0) - w1.get("trips", 0), "reads": w2.get("reads", 0) - w1.get("reads", 0),
                       "depth": (w2.get("lastChainDepth", 0) if w2.get("chains", 0) > w1.get("chains", 0) else 0),
                       "hedges": w2.get("hedges", 0) - w1.get("hedges", 0),
                       "hot_ms": hot_ms}
                if q in ("Q1", "Q1d") and len(docs) == sp.get("limit"):
                    # the next page back of the same client, after the read-ahead had time to run
                    quiet(admin)
                    last_ts = docs[-1]["ts"]
                    nsp = dict(sp)
                    f = dict(sp["filtro"])
                    f["ts"] = {"$lt": last_ts}
                    nsp["filtro"] = f
                    w3 = waiter(admin)
                    time.sleep(0.03)
                    nms, ndocs = run_find(db, nsp)
                    w4 = waiter(admin)
                    row.update({"next_ms": nms, "next_docs": len(ndocs), "next_trips": w4.get("trips", 0) - w3.get("trips", 0),
                                "next_reads": w4.get("reads", 0) - w3.get("reads", 0),
                                "next_depth": (w4.get("lastChainDepth", 0) if w4.get("chains", 0) > w3.get("chains", 0) else 0)})
                rows.append(row)
        res = {}
        for group in ("dormant", "active", "all"):
            rr = [r for r in rows if group == "all" or r["group"] == group]
            res[group] = {k: summary(rr, k) for k in ("ms", "hot_ms", "trips", "depth", "reads", "hedges", "docs", "next_ms", "next_trips", "next_depth", "next_reads")}
        out["queries"][q] = {"summary": res, "rows": rows}
        s = res["all"]
        print(f"[{a.label}] {q}: cold p50 {s['ms']['p50']:.1f} p95 {s['ms']['p95']:.1f} p99 {s['ms']['p99']:.1f} ms, "
              f"trips p50 {s['trips']['p50']} max {s['trips']['max']}, in a row p50 {s['depth']['p50']} p95 {s['depth']['p95']} max {s['depth']['max']}, reads p50 {s['reads']['p50']}, hot p50 {s['hot_ms']['p50']:.2f} ms"
              + (f", next p50 {s['next_ms']['p50']:.2f} p95 {s['next_ms']['p95']:.2f} ms, in a row p50 {s['next_depth']['p50']}" if s.get("next_ms") else ""), flush=True)
    out["end"] = quiet(admin)
    out["rss_mb_end"] = rss_mb(a.pid) if a.pid else None
    json.dump(out, open(a.out, "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
