"""first-course-short (2026-11-14): the cold short-history reads of bench-strong-point's history.py, run against one local
server process, with and without the first course, for customers who RETURN (they had a visit before the pause).

Same documents, index, plan generator, timed region, probe and sha256 check as history.py (imported from a
bench-strong-point checkout, --harness). What differs from history.py, and why:
  - the line: one mongo-server process on this machine, `--store local` wrapped by the engine's simulated S3
    (FENVIX_SIM_S3=20,0,100000: every GET/HEAD/LIST waits 20 ms, round B), `--memory-limit-mb 512`, no log copies;
    the restart is SIGTERM (a clean stop, as `docker restart -t 120`) then a fresh process, and the host's page cache
    is dropped;
  - classes: h50 only (200 customers with exactly 50 messages) plus newest-10 on 200 filler customers;
  - visit 1: before the restart every measured customer is read once with the same query that is measured after it
    (whole history for h50, newest 10 for fillers): the market's customer who comes back within days. history.py
    has no visit 1 (its customers were never read), so its "cold" is a first-ever read, which no opening can serve.
Usage:
  fc_local.py plan  --harness BSP --state DIR [--gb 0.4] [--fillers 8000]
  fc_local.py load  --harness BSP --state DIR --bin SERVER --data DIR
  fc_local.py run   --harness BSP --state DIR --bin SERVER --base DIR --out DIR --arm NAME [--flags "..."]
  fc_local.py safety --harness BSP --state DIR --bin SERVER --base DIR --out DIR --flags "..." [--negative]
      writes between visits: (1) by the same holder (must be seen: kept openings follow them), then (2) by another
      holder while ours is stopped (inserts and deletes: the kept openings must be refused). With --negative the server
      skips the ownership check (FENVIX_FIRST_COURSE_SHORT_NEGATIVE_CONTROL=1) and the test must find wrong reads.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time

PORT = 27061
URI = f"mongodb://127.0.0.1:{PORT}/?directConnection=true"
SIM = "20,0,100000"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def harness(path):
    sys.path.insert(0, os.path.join(path, "bench/strong-point"))
    import history  # noqa: E402
    history.CLASSES = {"h50": 50}
    return history


def up(binp, data, out, flags, sim=True, holder="fc-local", extra_env=None, mem="512"):
    env = dict(os.environ) | (extra_env or {})
    if sim:
        env["FENVIX_SIM_S3"] = SIM
    else:
        env.pop("FENVIX_SIM_S3", None)
    cmd = [binp, "--listen", f"127.0.0.1:{PORT}", "--store", "local", "--local-dir", data, "--holder", holder,
           "--customer-key", "messages=userId", "--memory-limit-mb", mem, "--idle-secs", "100000"] + flags.split()
    lf = open(os.path.join(out, "server.log"), "a")
    lf.write(f"\n=== {time.strftime('%FT%TZ', time.gmtime())} {' '.join(cmd)} (sim {sim})\n")
    lf.flush()
    p = subprocess.Popen(cmd, env=env, stdout=lf, stderr=subprocess.STDOUT)
    for _ in range(600):
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.2).close()
            break
        except OSError:
            if p.poll() is not None:
                raise RuntimeError("server exited at start")
            time.sleep(0.1)
    from pymongo import MongoClient
    MongoClient(URI, serverSelectionTimeoutMS=60000).admin.command("ping")
    return p


def down(p):
    t0 = time.time()
    p.send_signal(signal.SIGTERM)
    p.wait(timeout=300)
    subprocess.run("sync; echo 3 > /proc/sys/vm/drop_caches", shell=True, capture_output=True)
    log(f"stop (SIGTERM) in {time.time() - t0:.1f} s, exit {p.returncode}, page cache dropped")


def cmd_plan(h, a):
    p = h.make_plan(a.state, [a.gb], 200, 11, [a.fillers, a.fillers, a.fillers])
    log("plan:", p["slots_total"], "docs, avg bson", p["avg_bson_bytes"])


def cmd_load(h, a):
    os.makedirs(a.data, exist_ok=True)
    os.makedirs(a.state, exist_ok=True)
    p = up(a.bin, a.data, a.state, "", sim=False, mem="4096")  # the load only; every measured run is capped at 512 MiB
    log("schema", h.schema("fenvix-log", URI)["index"])
    # history.load fails on a write whose outcome is unknown (it refuses to guess); here the documents' _ids are a pure
    # function of the slot, so a chunk is retried unordered and only duplicate-key errors (already applied) are allowed
    from pymongo.errors import BulkWriteError, OperationFailure
    pl = h.Plan(a.state)
    arr = pl.parts[0]
    _, coll = h.mongo_coll(URI)
    t0, retries = time.time(), 0
    for lo in range(0, len(arr), 500):
        docs = [h.mongo_doc(lo + i, int(u)) for i, u in enumerate(arr[lo:lo + 500])]
        for attempt in range(20):
            try:
                coll.insert_many(docs, ordered=attempt == 0)
                break
            except BulkWriteError as e:
                if all(x.get("code") == 11000 for x in e.details.get("writeErrors", [])) and not e.details.get("writeConcernErrors"):
                    break
                retries += 1
                time.sleep(10)
            except OperationFailure:
                retries += 1
                time.sleep(10)
        else:
            raise RuntimeError(f"chunk {lo} failed 20 times")
        if lo % 50000 == 0:
            log(f"load {lo:,}/{len(arr):,} retries {retries}")
    n = coll.count_documents({})
    log("load", {"docs": len(arr), "counted": n, "seconds": round(time.time() - t0), "retried_chunks": retries})
    assert n == len(arr), "document count differs from the plan"
    time.sleep(90)  # let flushes and compaction settle a little
    down(p)


def cmd_run(h, a):
    os.makedirs(a.out, exist_ok=True)
    data = os.path.join(a.out, "data")
    if os.path.exists(data):
        shutil.rmtree(data)
    shutil.copytree(a.base, data)
    pl = h.Plan(a.state)
    P = pl.p
    hids, nids, wids = P["measured_ids"]["h50"], P["newest_ids"], P["warmup_ids"]
    exp = pl.expected(hids + nids + wids, 1)
    tasks = [("H", u) for u in hids] + [("N", u) for u in nids]
    random.Random(5).shuffle(tasks)
    # visit 1 (before the pause), with the same server flags
    p = up(a.bin, data, a.out, a.flags)
    rd = h.Reader("fenvix-log", URI)
    wrong = 0
    t0 = time.time()
    for kind, u in tasks:
        n, hw, h10 = exp[u]
        # --visit1-newest10: visit 1 reads only the newest 10 of every customer (the opening then answers a later whole
        # history read only in part)
        lim = 10 if (kind == "N" or a.visit1_newest10) else None
        _, _, rows = rd.read(u, lim)
        cnt, hh, _ = rd.digest(rows)
        wrong += (cnt, hh) != ((n, hw) if lim is None else (min(10, n), h10))
    log(f"visit 1: {len(tasks)} reads in {time.time() - t0:.1f} s, wrong {wrong}")
    fc1 = rd.c.admin.command("serverStatus").get("fenvix", {}).get("firstCourse", {})
    down(p)
    # the pause ends: a fresh process, warm-up on other customers, then each measured customer's first read
    p = up(a.bin, data, a.out, a.flags)
    rd = h.Reader("fenvix-log", URI)
    probe = h.Probe({}, URI)
    rows_f = open(os.path.join(a.out, "rows.jsonl"), "w")
    agg = {}

    def one(op, u, limit, want):
        nonlocal wrong
        b = probe.before()
        secs, _, rows = rd.read(u, limit)
        aa = probe.after()
        cnt, hh, nb = rd.digest(rows)
        ok = (cnt, hh) == want
        wrong += not ok
        res = h.Probe.delta(b, aa)
        gets = res.get("srv.s3.get", 0) + res.get("srv.s3.getRange", 0)
        trips = res.get("srv.waiter.trips", 0)
        rows_f.write(json.dumps({"op": op, "u": u, "ms": round(secs * 1000, 3), "n": cnt, "ok": ok, "gets": gets,
                                 "trips": trips, "res": res}) + "\n")
        g = agg.setdefault(op, {"ms": [], "gets": [], "trips": [], "served": 0})
        g["ms"].append(secs * 1000)
        g["gets"].append(gets)
        g["trips"].append(trips)
        g["served"] += res.get("srv.firstCourse.servedQueries", 0)
        g.setdefault("partial", 0)
        g["partial"] += res.get("srv.firstCourse.partialQueries", 0)

    for u in wids:
        one("warmup", u, 10, (min(10, exp[u][0]), exp[u][2]))
    for kind, u in tasks:
        n, hw, h10 = exp[u]
        if kind == "H":
            one("whole_cold", u, None, (n, hw))
            one("whole_warm", u, None, (n, hw))
        else:
            one("newest10_cold", u, 10, (min(10, n), h10))
            one("newest10_warm", u, 10, (min(10, n), h10))
    st = rd.c.admin.command("serverStatus").get("fenvix", {})
    down(p)
    summ = {"arm": a.arm, "flags": a.flags, "sim_s3": SIM, "wrong_reads": wrong, "first_course_before_pause": fc1,
            "first_course_after": st.get("firstCourse", {}), "ops": {}}
    for k, g in agg.items():
        summ["ops"][k] = {"n": len(g["ms"]), "p50_ms": round(h.pct(g["ms"], .5), 2), "p99_ms": round(h.pct(g["ms"], .99), 2),
                          "mean_ms": round(sum(g["ms"]) / len(g["ms"]), 2),
                          "gets_mean": round(sum(g["gets"]) / len(g["gets"]), 3), "gets_p50": h.pct(g["gets"], .5),
                          "gets_p99": h.pct(g["gets"], .99), "trips_mean": round(sum(g["trips"]) / len(g["trips"]), 3),
                          "trips_p50": h.pct(g["trips"], .5), "trips_p99": h.pct(g["trips"], .99), "served_from_opening": g["served"], "partial_from_opening": g.get("partial", 0)}
    json.dump(summ, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    shutil.rmtree(data)
    for k in ("whole_cold", "newest10_cold", "whole_warm", "newest10_warm"):
        d = summ["ops"].get(k)
        if d:
            log(f"{a.arm:14s} {k:14s} n={d['n']} p50 {d['p50_ms']:7.2f} p99 {d['p99_ms']:7.2f} ms  GETs mean {d['gets_mean']} "
                f"p50 {d['gets_p50']} p99 {d['gets_p99']}  trips mean {d['trips_mean']} p99 {d['trips_p99']}  served {d['served_from_opening']} partial {d['partial_from_opening']}")
    log(f"{a.arm}: wrong reads {wrong}")
    return 3 if wrong else 0


def cmd_safety(h, a):
    os.makedirs(a.out, exist_ok=True)
    data = os.path.join(a.out, "data")
    if os.path.exists(data):
        shutil.rmtree(data)
    shutil.copytree(a.base, data)
    pl = h.Plan(a.state)
    hids = pl.p["measured_ids"]["h50"]
    slots = {u: pl.slots_of(u, 1) for u in hids}
    g1, g2, g3 = hids[0:50], hids[50:100], hids[100:150]
    nxt = [h.SAVE_BASE]
    neg = {"FENVIX_FIRST_COURSE_SHORT_NEGATIVE_CONTROL": "1"} if a.negative else {}

    def add(coll, us):
        for u in us:
            for _ in range(2):
                n = nxt[0]
                nxt[0] += 1
                coll.insert_one(h.mongo_doc(n, u))
                slots[u].append(n)

    def delete_newest(coll, us):
        from bson import ObjectId
        for u in us:
            n = max(slots[u])
            r = coll.delete_one({"_id": ObjectId(h.doc_parts(n, u)["oid"])})
            assert r.deleted_count == 1
            slots[u].remove(n)

    def check(tag):
        rd = h.Reader("fenvix-log", URI)
        bad = []
        for u in hids:
            _, _, rows = rd.read(u, None)
            cnt, hh, _ = rd.digest(rows)
            if (cnt, hh) != (len(slots[u]), h.expected_hash(slots[u], u)):
                bad.append(u)
        fc = rd.c.admin.command("serverStatus")["fenvix"]["firstCourse"]
        open(os.path.join(a.out, "fc-stats.jsonl"), "a").write(json.dumps({"tag": tag, "fc": fc}) + "\n")
        log(f"{tag}: wrong {len(bad)} of {len(hids)} (g1 {len(set(bad) & set(g1))}, g2 {len(set(bad) & set(g2))}, "
            f"g3 {len(set(bad) & set(g3))}); restored {fc.get('restored')} refused {fc.get('restoreRefused')} served {fc.get('servedQueries')}")
        return len(bad)

    p = up(a.bin, data, a.out, a.flags, extra_env=neg)
    check("A visit 1")
    _, coll = h.mongo_coll(URI)
    add(coll, g1)
    delete_newest(coll, g1[:25])
    down(p)
    p = up(a.bin, data, a.out, a.flags, extra_env=neg)
    w1 = check("A after its own writes and a restart (kept openings must be used and exact)")
    down(p)
    p = up(a.bin, data, a.out, "", holder="fc-other")
    _, coll = h.mongo_coll(URI)
    add(coll, g2)
    delete_newest(coll, g3)
    check("B (another holder) wrote g2 and g3")
    down(p)
    p = up(a.bin, data, a.out, a.flags, extra_env=neg)
    w2 = check("A after B's writes (kept openings must be refused)")
    down(p)
    shutil.rmtree(data)
    json.dump({"negative": a.negative, "wrong_own": w1, "wrong_after_other": w2}, open(os.path.join(a.out, "safety.json"), "w"))
    return 3 if (w1 or w2) and not a.negative else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["plan", "load", "run", "safety"])
    ap.add_argument("--harness", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--gb", type=float, default=0.4)
    ap.add_argument("--fillers", type=int, default=8000)
    ap.add_argument("--bin")
    ap.add_argument("--data")
    ap.add_argument("--base")
    ap.add_argument("--out")
    ap.add_argument("--arm", default="off")
    ap.add_argument("--flags", default="")
    ap.add_argument("--negative", action="store_true")
    ap.add_argument("--visit1-newest10", action="store_true")
    a = ap.parse_args()
    h = harness(a.harness)
    sys.exit({"plan": cmd_plan, "load": cmd_load, "run": cmd_run, "safety": cmd_safety}[a.mode](h, a) or 0)


if __name__ == "__main__":
    main()
