#!/usr/bin/env python3
"""First course in the engine: two visits of 1,000 users (prompts/P-first-course-engine.md in fenvix-brain).

Data: the 2 GB chat dataset of the compaction bench (bench/compaction/cold_customer.py: 4.57 M messages, 20,000 zipf
customers, index {userId: 1, ts: -1}, customer key userId), loaded by run.sh. The server runs with the simulated store
(5 ms per GET) and a 1 MiB block cache, so data blocks are really cold.

  1. warm-up: 300 finds of other users (SST indexes and filters warm, data blocks cold, as in compaction-1);
  2. visit 1 of 1,000 users (500 drawn by activity, 500 uniformly): Q1 "the 20 newest messages", then Q2 and Q3 (the
     next two pages of 20) and Q4 (the 50 newest);
  3. the gap: new messages at the medium (or busy) profile scaled to the dataset, for other users and for 30% of the
     test users, with the compaction passes they cause; then 300 finds of other users again (maps of the new SSTs warm);
  4. visit 2: Q1 again (with the first course on, answered from the opening when exact), Q1 once more (a normal read:
     the second query of a visit never uses the opening), then Q2, Q3, Q4.

Every visit-2 Q1 answer is checked twice: against the normal read that follows it, and against an exact model (the
visit-1 answer, which came from the normal path, plus the gap's messages of that user; the gap has no deletes).

GETs of a query = the server's own count of object-store reads (serverStatus fenvix.waiter.reads) before and after,
one query at a time on a quiet server.

Gap sizes (ESTIMATED scaling: the 1 TB customer's writes per second x 2 GiB / 1 TiB): medium 1,000 writes/s and busy
5,000 writes/s (fenvix-brain cost.py PROFILES and fable-impossible-1tb baseline.py) give 168,750 messages a day
(medium) and 843,750 (busy). short = 1 day medium; long = 14 days medium, written one day at a time; busy = 1 day busy.

Usage:
  two_visits.py settle --uri URI
  two_visits.py visits --uri URI --state state.json --gap short|long|busy --users 1000 --pid PID --out result.json
"""
import argparse
import datetime as dt
import json
import multiprocessing as mp
import os
import random
import statistics
import sys
import time

from bson import ObjectId
from pymongo import MongoClient

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compaction"))
from cold_customer import vocab, zipf_weights  # noqa: E402  (same generator as the load)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "c2-step6"))
import sealed  # noqa: E402  (compaction 2 step 6: publishes through the server, the counter's contents)

DB, COLL = "chat", "messages"
PER_DAY = {"medium": 1000 * 86400 // 512, "busy": 5000 * 86400 // 512}
GAPS = {"short": ("medium", 1), "long": ("medium", 14), "busy": ("busy", 1)}
T_GAP0 = dt.datetime(2026, 1, 3)  # after every loaded message (the load spans about 12.7 hours from 2026-01-01)


def fen(admin):
    return admin.command("serverStatus").get("fenvix", {})


def settle(uri, quiet_secs=20, limit=3600):
    c = MongoClient(uri)
    q, t0 = None, time.time()
    while time.time() - t0 < limit:
        if fen(c.admin)["compaction"]["running"] == 0:
            q = q or time.time()
            if time.time() - q >= quiet_secs:
                break
        else:
            q = None
        time.sleep(1)
    print(f"settled after {time.time() - t0:.0f} s", flush=True)


def quiet(admin, timeout=120.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        f = fen(admin)
        w = f.get("waiter", {})
        busy = w.get("readsInFlight", 0) + w.get("readAhead", {}).get("busy", 0) + w.get("maps", {}).get("busy", 0) + f.get("compaction", {}).get("running", 0)
        if busy == 0:
            return f
        time.sleep(0.02)
    return fen(admin)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(p / 100.0 * (len(xs) - 1)))))] if xs else None


def cpu_secs(pid):
    try:
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")
    except Exception:
        return None


def maint(admin):
    """Compaction counters of the process (tenantStats), whatever their nesting."""
    out = {}

    def walk(d):
        for k, v in d.items():
            if isinstance(v, dict):
                walk(v)
            elif k in ("compactRuns", "compactPuts", "compactGets"):
                out[k] = v
    walk(admin.command("tenantStats"))
    return out


def opening_sizes(admin):
    try:
        r = admin.command("fenvixFirstCourse", DB)
        return r["bytes"], r["entries"]
    except Exception:
        return [], []


def size_summary(admin):
    b, e = opening_sizes(admin)
    if not b:
        return None
    return {"n": len(b), "bytes_median": statistics.median(b), "bytes_p95": pct(b, 95), "bytes_max": max(b), "entries_median": statistics.median(e), "bytes_total": sum(b)}


def gap_worker(args):
    uri, wid, nworkers, n_docs, base_index, customers, seed, written_test, test_set = args
    words, ww = vocab()
    r = random.Random(seed * 7919 + wid + base_index)
    cw = zipf_weights(customers, 1.0)
    perm = list(range(customers))
    random.Random(seed).shuffle(perm)
    c = MongoClient(uri, w=1)
    coll = c[DB][COLL]
    mine = {}
    i = 0
    while i < n_docs:
        k = min(1000, n_docs - i)
        ranks = r.choices(range(customers), weights=cw, k=k)
        batch = []
        for j in range(k):
            u = perm[ranks[j]]
            # test users not chosen for new messages never get any; their share goes to other users
            while u in test_set and u not in written_test:
                u = perm[r.choices(range(customers), weights=cw)[0]]
            n = base_index + (i + j) * nworkers + wid
            ts = T_GAP0 + dt.timedelta(milliseconds=n * 10)
            oid = ObjectId(int(ts.timestamp()).to_bytes(4, "big") + ((0x7F << 56) | n).to_bytes(8, "big"))
            text = " ".join(r.choices(words, weights=ww, k=r.randint(30, 80)))
            batch.append({"_id": oid, "userId": u, "convId": u * 16 + r.randint(0, 15), "role": "user" if n % 2 else "assistant", "ts": ts, "text": text, "tokens": len(text) // 4})
            if u in written_test:
                mine.setdefault(u, []).append((ts.isoformat(), str(oid)))
        coll.insert_many(batch, ordered=False)
        i += k
    return mine


def write_gap(uri, n, base_index, customers, seed, written_test, test_set, workers=4):
    per = n // workers
    with mp.Pool(workers) as p:
        res = p.map(gap_worker, [(uri, w, workers, per, base_index, customers, seed, written_test, test_set) for w in range(workers)])
    out = {}
    for m in res:
        for u, v in m.items():
            out.setdefault(u, []).extend(v)
    return out, per * workers


def visits(a):
    state = json.load(open(a.state))
    counts = {int(k): v for k, v in state["counts"].items()}
    customers = state["customers"]
    c = MongoClient(a.uri)
    admin, coll = c.admin, c[DB][COLL]
    r = random.Random(a.seed)
    eligible = [u for u, n in counts.items() if n >= 20]
    weights = [counts[u] for u in eligible]
    picked = set()

    def pick(weighted):
        while True:
            u = r.choices(eligible, weights=weights)[0] if weighted else r.choice(eligible)
            if u not in picked:
                picked.add(u)
                return u

    def q(u, flt=None, limit=20):
        f = {"userId": u}
        if flt:
            f.update(flt)
        return list(coll.find(f).sort("ts", -1).limit(limit))

    def timed(fn):
        f1 = quiet(admin)
        t = time.perf_counter()
        docs = fn()
        ms = (time.perf_counter() - t) * 1000
        f2 = quiet(admin)
        return docs, f2["waiter"]["reads"] - f1["waiter"]["reads"], ms

    def warm(n):
        t = time.time()
        for _ in range(n):
            q(pick(False))
        return time.time() - t

    def later(u, first):
        """Q2, Q3, Q4 of a visit: GETs and ms of each."""
        out = []
        last = first[-1]["ts"] if first else None
        for _ in range(2):
            docs, g, ms = timed(lambda: q(u, {"ts": {"$lt": last}}) if last else [])
            out.append({"gets": g, "ms": ms})
            last = docs[-1]["ts"] if docs else last
        docs, g, ms = timed(lambda: q(u, limit=50))
        out.append({"gets": g, "ms": ms})
        return out

    res = {"gap": a.gap, "users": a.users, "seed": a.seed, "dataset": {k: v for k, v in state.items() if k != "counts"}}
    res["warmup1_secs"] = warm(a.warm)
    if a.sealed:
        # compaction 2 step 6: measure the counter of our runs from here (the visit-1 reads of the test users only)
        sealed.counter(admin, clear=True)
    test = [(pick(True), "active") for _ in range(a.users // 2)] + [(pick(False), "uniform") for _ in range(a.users - a.users // 2)]
    test_set = {u for u, _ in test}
    rw = random.Random(a.seed + 1)
    written_test = {u for u, _ in test if rw.random() < 0.30}
    fc0 = fen(admin).get("firstCourse", {})

    # visit 1
    v1 = {}
    t = time.time()
    for u, grp in test:
        docs, g, ms = timed(lambda: q(u))
        assert len(docs) == 20 and all(d["userId"] == u for d in docs), u
        v1[u] = {"group": grp, "q1": {"gets": g, "ms": ms}, "answer": [(d["ts"].isoformat(), str(d["_id"])) for d in docs], "later": later(u, docs)}
    res["visit1_secs"] = time.time() - t
    res["sizes_after_visit1"] = size_summary(admin)
    if a.sealed:
        # right after the visit (slices still whole), then trimmed to what the visit relied on (the server does it
        # --sealed-trim-ms after a slice is made)
        for name, trim in (("counter_after_visit1", False), ("counter_after_visit1_trimmed", True)):
            cnt = sealed.counter(admin, trim=trim)
            per = sorted(cnt.get("perCustomer", []))
            res[name] = {k: v for k, v in cnt.items() if k not in ("perCustomer", "ok")}
            res[name]["ram_per_hot_customer_mean"] = cnt["bytes"] / max(1, len(v1))
            res[name]["customers_charged"] = len(per)
            res[name]["charged_p50"] = per[len(per) // 2] if per else 0
            res[name]["charged_p95"] = per[int(len(per) * 0.95)] if per else 0
            print(f"{name}: {res[name]}", flush=True)
    fc1 = fen(admin).get("firstCourse", {})
    print(f"visit 1: {len(v1)} users in {res['visit1_secs']:.0f} s; openings {res['sizes_after_visit1']}", flush=True)

    # the gap
    profile, days = GAPS[a.gap]
    per_day = int(PER_DAY[profile] * a.scale)
    m0, cpu0, p0 = maint(admin), cpu_secs(a.pid), fen(admin)["compaction"]["passes"]
    t = time.time()
    new_msgs, written = {}, 0
    for day in range(days):
        m, n = write_gap(a.uri, per_day, day * per_day * 2, customers, a.seed + day, written_test, test_set)
        written += n
        for u, v in m.items():
            new_msgs.setdefault(u, []).extend(v)
        print(f"gap day {day + 1}/{days}: {written} messages, compaction passes so far {fen(admin)['compaction']['passes'] - p0}", flush=True)
        if a.sealed:
            # our runs follow the gap: a delta run every day, a reseal of every group every third day and on the last day
            # (the stub of step 4's triggers), so visit 2 reads right after a reseal
            rs = "all" if (day + 1) % 3 == 0 or day == days - 1 else "none"
            rep = sealed.publish(admin, DB, rs)
            if os.environ.get("SEALED_BUCKET"):
                rep["bench_gc"] = sealed.gc(os.environ["SEALED_BUCKET"], DB)
            res.setdefault("publishes", []).append(rep)
            print(f"  publish ({rs}): version {rep['version']} in {rep['millis']} ms, stage {rep.get('stage')}", flush=True)
    # every test user chosen for new messages gets at least one
    extra = [u for u in written_test if u not in new_msgs]
    for i, u in enumerate(sorted(extra)):
        ts = T_GAP0 + dt.timedelta(days=30, milliseconds=i * 10)
        oid = ObjectId(int(ts.timestamp()).to_bytes(4, "big") + ((0x7E << 56) | i).to_bytes(8, "big"))
        coll.insert_one({"_id": oid, "userId": u, "convId": u * 16, "role": "user", "ts": ts, "text": "hello again", "tokens": 3})
        new_msgs.setdefault(u, []).append((ts.isoformat(), str(oid)))
        written += 1
    write_secs = time.time() - t
    settle(a.uri)
    gap_secs = time.time() - t
    m1, cpu1, p1 = maint(admin), cpu_secs(a.pid), fen(admin)["compaction"]["passes"]
    fc2 = fen(admin).get("firstCourse", {})
    res["gap_stats"] = {
        "profile": profile, "days": days, "messages": written, "test_users_written": len(written_test),
        "test_messages": sum(len(v) for v in new_msgs.values()), "write_secs": write_secs, "gap_secs": gap_secs,
        "compaction_passes": p1 - p0, "compaction": {k: m1.get(k, 0) - m0.get(k, 0) for k in m1},
        "server_cpu_secs": (cpu1 - cpu0) if cpu0 is not None and cpu1 is not None else None,
        "first_course_writes_applied": fc2.get("writesApplied", 0) - fc1.get("writesApplied", 0),
        "first_course_write_hook_ms": (fc2.get("writeHookNs", 0) - fc1.get("writeHookNs", 0)) / 1e6,
        "trimmed": fc2.get("trimmed", 0) - fc1.get("trimmed", 0), "dropped_too_big": fc2.get("droppedTooBig", 0) - fc1.get("droppedTooBig", 0),
    }
    res["sizes_after_gap"] = size_summary(admin)
    print(f"gap: {res['gap_stats']}", flush=True)
    res["warmup2_secs"] = warm(a.warm)
    # every test user idle long enough for its next query to start a visit
    idle = a.idle_secs + 2
    time.sleep(max(0.0, idle - (time.time() - t)))
    fc3 = fen(admin).get("firstCourse", {})

    # visit 2, in another order
    order = list(test)
    random.Random(a.seed + 2).shuffle(order)
    rows = []
    wrong = []
    for u, grp in order:
        docs, g, ms = timed(lambda: q(u))
        ans = [(d["ts"].isoformat(), str(d["_id"])) for d in docs]
        # the later queries first (a re-read before them would warm the block cache for them)
        lat = later(u, docs)
        # then the same query with {$in: [u]}: same answer, never answered by the first course (normal path)
        normal, g_normal, ms_normal = timed(lambda: list(coll.find({"userId": {"$in": [u]}}).sort("ts", -1).limit(20)))
        ans_normal = [(d["ts"].isoformat(), str(d["_id"])) for d in normal]
        model = sorted(set(map(tuple, v1[u]["answer"])) | set(map(tuple, new_msgs.get(u, []))), key=lambda x: x[0], reverse=True)[:20]
        model = [list(x) for x in model]
        ok_normal = ans == ans_normal
        ok_model = [list(x) for x in ans] == model
        full_ok = ok_normal and ok_model and all(d["userId"] == u for d in docs)
        if not full_ok:
            wrong.append({"user": u, "got": ans, "normal": ans_normal, "model": model})
        rows.append({"user": u, "group": grp, "written": u in written_test, "new_messages": len(new_msgs.get(u, [])),
                     "v1_q1": v1[u]["q1"], "v1_later": v1[u]["later"],
                     "q1": {"gets": g, "ms": ms}, "q1_normal": {"gets": g_normal, "ms": ms_normal},
                     "exact_vs_normal": ok_normal, "exact_vs_model": ok_model, "later": lat})
    fc4 = fen(admin).get("firstCourse", {})
    res["sizes_after_visit2"] = size_summary(admin)
    res["first_course_counters"] = {"visit1": {k: fc1.get(k, 0) - fc0.get(k, 0) for k in fc1 if isinstance(fc1.get(k), int)},
                                    "visit2": {k: fc4.get(k, 0) - fc3.get(k, 0) for k in fc4 if isinstance(fc4.get(k), int)}, "end": fc4}
    res["wrong"] = wrong
    res["rows"] = rows

    def share(sel):
        xs = [x for x in rows if sel(x)]
        z = [x for x in xs if x["q1"]["gets"] == 0 and x["exact_vs_normal"] and x["exact_vs_model"]]
        return {"n": len(xs), "zero_get_exact": len(z), "share": len(z) / max(1, len(xs)),
                "q1_gets_mean": statistics.mean(x["q1"]["gets"] for x in xs) if xs else None,
                "q1_ms_p50": pct([x["q1"]["ms"] for x in xs], 50), "q1_ms_p99": pct([x["q1"]["ms"] for x in xs], 99),
                "v1_q1_gets_mean": statistics.mean(x["v1_q1"]["gets"] for x in xs) if xs else None,
                "v1_q1_ms_p50": pct([x["v1_q1"]["ms"] for x in xs], 50)}
    res["summary"] = {
        "all": share(lambda x: True), "active": share(lambda x: x["group"] == "active"), "uniform": share(lambda x: x["group"] == "uniform"),
        "written": share(lambda x: x["written"]), "not_written": share(lambda x: not x["written"]),
        "wrong_answers": len(wrong),
        "later_ms_visit1": {"p50": pct([l["ms"] for x in rows for l in x["v1_later"]], 50), "p99": pct([l["ms"] for x in rows for l in x["v1_later"]], 99),
                            "gets_mean": statistics.mean(l["gets"] for x in rows for l in x["v1_later"])},
        "later_ms_visit2": {"p50": pct([l["ms"] for x in rows for l in x["later"]], 50), "p99": pct([l["ms"] for x in rows for l in x["later"]], 99),
                            "gets_mean": statistics.mean(l["gets"] for x in rows for l in x["later"])},
    }
    json.dump(res, open(a.out, "w"), indent=1)
    s = res["summary"]
    print(f"visit 2: zero-GET exact answers {s['all']['zero_get_exact']}/{s['all']['n']} ({100 * s['all']['share']:.1f}%), "
          f"active {100 * s['active']['share']:.1f}%, uniform {100 * s['uniform']['share']:.1f}%, written {100 * s['written']['share']:.1f}%, "
          f"not written {100 * s['not_written']['share']:.1f}%; wrong answers {s['wrong_answers']}", flush=True)
    print(f"Q1 visit 1: GETs mean {s['all']['v1_q1_gets_mean']:.2f}, ms p50 {s['all']['v1_q1_ms_p50']:.1f}; visit 2: GETs mean {s['all']['q1_gets_mean']:.2f}, "
          f"ms p50 {s['all']['q1_ms_p50']:.2f}, p99 {s['all']['q1_ms_p99']:.2f}", flush=True)
    print(f"later queries ms: visit 1 p50 {s['later_ms_visit1']['p50']:.2f} p99 {s['later_ms_visit1']['p99']:.2f}; visit 2 p50 {s['later_ms_visit2']['p50']:.2f} p99 {s['later_ms_visit2']['p99']:.2f}", flush=True)
    print(f"opening sizes: after visit 1 {res['sizes_after_visit1']}; after gap {res['sizes_after_gap']}; after visit 2 {res['sizes_after_visit2']}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["settle", "visits"])
    ap.add_argument("--sealed", action="store_true", help="compaction 2 step 6: publish our runs during the gap, measure the counter")
    ap.add_argument("--uri", default="mongodb://127.0.0.1:27051/?directConnection=true")
    ap.add_argument("--state")
    ap.add_argument("--gap", choices=list(GAPS), default="short")
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--pid", type=int, default=0)
    ap.add_argument("--idle-secs", type=int, default=20)
    ap.add_argument("--warm", type=int, default=300, help="finds of other users before each visit")
    ap.add_argument("--scale", type=float, default=1.0, help="gap writes x this (smoke tests only)")
    ap.add_argument("--out", default="result.json")
    a = ap.parse_args()
    settle(a.uri) if a.cmd == "settle" else visits(a)


if __name__ == "__main__":
    main()
