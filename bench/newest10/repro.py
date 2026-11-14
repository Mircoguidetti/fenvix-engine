"""newest10-trips: smallest local reproduction of "newest 10 of one customer, cold" costing many GETs.

Collection messages, index {userId: 1, ts: -1}, server with --customer-key messages=userId:ts (the shape of
bench/strong-point/history.py), on `--store local` with the engine's simulated S3 latency (FENVIX_SIM_S3, a fixed
DELAY ms per GET/HEAD/LIST, sigma 0): MinIO is not available in this sandbox, and the delay model is the same one
s3delay adds (a fixed time to first byte per request). Counters per read from serverStatus().fenvix:
  - GETs: s3.get + s3.getRange (every object-store read the read sent);
  - rounds: waiter.trips (0 -> 1 transitions of reads in flight; one query at a time, nothing in the background) and
    waiter.lastChainDepth (reads waited for one after the other).

  repro.py load  --dir D                      load phase 1 (sealed), then K rounds of more writes (delta runs)
  repro.py run   --dir D --out F.json [--bin B] [--delay 20] [--flags "..."]
                                              restart, warm-up of 300 other customers, newest-10 cold of 200 customers,
                                              then each answer checked against the plan AND against a full scan
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.request

from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BIN = os.path.join(ROOT, "target/release/mongo-server")
DB, COLL = "app_demo", "messages"
WORDS = None


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def words():
    global WORDS
    if WORDS is None:
        r = random.Random(7)
        WORDS = ["".join(r.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(r.randint(2, 9))) for _ in range(4000)]
    return WORDS


def text_of(slot: int) -> str:
    r = random.Random(slot * 2654435761 + 12345)
    n = r.randint(120, 480)        # ~800-3,200 characters
    w = words()
    return " ".join(w[min(int(r.paretovariate(1.1)) - 1, 3999)] for _ in range(n))


def doc(slot: int, u: int) -> dict:
    return {"_id": slot, "userId": u, "convId": u * 16 + slot % 16, "role": "user" if slot % 2 else "assistant",
            "ts": slot * 10, "text": text_of(slot), "tokens": slot % 997}


def digest(docs) -> str:
    h = hashlib.sha256()
    for d in docs:
        h.update(f"{d['_id']}|{d['userId']}|{d['ts']}|".encode() + d["text"].encode() + b"\n")
    return h.hexdigest()


def fresh_store(d):
    """Every run starts from the store as the load left it (a run's close pass would otherwise absorb the held L0 SSTs
    and the next run would read another layout)."""
    pristine, store = os.path.join(d, "store.pristine"), os.path.join(d, "store")
    if not os.path.exists(pristine):
        os.rename(store, pristine)
    if os.path.exists(store):
        shutil.rmtree(store)
    subprocess.run(["cp", "-a", pristine, store], check=True)
    for x in os.listdir("/tmp"):
        if x.startswith("fenvix-box-floor-"):
            shutil.rmtree(os.path.join("/tmp", x), ignore_errors=True)


class Srv:
    def __init__(self, d, port=27991, delay=20, flags="", binary=BIN, tag="srv", env=None):
        self.d, self.port, self.delay, self.flags, self.bin = d, port, delay, flags.split(), binary
        self.extra_env = env or {}
        self.log = os.path.join(d, f"{tag}.log")
        self.p = None

    def start(self):
        env = dict(os.environ, MONGO_ADMIN_TOKEN="newest10-local", **self.extra_env)
        if self.delay:
            env["FENVIX_SIM_S3"] = f"{self.delay},0,100000"
        cmd = [self.bin, "--listen", f"127.0.0.1:{self.port}", "--store", "local", "--local-dir", os.path.join(self.d, "store"),
               "--holder", "n10", "--no-ttl", "--idle-secs", "3600", "--customer-key", f"{COLL}=userId:ts"] + self.flags
        self.p = subprocess.Popen(cmd, stdout=open(self.log, "ab"), stderr=subprocess.STDOUT, env=env)
        t0 = time.time()
        while time.time() - t0 < 60:
            if self.p.poll() is not None:
                raise RuntimeError(f"server exited {self.p.returncode}, see {self.log}")
            try:
                c = self.client()
                c.admin.command("ping")
                return c
            except Exception:
                time.sleep(0.3)
        raise RuntimeError("server not ready")

    def client(self):
        return MongoClient(f"mongodb://127.0.0.1:{self.port}/?directConnection=true", serverSelectionTimeoutMS=5000)

    def stop(self):
        if self.p and self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(60)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()


def fx(c):
    return c.admin.command("serverStatus")["fenvix"]


def counters(c):
    f = fx(c)
    s, w = f["s3"], f["waiter"]
    return {"gets": s["get"] + s["getRange"], "heads": s.get("head", 0), "lists": s.get("list", 0), "trips": w["trips"],
            "chain": w["lastChainDepth"], "chains": w["chains"], "maxInflight": w["maxReadsInFlight"]}


# ---------------------------------------------------------------------------------------------------------------------
# the plan: phase 1 = CUST customers x PER1 messages interleaved; then ROUNDS rounds, each a random half of the customers
# writes PER_R more messages (they "kept writing after compaction": their newest messages sit in newer delta runs)
# ---------------------------------------------------------------------------------------------------------------------

def plan(cust, per1, rounds, per_r, seed=11):
    rng = random.Random(seed)
    phases = []
    p1 = [u for u in range(1, cust + 1) for _ in range(per1)]
    rng.shuffle(p1)
    phases.append(p1)
    for _ in range(rounds):
        who = rng.sample(range(1, cust + 1), cust // 2)
        p = [u for u in who for _ in range(per_r)]
        rng.shuffle(p)
        phases.append(p)
    slots, n = [], 0
    for p in phases:
        s = []
        for u in p:
            n += 1
            s.append((n, u))
        slots.append(s)
    return slots


def by_customer(slots):
    m = {}
    for ph in slots:
        for n, u in ph:
            m.setdefault(u, []).append(n)
    return m


def wait_quiet(c, secs, what):
    """Let the compactor/flush settle: wait until the published version stops changing for a while."""
    t0, last, same = time.time(), None, 0
    while time.time() - t0 < secs:
        f = fx(c)
        cur = json.dumps([f["sealedRuns"]["publishes"], {k: v for k, v in f["compactor"].items() if not isinstance(v, (dict, list))}], sort_keys=True, default=str)
        same = same + 1 if cur == last else 0
        last = cur
        if same >= 10:
            break
        time.sleep(1)
    log(f"{what}: settled after {time.time() - t0:.0f}s")


def cmd_load(a):
    if os.path.exists(a.dir):
        shutil.rmtree(a.dir)
    os.makedirs(a.dir)
    slots = plan(a.cust, a.per1, a.rounds, a.per_r)
    json.dump({"cust": a.cust, "per1": a.per1, "rounds": a.rounds, "per_r": a.per_r}, open(os.path.join(a.dir, "plan.json"), "w"))
    s = Srv(a.dir, delay=0, flags=a.flags, tag="load")
    c = s.start()
    col = c[DB][COLL]
    col.create_index([("userId", 1), ("ts", -1)])
    for i, ph in enumerate(slots):
        if a.hold_last and i == len(slots) - a.hold_last:
            # the last rounds stay in SlateDB's L0, not absorbed (a compactor that lags the writes, as at 10 GB)
            c.close()
            s.stop()
            s = Srv(a.dir, delay=0, flags=a.flags, tag="load-hold", env={"FENVIX_TEST_ABSORB_MIN_L0": "1000"})
            c = s.start()
            col = c[DB][COLL]
        t0 = time.time()
        B = 500
        for k in range(0, len(ph), B):
            col.insert_many([doc(n, u) for n, u in ph[k:k + B]], ordered=False)
        log(f"phase {i}: {len(ph)} docs in {time.time() - t0:.1f}s")
        # a flush + compaction between phases, so each later phase lands in runs of its own
        try:
            c.admin.command("fsync")
        except Exception as e:
            log("fsync:", e)
        wait_quiet(c, a.settle, f"phase {i}")
    json.dump(fx(c), open(os.path.join(a.dir, "fx_after_load.json"), "w"), indent=1, default=str)
    c.close()
    if a.hold_last:
        # no close pass (it would absorb the held L0 SSTs): every write is durable (fsync above), then SIGKILL
        time.sleep(3)
        s.p.kill()
        s.p.wait()
    else:
        s.stop()
    du = subprocess.run(["du", "-sh", os.path.join(a.dir, "store")], capture_output=True, text=True).stdout.strip()
    log("loaded:", du)


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))] if v else None


def cmd_run(a):
    p = json.load(open(os.path.join(a.dir, "plan.json")))
    slots = plan(p["cust"], p["per1"], p["rounds"], p["per_r"])
    bc = by_customer(slots)
    rng = random.Random(a.seed)
    custs = list(bc)
    rng.shuffle(custs)
    warm, meas = custs[:a.warm], custs[a.warm:a.warm + a.n]
    env = {"FENVIX_TEST_ABSORB_MIN_L0": "1000"} if a.hold else {}
    if a.trace:
        env["FENVIX_SIM_TRACE"] = "1"
    fresh_store(a.dir)
    s = Srv(a.dir, delay=a.delay, flags=a.flags, binary=a.bin, tag=a.tag, env=env)
    c = s.start()
    col = c[DB][COLL]
    if a.pretraffic:
        # read traffic first (newest 10 of other customers, each cold once), the compactor acts on it, then a cold
        # restart: no close pass (SIGKILL, every write is durable), so the held L0 SSTs stay as they were
        pre = custs[a.warm + a.n:a.warm + a.n + a.pretraffic]
        # a write elsewhere in the database starts its compactor (a database only read since the open has none)
        c[DB]["other"].insert_one({"x": 1})
        time.sleep(3)
        f0 = fx(c)
        for u in pre:
            list(col.find({"userId": u}).sort("ts", -1).limit(a.limit))
        time.sleep(a.pre_settle)
        f1 = fx(c)
        log(f"pretraffic {len(pre)}: reseals {f0['compactor']['reseals']} -> {f1['compactor']['reseals']}, "
            f"r3BoxGets {f1['customerBox'].get('r3BoxGets')}")
        c.close()
        s.p.kill()
        s.p.wait()
        for x in os.listdir("/tmp"):
            if x.startswith("fenvix-box-floor-"):
                shutil.rmtree(os.path.join("/tmp", x), ignore_errors=True)
        c = s.start()
        col = c[DB][COLL]
    try:
        for u in warm:
            list(col.find({"userId": u}).sort("ts", -1).limit(10))
        time.sleep(1.0)
        rows, answers = [], {}
        for u in meas:
            time.sleep(0.05)          # > CHAIN_GAP: each read is its own chain
            b = counters(c)
            t0 = time.perf_counter()
            docs = list(col.find({"userId": u}).sort("ts", -1).limit(a.limit))
            ms = (time.perf_counter() - t0) * 1e3
            e = counters(c)
            want = sorted(bc[u], reverse=True)[:a.limit]
            got_ids = [d["_id"] for d in docs]
            ok_plan = got_ids == want and digest(docs) == digest([doc(n, u) for n in want])
            rows.append({"u": u, "ms": ms, "gets": e["gets"] - b["gets"], "trips": e["trips"] - b["trips"], "chain": e["chain"],
                         "heads": e["heads"] - b["heads"], "lists": e["lists"] - b["lists"], "ok_plan": ok_plan,
                         "msgs": len(bc[u])})
            answers[u] = digest(docs)
        scan_path = os.path.join(a.dir, "fullscan.json")
        scan = json.load(open(scan_path)) if os.path.exists(scan_path) else None
        scan_ok = sum(scan[str(u)] == answers[u] for u in meas) if scan else 0
        ms = [r["ms"] for r in rows]
        g = [r["gets"] for r in rows]
        t = [r["trips"] for r in rows]
        ch = [r["chain"] for r in rows]
        summ = {"tag": a.tag, "n": len(rows), "delay_ms": a.delay, "flags": a.flags,
                "p50_ms": pct(ms, .5), "p99_ms": pct(ms, .99), "max_ms": max(ms),
                "gets_mean": sum(g) / len(g), "gets_p50": pct(g, .5), "gets_max": max(g),
                "trips_mean": sum(t) / len(t), "trips_p50": pct(t, .5), "trips_max": max(t),
                "chain_p50": pct(ch, .5), "chain_max": max(ch),
                "wrong_vs_plan": sum(not r["ok_plan"] for r in rows), "wrong_vs_fullscan": (len(meas) - scan_ok) if scan else "no fullscan.json",
                "answers_sha": hashlib.sha256(json.dumps(answers, sort_keys=True).encode()).hexdigest()}
        json.dump({"summary": summ, "rows": rows, "answers": answers, "fx": fx(c)}, open(a.out, "w"), indent=1, default=str)
        log(json.dumps(summ))
    finally:
        c.close()
        s.stop()


def cmd_scan(a):
    """The newest-`limit` of every customer from a full scan (no index, natural order, sorted client-side), no delay."""
    fresh_store(a.dir)
    s = Srv(a.dir, delay=0, flags=a.flags, binary=a.bin, tag="scan")
    c = s.start()
    try:
        full = {}
        for d in c[DB][COLL].find({}, hint=[("$natural", 1)]):
            full.setdefault(d["userId"], []).append(d)
        out = {str(u): digest(sorted(ds, key=lambda d: d["ts"], reverse=True)[:a.limit]) for u, ds in full.items()}
        json.dump(out, open(os.path.join(a.dir, "fullscan.json"), "w"))
        log(f"full scan: {sum(len(v) for v in full.values())} docs, {len(out)} customers")
    finally:
        c.close()
        s.stop()


def main():
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    l = sp.add_parser("load")
    l.add_argument("--dir", required=True)
    l.add_argument("--cust", type=int, default=3000)
    l.add_argument("--per1", type=int, default=40)
    l.add_argument("--rounds", type=int, default=8)
    l.add_argument("--per-r", type=int, default=3)
    l.add_argument("--settle", type=int, default=90)
    l.add_argument("--flags", default="")
    l.add_argument("--hold-last", type=int, default=0)
    r = sp.add_parser("run")
    r.add_argument("--dir", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--bin", default=BIN)
    r.add_argument("--delay", type=int, default=20)
    r.add_argument("--flags", default="")
    r.add_argument("--tag", default="run")
    r.add_argument("--warm", type=int, default=300)
    r.add_argument("--n", type=int, default=200)
    r.add_argument("--limit", type=int, default=10)
    r.add_argument("--seed", type=int, default=5)
    r.add_argument("--hold", action="store_true", help="keep L0 unabsorbed while measuring")
    r.add_argument("--trace", action="store_true")
    r.add_argument("--pretraffic", type=int, default=0)
    r.add_argument("--pre-settle", type=int, default=40)
    f = sp.add_parser("scan")
    f.add_argument("--dir", required=True)
    f.add_argument("--bin", default=BIN)
    f.add_argument("--flags", default="")
    f.add_argument("--limit", type=int, default=10)
    a = ap.parse_args()
    {"load": cmd_load, "run": cmd_run, "scan": cmd_scan}[a.cmd](a)


if __name__ == "__main__":
    main()
