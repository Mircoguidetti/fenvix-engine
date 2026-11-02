"""Memory and time of blocking sorts, before/after (session M3, fix-piccoli). Local store (--store local), 2 cores.

Usage: python3 sortmem.py load <binary> <dir> <N>          fill <dir> with N documents (~120 bytes each, as tools/sort-top1)
       python3 sortmem.py run <binary> <dir> <label> [case]  each case on a freshly started server, FIRST call (no warm-up:
                                                             an allocator can keep the memory of a first call)
For each case: time, RSS just before the call (the tenant open, the block cache filled by a scan that keeps nothing;
WARM=0 to skip it), largest RSS during the call (VmHWM after
clear_refs), and the result (number of documents, or the error code). One JSON line per case."""
import json, os, random, socket, subprocess, sys, threading, time
from pymongo import MongoClient
from pymongo.errors import OperationFailure, AutoReconnect

CASES = {
    # an index that does not give the order (filter on g, sort on k), every document a candidate
    "ix_nonorder_limit1": lambda c: c.find({"g": {"$gte": 0}}).sort("k", 1).limit(1),
    "or_limit10": lambda c: c.find({"$or": [{"g": {"$lte": 4}}, {"g": {"$gte": 5}}]}).sort("k", -1).limit(10),
    "ix_nonorder_nolimit": lambda c: c.find({"g": {"$gte": 0}}).sort("k", 1),
    "coll_nolimit": lambda c: c.find({}).sort("k", 1),
    # an index on j gives the order: the planner prefers scan + sort without a limit
    "ix_order_nolimit": lambda c: c.find({}).sort("j", 1),
    "ix_nonorder_limit1000": lambda c: c.find({"g": {"$gte": 0}}).sort("k", 1).limit(1000),
}


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def start(binary, d, log):
    port = free_port()
    p = subprocess.Popen([binary, "--listen", f"127.0.0.1:{port}", "--store", "local", "--local-dir", d, "--no-ttl", "--idle-secs", "3600"],
                         stdout=log, stderr=log)
    for _ in range(300):
        try:
            mc = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=500, socketTimeoutMS=1800000)
            mc.admin.command("ping")
            return p, mc
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server did not start")


def status(pid, key):
    for l in open(f"/proc/{pid}/status"):
        if l.startswith(key):
            return int(l.split()[1]) // 1024
    return None


def load(binary, d, n):
    os.makedirs(d, exist_ok=True)
    p, mc = start(binary, d, open(os.path.join(d, "..", "load.log"), "a"))
    c = mc["bench"]["c"]
    rnd = random.Random(7)
    batch = []
    t = time.time()
    for i in range(n):
        k = rnd.random()
        batch.append({"_id": i, "k": k, "j": k, "g": i % 10, "n": 0, "pad": "x" * 50})
        if len(batch) == 10000:
            c.insert_many(batch); batch = []
    if batch:
        c.insert_many(batch)
    c.create_index("g")
    c.create_index("j")
    print(json.dumps({"loaded": n, "secs": round(time.time() - t), "avgObjSize": mc["bench"].command("collStats", "c").get("avgObjSize")}), flush=True)
    mc["bench"].command("tenantAdmin", close=True)
    p.terminate(); p.wait(120)


def run(binary, d, label, only=None):
    out = []
    for name, q in CASES.items():
        if only and name not in only.split(","):
            continue
        log = open(os.path.join(d, "..", f"server-{label}.log"), "a")
        p, mc = start(binary, d, log)
        c = mc["bench"]["c"]
        for _ in range(120):            # opens the tenant (after a killed process: once its lease has expired)
            try:
                c.find_one({"_id": 0})
                break
            except OperationFailure as e:
                if e.code != 134:
                    raise
                time.sleep(1)
        if os.environ.get("WARM", "1") == "1":
            # the store's block cache filled first by a scan that holds nothing (no document has n = 1): what is
            # measured is the command's own memory, not the cache
            list(c.find({"n": 1}))
        time.sleep(1)
        base = status(p.pid, "VmRSS")
        open(f"/proc/{p.pid}/clear_refs", "w").write("5")   # resets VmHWM to the current RSS
        # watchdog: a process over GUARD_MB (default 5,000 MB on this 7 GB machine) is killed and the case says so
        guard, killed, done = int(os.environ.get("GUARD_MB", "5000")), [False], [False]

        def watch():
            while not done[0] and p.poll() is None:
                r = status(p.pid, "VmRSS") or 0
                if r > guard:
                    killed[0] = r
                    p.kill()
                    return
                time.sleep(0.05)
        threading.Thread(target=watch, daemon=True).start()
        t = time.perf_counter()
        res = {}
        try:
            n = 0
            for _ in q(c):
                n += 1
            res = {"docs": n}
        except OperationFailure as e:
            res = {"error": e.code}
        except (AutoReconnect, Exception) as e:
            res = {"error": f"client: {type(e).__name__}: {str(e)[:160]}", "exit": p.poll()}
        dt = time.perf_counter() - t
        done[0] = True
        peak = status(p.pid, "VmHWM") if p.poll() is None else None
        if killed[0]:
            res = {"error": f"killed by the watchdog at {killed[0]} MB (guard {guard} MB)"}
        r = {"label": label, "case": name, "ms": round(dt * 1000), "rss_before_mb": base, "peak_mb": peak, "extra_peak_mb": (peak - base) if peak else None, **res}
        print(json.dumps(r), flush=True)
        out.append(r)
        if p.poll() is None:
            p.terminate(); p.wait(120)
    return out


if __name__ == "__main__":
    if sys.argv[1] == "load":
        load(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    else:
        run(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5] if len(sys.argv) > 5 else None)
