"""P-durable-changelog measurements: the same binary with --change-history off and on.

Two loads, each against a fresh server and bucket:
- `save`: W client processes, each one insert_one of a 1 KB document at a time (latency of a save: p50 and p99) for T seconds.
- `bulk`: W client processes, insert_many of 100 documents of 1 KB as fast as they can (events per second; the target
  is about 20,000 per second), with one change stream reading along in on mode.
Per run: writes per second, p50/p99 (save), the server's RSS (max of samples every 0.5 s), PUT and GET requests and the
history counters (tenantStats), the client processes' CPU (a client at about 100% of a core is the limit, RULES).
Usage: python bench.py <binary> <out.json> [secs] [workers]
"""
import json
import multiprocessing as mp
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ctl-tests"))
from lib import Server, make_bucket, free_port  # noqa: E402
from pymongo import MongoClient  # noqa: E402
from bson import SON  # noqa: E402

DOC = "x" * 900


def client_proc(uri, mode, secs, wid, q):
    c = MongoClient(uri)
    d = c.bench
    lat, n = [], 0
    t_end = time.time() + secs
    cpu0 = time.process_time()
    while time.time() < t_end:
        if mode == "save":
            t = time.perf_counter()
            d.cs.insert_one({"w": wid, "n": n, "p": DOC})
            lat.append(time.perf_counter() - t)
            n += 1
        else:
            d.cs.insert_many([{"w": wid, "n": n + i, "p": DOC} for i in range(100)], ordered=False)
            n += 100
    q.put({"n": n, "lat": lat, "cpu": time.process_time() - cpu0})


def rss(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) * 1024
    return 0


def cpu_secs(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")


def minio_pid():
    out = subprocess.run(["pgrep", "-f", "minio server"], capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None


def stats(c):
    t = c.admin.command("tenantStats")
    put = sum(t.get("putByCategory", {}).values())
    get = sum(t.get("getByCategory", {}).values())
    return {"put": put, "get": get, "hist": t["maintenance"].get("changeHistory", {})}


def run(binary, history, mode, secs, workers):
    b = make_bucket("chgbench")
    p = free_port()
    s = Server(b, f"chgbench-{history}-{mode}", idle=600, port=p, binary=binary,
               extra=["--replica-set", "rs0", "--advertise", f"localhost:{p}", "--change-history", history] + os.environ.get("BENCH_EXTRA", "").split()).start()
    try:
        c = s.client()
        c.bench.create_collection("cs")
        stream_n = [0]
        stop = threading.Event()
        if mode == "bulk" and os.environ.get("BENCH_STREAM", "auto") in (("1",) if history == "off" else ("1", "auto")):
            def reader():
                rc = s.client()
                r = rc.bench.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]))
                cid = r["cursor"]["id"]
                while not stop.is_set():
                    g = rc.bench.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 200)]))
                    stream_n[0] += len(g["cursor"]["nextBatch"])
            threading.Thread(target=reader, daemon=True).start()
        st0 = stats(c)
        mpid = minio_pid()
        cpu0 = (cpu_secs(s.proc.pid), cpu_secs(mpid) if mpid else 0)
        q = mp.Queue()
        procs = [mp.Process(target=client_proc, args=(s.uri, mode, secs, w, q)) for w in range(workers)]
        t0 = time.time()
        for pr in procs:
            pr.start()
        peak = 0
        res = []
        # (read the results while the clients run: a client cannot exit before its result is read)
        while len(res) < len(procs):
            peak = max(peak, rss(s.proc.pid))
            try:
                res.append(q.get(timeout=0.5))
            except Exception:
                pass
        for pr in procs:
            pr.join()
        wall = time.time() - t0
        cpu1 = (cpu_secs(s.proc.pid), cpu_secs(mpid) if mpid else 0)
        time.sleep(1.5)
        stop.set()
        st1 = stats(c)
        n = sum(r["n"] for r in res)
        lat = sorted(x for r in res for x in r["lat"])
        out = {"history": history, "mode": mode, "secs": secs, "workers": workers, "writes": n, "per_sec": n / wall,
               "rss_peak_mb": peak / 2**20, "client_cpu_per_proc": [r["cpu"] / wall for r in res],
               "puts": st1["put"] - st0["put"], "gets": st1["get"] - st0["get"], "stream_events": stream_n[0],
               "hist": {k: st1["hist"].get(k, 0) - st0["hist"].get(k, 0) for k in st1["hist"]}}
        if lat:
            out["p50_ms"] = lat[len(lat) // 2] * 1e3
            out["p99_ms"] = lat[int(len(lat) * 0.99)] * 1e3
        out["puts_per_write"] = out["puts"] / max(n, 1)
        out["server_cpu_us_per_write"] = (cpu1[0] - cpu0[0]) * 1e6 / max(n, 1)
        out["minio_cpu_us_per_write"] = (cpu1[1] - cpu0[1]) * 1e6 / max(n, 1)
        out["server_cores"] = (cpu1[0] - cpu0[0]) / wall
        out["minio_cores"] = (cpu1[1] - cpu0[1]) / wall
        print(json.dumps(out), flush=True)
        return out
    finally:
        s.term()


if __name__ == "__main__":
    binary, path = sys.argv[1], sys.argv[2]
    secs = int(sys.argv[3]) if len(sys.argv) > 3 else 30
    workers = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    results = []
    for mode in os.environ.get("BENCH_MODES", "save,bulk").split(","):
        for rep in range(2):
            for history in ("off", "on") if rep == 0 else ("on", "off"):
                results.append(run(binary, history, mode, secs, workers))
    json.dump(results, open(path, "w"), indent=1)
