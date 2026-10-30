"""One measured scenario: N databases, a mode, a rate per database and a mix; S3 requests counted by the object store
(MinIO's own counters, or on real S3 the engine's tenantStats), latencies by the clients.

Steps: fresh bucket, engine, N databases prefilled with 200 documents of ~1 KB, clean restart (every database closed),
load for SECS seconds; the counted window is [WARM, SECS] (the first WARM seconds hold the cold opens).

usage: run.py NAME --mode MODE --dbs N --rate R --mix read|write|half --secs S [--warm W] [--fleet mixed]
modes: before (the binary before this work, its flags as in results-density.md), immediate, adaptive, relaxed, local"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine as E

HERE = os.path.dirname(os.path.abspath(__file__))
BEFORE_BIN = os.environ.get("ROUND3_BEFORE_BIN", "/tmp/claude-0/base/mongo-server")
BASE_FLAGS = ["--idle-secs", "60", "--retention-window-secs", "604800"]
LAT_AFTER = 60


def mode_flags(mode, name):
    if mode == "before":
        return BASE_FLAGS + ["--wal-gc-mode", "bulk"], BEFORE_BIN
    f = BASE_FLAGS + ["--adaptive-group-commit", "true" if mode == "adaptive" else "false"]
    if mode == "relaxed":
        f += ["--relaxed-flush-ms", os.environ.get("ROUND3_RELAXED_MS", "5000")]
    if mode == "local":
        d = os.path.join(WORK, f"llog-{name}")
        subprocess.run(["rm", "-rf", d])
        f += ["--local-log-dir", d, "--local-log-upload-secs", os.environ.get("ROUND3_LOCAL_SECS", "30")]
    return f, None


class Counter:
    """S3 requests: MinIO's counters locally, the engine's own (tenantStats) on real S3."""

    def __init__(self, eng):
        self.eng = eng

    def snap(self):
        if REAL_S3:
            st = self.eng.stats()
            g = st.get("requests", {}) if isinstance(st, dict) else {}
            return {"PutObject": g.get("put", 0), "ListObjectsV2": g.get("list", 0), "GetObject": g.get("get", 0), "HeadObject": g.get("head", 0),
                    "DeleteObject": g.get("delete", 0), "CopyObject": g.get("copy", 0)}
        return minio_counts()


def load_cmd(eng, dbs, rate, mix, secs, out, wc, t_start, pattern="steady", extra=()):
    return [sys.executable, os.path.join(HERE, "load.py"), "--port", str(eng.port), "--dbs", ",".join(dbs), "--rate", str(rate), "--mix", mix,
            "--secs", str(secs), "--out", out, "--wc", wc, "--t-start", str(t_start), "--pattern", pattern,
            "--procs", str(max(1, min(4, len(dbs) // 25 + (1 if rate * len(dbs) > 150 else 0)))), "--lat-after", str(LAT_AFTER), "--pool", os.environ.get("ROUND3_POOL", "64"), *extra]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name")
    ap.add_argument("--mode", required=True, choices=["before", "immediate", "adaptive", "relaxed", "local"])
    ap.add_argument("--dbs", type=int, default=1)
    ap.add_argument("--rate", type=float, default=1)
    ap.add_argument("--mix", default="half")
    ap.add_argument("--secs", type=float, default=240)
    ap.add_argument("--warm", type=float, default=60)
    ap.add_argument("--fleet", default=None, choices=[None, "mixed"])
    ap.add_argument("--out-dir", default=os.path.join(ROOT, "results-round3", "real-s3" if REAL_S3 else "minio"))
    ap.add_argument("--bucket", default=None)
    a = ap.parse_args()
    global LAT_AFTER
    LAT_AFTER = a.warm
    flags, bin_ = mode_flags(a.mode, a.name)
    bucket = a.bucket or (os.environ["ROUND3_BUCKET"] if REAL_S3 else E.make_bucket(f"r3-{a.name}".lower().replace("_", "-")[:60]))
    prefix = f"{a.name.replace('_', '-')[:20]}-" if REAL_S3 else ""
    n = 100 if a.fleet == "mixed" else a.dbs
    dbs = [f"{prefix}d{i:04d}" for i in range(n)]
    eng = E.Engine(a.name, bucket, flags, bin=bin_)
    eng.start()
    t0 = time.time()
    E.prefill(eng, dbs)
    setup_stats = eng.stats()
    eng.stop()
    setup_s = time.time() - t0
    eng.start()
    cnt = Counter(eng)
    wc = "j0" if a.mode == "relaxed" else "default"
    t_start = time.time() + 3
    procs = []
    outs = []
    if a.fleet == "mixed":
        groups = [("burst", dbs[:50], 2.0, "half", "burst"), ("reads", dbs[50:70], 1.0, "read", "steady")]  # dbs[70:] stay idle
        for g, ds, r, mix, pat in groups:
            o = os.path.join(WORK, f"{a.name}-{g}.json")
            outs.append((g, o))
            procs.append(subprocess.Popen(load_cmd(eng, ds, r, mix, a.secs, o, wc, t_start, pat, ["--burst-on", "30", "--burst-every", "300"])))
    else:
        o = os.path.join(WORK, f"{a.name}-load.json")
        outs.append(("all", o))
        procs.append(subprocess.Popen(load_cmd(eng, dbs, a.rate, a.mix, a.secs, o, wc, t_start)))
    samples = []
    marks = {}
    while time.time() < t_start:
        time.sleep(0.05)
    marks["start"] = cnt.snap()
    t_warm = t_start + a.warm
    while any(p.poll() is None for p in procs):
        now = time.time()
        if "warm" not in marks and now >= t_warm:
            marks["warm"] = cnt.snap()
            marks["warm_stats"] = eng.stats()
            t_warm_real = now
        s = proc_stat(eng.pid)
        s["t"] = round(now - t_start, 1)
        samples.append(s)
        time.sleep(min(5, max(0.2, t_warm - now)) if "warm" not in marks else 5)
    t_end = time.time()
    marks["end"] = cnt.snap()
    stats_end = eng.stats()
    # everything the two engine processes of the step sent (for the spend of a real S3 run)
    tot = lambda st: (st.get("requests", {}).get("billedPutTier", 0), st.get("requests", {}).get("billedGetTier", 0)) if isinstance(st, dict) else (0, 0)
    step_put = tot(setup_stats)[0] + tot(stats_end)[0]
    step_get = tot(setup_stats)[1] + tot(stats_end)[1]
    window = t_end - t_warm_real
    d = diff(marks["warm"], marks["end"])
    put, get, free, other = classify(d)
    loads = {g: json.load(open(o)) for g, o in outs}
    client_total = sum(l["n_read"] + l["n_write"] for l in loads.values())
    client_window = client_total * window / max(1, (t_end - t_start))
    eng.stop()
    time.sleep(2)
    size, nobj = E.bucket_bytes(bucket, prefix + "d") if not REAL_S3 else E.bucket_bytes(bucket, f"tenants/{prefix}")
    res = {
        "name": a.name, "mode": a.mode, "dbs": n, "rate_per_db": a.rate, "mix": a.mix, "fleet": a.fleet, "secs": a.secs, "warm": a.warm,
        "window_s": round(window, 1), "setup_s": round(setup_s, 1), "flags": flags, "binary": bin_ or ENGINE_BIN, "real_s3": REAL_S3,
        "s3_by_api_window": d, "put_class": put, "get_class": get, "free_class": free, "other": other,
        "s3_per_client_request": round((put + get) / max(1, client_window), 3),
        "usd_requests_per_db_month": round(per_db_month(put, get, window, n), 4),
        "put_per_db_min": round(put / window * 60 / n, 2), "get_per_db_min": round(get / window * 60 / n, 2),
        "storage_bytes": size, "storage_objects": nobj, "usd_storage_per_db_month": round(size / n / 2**30 * GB_MONTH, 6),
        "client": loads, "client_requests_window": round(client_window),
        "rss_mb_max": round(max(s["rss_mb"] for s in samples), 1) if samples else None,
        "rss_mb_end": round(samples[-1]["rss_mb"], 1) if samples else None,
        "cpu_s_window": round(samples[-1]["cpu_s"] - next((s["cpu_s"] for s in samples if s["t"] >= a.warm), samples[0]["cpu_s"]), 2) if samples else None,
        "fds_max": max(s["fds"] for s in samples) if samples else None,
        "engine_stats_warm": marks.get("warm_stats"), "engine_stats_end": stats_end, "samples": samples,
        "step_requests_put_tier": step_put, "step_requests_get_tier": step_get,
        "latency_steady": {g: {k: l.get(k) for k in ("read_steady_p50_ms", "read_steady_p99_ms", "write_steady_p50_ms", "write_steady_p99_ms")} for g, l in loads.items()},
    }
    dump(os.path.join(a.out_dir, f"{a.name}.json"), res)
    lat = {g: {k: l.get(k) for k in ("read_steady_p50_ms", "read_steady_p99_ms", "write_steady_p50_ms", "write_steady_p99_ms", "errors")} for g, l in loads.items()}
    print(json.dumps({"name": a.name, "usd_db_month": res["usd_requests_per_db_month"], "put_db_min": res["put_per_db_min"],
                      "get_db_min": res["get_per_db_min"], "s3_per_req": res["s3_per_client_request"], "lat": lat}), flush=True)


if __name__ == "__main__":
    main()
