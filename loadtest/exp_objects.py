#!/usr/bin/env python3
"""One hot tenant on MinIO for a fixed time: throughput and latency, the number of objects of the tenant over time
(wal, compacted, manifest, compactions), CPU of the server and of MinIO, and the server's request counters.

    exp_objects.py <label> <binary> <seconds> [--conns 8] [--read-pct 0] [--keep] [-- extra server args]
Writes results/objects/<label>.json. MINIO_DATA_DIR (default /tmp/minio-data) is where MinIO keeps the bucket.
"""
import json, os, subprocess, sys, threading, time
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOADGEN = os.environ.get("LOADGEN", os.path.join(ROOT, "target", "release", "loadgen"))
DATA = os.environ.get("MINIO_DATA_DIR", "/tmp/minio-data") + "/bench/tenants"
EP = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
OUT = os.path.join(ROOT, "loadtest", "out", "objects"); RES = os.path.join(ROOT, "loadtest", "results", "objects")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
PORT = 28111
DIRS = ["wal", "compacted", "manifest", "compactions"]


def cpu_s(pid):
    try:
        st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(st[11]) + int(st[12])) / os.sysconf("SC_CLK_TCK")
    except OSError:
        return None


def count(d):
    try:
        return len(os.listdir(d))
    except OSError:
        return 0


def s3_requests():
    """MinIO's own count of S3 requests by API (needs MINIO_PROMETHEUS_AUTH_TYPE=public); {} when unreachable."""
    import urllib.request, re
    try:
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        txt = op.open(EP + "/minio/v2/metrics/cluster", timeout=10).read().decode()
    except Exception:
        return {}
    out = {}
    for m in re.finditer(r'^minio_s3_requests_total\{[^}]*api="([^"]+)"[^}]*\} ([0-9.e+]+)', txt, re.M):
        out[m.group(1)] = out.get(m.group(1), 0) + float(m.group(2))
    return out


def main():
    a = sys.argv[1:]
    extra = a[a.index("--") + 1:] if "--" in a else []
    a = a[: a.index("--")] if "--" in a else a
    label, binary, secs = a[0], a[1], float(a[2])
    opt = lambda k, d: a[a.index(k) + 1] if k in a else d
    conns, read_pct = int(opt("--conns", 8)), int(opt("--read-pct", 0))
    rate = float(opt("--rate", 0))
    prefix = f"ob{label[:10].replace('-', '')}{int(time.time()) % 100000}x"
    name = f"{prefix}000000"
    p = subprocess.Popen([binary, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "3600", "--holder", "O", "--lease-ttl-secs", "30", "--endpoint", EP] + extra,
                         stdout=open(os.path.join(OUT, f"{label}.log"), "wb"), stderr=subprocess.STDOUT)
    uri = f"mongodb://127.0.0.1:{PORT}"
    for _ in range(300):
        try:
            MongoClient(uri, serverSelectionTimeoutMS=200).admin.command("ping"); break
        except Exception:
            time.sleep(0.05)
    subprocess.run([LOADGEN, "load", "--uris", uri, "--prefix", prefix, "--tenants", "1", "--first", "0", "--out", "/tmp/ob-load.json", "--close-after", "false"], check=True, stdout=subprocess.DEVNULL)
    minio = subprocess.run(["pgrep", "-x", "minio"], capture_output=True, text=True).stdout.split()
    minio_pid = int(minio[0]) if minio else None
    c = MongoClient(uri)
    s3a = s3_requests()
    s0 = c.admin.command("tenantStats"); cpu0 = (cpu_s(p.pid), cpu_s(minio_pid) if minio_pid else None); t0 = time.time()
    samples, stop = [], threading.Event()

    def sampler():
        while not stop.is_set():
            samples.append([round(time.time() - t0)] + [count(f"{DATA}/{name}/{d}") for d in DIRS]); stop.wait(10)
    th = threading.Thread(target=sampler); th.start()
    subprocess.run([LOADGEN, "hot", "--uris", uri, "--prefix", prefix, "--tenant", "0", "--connections", str(conns), "--read-pct", str(read_pct),
                    "--duration-secs", str(int(secs)), "--warmup-secs", "0", "--rate", str(rate), "--out", f"/tmp/ob-{label}.json"], check=True, stdout=subprocess.DEVNULL)
    stop.set(); th.join()
    wall = time.time() - t0
    s1 = c.admin.command("tenantStats"); cpu1 = (cpu_s(p.pid), cpu_s(minio_pid) if minio_pid else None)
    s3b = s3_requests()
    final = [count(f"{DATA}/{name}/{d}") for d in DIRS]
    r = json.load(open(f"/tmp/ob-{label}.json"))
    dl = lambda sec, k: s1[sec][k] - s0[sec][k]
    res = {"label": label, "seconds": secs, "connections": conns, "read_pct": read_pct, "extra_args": extra, "ops": r["ops"], "ops_per_sec": r["ops_per_sec"], "errors": r["errors"],
           "p50_ms": r["p50_ms"], "p99_ms": r["p99_ms"], "objects_final": dict(zip(DIRS, final)),
           "objects_max": {d: max(s[i + 1] for s in samples) for i, d in enumerate(DIRS)}, "samples": samples,
           "server_cpu_s": round(cpu1[0] - cpu0[0], 1), "minio_cpu_s": round(cpu1[1] - cpu0[1], 1) if minio_pid else None, "wall_s": round(wall, 1),
           "offered_ops_per_sec": rate, "s3_requests_minio": {k: round(s3b.get(k, 0) - s3a.get(k, 0)) for k in sorted(set(s3b) | set(s3a)) if s3b.get(k, 0) - s3a.get(k, 0)},
           "requests": {k: dl("requests", k) for k in ("put", "get", "list", "head", "delete")},
           "walGc": {k: s1.get("walGc", {}).get(k, 0) - s0.get("walGc", {}).get(k, 0) for k in ("passes", "deleted", "listUs", "deleteUs", "totalUs")},
           "sstGc": {k: s1.get("sstGc", {}).get(k, 0) - s0.get("sstGc", {}).get(k, 0) for k in ("passes", "deletedSsts", "deletedManifests")}}
    p.terminate()
    try: p.wait(90)
    except Exception: p.kill()
    json.dump(res, open(os.path.join(RES, f"{label}.json"), "w"), indent=1)
    if "--keep" not in a:
        subprocess.run(["rm", "-rf", f"{DATA}/{name}"])
    print({k: v for k, v in res.items() if k not in ("samples", "extra_args")}, flush=True)


main()
