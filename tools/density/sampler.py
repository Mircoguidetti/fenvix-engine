"""Every INTERVAL seconds: engine RSS / CPU / threads / fds from /proc and MinIO's S3 request counters (Prometheus
/minio/metrics/v3/api/requests, minio_api_requests_total{type="s3"} by API name: what the S3 server received, i.e. what
AWS would bill). The engine's own counters (`tenantStats`) cannot be read: with --auth it needs the `anyAction`
privilege, which no user can be given (root is refused, the `__system` role cannot be granted). Open databases are
counted from the engine log (LEASE acquired / release lines) by analyze.py. One JSON line per sample.
usage: sampler.py ENGINE_PID OUT.jsonl [INTERVAL]   (stops when OUT.jsonl.stop exists or the engine exits)"""
import json
import os
import re
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa

CLK = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")


def proc(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    ut, st = int(parts[11]), int(parts[12])
    threads = int(parts[17])
    with open(f"/proc/{pid}/statm") as f:
        rss = int(f.read().split()[1]) * PAGE
    fds = len(os.listdir(f"/proc/{pid}/fd"))
    return {"cpu_s": (ut + st) / CLK, "rss_mb": rss / 2**20, "threads": threads, "fds": fds}


RE = re.compile(r'^minio_api_requests_total\{([^}]*)\}\s+([0-9.e+]+)', re.M)


def minio():
    try:
        with urllib.request.urlopen("http://127.0.0.1:9000/minio/metrics/v3/api/requests", timeout=10) as r:
            txt = r.read().decode()
    except Exception as e:
        return {"error": str(e)}
    out = {}
    for labels, val in RE.findall(txt):
        m = re.search(r'name="([^"]+)"', labels)
        if m and 'type="s3"' in labels:
            out[m.group(1)] = out.get(m.group(1), 0) + float(val)
    return out


def main():
    pid, path = int(sys.argv[1]), sys.argv[2]
    iv = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0
    stop = path + ".stop"
    nxt = time.time()
    with open(path, "a") as f:
        while not os.path.exists(stop):
            row = {"ts": time.time()}
            try:
                row.update(proc(pid))
            except FileNotFoundError:
                row["engine_gone"] = True
                f.write(json.dumps(row) + "\n")
                break
            row["minio"] = minio()
            f.write(json.dumps(row) + "\n")
            f.flush()
            nxt += iv
            time.sleep(max(0, nxt - time.time()))


if __name__ == "__main__":
    main()
