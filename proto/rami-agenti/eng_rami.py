"""M22: N rami del nostro motore (fork `clone` di ctl/fork.rs) su un solo mongo-server, tutti fermi ad aspettare.

Un padre da circa 1 GB (documenti da circa 1 KB), poi N fork. Per ogni ramo: tempo del fork, prima lettura, il lavoro di
B15 (100 letture, 20 scritture), 30 s di pausa, RAM del processo (PSS) divisa per N, prima lettura dopo la pausa con il
ramo ancora aperto e con il ramo chiuso (riapertura dal magazzino locale).

Uso: python3 eng_rami.py --docs 1000000 --n 50 --out risultati/eng-1g.json
"""
import argparse
import json
import os
import random
import signal
import statistics
import subprocess
import sys
import time
import urllib.request

from pymongo import MongoClient

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "..", "target", "release", "mongo-server")
TOKEN = "m22-token-0123456789abcdef"


def rollup(pid):
    out = {}
    for line in open(f"/proc/{pid}/smaps_rollup"):
        p = line.split()
        if len(p) >= 3 and p[0].endswith(":") and p[2] == "kB":
            out[p[0][:-1]] = int(p[1])
    return out


def stats(xs):
    xs = sorted(xs)
    n = len(xs)
    return {"n": n, "min": xs[0], "mediana": statistics.median(xs), "p90": xs[min(n - 1, int(n * 0.9))], "max": xs[-1],
            "media": statistics.fmean(xs)}


def admin(port, method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.status, json.loads(r.read() or b"null")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=1_000_000)
    ap.add_argument("--size", type=int, default=1000, help="byte di testo per documento")
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--pausa", type=float, default=30)
    ap.add_argument("--dir", default="/home/claude/engstore")
    ap.add_argument("--riusa", action="store_true", help="il padre c'è già nella cartella")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    port, aport = 27100, 27101
    os.makedirs(a.dir, exist_ok=True)
    log = open(os.path.join(HERE, "risultati", "eng-server.log"), "ab")
    cmd = [BIN, "--listen", f"127.0.0.1:{port}", "--admin-listen", f"127.0.0.1:{aport}", "--store", "local", "--local-dir", a.dir,
           "--idle-secs", "3600", "--no-ttl", "--holder", "m22"]
    srv = subprocess.Popen(cmd, stdout=log, stderr=log, env=dict(os.environ, MONGO_ADMIN_TOKEN=TOKEN))
    res = {"docs": a.docs, "size": a.size, "n": a.n, "pausa_s": a.pausa, "cmd": " ".join(cmd[1:])}
    try:
        for _ in range(300):
            try:
                admin(aport, "GET", "/v1/health")
                break
            except Exception:
                time.sleep(0.1)
        c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=20000)
        pad = "x" * a.size
        if not a.riusa:
            t0 = time.time()
            coll = c["padre"]["docs"]
            B = 1000
            for s in range(0, a.docs, B):
                coll.insert_many([{"_id": i, "grp": i % 100, "saldo": 0, "testo": pad} for i in range(s, min(s + B, a.docs))], ordered=False)
            res["carico_padre_s"] = time.time() - t0
        c["padre"].command("tenantAdmin", close=True)
        time.sleep(2)
        res["padre_byte_su_disco"] = int(subprocess.run(["du", "-sb", a.dir], capture_output=True, text=True).stdout.split()[0])
        random.seed(22)
        time.sleep(a.pausa)               # il processo si assesta con il padre chiuso
        base = rollup(srv.pid)
        res["processo_base_pss_mb"] = base["Pss"] / 1024
        fork_ms, first_ms = [], []
        for i in range(a.n):
            t0 = time.perf_counter()
            st, r = admin(aport, "POST", "/v1/databases/padre/fork", {"target": f"ramo{i:03d}", "mode": "clone"})
            assert st == 201, r
            t1 = time.perf_counter()
            assert c[f"ramo{i:03d}"]["docs"].find_one({"_id": random.randrange(a.docs)}) is not None
            t2 = time.perf_counter()
            fork_ms.append((t1 - t0) * 1e3)
            first_ms.append((t2 - t0) * 1e3)
        res["fork_ms"] = stats(fork_ms)
        res["fork_piu_prima_lettura_ms"] = stats(first_ms)
        for i in range(a.n):
            d = c[f"ramo{i:03d}"]["docs"]
            for _ in range(100):
                d.find_one({"_id": random.randrange(a.docs)})
            for _ in range(20):
                d.update_one({"_id": random.randrange(a.docs)}, {"$inc": {"saldo": 1}})
        time.sleep(a.pausa)
        m = rollup(srv.pid)
        res["aperti_processo_pss_mb"] = m["Pss"] / 1024
        res["aperti_processo_rss_mb"] = m["Rss"] / 1024
        res["aperti_pss_per_ramo_mb"] = (m["Pss"] - base["Pss"]) / 1024 / a.n

        def first_reads():
            out = []
            for i in range(a.n):
                t0 = time.perf_counter()
                c[f"ramo{i:03d}"]["docs"].find_one({"_id": random.randrange(a.docs)})
                out.append((time.perf_counter() - t0) * 1e3)
            return out
        res["aperti_prima_lettura_dopo_pausa_ms"] = stats(first_reads())
        # rami chiusi: come quando scatta il timer di inattività (scarico e chiusura)
        for i in range(a.n):
            c[f"ramo{i:03d}"].command("tenantAdmin", close=True)
        time.sleep(a.pausa)
        m = rollup(srv.pid)
        res["chiusi_processo_pss_mb"] = m["Pss"] / 1024
        res["chiusi_pss_per_ramo_mb"] = (m["Pss"] - base["Pss"]) / 1024 / a.n
        res["aperti_meno_chiusi_per_ramo_mb"] = (res["aperti_processo_pss_mb"] - res["chiusi_processo_pss_mb"]) / a.n
        subprocess.run(["sync"])
        open("/proc/sys/vm/drop_caches", "w").write("3\n")
        res["chiusi_risveglio_ms_cache_fredda"] = stats(first_reads())
        res["spazio_rami_byte"] = int(subprocess.run(["du", "-sb", a.dir], capture_output=True, text=True).stdout.split()[0]) - res["padre_byte_su_disco"]
        # pulizia: si buttano i rami (erase), il padre resta per un'altra corsa
        er = []
        for i in range(a.n):
            t0 = time.perf_counter()
            st, _ = admin(aport, "DELETE", f"/v1/databases/ramo{i:03d}")
            er.append((time.perf_counter() - t0) * 1e3)
        res["butta_ramo_ms"] = stats(er)
    finally:
        srv.send_signal(signal.SIGTERM)
        srv.wait(60)
    json.dump(res, open(a.out, "w"), indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
