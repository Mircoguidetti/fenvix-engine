#!/usr/bin/env python3
"""Functional checks of tenant ownership across two server processes (Step 1), using pymongo.

Two mongo-server processes share one bucket. Checks:
  1. a process that does not hold a tenant's lease gets a retryable error (code 134, label RetryableWriteError), for reads and writes;
  2. a clean close releases the lease and the other process can then open the tenant and sees every write;
  3. a read of a database that does not exist leaves no lease object behind;
  4. SIGKILL of the owner: the other process is refused until the lease expires, then takes over on the
     unclean (WAL replay) path and sees every acknowledged write;
  5. the same holder id restarting gets its own lease back at once (unclean path too);
  6. 40 threads racing to write one new tenant through both processes: exactly one process ever holds the lease.
"""
import json
import os

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
import signal
import subprocess
import sys
import threading
import time
import uuid

from pymongo import MongoClient
from pymongo.errors import OperationFailure, AutoReconnect

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release", "mongo-server")
OUT = os.path.join(ROOT, "loadtest", "out", "lease-test")
os.makedirs(OUT, exist_ok=True)
TTL = 4
results = []


def start(port, holder):
    f = open(os.path.join(OUT, f"server-{port}.log"), "ab")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--idle-secs", "600", "--holder", holder, "--lease-ttl-secs", str(TTL)] + os.environ.get("EXTRA_SERVER_FLAGS", "").split(), stdout=f, stderr=f)
    for _ in range(100):
        try:
            MongoClient(f"mongodb://127.0.0.1:{port}", serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("no start")


def client(port):
    return MongoClient(f"mongodb://127.0.0.1:{port}", serverSelectionTimeoutMS=3000, retryWrites=False, retryReads=False)


def check(name, ok, detail=""):
    results.append({"check": name, "ok": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""), flush=True)


def refused(fn):
    try:
        fn()
        return None
    except OperationFailure as e:
        return e
    except Exception as e:  # noqa
        return e


def main():
    a = start(29001, "A")
    b = start(29002, "B")
    ca, cb = client(29001), client(29002)
    tag = uuid.uuid4().hex[:8]
    try:
        # 1. refusal while the other process owns the tenant
        db = f"own1_{tag}"
        ca[db].c.insert_many([{"_id": i} for i in range(10)])
        e = refused(lambda: cb[db].c.find_one({"_id": 1}))
        check("non-owner read is refused with a retryable error", isinstance(e, OperationFailure) and e.code == 134 and e.details.get("codeName") == "TenantLeaseHeld", f"{e!r}"[:120])
        e = refused(lambda: cb[db].c.insert_one({"_id": 99}))
        check("non-owner write is refused with code 134 and the RetryableWriteError label", isinstance(e, OperationFailure) and e.code == 134 and "RetryableWriteError" in (e.details.get("errorLabels") or []), f"{e!r}"[:120])
        check("owner still reads its data", ca[db].c.count_documents({}) == 10)
        # 2. clean close releases
        ca[db].command({"tenantAdmin": 1, "close": True})
        time.sleep(0.7)  # a refusal is remembered for 500 ms so refused requests do not each cost a GET
        n = cb[db].c.count_documents({})
        check("after the owner's clean close the other process opens the tenant and sees all writes", n == 10, f"count={n}")
        e = refused(lambda: ca[db].c.count_documents({}))
        check("and now the first process is refused", isinstance(e, OperationFailure) and e.code == 134)
        cb[db].command({"tenantAdmin": 1, "close": True})
        time.sleep(0.7)
        # 3. non-existent database leaves nothing behind
        ghost = f"ghost_{tag}"
        r = ca[ghost].c.find_one({})
        data = os.environ.get("MINIO_DATA_DIR")  # MinIO running outside docker: its data directory
        cmd = ["sh", "-c", f"ls {data}/bench/tenants/{ghost} 2>&1"] if data else ["docker", "exec", "minio", "sh", "-c", f"ls /data/bench/tenants/{ghost} 2>&1"]
        listing = subprocess.run(cmd, capture_output=True, text=True).stdout
        check("reading a database that does not exist creates no objects", r is None and "No such file" in listing, listing.strip()[:80])
        # 4. SIGKILL of the owner
        db = f"crash_{tag}"
        for i in range(25):
            ca[db].c.insert_one({"_id": i, "v": i})
        a.send_signal(signal.SIGKILL)
        a.wait()
        t_kill = time.time()
        e = refused(lambda: cb[db].c.count_documents({}))
        check("right after the owner was killed the other process is still refused (lease not expired)", isinstance(e, OperationFailure) and e.code == 134, f"{e!r}"[:100])
        n = None
        t_ok = None
        while time.time() - t_kill < TTL + 10:
            try:
                n = cb[db].c.count_documents({})
                t_ok = time.time()
                break
            except OperationFailure as ex:
                if ex.code != 134:
                    raise
                time.sleep(0.2)
        check("after the lease expired the other process took the tenant over and sees all 25 acknowledged writes", n == 25, f"count={n}, took {t_ok - t_kill:.1f}s with ttl={TTL}s" if t_ok else "never")
        st = cb[db].command({"tenantAdmin": 1})
        check("the takeover woke on the unclean path (WAL replay)", st.get("uncleanWake") is True, f"uncleanWake={st.get('uncleanWake')}")
        check("and not before the expiry", t_ok is not None and t_ok - t_kill >= TTL - 1.0, f"{t_ok - t_kill:.1f}s" if t_ok else "")
        # 5. same holder restarts: gets its lease back at once
        db2 = f"restart_{tag}"
        for i in range(10):
            cb[db2].c.insert_one({"_id": i})
        b.send_signal(signal.SIGKILL)
        b.wait()
        b = start(29002, "B")
        cb = client(29002)
        t0 = time.time()
        n = cb[db2].c.count_documents({})
        check("a restarted process with the same holder id re-takes its own tenants immediately and replays the WAL", n == 10 and time.time() - t0 < TTL, f"count={n}, {time.time()-t0:.2f}s, unclean={cb[db2].command({'tenantAdmin': 1}).get('uncleanWake')}")
        # 6. race on one new tenant
        a = start(29001, "A")
        ca = client(29001)
        db3 = f"race_{tag}"
        outcomes = []
        lock = threading.Lock()

        def w(i):
            c = (ca, cb)[i % 2]
            try:
                c[db3].c.insert_one({"_id": f"x{i}"})
                r = ("ok", i % 2)
            except OperationFailure as e:
                r = (e.code, i % 2)
            with lock:
                outcomes.append(r)
        ts = [threading.Thread(target=w, args=(i,)) for i in range(40)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        winners = {side for r, side in outcomes if r == "ok"}
        losers_codes = {r for r, side in outcomes if r != "ok"}
        check("40 concurrent first writes through both processes: only one process succeeds, the other gets 134", len(winners) == 1 and losers_codes <= {134}, f"winning side={winners}, other codes={losers_codes}, ok={sum(1 for r,_ in outcomes if r=='ok')}")
        # Lease log: exactly one holder acquired the race tenant
        holders = set()
        for port in (29001, 29002):
            for line in open(os.path.join(OUT, f"server-{port}.log"), errors="replace"):
                if f"tenant={db3} " in line and line.startswith("LEASE acquired"):
                    holders.add(line.split("holder=")[1].split()[0])
        check("the lease log shows a single holder for the raced tenant", len(holders) == 1, str(holders))
    finally:
        for p in (a, b):
            try:
                p.send_signal(signal.SIGTERM)
                p.wait(20)
            except Exception:
                pass
    json.dump(results, open(os.path.join(OUT, "results.json"), "w"), indent=1)
    ok = all(r["ok"] for r in results)
    print(f"{sum(r['ok'] for r in results)}/{len(results)} checks passed")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
