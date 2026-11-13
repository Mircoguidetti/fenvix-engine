"""P-fix-slow-store (2026-11-13): the defects that a slow or failing store turns into outages.

Item 1, the clean close on a slow store. On SIGTERM every database's close waited on the store step by step (a memtable
flush with the compactor running, its absorption, the compactor's stop, the final flush and SlateDB's close, the marker,
the lease); only the process-wide `--shutdown-timeout-secs` (25 s) bounded it, and the process then exited with leases
and usage counters not written. MEASURED on integration-5-stack 359f09b: 8.7 s at 200 ms per store request, 25.0 s at
1 s per request. Now one deadline (`--close-budget-secs`, 8 s) is shared by every step of every close; what does not fit
is cut where cutting is safe (an unclean close: no marker, the WAL replayed by the next owner).

- test_sigterm_on_a_slow_store_ends_within_the_budget: every store request delayed 1 s, SIGTERM. The process must exit
  within the budget plus the bounded steps after it (14 s); on the base it waits the whole 25 s.
- test_sigterm_with_the_store_not_answering_ends_within_the_budget: every PUT held without an answer, SIGTERM: the same
  bound.
Both then restart on a healthy store and read back every acknowledged insert and update (rule 8), and require no
incomplete multipart upload under the databases once their next owner has opened them.

The proxy is tools/lease-stall/pathproxy.py (a forwarder that delays or holds requests): deterministic, no timing luck.
"""
import os
import signal
import sys
import threading
import time

import pytest
from pymongo import MongoClient

from lib import Server, make_bucket, MINIO, s3

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lease-stall"))
from pathproxy import PathProxy  # noqa: E402

DBS = ["slowa", "slowb", "slowc"]
BUDGET_S = 8           # --close-budget-secs (the default)
EXIT_BOUND_S = 14      # the budget, the fallback close's 2 s grace, then 1 s for each of the steps after the closes


def proxy():
    host, port = MINIO.split("//")[1].split(":")
    return PathProxy((host, int(port)))


def incomplete_uploads(bucket, prefix="tenants/"):
    out, c, kw = [], s3(), {"Bucket": bucket}
    while True:
        r = c.list_multipart_uploads(**kw)
        out += [(u["Key"], u["UploadId"]) for u in r.get("Uploads", []) if u["Key"].startswith(prefix)]
        if not r.get("IsTruncated"):
            return out
        kw["KeyMarker"], kw["UploadIdMarker"] = r["NextKeyMarker"], r["NextUploadIdMarker"]


def load(srv):
    """Inserts and $inc updates on three databases, every one acknowledged: returns what must be read back."""
    c = srv.client()
    acked = {}
    for d in DBS:
        ids = []
        for i in range(30):
            docs = [{"_id": i * 100 + k, "v": "x" * 400, "n": 0} for k in range(100)]
            c[d].t.insert_many(docs)
            ids += [x["_id"] for x in docs]
        for k in range(20):
            c[d].t.update_one({"_id": k}, {"$inc": {"n": 1}})
        acked[d] = ids
    return acked


def writers(srv, stop, acked):
    """Writes during the close, each acknowledged id recorded (the close must keep them too)."""
    def run(d):
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=2000, socketTimeoutMS=4000, retryWrites=False)
        i = 1_000_000
        while not stop.is_set():
            try:
                c[d].t.insert_one({"_id": i, "late": True})
                acked[d].append(i)
            except Exception:
                pass
            i += 1
    ths = [threading.Thread(target=run, args=(d,), daemon=True) for d in DBS]
    for t in ths:
        t.start()
    return ths


def read_back(bucket, acked, tag):
    srv = Server(bucket, tag, idle=600).start()
    try:
        c = srv.client()
        deadline = time.time() + 60   # the stopped process's leases (6 s) may still be running out
        for d in DBS:
            while True:
                try:
                    have = {x["_id"] for x in c[d].t.find({}, {"_id": 1})}
                    break
                except Exception:
                    if time.time() > deadline:
                        raise
                    time.sleep(1)
            missing = [i for i in acked[d] if i not in have]
            assert not missing, f"{d}: {len(missing)} acknowledged inserts missing, e.g. {missing[:5]}"
            n = [x["n"] for x in c[d].t.find({"_id": {"$lt": 20}}, {"n": 1})]
            assert len(n) == 20 and all(v == 1 for v in n), f"{d}: acknowledged updates lost: {n}"
            print(f"{d}: {len(acked[d])} acknowledged inserts read back, 20 updates")
        # every database is opened by its next writer (one write each): then no upload of the stopped process may be left
        # (item 2 aborts, at a writer's open, those of an older writer epoch)
        for d in DBS:
            c[d].t.insert_one({"_id": "next-owner"})
        t0 = time.time()
        while incomplete_uploads(bucket) and time.time() - t0 < 20:
            time.sleep(1)
        left = incomplete_uploads(bucket)
        assert not left, f"incomplete uploads left: {left[:3]}"
    finally:
        srv.kill9()


def sigterm_case(slow_rule, tag):
    px = proxy()
    bucket = make_bucket(tag)
    srv = Server(bucket, tag, idle=600, endpoint=px.endpoint).start()
    try:
        acked = load(srv)
        time.sleep(2)
        stop = threading.Event()
        ths = writers(srv, stop, acked)
        time.sleep(1)
        slow_rule(px)
        t0 = time.time()
        srv.proc.send_signal(signal.SIGTERM)
        try:
            srv.proc.wait(60)
            took = time.time() - t0
        except Exception:
            took = None
        stop.set()
        for t in ths:
            t.join(10)
        print(f"SIGTERM to exit: {took if took is None else round(took, 1)} s (bound {EXIT_BOUND_S} s)")
        print("\n".join(l for l in srv.log_text().splitlines() if "TIMING shutdown" in l or "left behind" in l))
    finally:
        px.release()
        srv.kill9()
        px.close()
    assert took is not None and took <= EXIT_BOUND_S, f"the clean close took {took} s on a slow store (bound {EXIT_BOUND_S} s)"
    read_back(bucket, acked, tag + "2")


def test_sigterm_on_a_slow_store_ends_within_the_budget():
    sigterm_case(lambda px: px.slow(("GET", "PUT", "POST", "DELETE", "HEAD"), "/", 1.0), "slowclose")


def test_sigterm_with_the_store_not_answering_ends_within_the_budget():
    sigterm_case(lambda px: px.hold("PUT", "/"), "deadclose")


# ---- item 2: uploads of older writers, aborted whatever their age, never a live writer's

CRASH_DB = "crashloop"


def run_uploads(bucket, db=CRASH_DB):
    """Incomplete run uploads of `db` with the writer epoch their key names: [(key, epoch)]."""
    out = []
    for k, _ in incomplete_uploads(bucket, f"tenants/{db}/fx/run/"):
        e = k.rsplit("/", 1)[-1].split("-")
        out.append((k, int(e[1]) if len(e) == 3 else 0))
    return out


def write_until(srv, cond, start, timeout=60):
    """Insert batches into CRASH_DB until cond() (retrying while the previous owner's lease runs out); returns the
    acknowledged ids."""
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000)
    acked, i, t0 = [], start, time.time()
    while time.time() - t0 < timeout:
        docs = [{"_id": i + k, "v": "y" * 300} for k in range(50)]
        try:
            c[CRASH_DB].t.insert_many(docs)
            acked += [d["_id"] for d in docs]
            i += 50
        except Exception:
            time.sleep(0.5)
            continue
        if cond():
            return acked
    raise AssertionError("condition not reached")


def test_crash_loop_leaves_no_older_writers_upload():
    px = proxy()
    bucket = make_bucket("crashloop")
    acked, epochs = [], []
    try:
        # three processes in a row, each killed (-9) while its run uploads are held open by the proxy
        for n in range(3):
            srv = Server(bucket, f"crash{n}", idle=600, endpoint=px.endpoint).start()
            before = {k for k, _ in run_uploads(bucket)}
            px.hold("PUT", "/fx/run/")
            acked += write_until(srv, lambda: any(k not in before for k, _ in run_uploads(bucket)), 10_000 * (n + 1))
            mine = [(k, e) for k, e in run_uploads(bucket) if k not in before]
            epochs.append(max(e for _, e in mine))
            srv.kill9()
            px.release()
            print(f"process {n}: killed with {len(mine)} run uploads open (writer epoch {epochs[-1]}); "
                  f"{len(run_uploads(bucket))} open in all")
        # the next owner opens the database: every upload of an older writer must go, whatever its age (minutes here,
        # the startup sweep's age rule is an hour)
        live = Server(bucket, "crashlive", idle=600, endpoint=px.endpoint).start()
        acked += write_until(live, lambda: True, 90_000)
        t0 = time.time()
        while time.time() - t0 < 15:
            left = run_uploads(bucket)
            if not left:
                break
            time.sleep(0.5)
        left = run_uploads(bucket)
        print(f"after the next owner's open: {len(left)} run uploads open: {left[:3]}")
        assert not left, f"uploads of older writers left after the next owner opened: {left[:3]}"

        # never a live writer's upload: hold the live writer's run uploads, then start another process on the bucket
        # (its startup sweep lists everything); the live upload must stay and complete
        px.hold("PUT", "/fx/run/")
        before = set()
        acked += write_until(live, lambda: bool(run_uploads(bucket)), 100_000)
        held = run_uploads(bucket)
        other = Server(bucket, "crashother", idle=600).start()
        t0 = time.time()
        while "unfinished multipart uploads" not in other.log_text() and time.time() - t0 < 20:
            time.sleep(0.2)
        assert "unfinished multipart uploads" in other.log_text(), "the other process's startup sweep did not run"
        still = run_uploads(bucket)
        print(f"live writer's uploads {held} -> after another process's startup sweep {still}")
        assert set(held) <= set(still), f"a live writer's upload was aborted: {set(held) - set(still)}"
        other.kill9()
        px.release()
        # the live writer goes on: its uploads complete, every acknowledged write reads back
        c = live.client()
        have = {x["_id"] for x in c[CRASH_DB].t.find({}, {"_id": 1})}
        missing = [i for i in acked if i not in have]
        assert not missing, f"{len(missing)} acknowledged inserts missing, e.g. {missing[:5]}"
        t0 = time.time()
        while run_uploads(bucket) and time.time() - t0 < 30:
            time.sleep(0.5)
        assert not run_uploads(bucket), f"the live writer's uploads did not complete: {run_uploads(bucket)}"
        live.kill9()
    finally:
        px.release()
        px.close()
