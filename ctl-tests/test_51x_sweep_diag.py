"""Review of c2-fix-clone-uploads: try to break the stale-read fix (the gate on the old pass, the close order).

Each test writes rounds of updates (every document's last value is in the round's last batches, which the close flushes),
then attacks the close, and checks that every acknowledged value reads back: never an older value, never a missing one.

1. kill -9 at a random moment of a close (before, during or after the flush, the bounded absorption, the stop), restart.
2. a reopen (writes) while the old compaction pass runs on the closed database.
3. a close that hits the 10 s absorption bound (a slow store: every chunk of the store's answers delayed).
4. a takeover in the middle of a close: the owner frozen during its close, the other process takes the database over,
   the old owner resumed.
"""
import os
import random
import signal
import subprocess
import sys
import threading
import time

import pytest
from pymongo import MongoClient, UpdateOne
from pymongo.errors import AutoReconnect, OperationFailure

from lib import Server, s3, wait_until, free_port, MINIO

ENV = {"FENVIX_SEALED_RUNS": "7", "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
       "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}
SMALL_L0 = ["--l0-sst-bytes", "262144"]
ROUNDS = int(os.environ.get("R51_ROUNDS", "8"))


def bucket(tag):
    # not "ctl-": the integration-3 rerun script deletes every ctl- bucket between files
    name = f"rv51-{tag}-{os.urandom(4).hex()}"
    s3().create_bucket(Bucket=name)
    return name


REFUSED_RETRIED = [0, 0]


def write_round(col, rnd, n, expect, batches=8):
    for b in range(batches):
        ops = []
        for i in range(rnd * n, rnd * n + n):
            if (i + b) % 3 == 0:
                v = rnd * 100 + b
                ops.append(UpdateOne({"_id": i}, {"$set": {"v": v, "pad": "x" * 200}}, upsert=True))
                expect[i] = v
        # review of c2-fix-outage-loss: a write refused with DatabaseReopened (code 134) applied nothing; send it again
        for attempt in range(20):
            try:
                col.bulk_write(ops)
                REFUSED_RETRIED[1] += 1
                break
            except OperationFailure as e:
                if e.code != 134:
                    raise
                REFUSED_RETRIED[0] += 1
                time.sleep(0.5)
        else:
            raise AssertionError("a batch was refused 20 times")


def wrong(col, expect):
    got = {d["_id"]: d.get("v") for d in col.find({}, {"v": 1})}
    return [(i, v, got.get(i)) for i, v in expect.items() if got.get(i) != v]


def close_async(srv, db):
    def f():
        try:
            MongoClient(srv.uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=60000)[db].command("tenantAdmin", close=True)
        except Exception:
            pass
    t = threading.Thread(target=f, daemon=True)
    t.start()
    return t


def fenvix(c):
    return c.admin.command("serverStatus")["fenvix"]


class Relay:
    """A byte relay to MinIO that delays every chunk (a slow store)."""

    def __init__(self, delay_ms):
        self.port = free_port()
        mport = int(MINIO.rsplit(":", 1)[1])
        here = os.path.dirname(os.path.abspath(__file__))
        self.p = subprocess.Popen([sys.executable, os.path.join(here, "..", "research", "integration-3", "tcprelay.py"), str(self.port), str(mport), str(delay_ms)])
        time.sleep(0.5)

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def stop(self):
        self.p.kill()




def test_sweep_diag():
    """The report says aborting a live upload loses nothing (a version lists a run only after its upload completed). When
    the owner has a run upload open, its store is frozen (its relay stopped), a second process on the same bucket starts
    with a 1 s bound and its sweep aborts that live upload, then the owner's store resumes. Every acknowledged value
    reads back, and every run a stored version lists exists."""
    import json
    b = bucket("sweep")
    relay = Relay(0)
    env = dict(ENV, FENVIX_SEALED_TPIN_MS="20000")
    p1 = Server(b, "r51-sweep1", idle=600, endpoint=relay.endpoint, extra=SMALL_L0, env=env).start()
    expect, stop, aborted, live = {}, threading.Event(), [], []

    def runs_open():
        r = s3().list_multipart_uploads(Bucket=b)
        return [(u["Key"], u["UploadId"]) for u in r.get("Uploads", []) if u["Key"].startswith("tenants/g/fx/run/")]

    def sweeper():
        # armed once the compactor runs: its own uploads (absorptions, merges, reseals) are the ones aborted
        st = MongoClient(p1.uri, serverSelectionTimeoutMS=5000)
        while not stop.is_set() and fenvix(st)["compactor"]["started"] == 0:
            time.sleep(0.05)
        while not stop.is_set() and len(aborted) < 5:
            u = runs_open()
            if not u:
                time.sleep(0.003)
                continue
            relay.p.send_signal(signal.SIGSTOP)
            try:
                live.extend(u)
                time.sleep(1.5)
                p2 = Server(b, "r51-sweep2", idle=600, extra=["--abandoned-upload-secs", os.environ.get("R51_SWEEP_SECS", "1")], env=env).start()
                try:
                    wait_until(lambda: "unfinished multipart uploads under" in p2.log_text(), 20, what="the sweep")
                    gone = [x for x in u if x not in runs_open()]
                    aborted.extend(gone)
                finally:
                    p2.term()
            finally:
                relay.p.send_signal(signal.SIGCONT)
            time.sleep(1)

    th = threading.Thread(target=sweeper, daemon=True)
    try:
        th.start()
        col = p1.client()["g"]["c"]
        for rnd in range(14):
            write_round(col, rnd, 3000, expect)
            if len(aborted) >= 5:
                break
        stop.set()
        th.join(60)
        time.sleep(5)
        w = wrong(col, expect)
        st = fenvix(p1.client())["compactor"]
        print(f"live run uploads aborted by the other process's sweep: {len(aborted)} of {len(live)} seen; wrong {len(w)}; compactor {st}")
        p1.term()
        p1.proc = None
        p1.start()
        w2 = wrong(p1.client()["g"]["c"], expect)
        missing = []
        keys = [o["Key"] for pg in s3().get_paginator("list_objects_v2").paginate(Bucket=b, Prefix="tenants/g/fx/manifest/") for o in pg.get("Contents", [])]
        runs = {o["Key"] for pg in s3().get_paginator("list_objects_v2").paginate(Bucket=b, Prefix="tenants/g/fx/run/") for o in pg.get("Contents", [])}
        for k in keys:
            v = json.loads(s3().get_object(Bucket=b, Key=k)["Body"].read())
            for p in [g["sealed"]["path"] for g in v.get("groups", []) if g.get("sealed")] + [d["path"] for d in v.get("delta", [])]:
                if p not in runs:
                    missing.append((k, p))
        print(f"after a restart: wrong {len(w2)}; versions {len(keys)}, runs listed and missing {len(missing)}; aborted keys {[k for k, _ in aborted][:3]}")
        # review of c2-fix-outage-loss: are the missing documents found by _id, and did p1 reopen the database?
        c1 = p1.client()["g"]["c"]
        gone = [i for i, v, got in w2 if got is None]
        stale = [x for x in w2 if x[2] is not None]
        by_id = [i for i in gone if c1.find_one({"_id": i}) is not None]
        by_id_ok = [i for i in gone if (c1.find_one({"_id": i}) or {}).get("v") == expect[i]]
        lt = p1.log_text()
        print(f"DIAG batches refused and sent again {REFUSED_RETRIED[0]} (batches acknowledged {REFUSED_RETRIED[1]}); missing from the scan {len(gone)}, stale values {len(stale)}; of the missing, found by _id {len(by_id)} (with the acknowledged value {len(by_id_ok)}); "
              f"p1 log: LEASE lost {lt.count('LEASE lost')}, LEASE invalid {lt.count('LEASE invalid')}, LEASE acquired {lt.count('LEASE acquired')}, writer_open {lt.count('LEASE writer_open')}; "
              f"reopenedWritesRefused {c1.database.client.admin.command('tenantStats')['maintenance'].get('reopenedWritesRefused')}; first missing {sorted(gone)[:5]}", flush=True)
        if w2:
            p1.term()
            p3 = Server(b, "r51-sweep3", idle=600, env={"FENVIX_SEALED_RUNS": "off"}).start()
            try:
                w3 = wrong(p3.client()["g"]["c"], expect)
                print(f"read with sealed runs off (SlateDB only): wrong {len(w3)} {w3[:3]}")
            finally:
                p3.term()
        assert aborted or os.environ.get("R51_SWEEP_SECS"), "no live upload was aborted: the test did not exercise the window"
        assert not w and not w2, f"stale or missing answers: {(w + w2)[:5]}"
        assert not missing, f"versions list runs that do not exist: {missing[:3]}"
    finally:
        stop.set()
        try:
            relay.p.send_signal(signal.SIGCONT)
        except Exception:
            pass
        p1.term()
        relay.stop()


