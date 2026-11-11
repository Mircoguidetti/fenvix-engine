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


def write_round(col, rnd, n, expect, batches=8):
    for b in range(batches):
        ops = []
        for i in range(rnd * n, rnd * n + n):
            if (i + b) % 3 == 0:
                v = rnd * 100 + b
                ops.append(UpdateOne({"_id": i}, {"$set": {"v": v, "pad": "x" * 200}}, upsert=True))
                expect[i] = v
        col.bulk_write(ops)


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


def test_kill_during_a_close_then_no_stale_answer():
    b = bucket("killclose")
    srv = Server(b, "r51-killclose", idle=600, extra=SMALL_L0, env=ENV).start()
    rng = random.Random(51)
    bad, absorbs = [], 0
    try:
        expect = {}
        for rnd in range(ROUNDS):
            col = srv.client(retryWrites=False)["g"]["c"]
            write_round(col, rnd, 2000, expect)
            absorbs += fenvix(srv.client())["compactor"]["absorbs"]
            close_async(srv, "g")
            time.sleep(rng.uniform(0.0, 2.5))
            srv.kill9()
            srv.proc = None
            srv.start()
            w = wrong(srv.client()["g"]["c"], expect)
            print(f"round {rnd}: wrong {len(w)} {w[:3]}")
            bad += w
        assert absorbs > 0, "no absorption ran: the test did not exercise the compactor"
        assert not bad, f"{len(bad)} stale or missing answers after kill -9 during a close, e.g. {bad[:5]}"
    finally:
        srv.term()


def test_reopen_while_the_old_pass_runs():
    b = bucket("reopen")
    srv = Server(b, "r51-reopen", idle=600, extra=SMALL_L0, env=ENV).start()
    try:
        c = srv.client()
        col = c["g"]["c"]
        expect, bad, hits = {}, [], 0
        for rnd in range(ROUNDS):
            write_round(col, rnd, 2000, expect)
            p0 = fenvix(c)["compaction"]["passes"]
            c["g"].command("tenantAdmin", close=True)
            # reopen the moment a pass runs (or after 4 s anyway)
            t0 = time.time()
            while time.time() - t0 < 4:
                f = fenvix(c)["compaction"]
                if f["running"] > 0 or f["passes"] > p0:
                    hits += f["running"] > 0
                    break
                time.sleep(0.01)
            # the reopen: writes of an older round's documents, at once
            write_round(col, rnd, 2000, expect, batches=2)
            w = wrong(col, expect)
            print(f"round {rnd}: pass running at the reopen {hits}, wrong {len(w)} {w[:3]}")
            bad += w
        c["g"].command("tenantAdmin", close=True)
        time.sleep(3)
        w = wrong(col, expect)
        print(f"after the last close: wrong {len(w)}")
        bad += w
        srv.term()
        srv.proc = None
        srv.start()
        w = wrong(srv.client()["g"]["c"], expect)
        print(f"after a restart: wrong {len(w)}")
        bad += w
        print("rounds with a pass running at the reopen:", hits)
        assert hits > 0, "the reopen never met a running pass: the test did not exercise the window"
        assert not bad, f"{len(bad)} stale or missing answers, e.g. {bad[:5]}"
    finally:
        srv.term()


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


def retrying(fn, tries=60):
    for i in range(tries):
        try:
            return fn()
        except OperationFailure as e:
            if e.code != 134 or i == tries - 1:
                raise
            time.sleep(0.5)


def test_a_close_that_hits_the_absorb_bound():
    """The store freezes (its relay stopped for 25 s) at a random moment of a close: the flush (bounded 20 s) or the
    absorption (bounded 10 s) waits past its bound, the close goes on, and nothing older is ever read."""
    b = bucket("bound")
    relay = Relay(0)
    env = dict(ENV, FENVIX_SEALED_TPIN_MS="20000")
    srv = Server(b, "r51-bound", idle=600, endpoint=relay.endpoint, extra=SMALL_L0, env=env).start()
    rng = random.Random(11)
    try:
        expect, bad = {}, []
        for rnd in range(ROUNDS):
            col = srv.client()["g"]["c"]
            retrying(lambda: write_round(col, rnd, 2000, expect))
            # a client connected beforehand: the close command reaches the server at once
            cc = MongoClient(srv.uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=90000)
            cc.admin.command("ping")
            th = threading.Thread(target=lambda: cc["g"].command("tenantAdmin", close=True), daemon=True)
            th.start()
            time.sleep(rng.uniform(0.0, 0.06))
            relay.p.send_signal(signal.SIGSTOP)
            time.sleep(25)
            relay.p.send_signal(signal.SIGCONT)
            th.join(120)
            log = srv.log_text()
            print(f"round {rnd}: absorption bound hit {log.count('were not absorbed before the close')}, flush bound hit {log.count('with the compactor running did not finish')}")
            w = retrying(lambda: wrong(srv.client()["g"]["c"], expect))
            print(f"round {rnd}: wrong {len(w)} {w[:3]}")
            bad += w
        log = srv.log_text()
        hit = log.count("were not absorbed before the close") + log.count("with the compactor running did not finish")
        srv.term()
        srv.proc = None
        srv.start()
        w = retrying(lambda: wrong(srv.client()["g"]["c"], expect))
        print(f"after a restart: wrong {len(w)}")
        bad += w
        assert hit > 0, "no close hit a bound: the test did not exercise it"
        assert not bad, f"{len(bad)} stale or missing answers, e.g. {bad[:5]}"
    finally:
        try:
            relay.p.send_signal(signal.SIGCONT)
        except Exception:
            pass
        srv.term()
        relay.stop()


def client(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=60000, retryWrites=False, retryReads=False)


def test_takeover_in_the_middle_of_a_close():
    TTL = 4
    b = bucket("tko")
    flags = SMALL_L0 + ["--lease-ttl-secs", str(TTL)]
    p1 = Server(b, "r51-tko1", idle=600, extra=flags, env=ENV).start()
    p2 = Server(b, "r51-tko2", idle=600, extra=flags, env=ENV).start()
    rng = random.Random(7)
    expect, bad = {}, []
    owner, other = p1, p2
    try:
        for rnd in range(ROUNDS // 2 + 2):
            col = client(owner)["g"]["c"]
            write_round(col, rnd, 2000, expect)
            close_async(owner, "g")
            time.sleep(rng.uniform(0.0, 1.5))
            os.kill(owner.proc.pid, signal.SIGSTOP)
            # the other process takes the database over once the frozen owner's lease lapsed
            deadline = time.time() + TTL * 8
            took = False
            while time.time() < deadline:
                try:
                    client(other)["g"]["c"].update_one({"_id": -1}, {"$set": {"v": rnd}}, upsert=True)
                    expect[-1] = rnd
                    took = True
                    break
                except (OperationFailure, AutoReconnect):
                    time.sleep(0.5)
            assert took, f"round {rnd}: no takeover"
            os.kill(owner.proc.pid, signal.SIGCONT)
            # writes on the new owner right away, while the old one finishes its close
            write_round(client(other)["g"]["c"], rnd, 2000, expect, batches=2)
            time.sleep(1)
            w = wrong(client(other)["g"]["c"], expect)
            print(f"round {rnd}: wrong {len(w)} {w[:3]}")
            bad += w
            owner, other = other, owner
        client(owner)["g"].command("tenantAdmin", close=True)
        w = wrong(client(owner)["g"]["c"], expect)
        print(f"after the last close: wrong {len(w)}")
        bad += w
        assert not bad, f"{len(bad)} stale or missing answers, e.g. {bad[:5]}"
    finally:
        for p in (p1, p2):
            try:
                os.kill(p.proc.pid, signal.SIGCONT)
            except Exception:
                pass
            p.term()


def test_compactor_off_then_on_again():
    """The gate on the old pass holds only while compactors are on (`SERVER_ON`). With the compactor switched off
    (`--no-sealed-compactor`, FENVIX_SEALED_COMPACTOR=0: the documented switch back to step 3's publishes) the pass is
    ungated again and folds L0 rows above X_d into a sorted run; the builder's publishes cover them while the compactor
    is off. Switched on again, the compactor absorbs only L0: its first absorption moves X_d past those sorted-run rows,
    and the merged reader ignores them."""
    b = bucket("offon")
    on = dict(ENV)
    off = dict(ENV, FENVIX_SEALED_COMPACTOR="0")
    expect, bad = {}, []
    srv = Server(b, "r51-offon", idle=600, extra=SMALL_L0, env=on).start()
    try:
        col = srv.client()["g"]["c"]
        write_round(col, 0, 2000, expect)
        srv.client()["g"].command("tenantAdmin", close=True)
        srv.term()
        # compactor off: rounds and closes; the pass runs on the closed database, ungated
        srv.env, srv.proc = off, None
        srv.start()
        c = srv.client()
        assert not fenvix(c)["compactor"]["enabled"]
        for rnd in range(1, 4):
            p0 = fenvix(c)["compaction"]["passes"]
            write_round(c["g"]["c"], rnd, 2000, expect)
            c["g"].command("tenantAdmin", close=True)
            wait_until(lambda: fenvix(c)["compaction"]["passes"] > p0 and fenvix(c)["compaction"]["running"] == 0, 30, what="a pass on the closed database")
        w = wrong(c["g"]["c"], expect)
        print(f"compactor off: wrong {len(w)}")
        bad += w
        srv.term()
        # compactor on again: rounds that update other documents only, then closes
        srv.env, srv.proc = on, None
        srv.start()
        c = srv.client()
        for rnd in range(4, 7):
            write_round(c["g"]["c"], rnd, 2000, expect)
            c["g"].command("tenantAdmin", close=True)
            time.sleep(2)
            w = wrong(c["g"]["c"], expect)
            print(f"compactor on again, round {rnd}: wrong {len(w)} {w[:3]}")
            bad += w
        st = fenvix(c)["compactor"]
        print("compactor", st)
        assert st["absorbs"] > 0, "no absorption after the switch: the test did not exercise it"
        assert not bad, f"{len(bad)} stale or missing answers after the compactor was switched off and on, e.g. {bad[:5]}"
    finally:
        srv.term()


def newest_runs_bytes(b, db):
    """The bytes of every run the newest version of our manifest lists."""
    import json
    keys = []
    for pg in s3().get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"tenants/{db}/fx/manifest/"):
        keys += [o["Key"] for o in pg.get("Contents", [])]
    if not keys:
        return None, []
    v = json.loads(s3().get_object(Bucket=b, Key=sorted(keys)[-1])["Body"].read())
    paths = [g["sealed"]["path"] for g in v.get("groups", []) if g.get("sealed")] + [d["path"] for d in v.get("delta", [])]
    return v, [(p, s3().get_object(Bucket=b, Key=p)["Body"].read()) for p in paths]


def test_the_compactors_erasure_removes_the_rows_from_the_runs():
    """test_48 no longer checks, with a compactor, that an erased customer's rows leave our runs (its rows were never in a
    run there). Here they are in runs first (lz4 runs: the mark is visible in their bytes), then the customer is erased."""
    b = bucket("erase")
    env = dict(ENV, FENVIX_SEALED_RUNS="2", FENVIX_SEALED_CODEC="lz4", FENVIX_SEALED_DERIVE="0", FENVIX_TEST_DROP="1")
    srv = Server(b, "r51-erase", idle=600, extra=SMALL_L0, env=env).start()
    try:
        c = srv.client()
        db = "er"
        c[db].create_collection("msgs", customerKey="userId")
        rnd = random.Random(3)
        for r in range(12):
            c[db]["msgs"].insert_many([{"_id": f"{u}-{r}-{i}", "userId": u, "t": r * 1000 + i,
                                        "body": (f"ERASEDMARK-{rnd.getrandbits(64):x}" if u == "u1" else f"keep-{rnd.getrandbits(64):x}") + "y" * 300}
                                       for u in ("u1", "u2", "u3") for i in range(300)])
        def in_runs():
            v, runs = newest_runs_bytes(b, db)
            return v is not None and any(b"ERASEDMARK" in body for _, body in runs)
        wait_until(in_runs, 60, what="the customer's rows in our runs (the precondition)")
        n = c[db]["msgs"].delete_many({"userId": "u1"}).deleted_count
        assert n == 12 * 300
        st, r = srv.admin.get(f"/v1/databases/{db}/erasures")
        assert st == 200 and len(r["erasures"]) == 1, r
        # writes after the erasure (absorption moves on), then wait for the scrub
        for r2 in range(4):
            c[db]["msgs"].insert_many([{"_id": f"u2-x{r2}-{i}", "userId": "u2", "t": i, "body": "z" * 300} for i in range(300)])
        def scrubbed():
            e = srv.admin.get(f"/v1/databases/{db}/erasures")[1]["erasures"][0]
            return e["state"] == "scrubbed" and e["live_done_ms"] is not None and e
        e = wait_until(scrubbed, 90, what="the erasure scrubbed")
        print("erasure", e, "compactor", fenvix(c)["compactor"])
        v, runs = newest_runs_bytes(b, db)
        left = [p for p, body in runs if b"ERASEDMARK" in body]
        print(f"newest version {v['version']}: {len(runs)} runs, {len(left)} still hold the erased customer's rows")
        assert c[db]["msgs"].count_documents({"userId": "u1"}) == 0
        assert not left, f"runs of the newest version still hold the erased customer's rows: {left[:3]}"
    finally:
        srv.term()


def test_a_sweep_that_aborts_live_uploads_loses_nothing():
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
                p2 = Server(b, "r51-sweep2", idle=600, extra=["--abandoned-upload-secs", "1"], env=env).start()
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
        assert aborted, "no live upload was aborted: the test did not exercise the window"
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
