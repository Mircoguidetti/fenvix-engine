"""Review of erase-cost-1tb (2026-11-13): a takeover in the strict sense during the ride pass.

P1 runs the forced pass and stops at `step` (failpoint with a pause); it is frozen (SIGSTOP) there until its process
lease lapses; P2 takes the database over, the customer writes again through P2, P2 finishes the erasure. Then P1 is
resumed: its pass goes on from the step on a stale view. Checks, through P2 and after a restart of P2: no erased document
readable, every acknowledged document there (the others' and the customer's new ones), every erasure done, and the
proof at 0 objects of ours holding the customer.
"""
import os
import signal
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, wait_until
from test_77_erase_cost import BASE, KNOBS, fill, unreadable, others_intact, erasures, proof

TTL = 4
STEPS = ["erase.after_forced_reseal", "erase.after_clear_saved", "erase.before_mark_clean"]


def client(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=30000, retryWrites=False, retryReads=False)


def retry(fn, t=TTL * 8):
    end = time.time() + t
    while True:
        try:
            return fn()
        except OperationFailure as e:
            if e.code != 134 or time.time() > end:
                raise
            time.sleep(0.5)


@pytest.mark.parametrize("step", STEPS)
def test_takeover_during_the_ride_pass(step):
    tag = "ecr" + step.split(".")[1][:6].replace("_", "")
    b = make_bucket(tag)
    flags = BASE + ["--lease-ttl-secs", str(TTL), "--sealed-erasure-batch-ms", "0"]
    p1 = Server(b, f"{tag}1", idle=600, extra=flags, env={"CTL_FAILPOINT": step, "CTL_FAILPOINT_PAUSE_MS": "2000", **KNOBS}).start()
    # the forced pass is the subject: default compactor knobs (no ordinary reseal of this tiny database first)
    p2 = Server(b, f"{tag}2", idle=600, extra=flags, env=dict(KNOBS)).start()
    db = "ecr"
    p2.bucket_name = b
    try:
        c1 = client(p1)
        fill(c1, db)
        c1[db]["msgs"].insert_many([{"_id": f"u1-late-{i}", "userId": "u1", "t": 900 + i, "email": f"u1-late-{i}@m", "body": "ERASEDMARK"} for i in range(30)])
        assert c1[db]["msgs"].delete_many({"userId": "u1"}).deleted_count == 90
        wait_until(lambda: f"FAILPOINT {step} reached" in p1.log_text(), 60, what=f"P1 at {step}")
        os.kill(p1.proc.pid, signal.SIGSTOP)
        try:
            c2 = client(p2)
            retry(lambda: c2[db]["msgs"].insert_one({"_id": "u1-new", "userId": "u1", "t": 1, "email": "new@m", "body": "after"}))
            unreadable(c2, db, "u1", ["u1-late-3"], allow=("u1-new",))
            others_intact(c2, db)
            def done():
                c2[db]["other"].insert_one({"t": time.time()})
                st, r = p2.admin.post(f"/v1/databases/{db}/erasures/run", {})
                assert st == 200, r
                return all(e["done_ms"] is not None for e in r["erasures"]) and r
            wait_until(done, timeout=180, every=2, what="P2 finishes the erasure")
        finally:
            os.kill(p1.proc.pid, signal.SIGCONT)
        wait_until(lambda: f"FAILPOINT {step} resumed" in p1.log_text(), 30, what="P1 resumed")
        time.sleep(8)  # P1's pass goes on, and its agent polls again
        p2.restart()
        c2 = client(p2)
        for _ in range(2):
            unreadable(c2, db, "u1", ["u1-late-3", "u1-0-0"], allow=("u1-new",))
            others_intact(c2, db)
            assert c2[db]["msgs"].find_one({"_id": "u1-new"})["body"] == "after"
            time.sleep(3)
        es = erasures(p2, db)
        assert all(e["done_ms"] is not None for e in es), es
        p = proof(p2, db, [f"u1-{r}-" for r in range(3)] + ["u1-late-"])
        assert p["our_objects_holding"] == 0, p
    finally:
        p1.term(); p2.term()
