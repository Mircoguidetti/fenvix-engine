"""fix-store-errors (2026-11-12): a takeover's WAL replay must fail closed when the store answers badly.

Process A acknowledges 1,000 single inserts (one WAL object each, none flushed to L0), then is killed with -9. Process B
takes the database over through a forwarder that, for 20 s, answers every HEAD or GET of the database's WAL objects with
503 SlowDown, 500 or a reset connection. B may refuse or delay reads meanwhile; once the forwarder answers normally,
the database must answer and hold every acknowledged insert (rule 8). A replay that took an unreadable WAL object for
an empty fence and skipped it would lose the inserts that object holds.

Why: the fix-store-errors probe first saw acknowledged writes "lost" after a lease loss. The cause was the forwarder
(tools/lease-stall/pathproxy.py) answering HEAD with a length of 0: SlateDB took the WAL objects for empty fences and
skipped them. Real S3 sends the true length; this test checks that the engine treats the real failure modes of a HEAD
or GET (5xx, reset) as errors and never as an empty object. MEASURED on the fix-store-errors head: 0 lost in each case.
"""
import os
import sys
import threading
import time

import pytest
from pymongo import MongoClient

from lib import Server, make_bucket, MINIO

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lease-stall"))
from pathproxy import PathProxy  # noqa: E402

N = 1000


@pytest.mark.parametrize("method,answer", [("HEAD", 503), ("HEAD", 500), ("HEAD", "reset"), ("GET", 503), ("GET", "reset")])
def test_takeover_replay_fails_closed(method, answer):
    host, port = MINIO.split("//")[1].split(":")
    px = PathProxy((host, int(port)))
    bucket = make_bucket("replayf")
    a = Server(bucket, "replayfa", idle=600, endpoint=px.endpoint).start()
    acked = []
    try:
        c = a.client()
        for i in range(N):
            c.big.t.insert_one({"_id": i, "v": "x" * 50})
            acked.append(i)
    finally:
        a.kill9()
    px.refuse((method,), "/tenants/big/wal/", answer)
    threading.Timer(20, px.release).start()
    b = Server(bucket, "replayfb", idle=600, endpoint=px.endpoint).start()
    try:
        t0 = time.time()
        n = None
        while time.time() - t0 < 300:
            cl = MongoClient(b.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000)
            try:
                n = cl.big.t.count_documents({})
                break
            except Exception:
                time.sleep(0.5)
            finally:
                cl.close()
        print(f"{method} {answer}: {px.refused} answers replaced; the database answered after {time.time() - t0:.1f} s")
        assert n is not None, "the database did not answer within 300 s"
        have = {d["_id"] for d in MongoClient(b.uri, socketTimeoutMS=300000).big.t.find({}, {"_id": 1})}
        missing = sorted(set(acked) - have)
        assert not missing, f"{len(missing)} of {len(acked)} acknowledged inserts missing, e.g. {missing[:5]}"
        assert px.refused > 0, "no fault was injected: the test did not reach the case"
    finally:
        px.release()
        b.term()
        px.close()
