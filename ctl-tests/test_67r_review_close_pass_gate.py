"""Reviewer's product-level check of the close-time compaction pass (fix-close-pass-gate): is the rule 8 defect found by
fix-write-stalls reachable through the server, and does the gate close it?

The shape: sealed runs on with the server's compactor (the default); 1 MB memtables, so a burst of inserts leaves several
L0 SSTs; the database closed right after the burst (its compactor stops before it absorbed them, the close sees L0 >= 4
and queues the close-time pass); then reopened by a second burst (the compactor absorbs the new L0 SSTs, X_d moves);
then every document is read back, live and after a restart. Rounds on fresh databases.
"""
import os
import time

from pymongo import MongoClient

from lib import Server, make_bucket

ROUNDS = int(os.environ.get("R67_ROUNDS", "4"))
ENV = {"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
       "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}


def burst(c, db, start, n):
    for s in range(start, start + n, 2000):
        c[db].t.insert_many([{"_id": i, "p": "x" * 300} for i in range(s, min(start + n, s + 2000))])


def test_close_time_pass_then_reopen_keeps_every_row():
    bucket = make_bucket("r67")
    srv = Server(bucket, "r67", idle=600, extra=["--memtable-max-mb", "1"], env=ENV).start()
    bad, lines = [], []
    try:
        for r in range(ROUNDS):
            db = f"g{r}"
            c = srv.client()
            c[db].t.insert_many([{"_id": -i - 1} for i in range(50)])
            c[db].t.insert_many([{"_id": -i - 1000} for i in range(50)])
            time.sleep(3)                                  # the first publish and the compactor
            burst(c, db, 0, 20_000)                        # several L0 SSTs ...
            c[db].command("tenantAdmin", close=True)       # ... closed before the compactor absorbs them
            time.sleep(15)                                 # the close-time pass runs
            burst(c, db, 100_000, 20_000)                  # reopen: new L0 SSTs, absorption, X_d moves
            time.sleep(15)
            want = 100 + 40_000
            got = c[db].t.count_documents({})
            ids = {d["_id"] for d in c[db].t.find({}, {"_id": 1})}
            gap = sum(1 for i in range(20_000) if i not in ids)
            lines.append(f"round {r}: count {got}, scan {len(ids)} of {want}; first burst missing {gap}")
            if got != want or len(ids) != want:
                bad.append(lines[-1])
        srv.restart()
        c = srv.client()
        for r in range(ROUNDS):
            n = len({d["_id"] for d in c[f"g{r}"].t.find({}, {"_id": 1})})
            lines.append(f"after a restart g{r}: {n} of 40100")
            if n != 40_100:
                bad.append(lines[-1])
        log = srv.log_text()
        lines.append(f"passes logged: {log.count('compaction of tenant')} lines; gate kept lines: {log.count('kept')}")
    finally:
        srv.term()
    print("\n".join(lines))
    assert not bad, bad
