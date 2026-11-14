"""sim-test-3 (2026-11-14), hole K on the real engine: a process's kept records re-create a dropped database.

Found by the simulator (gen-3 batch, CI run 43496082968, seed 302206 and others; test
`fx::sim::tests::sim_hole_k_kept_records_recreate_a_dropped_database`). With test_91's harness:

  1. B holds database DB; its uploads are held (`hang_put /wal/`) and W1 is acknowledged (in the log only).
  2. A restarts: it reads W1 back from the log; its replay is refused (B holds the lease). The failpoint
     `hold_kept_records` stops it before it keeps the records (so its retry runs after the drop, as in the simulator).
  3. B's uploads go on (W1 reaches object storage), then the client drops the database through B (dropDatabase: the log
     gets a drop record; B forgets its own kept records, `forget_pinned`; A's stay).
  4. A goes on and keeps the records in memory (`pinned`); its retry (`spawn_retry`) finds no stored marker for the name (the database is gone), takes the lease of the absent
     name, and its `apply_durable` creates the database again with W1.

test_dropped_database_stays_dropped: after the drop is acknowledged and A's retry has run, no document of the dropped
database is readable. FAILS while hole K is open (expected on night-head 36c6a3e).

test_kept_records_of_the_current_incarnation_are_applied (fix-hole-k, the opposite risk): the database is dropped and
created again by B, then B acknowledges W1 in the log only; A restarts, keeps W1 (for the current incarnation), B is
killed before any upload, and A's retry applies W1: every acknowledged write of the new incarnation is read, none of the
dropped one.

fix-hole-k: kept records remember the incarnation they were acknowledged for (read at A's start, proved by a second read
of the log) and are applied into that one only. `MONGO_TEST_KEPT_INC_OFF=1` on A switches the check off (mutation control).

Needs MinIO (or the S3 endpoint of the ctl tests) and the log's binary `pq`, as test_91.
"""
import os
import re
import tempfile
import time

from lib import make_bucket, wait_until
from test_91_takeover_retry import DB, W, Log, engine, put_rules, insert, read_all

A_ENV = {"MONGO_TEST_KEPT_INC_OFF": "1"} if os.environ.get("HOLE_K_MUTATION") == "1" else None


def count(srv):
    c = srv.client()
    try:
        return c[DB]["t"].count_documents({})
    finally:
        c.close()


def _try_count(srv):
    try:
        return count(srv)
    except Exception:
        return None


def test_dropped_database_stays_dropped():
    log = Log().start()
    bucket = make_bucket("kdr")
    fa, fb = tempfile.mktemp(prefix="kdrfa"), tempfile.mktemp(prefix="kdrfb")
    put_rules(fa)
    put_rules(fb)
    b = engine(bucket, "kdrb", log, fb)
    a = engine(bucket, "kdra", log, fa, env=A_ENV)
    try:
        b.start()
        insert(b, [f"w0-{i}" for i in range(10)])
        time.sleep(3)
        put_rules(fb, "hang_put /wal/", "log_put /wal/")
        time.sleep(1.5)
        insert(b, [f"w1-{i}" for i in range(W)])
        wait_until(lambda: "FAULT hang_put" in b.log_text(), 30, what="B's upload of W1 to be held")
        # 2. A restarts while B holds the database: its replay is refused; it stops before keeping the records
        #    (`hold_kept_records`), so its retry cannot run before the drop
        put_rules(fa, "hold_kept_records 1")
        a.start(wait=False)
        wait_until(lambda: f"FAULT hold_kept_records {DB}" in a.log_text(), 60, what="A to stop at hold_kept_records")
        assert re.search(rf"replay of database {DB} failed \(tenant {DB} is owned by server process", a.log_text()), "setup: A's replay was not refused by B's lease"
        # 3. B's uploads go on; the client drops the database through B
        put_rules(fb)
        time.sleep(3)
        c = b.client()
        try:
            c.drop_database(DB)
        finally:
            c.close()
        print("drop acknowledged")
        # 4. A goes on: it keeps its records, and its retry runs (1 s, then doubling)
        put_rules(fa)
        a.wait_ready(60)
        wait_until(lambda: re.search(rf"database {DB}: kept records .*by the retry|database {DB}: \d+ kept records conflict", a.log_text()), 90, what="A's retry to end")
        time.sleep(2)
        # read through the process that holds the name now (after the retry, A may: it took the lease of the absent name)
        # (a tuple: a count of 0 is the answer, not "not yet")
        n = wait_until(lambda: next(((x,) for x in [_try_count(a), _try_count(b)] if x is not None), None), 60, what="a count of the dropped database")[0]
        print("A:", "\n".join(l for l in a.log_text().splitlines() if "quaderno" in l or "local log" in l or "LEASE" in l)[:3000])
        print(f"documents of the dropped database read after A's retry: {n}")
        assert n == 0, f"hole K: the dropped database came back with {n} documents (A's kept records re-created it)"
    finally:
        for s in (a, b):
            try:
                s.kill9()
            except Exception:
                pass
        log.stop()


def test_kept_records_of_the_current_incarnation_are_applied():
    log = Log().start()
    bucket = make_bucket("kcr")
    fa, fb = tempfile.mktemp(prefix="kcrfa"), tempfile.mktemp(prefix="kcrfb")
    put_rules(fa)
    put_rules(fb)
    b = engine(bucket, "kcrb", log, fb)
    a = engine(bucket, "kcra", log, fa, env=A_ENV)
    try:
        b.start()
        # incarnation 0: written, uploaded, then dropped; incarnation 1: created again, written and uploaded
        insert(b, [f"w0-{i}" for i in range(10)])
        time.sleep(3)
        c = b.client()
        try:
            c.drop_database(DB)
        finally:
            c.close()
        w2 = [f"w2-{i}" for i in range(10)]
        insert(b, w2)
        time.sleep(3)
        # B's uploads held: W1 acknowledged in the log only (incarnation 1)
        put_rules(fb, "hang_put /wal/", "log_put /wal/")
        time.sleep(1.5)
        w1 = [f"w1-{i}" for i in range(W)]
        insert(b, w1)
        wait_until(lambda: "FAULT hang_put" in b.log_text(), 30, what="B's upload of W1 to be held")
        # A restarts while B holds the database: its replay is refused, it keeps W1 (for incarnation 1)
        put_rules(fa, "hold_kept_records 1")
        a.start(wait=False)
        wait_until(lambda: f"FAULT hold_kept_records {DB}" in a.log_text(), 60, what="A to stop at hold_kept_records")
        assert re.search(rf"replay of database {DB} failed \(tenant {DB} is owned by server process", a.log_text()), "setup: A's replay was not refused by B's lease"
        # B dies before any upload of W1: only the log and A's kept records have it
        b.kill9()
        put_rules(fa)
        a.wait_ready(60)
        wait_until(lambda: re.search(rf"database {DB}: kept records .*by the retry|database {DB}: \d+ kept records conflict", a.log_text()), 120, what="A's retry to end")
        got = set(read_all(a))
        print("A:", "\n".join(l for l in a.log_text().splitlines() if "quaderno" in l or "local log" in l or "LEASE" in l)[:3000])
        acked = w2 + w1
        lost = [i for i in acked if i not in got]
        back = sorted(i for i in got if i.startswith("w0-"))
        print(f"acknowledged in the current incarnation {len(acked)}, lost {len(lost)}; dropped documents read {len(back)}")
        assert not lost, f"RULE 8: {len(lost)} of {len(acked)} acknowledged writes of the current incarnation lost (first: {lost[:5]})"
        assert not back, f"hole K: {len(back)} documents of the dropped incarnation read"
    finally:
        for s in (a, b):
            try:
                s.kill9()
            except Exception:
                pass
        log.stop()
