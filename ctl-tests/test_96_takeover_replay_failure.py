"""sim-test-3 (2026-11-14), hole T on the real engine: a takeover whose log replay fails after its writer opened makes
the next takeover drop acknowledged records as stale.

Found by the simulator (gen-3 batch, CI run 43496082968, seeds 300169 and others; test
`fx::sim::tests::sim_hole_t_failed_takeover_replay_makes_records_stale`), on its port of the log's rules. This test
checks it on the real engine, with test_91's harness (three copies of the replicated log, two engine processes):

  1. B holds database DB; W0 is acknowledged and uploaded. B's uploads are held (`hang_put /wal/`): W1 is acknowledged
     and is in the log only. B is killed (-9).
  2. A (a warm spare, started before) gets a command for DB: its takeover reads W1 back from the log
     (`locallog::replay_on_wake`, which reads the stored writer epoch first) and applies it with `apply_durable`, which
     opens A's writer (the stored writer epoch moves). The store refuses A's uploads there (`fail_put /wal/`,
     `fail_put /fx/manifest/`), so the apply fails, the takeover answers an error and gives the database up. W1's records
     stay in the log with B's writer epoch.
  3. The store answers again; A's next command takes the database over again. `drop_stale` reads the stored writer epoch
     (A's failed writer) and drops W1's records as "confirmed after a newer writer opened, never acknowledged".

test_failed_takeover_replay_keeps_writes: every acknowledged write (W0 and W1) is read after the store recovers. It
FAILS while hole T is open (expected on night-head 36c6a3e). The test checks its own preconditions (W1 in the log only,
the first takeover's replay failed after reading the records) so it cannot pass by missing the order.

Needs MinIO (or any S3 endpoint the ctl tests use) and the log's binary `pq`, as test_91.
"""
import re
import tempfile
import time

from lib import make_bucket, wait_until
from test_91_takeover_retry import DB, W, Log, engine, put_rules, insert, read_all


def attempt_read(srv):
    c = srv.client()
    try:
        return sorted(d["_id"] for d in c[DB]["t"].find({}, {"_id": 1}))
    except Exception as e:  # the failed takeover answers an error
        return e
    finally:
        c.close()


def test_failed_takeover_replay_keeps_writes():
    log = Log().start()
    bucket = make_bucket("htr")
    fa, fb = tempfile.mktemp(prefix="htrfa"), tempfile.mktemp(prefix="htrfb")
    put_rules(fa)
    put_rules(fb)
    b = engine(bucket, "htrb", log, fb)
    a = engine(bucket, "htra", log, fa)
    try:
        b.start()
        a.start()
        w0 = [f"w0-{i}" for i in range(W)]
        insert(b, w0)
        time.sleep(3)                                   # W0 uploaded and confirmed to the log (uploads every 1 s)
        put_rules(fb, "hang_put /wal/", "log_put /wal/")
        time.sleep(1.5)
        w1 = [f"w1-{i}" for i in range(W)]
        insert(b, w1)
        wait_until(lambda: "FAULT hang_put" in b.log_text(), 30, what="B's upload of W1 to be held")
        b.kill9()
        # 2. A's takeover: its replay's uploads fail
        put_rules(fa, "fail_put /wal/", "fail_put /fx/manifest/", "log_put /wal/")
        mark = len(a.log_text())
        first = None
        for _ in range(60):
            r = attempt_read(a)
            if "could not be read back" in a.log_text()[mark:] or re.search(rf"takeover of {DB}: \d+ records applied", a.log_text()[mark:]):
                first = r
                break
            time.sleep(1)
        t = a.log_text()[mark:]
        print("A after the first takeover:", "\n".join(l for l in t.splitlines() if "quaderno" in l or "FAULT fail_put" in l or "read back" in l)[:4000])
        assert "could not be read back" in t, "setup: the first takeover's replay did not fail (no 'could not be read back' in A's log)"
        assert "FAULT fail_put" in t, "setup: the store never refused A's upload"
        # 3. the store answers again; A takes the database over again
        put_rules(fa)
        got = set(read_all(a))
        acked = w0 + w1
        lost = [i for i in acked if i not in got]
        t = a.log_text()[mark:]
        print(f"acknowledged {len(acked)}, lost {len(lost)} (W1 lost {sum(1 for i in lost if i.startswith('w1-'))})")
        print("A:", "\n".join(l for l in t.splitlines() if "quaderno" in l or "stale" in l)[:4000])
        assert not lost, f"RULE 8 (hole T): {len(lost)} of {len(acked)} acknowledged writes lost after a failed takeover replay (first: {lost[:5]})"
    finally:
        for s in (a, b):
            try:
                s.kill9()
            except Exception:
                pass
        log.stop()
