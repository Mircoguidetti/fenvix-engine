"""P-perf-bytes: GC of our runs after the database closed (`fx::gc::gc_closed`, run by the maintenance pass of a closed
tenant). Before it, the compactor's live GC stopped with the close (60 s idle by default), so every run retired within
the GC grace before the close stayed in the store until the next open.

- The runs and versions only older versions listed leave once the database closed and the grace passed; every document
  reads back exactly after a reopen (the runs of the newest version are kept).
- The grace is kept: with a grace longer than the time since the last publish, nothing is deleted yet; it is deleted
  later (the pass is due again when the grace ends).
"""
import time

from lib import Server, make_bucket, list_prefix

BASE = ["--sealed-t-pin-ms", "500", "--sealed-lease-ms", "1500", "--sealed-lease-margin-ms", "400", "--sealed-put-timeout-ms", "1000",
        "--sealed-hold-life-ms", "2000", "--sealed-remote-lease-ms", "4000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]


def runs(bucket, db):
    objs = list_prefix(bucket, f"tenants/{db}/fx/")
    return len([o for o in objs if "/fx/run/" in o[0]]), len([o for o in objs if "/fx/manifest/" in o[0]])


def fill(s, db, rounds):
    c = s.client()
    want = 0
    for r in range(rounds):
        c[db].a.insert_many([{"_id": r * 10000 + i, "u": f"user{i % 20}", "s": "x" * 400} for i in range(2000)])
        want += 2000
        res = c.admin.command("fenvixReseal", db)
        assert res.get("compactor", True) is not False, res
    return c, want


def test_closed_database_drops_retired_runs_and_keeps_its_data():
    b = make_bucket("closedgc")
    s = Server(b, "closedgc", idle=5, extra=BASE + ["--sealed-gc-grace-ms", "4000"]).start()
    try:
        c, want = fill(s, "cg", 5)
        n0, v0 = runs(b, "cg")
        assert n0 > 3 and v0 > 3, f"the reseals must leave retired runs and versions to collect: {n0} runs, {v0} versions"
        t0 = time.time()
        while time.time() - t0 < 120:
            n, v = runs(b, "cg")
            if v == 1 and n < n0:
                break
            time.sleep(2)
        n1, v1 = runs(b, "cg")
        assert v1 == 1 and n1 < n0, f"after the close: {n1} runs (before {n0}), {v1} versions (before {v0})"
        # reopen: every document reads back
        got = sorted(d["_id"] for d in c.cg.a.find({}, {"_id": 1}))
        assert got == sorted(r * 10000 + i for r in range(5) for i in range(2000))
        assert c.cg.a.count_documents({"u": "user7"}) == want // 20
        st = c.admin.command("tenantStats")["fxGcClosed"]
        assert st["runsDeleted"] > 0 and st["versionsDeleted"] > 0, st
    finally:
        s.term()


def test_closed_gc_waits_for_the_grace():
    b = make_bucket("closedgcwait")
    grace = 75
    s = Server(b, "closedgcwait", idle=5, extra=BASE + ["--sealed-gc-grace-ms", str(grace * 1000)]).start()
    try:
        t_pub = time.time()
        c, want = fill(s, "cw", 4)
        n0, _ = runs(b, "cw")
        # the close (5 s idle) and the maintenance pass (30 s after the last write) run well before the grace ends
        time.sleep(45)
        n_mid, v_mid = runs(b, "cw")
        assert time.time() - t_pub < grace - 5, "the test machine is too slow for this timing"
        assert n_mid == n0, f"runs deleted before the grace: {n_mid} of {n0} left {time.time() - t_pub:.0f} s after the last publish"
        assert v_mid == 1, f"old versions should be gone at once (retention 0): {v_mid}"
        t0 = time.time()
        while time.time() - t0 < 150:
            if runs(b, "cw")[0] < n0:
                break
            time.sleep(3)
        n1, _ = runs(b, "cw")
        assert n1 < n0, f"the retired runs were never deleted after the grace: {n1} of {n0}"
        assert c.cw.a.count_documents({}) == want
    finally:
        s.term()
