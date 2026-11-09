"""Compaction 2, step 7a: export and point-in-time restore read through our manifest and runs.

Once step 4's drop removes SlateDB rows at or below X_d, those rows exist only in our runs. Export and PITR opened raw
SlateDB readers (on a checkpoint, on a restore staging clone, on the live database), so they would miss them. These tests
drop the rows with the test hook (`POST /v1/test/databases/{db}/drop`, a stand-in for step 4's drop job, `fx/droptest.rs`)
and compare export and restore with the state the server answered before the drop.

Controls: `FENVIX_CONTROL_RAW_PATHS=1` makes export and PITR read SlateDB alone again (the code of dedb4e5); the same tests
must fail with it.
"""
import os
import tempfile
import time

import bson
import pytest

import export_client as xc
from lib import Server, make_bucket, dump_db, TOKEN

GAP = 1.15  # states are separated by more than the 1 s resolution of the object store's timestamps
# After the first version every publish writes a delta run only: step 3's offline builder reseals a group from SlateDB's
# rows, which a drop removed (`fx/build.rs`: it assumes SlateDB keeps every row; step 4's reseal reads the runs instead).
SEALED = ["--sealed-runs", "2", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0",
          "--retention-interval-secs", "2", "--retention-prune-secs", "3600"]
CONTROL = os.environ.get("FENVIX_CONTROL_RAW_PATHS") == "1"


def now_ms():
    return int(time.time() * 1000)


@pytest.fixture()
def srv():
    bucket = make_bucket("c2s7a")
    env = {"FENVIX_TEST_DROP": "1"}
    if CONTROL:
        env["FENVIX_CONTROL_RAW_PATHS"] = "1"
    s = Server(bucket, "c2s7a", idle=300, extra=SEALED, env=env).start()
    yield s
    s.term()


def drop(srv, db):
    st, r = srv.admin.post(f"/v1/test/databases/{db}/drop", {})
    assert st == 200, r
    assert r["dropped"] > 0, f"the drop removed nothing: {r}"
    return r


def sealed_counters(srv):
    return srv.client().admin.command("serverStatus")["fenvix"]["sealedRuns"]


def write_rounds(c, db, base, rounds=6):
    """Inserts, updates and deletes over several commands (one write batch each), so the server publishes several
    versions (`--sealed-runs 2`), with delta runs and reseals."""
    for r in range(rounds):
        c[db]["a"].insert_many([{"_id": base + r * 100 + i, "v": r, "s": "x" * (i % 13)} for i in range(100)])
        c[db]["b"].insert_one({"_id": f"{base}-{r}", "r": r})
        c[db]["a"].update_many({"_id": {"$gte": base + r * 100, "$lt": base + r * 100 + 30}}, {"$set": {"v": -r}})
        c[db]["a"].delete_many({"_id": {"$gte": base + r * 100 + 90, "$lt": base + r * 100 + 100}})


def read_bson(path):
    return bson.decode_all(open(path, "rb").read())


def test_export_after_a_drop_equals_the_database(srv):
    c = srv.client()
    write_rounds(c, "ex", 0)
    want = dump_db(c, "ex")
    assert sealed_counters(srv)["publishes"] > 0, "the server published no version"
    r = drop(srv, "ex")
    # the server itself reads through the merged reader: unchanged after the drop
    assert dump_db(c, "ex") == want, "the server's own reads changed after the drop"
    st, ex = srv.admin.post("/v1/databases/ex/exports")
    assert st == 201, ex
    dest = tempfile.mkdtemp(prefix="dump-c2s7a-")
    ver = xc.download(f"http://127.0.0.1:{srv.admin_port}", TOKEN, ex["id"], dest)
    assert all(v == "ok" for v in ver.values()), ver
    for coll in ("a", "b"):
        got = {repr(d["_id"]): d for d in read_bson(f"{dest}/ex/{coll}.bson")}
        exp = want[coll]
        missing = sorted(set(exp) - set(got))
        extra = sorted(set(got) - set(exp))
        wrong = [k for k in exp if k in got and got[k] != exp[k]]
        assert not missing and not extra and not wrong, f"export of {coll} after a drop at {r['floor']}: {len(missing)} missing, {len(extra)} extra, {len(wrong)} wrong of {len(exp)}"


def points(srv, db):
    st, r = srv.admin.get(f"/v1/databases/{db}/restore-points")
    assert st == 200, r
    return r["points"]


def test_restore_after_drops_equals_each_state(srv):
    c = srv.client()
    db = "pt"
    c[db]["a"].insert_one({"_id": "seed"})
    st, r = srv.admin.put(f"/v1/databases/{db}/retention", {"window_secs": 3600, "interval_secs": 2})
    assert st == 200, r
    # state 1: written, published, then its rows dropped from SlateDB
    write_rounds(c, db, 0)
    time.sleep(0.05)
    t1, want1 = now_ms(), dump_db(c, db)
    time.sleep(GAP)
    d1 = now_ms()
    drop(srv, db)
    # state 2: more writes after the drop, with a restore point taken after it (the agent takes one every 2 s)
    write_rounds(c, db, 10_000)
    deadline = time.time() + 20
    while not any(p["at_ms"] > d1 for p in points(srv, db)):
        assert time.time() < deadline, "no restore point after the drop"
        c[db]["b"].insert_one({"_id": f"tick-{now_ms()}"})
        time.sleep(0.5)
    time.sleep(0.05)
    t2, want2 = now_ms(), dump_db(c, db)
    assert len(want2["a"]) > len(want1["a"]), "the server's own reads lost rows after the drop"
    time.sleep(GAP)
    # state 3: more writes, and everything dropped again
    write_rounds(c, db, 20_000)
    drop(srv, db)
    for t, want, name in ((t2, want2, "state 2 (point after the first drop)"), (t1, want1, "state 1 (point before the drops)")):
        st, r = srv.admin.post(f"/v1/databases/{db}/restore", {"to": t})
        assert st == 200, r
        got = dump_db(c, db)
        for coll in sorted(set(want) | set(got)):
            g, w = got.get(coll, {}), want.get(coll, {})
            missing = sorted(set(w) - set(g))
            extra = sorted(set(g) - set(w))
            wrong = [k for k in w if k in g and g[k] != w[k]]
            assert not missing and not extra and not wrong, f"restore to {name}, collection {coll}: {len(missing)} missing, {len(extra)} extra, {len(wrong)} wrong of {len(w)} ({r['keys']})"
