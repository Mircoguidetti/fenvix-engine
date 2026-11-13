"""Review of big-speed fix 4: the `_id` check of an insert into a customer-keyed collection reads the document key
instead of the locator. Uniqueness per customer must hold wherever the first document lives: in the memtable, in the
same batch, after a restart (SSTs), after reseals into our runs with derived documents; and across customers the same
`_id` stays allowed. Control: --id-check-in-box false gives the same answers."""
import sys, time, os
sys.path.insert(0, "/home/user/bs/ctl-tests")
import pytest
from pymongo.errors import DuplicateKeyError, BulkWriteError
from lib import Server, make_bucket

SEALED = ["--no-sealed-compactor", "--sealed-runs", "1000000", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]


def fx(c):
    return c.admin.command("serverStatus")["fenvix"]


def load(db, n_threads=6, steps=40):
    for i in range(n_threads):
        t = f"th{i}"
        for s in range(steps):
            db.checkpoints.insert_one({"_id": f"{t}-{s}", "thread_id": t, "checkpoint_ns": "", "checkpoint_id": f"{s:05d}",
                                       "checkpoint": ("msg %d " % s) * 2000, "type": "json"})


def dup_checks(db, tag):
    errs = []
    # same customer, same _id: refused
    for i in range(6):
        t = f"th{i}"
        for s in (0, 17, 39):
            try:
                db.checkpoints.insert_one({"_id": f"{t}-{s}", "thread_id": t, "checkpoint_ns": "", "checkpoint_id": "dup"})
                errs.append(f"{tag}: duplicate ({t}, {t}-{s}) ACCEPTED")
            except DuplicateKeyError:
                pass
    # other customer, same _id: accepted (per-customer uniqueness, founder 2026-11-07)
    db.checkpoints.insert_one({"_id": "th0-0", "thread_id": f"other-{tag}", "checkpoint_ns": "", "checkpoint_id": "x"})
    # duplicates inside one batch
    try:
        db.checkpoints.insert_many([{"_id": f"b-{tag}", "thread_id": "th1"}, {"_id": f"b-{tag}", "thread_id": "th1"}])
        errs.append(f"{tag}: duplicate inside one insert_many ACCEPTED")
    except BulkWriteError:
        pass
    # each _id under th0..th5 is there once
    for i in range(6):
        t = f"th{i}"
        n = db.checkpoints.count_documents({"thread_id": t, "_id": {"$not": {"$regex": "^b-"}}})
        if n != 40:
            errs.append(f"{tag}: {t} holds {n} documents")
    nb = db.checkpoints.count_documents({"thread_id": "th1", "_id": f"b-{tag}"})
    if nb != 1:
        errs.append(f"{tag}: batch _id held {nb} times")
    return errs


@pytest.mark.parametrize("box", ["true", "false"])
def test_id_unique_per_customer_everywhere(box):
    bucket = make_bucket("rv-idbox")
    extra = SEALED + ["--id-check-in-box", box]
    s = Server(bucket, "rvidbox", idle=600, extra=extra, env={"FENVIX_TEST_DROP": "1"}).start()
    errs = []
    try:
        c = s.client()
        db = c.app
        load(db)
        errs += dup_checks(db, "memtable")
        # every row into our runs and out of SlateDB (test hook), then let the compactor reseal (derived documents)
        st, r = s.admin.post("/v1/test/databases/app/publish-drop", {})
        assert st == 200, r
        print("publish-drop:", r)
        for _ in range(30):
            time.sleep(1)
            st = fx(c)
            if st["compactor"]["reseals"] >= 1 and st["derived"]["encoded"] > 0:
                break
        st = fx(c)
        print("compactor publishes", st["compactor"]["publishes"], "reseals", st["compactor"]["reseals"], "derived", st["derived"])
    finally:
        s.term()
    s = Server(bucket, "rvidbox", idle=600, extra=extra).start()
    try:
        c = s.client()
        db = c.app
        r0 = fx(c)["sealedRuns"]["rowsFromRuns"]
        errs += dup_checks(db, "after-restart")
        sr2 = fx(c).get("sealedRuns", {})
        print("rows answered by runs during the checks", sr2.get("rowsFromRuns") - r0, "rebuilds", fx(c)["derived"]["rebuilds"])
        assert sr2.get("rowsFromRuns") - r0 > 0, "no row came from our runs: the test did not exercise them"
    finally:
        s.term()
    assert not errs, errs
