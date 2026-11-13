"""fix-box-first-trip: a customer's first save (an upsert that inserts) waits for the customer's box only.

Cause (MEASURED, test_79 in sealed mode): the insert path of an upsert checked the new `_id` in the shared locator
(`loc/{coll}/{_id}{customer}`), which lies outside every customer's box. When the run block holding that locator key was
not in the caches (a run the compactor had just published, or after a restart), the save waited a second trip: 57.6 ms
instead of one 27 ms trip. `Writes::exists` already checked the document key (customer, `_id`) inside the box instead
(P-big-speed fix 4); `Writes::put` now does the same.

The GETs counted here are the command's own (the engine's per-command meter, `FX_WAITS`: the count of ` get=<ms>/<n>` in
the update's `WAITS` line, every object-store GET made in the command's task, `counting.rs`), so background work of the
database (compactor, GC) cannot add to them or hide them.

  - `test_first_save_reads_only_the_box`: every row in our runs, a restart, a write of another collection (the writer
    opens), a find of the customer (its box warms), then the upsert of a new checkpoint of that customer: 0 GETs of its
    own. Control (`--id-check-in-box false`): the same upsert reads the locator and sends at least one GET.
  - `test_id_still_unique_per_customer`: in both modes, an upsert that would insert an `_id` the customer already holds
    is refused, a move of a document to a customer that holds its `_id` is refused, and the same `_id` under another
    customer is accepted.
"""
import re
import time

import pytest
from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from lib import Server, make_bucket

# every row goes into our runs by the test hook below (the compactor off, no publish by the builder in between)
SEALED = ["--no-sealed-compactor", "--sealed-runs", "1000000", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000",
          "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]
ENV = {"FENVIX_TEST_DROP": "1", "FX_WAITS": "0"}
THREADS = 40
STEPS = 25
# more documents of other customers, so that the locator spans several blocks of the run
FILL = 30_000


def tid(i):
    return f"user{i:03d}-thread{i % 3}"


def load(db):
    db.checkpoints.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1)], unique=True)
    for i in range(THREADS):
        for s in range(STEPS):
            # string `_id`s spread over the locator's whole range, so that a new `_id` placed among them lands in a
            # locator block that no other read touches (a new ObjectId would sort last, next to `meta/collections`,
            # which every command reads)
            db.checkpoints.update_one({"thread_id": tid(i), "checkpoint_ns": "", "checkpoint_id": f"1ef{s:05d}"},
                                      {"$set": {"type": "json", "checkpoint": ("m %d " % s) * 60, "metadata": {"step": s}},
                                       "$setOnInsert": {"_id": f"{i:03d}-{s:03d}"}}, upsert=True)
    for k in range(0, FILL, 2000):
        db.checkpoints.insert_many([{"_id": f"{(j * 7919) % 1000:03d}-f{j:05d}", "thread_id": f"fill{j % 500:03d}", "checkpoint_ns": "",
                                     "checkpoint_id": f"f{j:05d}", "checkpoint": "x"} for j in range(k, k + 2000)], ordered=False)


def update_meter_gets(server, coll, n_before):
    """The object-store GETs of their own of the `update` commands on `coll` logged after the first `n_before` of them."""
    lines = [l for l in server.log_text().splitlines() if l.startswith("WAITS ") and " cmd=update " in l and f" coll={coll} " in l]
    return [int(m.group(1)) if (m := re.search(r" get=[\d.]+/(\d+)", l)) else 0 for l in lines[n_before:]], len(lines)


@pytest.fixture(scope="module")
def loaded():
    bucket = make_bucket("boxsave")
    s = Server(bucket, "boxsave", idle=600, extra=SEALED, env=ENV).start()
    try:
        c = s.client()
        load(c.app)
        st, r = s.admin.post("/v1/test/databases/app/publish-drop", {})
        assert st == 200, r
        c.close()
    finally:
        s.term()
    return bucket


@pytest.mark.parametrize("box", ["true", "false"])
def test_first_save_reads_only_the_box(loaded, box):
    s = Server(loaded, "boxsave", idle=600, extra=SEALED + ["--id-check-in-box", box], env=ENV).start()
    try:
        c = s.client()
        db = c.app
        db.other.insert_one({"x": 1})  # the writer opens (a per-database cost, not the customer's)
        n = 20 if box == "true" else 21  # one customer per run: the bucket is shared
        t = tid(n)
        assert len(list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}))) == STEPS  # the customer's box warms
        time.sleep(0.2)
        _, n0 = update_meter_gets(s, "checkpoints", 0)
        r = db.checkpoints.update_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": "1ef99999-new"},
                                      {"$set": {"type": "json", "checkpoint": "new", "metadata": {}}, "$setOnInsert": {"_id": f"{n:03d}-new"}},
                                      upsert=True)
        assert r.upserted_id == f"{n:03d}-new"
        time.sleep(0.2)
        gets, _ = update_meter_gets(s, "checkpoints", n0)
        print(f"--id-check-in-box {box}: the first save's own GETs {gets}")
        assert len(gets) == 1, gets
        if box == "true":
            assert gets[0] == 0, f"the first save of a warm customer sent {gets[0]} GETs of its own"
        else:
            assert gets[0] >= 1, f"control: the locator read sent no GET ({gets}): the test could not fail"
        assert db.checkpoints.count_documents({"thread_id": t}) == STEPS + 1
    finally:
        s.term()


@pytest.mark.parametrize("box", ["true", "false"])
def test_id_still_unique_per_customer(loaded, box):
    s = Server(loaded, "boxsave", idle=600, extra=SEALED + ["--id-check-in-box", box], env=ENV).start()
    try:
        c = s.client()
        db = c.app
        a, b = (tid(3), tid(4)) if box == "true" else (tid(5), tid(6))  # the bucket is shared
        held = db.checkpoints.find_one({"thread_id": a, "checkpoint_ns": "", "checkpoint_id": "1ef00002"})["_id"]
        # an upsert that inserts an `_id` the customer already holds (in our runs, after a restart)
        with pytest.raises(DuplicateKeyError):
            db.checkpoints.update_one({"thread_id": a, "checkpoint_ns": "", "checkpoint_id": f"dup-{box}"},
                                      {"$set": {"x": 1}, "$setOnInsert": {"_id": held}}, upsert=True)
        # the same `_id` under another customer is accepted
        # (an insert: an upsert that inserts an `_id` another customer holds is refused on the base too, outside this test)
        db.checkpoints.insert_one({"_id": held, "thread_id": b, "checkpoint_ns": "", "checkpoint_id": f"same-id-{box}"})
        # moving that document to the customer that holds its `_id` is refused
        with pytest.raises(DuplicateKeyError):
            db.checkpoints.update_one({"_id": held, "thread_id": b}, {"$set": {"thread_id": a}})
        # a new `_id` written just now (memtable) is refused as well
        fresh = ObjectId()
        db.checkpoints.insert_one({"_id": fresh, "thread_id": a, "checkpoint_ns": "", "checkpoint_id": f"fresh-{box}"})
        with pytest.raises(DuplicateKeyError):
            db.checkpoints.update_one({"thread_id": a, "checkpoint_ns": "", "checkpoint_id": f"fresh2-{box}"},
                                      {"$set": {"x": 1}, "$setOnInsert": {"_id": fresh}}, upsert=True)
        assert db.checkpoints.count_documents({"_id": held}) == 2
        assert db.checkpoints.count_documents({"_id": held, "thread_id": a}) == 1
        assert db.checkpoints.count_documents({"_id": fresh}) == 1
    finally:
        s.term()
