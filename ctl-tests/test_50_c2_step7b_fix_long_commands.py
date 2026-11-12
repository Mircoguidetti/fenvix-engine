"""Compaction 2, step 7b fix round: option (b) for long commands (DECISIONS 2026-11-09, "Long commands keep working").

Only reads in flight are in the drop floor; a command's older reads leave it, and a command is never failed for its total
length. That is safe only if no read of a command reuses a version after its own step. These tests prove it end to end:
- for every command that reads storage more than once ($lookup, $unionWith, $graphLookup, $facet with $lookup, $merge, a find on
  a view whose pipeline reads another collection, createIndexes between its build steps), a publish and a drop (the step
  7a stand-in) run before its K-th storage access, for K = 2..6 (the hook `PUT /v1/test/interleave`), and its answer
  must equal the answer without them. Commands that read once (find, count, distinct, update and delete multi,
  findAndModify) read under one guard of the database, which no publish can enter; an aggregation with $merge reads its
  source and then its target under two, and is tested;
- control `FENVIX_CONTROL_KEEP_VIEW=1` (every read of a command reads with the first version it read with, as code that
  kept a view across steps would): the same tests must lose rows;
- long commands, with the hold life scaled down to 500 ms: `createIndexes` on 100,000 documents and a `$lookup` over
  100,000 documents succeed; control `FENVIX_CONTROL_OP_HOLDS=1` (step 7a's operation holds): they fail.
"""
import os

import pytest
from pymongo import IndexModel
from pymongo.errors import OperationFailure

from lib import Server, make_bucket

# integration-3: the compactor is on by default in the server; this test's hook publishes with step 3's builder and drops
# with step 7a's stand-in, which a running compactor forbids (it is then the only publisher), so the compactor is off here
SEALED = ["--no-sealed-compactor", "--sealed-runs", "1000000", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]
KEEP_VIEW = os.environ.get("FENVIX_CONTROL_KEEP_VIEW") == "1"
OP_HOLDS = os.environ.get("FENVIX_CONTROL_OP_HOLDS") == "1"
KS = [2, 3, 4, 5, 6]


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("c2fixb"), "c2fixb", idle=300, extra=SEALED, env={"FENVIX_TEST_DROP": "1"}).start()
    yield s
    s.term()


def publish_drop(srv, db):
    st, r = srv.admin.post(f"/v1/test/databases/{db}/publish-drop", {})
    assert st == 200, r
    return r


def interleave(srv, db, at):
    if at is None:
        st, r = srv.admin.delete("/v1/test/interleave")
    else:
        st, r = srv.admin.put("/v1/test/interleave", {"database": db, "at": at})
    assert st == 200, r
    return r["interleaved"]


def setup_db(srv, db):
    """Two collections, half of their rows in our runs (published and dropped from SlateDB) and half only in SlateDB,
    above X_d: a read that kept the first version across the hook's publish and drop misses the second half."""
    c = srv.client()
    for coll in ("a", "b"):
        c[db][coll].insert_many([{"_id": i, "k": i % 50, "g": i % 7, "v": f"{coll}-{i}", "next": i + 1} for i in range(0, 400)])
    publish_drop(srv, db)
    for coll in ("a", "b"):
        c[db][coll].insert_many([{"_id": i, "k": i % 50, "g": i % 7, "v": f"{coll}-{i}", "next": i + 1} for i in range(400, 800)])
    return c


COMMANDS = {
    "lookup": lambda d: list(d.a.aggregate([{"$match": {"g": 1}}, {"$lookup": {"from": "b", "localField": "k", "foreignField": "k", "as": "j"}}, {"$project": {"n": {"$size": "$j"}}}])),
    "unionWith": lambda d: sorted(str(x["_id"]) for x in d.a.aggregate([{"$match": {"g": 2}}, {"$unionWith": {"coll": "b", "pipeline": [{"$match": {"g": 3}}]}}, {"$project": {"_id": 1}}])),
    "graphLookup": lambda d: list(d.a.aggregate([{"$match": {"_id": {"$in": [0, 390, 700, 2000, 3000, 4000, 5000, 6000]}}}, {"$graphLookup": {"from": "b", "startWith": "$next", "connectFromField": "next", "connectToField": "_id", "as": "chain", "maxDepth": 30}}, {"$project": {"n": {"$size": "$chain"}}}])),
    "facet_lookup": lambda d: list(d.a.aggregate([{"$match": {"g": 4}}, {"$facet": {"x": [{"$lookup": {"from": "b", "localField": "_id", "foreignField": "_id", "as": "j"}}, {"$project": {"n": {"$size": "$j"}}}], "c": [{"$count": "n"}]}}])),
    "view_find": lambda d: sorted(str(x["_id"]) for x in d.va.find({})),
    "merge": lambda d: (list(d.a.aggregate([{"$match": {"g": 1}}, {"$project": {"v": 1}}, {"$merge": {"into": "m", "on": "_id", "whenMatched": "merge", "whenNotMatched": "insert"}}])),
                        sorted((str(x["_id"]), sorted(x.keys())) for x in d.m.find({})))[1],
}


@pytest.mark.parametrize("name", list(COMMANDS))
def test_a_publish_and_a_drop_between_two_reads_of_one_command_change_nothing(srv, name):
    db = f"il_{name.lower()}"
    c = setup_db(srv, db)
    if name == "view_find":
        c[db].command("create", "va", viewOn="a", pipeline=[{"$lookup": {"from": "b", "localField": "_id", "foreignField": "_id", "as": "j"}}, {"$match": {"j.0": {"$exists": True}}}])
    run = COMMANDS[name]
    fired, wrong = 0, []
    for k in KS:
        # rows the command reads, only in SlateDB above X_d: the hook's publish and drop move them into our runs
        for coll in ("a", "b"):
            c[db][coll].insert_many([{"_id": 1000 * k + i, "k": i % 50, "g": i % 7, "v": f"{coll}-r{k}-{i}", "next": 1000 * k + i + 1} for i in range(300)])
        # $merge's target: documents the merge must find (and keep their own field)
        c[db]["m"].insert_many([{"_id": 1000 * k + i, "old": k} for i in range(300) if i % 7 == 1])
        want = run(c[db])  # the same state, without the hook
        n0 = interleave(srv, db, k)
        try:
            got = run(c[db])
        finally:
            n1 = interleave(srv, db, None)
        fired += n1 > n0
        if got != want:
            wrong.append(k)
    assert fired > 0, "the hook never ran inside the command"
    if KEEP_VIEW:
        assert wrong, "control KEEP_VIEW: a command that keeps its first view across the drop must lose rows"
        pytest.fail(f"control KEEP_VIEW: {name} answered wrong with the hook before access {wrong}")
    assert not wrong, f"{name}: the answer changed with a publish and a drop before access {wrong}"


def test_create_index_with_a_publish_and_a_drop_between_its_build_steps(srv):
    db = "il_index"
    c = setup_db(srv, db)
    c[db]["a"].insert_many([{"_id": 10_000 + i, "k": i % 50, "g": i % 7} for i in range(5000)])  # several build steps
    total = c[db]["a"].count_documents({})
    n0 = interleave(srv, db, 3)
    try:
        c[db]["a"].create_index([("g", 1)])
    finally:
        n1 = interleave(srv, db, None)
    assert n1 > n0, "the hook never ran inside createIndexes"
    by_index = sum(c[db]["a"].count_documents({"g": g}, hint="g_1") for g in range(-1, 7))
    assert by_index == total, f"the index misses {total - by_index} of {total} documents after a publish and a drop between its steps"


@pytest.fixture()
def short_hold():
    # hold life 500 ms (120 s in production) and T_pin 10 s: every single read step of these commands fits, their total does not
    extra = ["--no-sealed-compactor", "--sealed-runs", "50", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "10000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait", "--sealed-hold-life-ms", "500"]
    s = Server(make_bucket("c2fixlong"), "c2fixlong", idle=300, extra=extra, env={"FENVIX_TEST_DROP": "1"}).start()
    yield s
    s.term()


def test_long_commands_outlive_the_hold_life(short_hold):
    """With the hold life at 500 ms (120 s in production), commands of several seconds: step 7a failed them with code 134
    (createIndexes then left no index); option (b) lets them finish. Their durations are checked to be above the hold life."""
    import time
    c = short_hold.client()
    for k in range(20):
        c.lg.big.insert_many([{"_id": k * 5000 + i, "x": (i * 7919) % 100000, "s": "y" * 200} for i in range(5000)])
    # the rows leave SlateDB, as step 4's drop will do: before a drop, step 3's merged reader walks SlateDB's rows at or
    # below X_d to skip them, so one bounded read step costs the rest of the collection (a single step longer than the
    # hold life fails under (a) and (b) alike; reported for step 4)
    publish_drop(short_hold, "lg")
    errs = []
    t0 = time.time()
    try:
        # four indexes in one command: since fix-query-defects (P2, P3) a single index build or a lookup by `_id` takes
        # about 0.4 s on CI, under the hold life; the command must outlast it to test anything (review of 62b8619)
        c.lg.big.create_indexes([IndexModel([("x", 1)]), IndexModel([("x", -1), ("_id", 1)]), IndexModel([("s", 1), ("x", 1)]), IndexModel([("x", 1), ("s", 1)])])
    except OperationFailure as e:
        errs.append(f"createIndexes: {e.code} {str(e)[:120]}")
    t1 = time.time()
    try:
        n = len(list(c.lg.big.aggregate([{"$lookup": {"from": "big", "localField": "_id", "foreignField": "_id", "as": "j"}},
                                          {"$lookup": {"from": "big", "localField": "x", "foreignField": "x", "as": "k"}},
                                          {"$lookup": {"from": "big", "localField": "_id", "foreignField": "x", "as": "l"}},
                                          {"$project": {"j": 0, "k": 0, "l": 0}}, {"$limit": 100000}])))
        assert n == 100000
    except OperationFailure as e:
        errs.append(f"$lookup: {e.code} {str(e)[:120]}")
    t2 = time.time()
    print(f"createIndexes {t1 - t0:.2f} s, $lookup {t2 - t1:.2f} s")
    names = [i["name"] for i in c.lg.big.list_indexes()]
    if not errs:
        assert {"x_1", "x_-1__id_1", "s_1_x_1", "x_1_s_1"} <= set(names), names
    assert not errs, f"long commands failed past the hold life: {errs}; indexes {names}"
    assert t1 - t0 > 0.5 and t2 - t1 > 0.5, f"the commands must outlast the hold life (500 ms) to test it: {t1 - t0:.2f} s, {t2 - t1:.2f} s"
