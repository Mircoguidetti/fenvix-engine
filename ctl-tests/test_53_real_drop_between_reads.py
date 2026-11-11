"""P-perf-bytes: the condition of DECISIONS 2026-11-09 ("Long commands keep working", option (b)) before the real drop
ships, with the REAL drop: the compactor in the server publishes (absorbs every L0 SST into our runs), the tenant installs
the version, and the drop job (`fx/drop.rs`, SlateDB's compaction filter in the live waiter) removes SlateDB's rows at
or below the new X_d. `test_50` proved the same with step 7a's stand-in (database closed, compactor off); this file runs
the product path (compactor on, drop on).

- Every command that reads storage more than once ($lookup, $unionWith, $graphLookup, $facet with $lookup, a find on a
  view whose pipeline reads another collection, $merge, mapReduce): a publish and a real drop before its K-th storage
  access (`PUT /v1/test/interleave`), K = 2..6; the answer must equal the answer without them, and the hook must have
  dropped SlateDB rows the command reads.
- createIndexes, with the publish and the drop between its build steps.
- A tailable cursor on a capped collection: a publish and a drop between the find and its getMore.
- A reader-mode process: a second server cannot read the database while the first holds its lease, so this case is the
  unit test `fx::compactor_tests::perf_bytes_follower_reads_across_a_real_drop` (a follower with its own lifecycle).
- Commands that read under one guard (find with paging and getMore, a find through a secondary index and its document
  fetches, count, distinct, aggregate): publishes and real drops run continuously in the background while they read.
- Negative control: the same server with FENVIX_CONTROL_KEEP_VIEW=1 (every read of a command reads with the first
  version it read with, as code that kept a view across steps would): the real drop must make it lose rows.
"""
import threading
import time

import pytest

from lib import Server, make_bucket

# short leases so a drop follows a publish within seconds (production: lease 60 s, remote lease 180 s); every rule of
# check_config holds: margin < lease, PUT timeout < lease - margin, lease > T_pin, remote lease and GC grace > hold life
# + PUT timeout
FAST = ["--sealed-drop", "true", "--sealed-t-pin-ms", "500", "--sealed-lease-ms", "1500", "--sealed-lease-margin-ms", "400",
        "--sealed-put-timeout-ms", "1000", "--sealed-hold-life-ms", "2000", "--sealed-remote-lease-ms", "4000",
        "--sealed-gc-grace-ms", "4000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]
KS = [2, 3, 4, 5, 6]


def start(tag, env=None, extra=(), bucket=None, holder=None):
    e = {"FENVIX_TEST_DROP": "1"}
    e.update(env or {})
    return Server(bucket or make_bucket(tag), tag, idle=600, extra=FAST + list(extra), env=e, holder=holder).start()


@pytest.fixture(scope="module")
def srv():
    s = start("realdrop")
    yield s
    s.term()


@pytest.fixture(scope="module")
def keepview():
    s = start("realdropkv", env={"FENVIX_CONTROL_KEEP_VIEW": "1"})
    yield s
    s.term()


def publish_drop(s, db):
    st, r = s.admin.post(f"/v1/test/databases/{db}/publish-drop", {})
    assert st == 200, r
    assert r.get("real") is True, f"the hook must use the real drop: {r}"
    return r


def interleave(s, db, at):
    if at is None:
        st, r = s.admin.delete("/v1/test/interleave")
    else:
        st, r = s.admin.put("/v1/test/interleave", {"database": db, "at": at})
    assert st == 200, r
    return r["interleaved"]


def dropped_total(s):
    """Rows the real drop removed, from the server's log lines of the hook."""
    n = 0
    for line in s.log_text().splitlines():
        if line.startswith("INTERLEAVE ") and '"dropped":' in line:
            n += int(line.split('"dropped":')[1].split(",")[0].split("}")[0])
    return n


def setup_db(s, db):
    """Two collections, half of their rows in our runs (published, dropped from SlateDB), half only in SlateDB above
    X_d: a read that kept the first version across the hook's publish and drop misses the second half."""
    c = s.client()
    for coll in ("a", "b"):
        c[db][coll].insert_many([{"_id": i, "k": i % 50, "g": i % 7, "v": f"{coll}-{i}", "next": i + 1} for i in range(0, 400)])
    publish_drop(s, db)
    for coll in ("a", "b"):
        c[db][coll].insert_many([{"_id": i, "k": i % 50, "g": i % 7, "v": f"{coll}-{i}", "next": i + 1} for i in range(400, 800)])
    return c


MAP = "function() { emit(this.g, 1); }"
REDUCE = "function(k, v) { return Array.sum(v); }"

COMMANDS = {
    "lookup": lambda d: list(d.a.aggregate([{"$match": {"g": 1}}, {"$lookup": {"from": "b", "localField": "k", "foreignField": "k", "as": "j"}}, {"$project": {"n": {"$size": "$j"}}}])),
    "unionWith": lambda d: sorted(str(x["_id"]) for x in d.a.aggregate([{"$match": {"g": 2}}, {"$unionWith": {"coll": "b", "pipeline": [{"$match": {"g": 3}}]}}, {"$project": {"_id": 1}}])),
    "graphLookup": lambda d: list(d.a.aggregate([{"$match": {"_id": {"$in": [0, 390, 700, 2000, 3000, 4000, 5000, 6000]}}}, {"$graphLookup": {"from": "b", "startWith": "$next", "connectFromField": "next", "connectToField": "_id", "as": "chain", "maxDepth": 30}}, {"$project": {"n": {"$size": "$chain"}}}])),
    "facet_lookup": lambda d: list(d.a.aggregate([{"$match": {"g": 4}}, {"$facet": {"x": [{"$lookup": {"from": "b", "localField": "_id", "foreignField": "_id", "as": "j"}}, {"$project": {"n": {"$size": "$j"}}}], "c": [{"$count": "n"}]}}])),
    "view_find": lambda d: sorted(str(x["_id"]) for x in d.va.find({})),
    "merge": lambda d: (list(d.a.aggregate([{"$match": {"g": 1}}, {"$project": {"v": 1}}, {"$merge": {"into": "m", "on": "_id", "whenMatched": "merge", "whenNotMatched": "insert"}}])),
                        sorted((str(x["_id"]), sorted(x.keys())) for x in d.m.find({})))[1],
    "mapReduce": lambda d: sorted((x["_id"], x["value"]) for x in d.command("mapReduce", "a", map=MAP, reduce=REDUCE, out={"inline": 1})["results"]),
}
# the families where a read that kept its first view loses rows (the negative control checks these)
CONTROL = ["lookup", "unionWith", "graphLookup", "facet_lookup", "view_find"]


def add_rows(c, db, k):
    # rows the command reads, only in SlateDB above X_d: the hook's publish and drop move them into our runs
    for coll in ("a", "b"):
        c[db][coll].insert_many([{"_id": 1000 * k + i, "k": i % 50, "g": i % 7, "v": f"{coll}-r{k}-{i}", "next": 1000 * k + i + 1} for i in range(300)])
    c[db]["m"].insert_many([{"_id": 1000 * k + i, "old": k} for i in range(300) if i % 7 == 1])


def run_interleaved(s, name, tag):
    db = f"rd_{tag}_{name.lower()}"
    c = setup_db(s, db)
    if name == "view_find":
        c[db].command("create", "va", viewOn="a", pipeline=[{"$lookup": {"from": "b", "localField": "_id", "foreignField": "_id", "as": "j"}}, {"$match": {"j.0": {"$exists": True}}}])
    run = COMMANDS[name]
    fired, wrong, dropped = 0, [], 0
    for k in KS:
        add_rows(c, db, k)
        want = run(c[db])  # the same state, without the hook
        d0 = dropped_total(s)
        n0 = interleave(s, db, k)
        try:
            got = run(c[db])
        finally:
            n1 = interleave(s, db, None)
        fired += n1 > n0
        dropped += dropped_total(s) - d0
        if got != want:
            wrong.append(k)
    return fired, wrong, dropped


@pytest.mark.parametrize("name", list(COMMANDS))
def test_a_publish_and_a_real_drop_between_two_reads_change_nothing(srv, name):
    fired, wrong, dropped = run_interleaved(srv, name, "p")
    assert fired > 0, f"{name}: the hook never ran inside the command (it reads storage once)"
    assert dropped > 0, f"{name}: the real drop removed no SlateDB row: the test would prove nothing"
    assert not wrong, f"{name}: the answer changed with a publish and a real drop before access {wrong}"


@pytest.mark.parametrize("name", CONTROL)
def test_control_keep_view_loses_rows_with_the_real_drop(keepview, name):
    fired, wrong, dropped = run_interleaved(keepview, name, "kv")
    assert fired > 0 and dropped > 0
    assert wrong, f"control KEEP_VIEW: {name} kept its first view across a real drop and still answered right: the test cannot see the fault"


def test_create_index_with_a_publish_and_a_real_drop_between_its_build_steps(srv):
    db = "rd_index"
    c = setup_db(srv, db)
    c[db]["a"].insert_many([{"_id": 10_000 + i, "k": i % 50, "g": i % 7} for i in range(5000)])  # several build steps
    total = c[db]["a"].count_documents({})
    d0 = dropped_total(srv)
    n0 = interleave(srv, db, 3)
    try:
        c[db]["a"].create_index([("g", 1)])
    finally:
        n1 = interleave(srv, db, None)
    assert n1 > n0, "the hook never ran inside createIndexes"
    assert dropped_total(srv) > d0, "the real drop removed nothing"
    by_index = sum(c[db]["a"].count_documents({"g": g}, hint="g_1") for g in range(-1, 7))
    assert by_index == total, f"the index misses {total - by_index} of {total} documents after a publish and a real drop between its steps"


def test_tailable_cursor_across_a_publish_and_a_real_drop(srv):
    db = "rd_tail"
    c = srv.client()
    c[db].create_collection("cap", capped=True, size=1 << 24)
    c[db]["cap"].insert_many([{"_id": i, "v": i} for i in range(200)])
    publish_drop(srv, db)
    cur = c[db]["cap"].find({}, cursor_type=__import__("pymongo").CursorType.TAILABLE, batch_size=50)
    seen = [next(cur)["_id"] for _ in range(50)]
    c[db]["cap"].insert_many([{"_id": i, "v": i} for i in range(200, 400)])
    d0 = dropped_total(srv)
    n0 = interleave(srv, db, 1)  # before the first access of the getMore commands that follow
    try:
        while len(seen) < 400 and cur.alive:
            try:
                seen.append(next(cur)["_id"])
            except StopIteration:
                break
    finally:
        n1 = interleave(srv, db, None)
    assert n1 > n0, "the hook never ran before a getMore"
    assert dropped_total(srv) > d0, "the real drop removed nothing"
    assert seen == list(range(400)), f"tailable cursor: {len(seen)} documents, first gap at {next((i for i, x in enumerate(seen) if x != i), None)}"


def test_single_guard_reads_while_real_drops_run_in_the_background(srv):
    """find with paging and getMore, a find through a secondary index (index pages, then the documents), count,
    distinct and a plain aggregate read under one guard each: a publish installs only under the exclusive lock, so the
    real drop cannot pass the version they read with. Checked by running them while publishes and drops loop."""
    db = "rd_bg"
    c = srv.client()
    c[db]["a"].create_index([("g", 1)])
    c[db]["a"].insert_many([{"_id": i, "g": i % 7, "s": "x" * 300} for i in range(6000)])
    publish_drop(srv, db)
    stop, errs, hooks = threading.Event(), [], [0]

    def loop():
        while not stop.is_set():
            try:
                publish_drop(srv, db)
                hooks[0] += 1
            except Exception as e:  # noqa: BLE001
                errs.append(repr(e))
                time.sleep(0.2)
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    wrong = []
    try:
        n, gs = 6000, [i % 7 for i in range(6000)]
        rnd = 0
        while (rnd < 12 or hooks[0] < 5) and rnd < 200:  # until several real drops landed among the reads
            rnd += 1
            c[db]["a"].insert_many([{"_id": n + i, "g": i % 7, "s": "y" * 300} for i in range(500)])
            n += 500
            gs += [i % 7 for i in range(500)]
            got = {
                "paged": sum(1 for _ in c[db]["a"].find({}, batch_size=700)),
                "by_index": sum(1 for _ in c[db]["a"].find({"g": 3}, batch_size=300).hint("g_1")),
                "count": c[db]["a"].count_documents({}),
                "distinct": sorted(c[db]["a"].distinct("g")),
                "agg": list(c[db]["a"].aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}]))[0]["n"],
            }
            want = {"paged": n, "by_index": gs.count(3),
                    "count": n, "distinct": list(range(7)), "agg": n}
            if got != want:
                wrong.append((rnd, got, want))
    finally:
        stop.set()
        t.join(timeout=120)
    assert hooks[0] >= 5, f"too few background publishes and drops ran: {hooks[0]} ({errs[:3]})"
    assert not wrong, f"reads under one guard changed with real drops in the background: {wrong[:2]}"
