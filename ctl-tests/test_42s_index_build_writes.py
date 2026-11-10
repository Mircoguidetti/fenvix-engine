"""Index builds and drops with concurrent writes (fix round 2 of fix-unique-txn).

An index on a collection that already has documents is built in chunks, the database released between chunks. While it
builds, writes are accepted and maintain its entries; the build scans the documents ahead of its cursor. MEASURED on the
reference mongod 7.0 of `judge/setup.sh` (hybrid index build): writes during a unique build are accepted, and the build
fails at the end with DuplicateKey (11000) when a value is held by two documents, whether the documents were scanned
already or not; when a later write removed the duplicate before the end, the build succeeds. A finished unique index never
holds a value twice.

Before this round our server finished the unique index with the duplicate when both documents were behind the cursor
(review test `test_42r_review_unique_build.py`). The cases below run on our server (one document per build chunk, so the
build is still running when the writes arrive) and on the reference mongod when it is reachable on port 27020 (a larger
collection, so its build is still running too; the outcome is checked only when it was).

Also here: a non-unique build with concurrent inserts, updates and deletes on both sides of the cursor must end with an
index that holds exactly one entry per document value (no entry missing, none doubled), and dropping and re-creating an
index while writers run must leave the indexes consistent with the data (`validate`).
"""
import os
import random
import threading
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, free_port, make_bucket

REAL = os.environ.get("REAL_MONGOD", "mongodb://127.0.0.1:27020/?directConnection=true")
OURS_DOCS = 3000          # one document per chunk on ours: the build lasts long enough
MONGOD_DOCS = 300000      # mongod builds in one pass: a large collection keeps it busy for about 0.4 s (MEASURED)


@pytest.fixture(scope="module")
def ours():
    port = free_port()
    srv = Server(make_bucket("buildw"), "buildw", port=port, env={"MONGO_BUILD_CHUNK": "1"},
                 extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    c = srv.client()
    yield c
    c.close()
    srv.term()


@pytest.fixture(params=["ours", "mongod"])
def client(request, ours):
    if request.param == "ours":
        return ours
    c = MongoClient(REAL, serverSelectionTimeoutMS=2000)
    try:
        c.admin.command("ping")
    except PyMongoError:
        pytest.skip("reference mongod not reachable on 27020")
    return c


def is_mongod(client):
    return client.address[1] == 27020


def fresh(client, name):
    client.drop_database(name)
    return client[name].c


def seed(client, co):
    n = MONGOD_DOCS if is_mongod(client) else OURS_DOCS
    # _id 1000.. : writes with a small _id land behind the build cursor, writes with a large _id ahead of it
    for i in range(0, n, 50000):
        co.insert_many([{"_id": 1000 + j, "k": j} for j in range(i, min(n, i + 50000))], ordered=False)
    return n


def code(f):
    try:
        f()
        return "ok"
    except OperationFailure as e:
        return e.code


def build_with_writes(client, co, unique, writes, delay=None):
    """Start create_index on k, run `writes` while it builds. Returns (build result, write results, build was running)."""
    res = {}

    def build():
        res["build"] = code(lambda: co.create_index("k", unique=unique))

    t = threading.Thread(target=build)
    t.start()
    time.sleep(delay if delay is not None else (0.1 if is_mongod(client) else 0.3))
    out = [code(w) for w in writes]
    running = t.is_alive()
    t.join()
    return res["build"], out, running


def unique_dups(co):
    return list(co.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}}}, {"$match": {"n": {"$gt": 1}}}], allowDiskUse=True))


def assert_no_finished_duplicate(co):
    unique = [i for i in co.list_indexes() if i["name"] == "k_1" and i.get("unique")]
    if unique:
        assert unique_dups(co) == [], "a finished unique index holds a value twice"
    return bool(unique)


def assert_valid(co):
    v = co.database.command("validate", co.name)
    assert v["valid"], v.get("errors")
    return v


# ------------------------------------------------------------------------------------------------ unique builds

UNIQUE_CASES = {
    # two new documents behind the cursor with the same value: the review's case
    "two_inserts_behind_cursor": (lambda co: [lambda: co.insert_one({"_id": 1, "k": -1}), lambda: co.insert_one({"_id": 2, "k": -1})], 11000),
    # one new document behind the cursor with the value of a document the build has already scanned
    "insert_dup_of_scanned_doc": (lambda co: [lambda: co.insert_one({"_id": 1, "k": 5})], 11000),
    # the same, but the new document lands ahead of the cursor
    "insert_dup_ahead_of_cursor": (lambda co: [lambda: co.insert_one({"_id": 10 ** 9, "k": 5})], 11000),
    # an update of a scanned document into the value of another scanned document
    "update_scanned_into_scanned": (lambda co: [lambda: co.update_one({"_id": 1001}, {"$set": {"k": 0}})], 11000),
    # two documents with the same value in one command
    "insert_many_same_value": (lambda co: [lambda: co.insert_many([{"_id": 1, "k": -7}, {"_id": 2, "k": -7}])], 11000),
    # an upsert creating the value of a scanned document
    "upsert_dup": (lambda co: [lambda: co.update_one({"_id": 3}, {"$set": {"k": 2}}, upsert=True)], 11000),
    # a duplicate written and then removed before the build ends: the build succeeds (as on mongod)
    "dup_then_deleted": (lambda co: [lambda: co.insert_one({"_id": 1, "k": -1}), lambda: co.insert_one({"_id": 2, "k": -1}), lambda: co.delete_one({"_id": 2})], "ok"),
    "dup_then_moved": (lambda co: [lambda: co.insert_one({"_id": 1, "k": 9}), lambda: co.update_one({"_id": 1}, {"$set": {"k": -9}})], "ok"),
    # no duplicate at all: values written on both sides of the cursor are fine
    "no_dup": (lambda co: [lambda: co.insert_one({"_id": 1, "k": -1}), lambda: co.insert_one({"_id": 10 ** 9, "k": -2}), lambda: co.update_one({"_id": 1002}, {"$set": {"k": -3}})], "ok"),
}


@pytest.mark.parametrize("case", list(UNIQUE_CASES))
def test_unique_build_with_concurrent_writes(client, case):
    co = fresh(client, f"bw_{case}")
    seed(client, co)
    writes, expected = UNIQUE_CASES[case]
    build, out, running = build_with_writes(client, co, True, writes(co))
    finished_unique = assert_no_finished_duplicate(co)
    assert all(w == "ok" for w in out), out        # as on mongod, the writes themselves are accepted
    if not running:
        if not is_mongod(client):
            pytest.fail("the build finished before the writes: the test did not exercise the window")
        return
    assert build == expected, {"build": build, "writes": out}
    assert finished_unique == (expected == "ok")
    if finished_unique:
        assert_valid(co)


def test_unique_build_duplicate_in_transaction(client):
    # a transaction writes the value of a scanned document while the build runs. mongod: the build waits for the
    # transaction, which commits, and the build fails with 11000. Ours: the transaction gets WriteConflict at commit
    # when a build step ran after its snapshot (the driver would retry it), or it commits and the build fails with 11000.
    # Either way the finished unique index never holds the value twice.
    co = fresh(client, "bw_txn")
    seed(client, co)
    res = {}

    def txn():
        with client.start_session() as s:
            s.start_transaction()
            co.insert_one({"_id": 1, "k": 5}, session=s)
            time.sleep(0.05)
            s.commit_transaction()

    def run():
        try:
            txn()
            res["txn"] = "ok"
        except OperationFailure as e:
            res["txn"] = e.code
    build, _, running = build_with_writes(client, co, True, [run])
    assert_no_finished_duplicate(co)
    assert res["txn"] in ("ok", 112), res
    if res["txn"] == "ok" and running:
        assert build == 11000, (build, res)
    if build == "ok":
        assert co.count_documents({"k": 5}) == 1


def test_unique_build_many_writers(client):
    # 4 writers insert and update at random on both sides of the cursor, some values colliding; whatever the build
    # answers, a finished unique index holds no value twice and is consistent with the data
    co = fresh(client, "bw_many")
    n = seed(client, co)
    stop = threading.Event()

    def writer(w):
        r = random.Random(w)
        i = 0
        while not stop.is_set():
            i += 1
            try:
                if r.random() < 0.5:
                    co.insert_one({"_id": r.choice([-1, 1]) * (10 ** 6 + w * 10 ** 5 + i), "k": -r.randrange(200)})
                else:
                    co.update_one({"_id": 1000 + r.randrange(n)}, {"$set": {"k": -r.randrange(200)}})
            except OperationFailure:
                pass
    ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    for t in ts:
        t.start()
    build = code(lambda: co.create_index("k", unique=True))
    stop.set()
    for t in ts:
        t.join()
    finished = assert_no_finished_duplicate(co)
    assert (build == "ok") == finished and build in ("ok", 11000), build
    if not finished:
        assert unique_dups(co) != [], "the build failed although no value is held twice"
    assert_valid(co)


# ------------------------------------------------------------------------------------------------ non-unique builds

def check_index_matches_data(co, sample=None):
    v = assert_valid(co)
    n = co.count_documents({})
    # every document has exactly one entry in k_1 (a single scalar value each): none missing, none doubled
    assert v["keysPerIndex"].get("k_1") == n and v["keysPerIndex"].get("_id_") == n, (v["keysPerIndex"], n)
    for val in (sample or []):
        assert co.count_documents({"k": val}, hint="k_1") == co.count_documents({"k": val}, hint=[("$natural", 1)]), val


def test_non_unique_build_with_writes_on_both_sides(client):
    co = fresh(client, "bw_nonunique")
    n = seed(client, co)
    stop = threading.Event()
    touched = set()

    def writer(w):
        r = random.Random(100 + w)
        i = 0
        while not stop.is_set():
            i += 1
            x = r.random()
            try:
                if x < 0.3:
                    co.insert_one({"_id": r.choice([-1, 1]) * (10 ** 6 + w * 10 ** 5 + i), "k": -r.randrange(50)})
                elif x < 0.7:
                    k = -r.randrange(50)
                    co.update_one({"_id": 1000 + r.randrange(n)}, {"$set": {"k": k}})
                    touched.add(k)
                else:
                    co.delete_one({"_id": 1000 + r.randrange(n)})
            except OperationFailure:
                pass
    ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    for t in ts:
        t.start()
    if not is_mongod(client):
        time.sleep(0.2)
    build = code(lambda: co.create_index("k"))
    stop.set()
    for t in ts:
        t.join()
    assert build == "ok"
    check_index_matches_data(co, sample=list(range(-49, 1)) + [5, 77, n - 1])


# ------------------------------------------------------------------------------------------------ index drop with writes

@pytest.mark.parametrize("unique", [False, True])
def test_drop_and_recreate_index_with_writes(client, unique):
    co = fresh(client, f"bw_drop_{unique}")
    n = seed(client, co)
    co.create_index("k", unique=unique)
    stop = threading.Event()
    nxt = [10 ** 7]
    lock = threading.Lock()

    def writer(w):
        r = random.Random(200 + w)
        while not stop.is_set():
            with lock:
                nxt[0] += 1
                v = nxt[0]
            x = r.random()
            try:
                if x < 0.4:
                    co.insert_one({"_id": r.choice([-1, 1]) * v, "k": v if unique else -r.randrange(50)})
                elif x < 0.8:
                    co.update_one({"_id": 1000 + r.randrange(n)}, {"$set": {"k": -v if unique else -r.randrange(50)}})
                else:
                    co.delete_one({"_id": 1000 + r.randrange(n)})
            except OperationFailure:
                pass
    ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    for t in ts:
        t.start()
    time.sleep(0.2)
    assert code(lambda: co.drop_index("k_1")) == "ok"
    time.sleep(0.2)
    assert "k_1" not in [i["name"] for i in co.list_indexes()]
    assert code(lambda: co.create_index("k", unique=unique)) == "ok"
    time.sleep(0.2)
    stop.set()
    for t in ts:
        t.join()
    assert "k_1" in [i["name"] for i in co.list_indexes()]
    if unique:
        assert unique_dups(co) == []
    check_index_matches_data(co, sample=list(range(-49, 1)) + [5, 77])
