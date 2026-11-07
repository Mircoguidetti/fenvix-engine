"""Unique indexes across concurrent transactions (found by the agent-adapters session, Mastra knowledge pages).

Several transactions that each write the same value under a unique index must not all commit: exactly one wins, the others
get what mongod 7.0 gives. MEASURED on the reference mongod of `judge/setup.sh`:
- a transaction that writes a unique value already written by another open transaction gets WriteConflict (112, label
  TransientTransactionError) at once, at the write;
- the same when the other transaction committed after this transaction's snapshot;
- a value committed before the snapshot gives DuplicateKey (11000);
- `with_transaction` retries on the label, so in the end one transaction commits and the others see DuplicateKey (11000).

Before the fix every concurrent transaction committed and the collection kept one document per transaction with the same
"unique" value. Every case runs on our server and, when it is reachable on port 27020, on the reference mongod (the same
assertions must hold on both). The last test kills our server with SIGKILL while transactions commit and checks that no
duplicate survives the restart and that no acknowledged commit is lost.
"""
import datetime
import os
import random
import threading
import time

import pytest
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure, PyMongoError

from lib import Server, free_port, make_bucket

REAL = os.environ.get("REAL_MONGOD", "mongodb://127.0.0.1:27020/?directConnection=true")
N = 4


def _real_client():
    c = MongoClient(REAL, serverSelectionTimeoutMS=2000)
    try:
        c.admin.command("ping")
        return c
    except PyMongoError:
        return None


@pytest.fixture(scope="module")
def ours():
    port = free_port()
    srv = Server(make_bucket("uniqtxn"), "uniqtxn", port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    c = srv.client()
    yield c
    c.close()
    srv.term()


@pytest.fixture(scope="module")
def real():
    c = _real_client()
    yield c
    if c:
        c.close()


@pytest.fixture(params=["ours", "mongod"])
def client(request, ours, real):
    if request.param == "mongod":
        if real is None:
            pytest.skip("reference mongod not reachable on 27020")
        return real
    return ours


def _fresh(client, name):
    db = client[f"uq_{name}"]
    client.drop_database(db.name)
    return db


def _race(client, body, n=N):
    """Run `body(session, i)` in n concurrent `with_transaction` calls; return the outcome of each."""
    barrier = threading.Barrier(n)
    out = [None] * n

    def one(i):
        try:
            barrier.wait()
            with client.start_session() as s:
                s.with_transaction(lambda s: body(s, i))
            out[i] = "ok"
        except DuplicateKeyError:
            out[i] = "dup"
        except BulkWriteError as e:
            codes = {w["code"] for w in e.details.get("writeErrors", [])}
            out[i] = "dup" if codes == {11000} else f"bulk{sorted(codes)}"
        except OperationFailure as e:
            out[i] = f"err{e.code}"

    ts = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return out


def _one_winner(out):
    assert out.count("ok") == 1, out
    assert sorted(x for x in out if x != "ok") == ["dup"] * (len(out) - 1), out


# ------------------------------------------------------------------------------------------------ inserts

def test_plain_unique(client):
    co = _fresh(client, "plain").c
    co.create_index("k", unique=True)
    out = _race(client, lambda s, i: co.insert_one({"_id": i, "k": "x"}, session=s))
    _one_winner(out)
    assert co.count_documents({"k": "x"}) == 1


def test_partial_unique_with_read_first(client):
    # the Mastra shape: read, then insert, under a unique partial index
    co = _fresh(client, "partial").c
    co.create_index("activeKey", unique=True, partialFilterExpression={"activeKey": {"$type": "string"}})
    co.insert_one({"_id": "seed"})

    def body(s, i):
        co.find_one({"activeKey": "p/install"}, session=s)
        co.insert_one({"_id": f"p{i}", "activeKey": "p/install"}, session=s)
    _one_winner(_race(client, body))
    assert co.count_documents({"activeKey": "p/install"}) == 1
    # documents outside the partial filter are not constrained
    co.insert_many([{"_id": "n1", "activeKey": 1}, {"_id": "n2", "activeKey": 1}])


def test_compound_unique(client):
    co = _fresh(client, "compound").c
    co.create_index([("a", 1), ("b", -1)], unique=True)
    out = _race(client, lambda s, i: co.insert_one({"_id": i, "a": 1, "b": "z"}, session=s))
    _one_winner(out)
    assert co.count_documents({"a": 1, "b": "z"}) == 1
    # a different compound value is not a conflict
    co.insert_one({"_id": "other", "a": 1, "b": "y"})


def test_subdocument_field_unique(client):
    co = _fresh(client, "subdoc").c
    co.create_index("meta.slug", unique=True)
    out = _race(client, lambda s, i: co.insert_one({"_id": i, "meta": {"slug": "same", "n": i}}, session=s))
    _one_winner(out)
    assert co.count_documents({"meta.slug": "same"}) == 1


def test_update_into_duplicate(client):
    co = _fresh(client, "update").c
    co.create_index("k", unique=True)
    co.insert_many([{"_id": i, "k": f"v{i}"} for i in range(N)])
    out = _race(client, lambda s, i: co.update_one({"_id": i}, {"$set": {"k": "same"}}, session=s))
    _one_winner(out)
    assert co.count_documents({"k": "same"}) == 1
    assert co.count_documents({}) == N


def test_update_subdocument_into_duplicate(client):
    co = _fresh(client, "update_sub").c
    co.create_index("meta.slug", unique=True)
    co.insert_many([{"_id": i, "meta": {"slug": f"s{i}"}} for i in range(N)])
    out = _race(client, lambda s, i: co.update_one({"_id": i}, {"$set": {"meta": {"slug": "same"}}}, session=s))
    _one_winner(out)
    assert co.count_documents({"meta.slug": "same"}) == 1


# ------------------------------------------------------------------------------------------------ other write paths

def test_upsert(client):
    co = _fresh(client, "upsert").c
    co.create_index("k", unique=True)
    out = _race(client, lambda s, i: co.update_one({"_id": i}, {"$set": {"k": "x"}}, upsert=True, session=s))
    _one_winner(out)
    assert co.count_documents({"k": "x"}) == 1


def test_find_and_modify_upsert(client):
    co = _fresh(client, "fam").c
    co.create_index("k", unique=True)
    out = _race(client, lambda s, i: co.find_one_and_update({"_id": i}, {"$set": {"k": "x"}}, upsert=True, return_document=ReturnDocument.AFTER, session=s))
    _one_winner(out)
    assert co.count_documents({"k": "x"}) == 1


def test_bulk_insert_many(client):
    co = _fresh(client, "bulk").c
    co.create_index("k", unique=True)
    # every transaction writes the same 5 values in a different order
    def body(s, i):
        vals = list(range(5))
        random.Random(i).shuffle(vals)
        co.insert_many([{"_id": f"{i}-{v}", "k": v} for v in vals], session=s)
    _one_winner(_race(client, body))
    assert co.count_documents({}) == 5
    for v in range(5):
        assert co.count_documents({"k": v}) == 1


def test_update_many_into_duplicate(client):
    co = _fresh(client, "upd_many").c
    co.create_index("k", unique=True)
    co.insert_many([{"_id": i, "g": i, "k": f"v{i}"} for i in range(N)])
    out = _race(client, lambda s, i: co.update_many({"g": i}, {"$set": {"k": "same"}}, session=s))
    _one_winner(out)
    assert co.count_documents({"k": "same"}) == 1


def test_two_databases_same_value_both_commit(client):
    # uniqueness is per collection: the same value in two databases is no conflict, and both transactions commit
    names = ["uq_twodb_a", "uq_twodb_b"]
    for n in names:
        client.drop_database(n)
        client[n].c.create_index("k", unique=True)
    out = _race(client, lambda s, i: client[names[i]].c.insert_one({"_id": i, "k": "x"}, session=s), n=2)
    assert out == ["ok", "ok"], out
    for n in names:
        assert client[n].c.count_documents({"k": "x"}) == 1


def test_id_collation_case_insensitive(client):
    # `_id` is unique under the collection's collation: "A" and "a" are the same `_id`
    db = _fresh(client, "idcoll")
    db.create_collection("c", collation={"locale": "en", "strength": 2})
    co = db.c
    out = _race(client, lambda s, i: co.insert_one({"_id": "AbC" if i % 2 else "abc", "n": i}, session=s))
    _one_winner(out)
    assert co.count_documents({}) == 1


# ------------------------------------------------------------------------------------------------ the error mongod gives

def _code(f):
    try:
        f()
        return "ok"
    except OperationFailure as e:
        labels = (e.details or {}).get("errorLabels", [])
        return (e.code, "TransientTransactionError" in labels)


def test_errors_like_mongod(client):
    co = _fresh(client, "errors").c
    co.create_index("k", unique=True)
    co.insert_one({"_id": "seed"})
    a, b = client.start_session(), client.start_session()
    try:
        # 1. the other transaction is still open
        a.start_transaction()
        b.start_transaction()
        assert _code(lambda: co.insert_one({"_id": 1, "k": 1}, session=a)) == "ok"
        assert _code(lambda: co.insert_one({"_id": 2, "k": 1}, session=b)) == (112, True)
        assert _code(lambda: a.commit_transaction()) == "ok"
        b.abort_transaction() if b.in_transaction else None
        # 2. the other transaction committed after this one's snapshot
        a.start_transaction()
        b.start_transaction()
        co.find_one({"_id": "seed"}, session=b)
        assert _code(lambda: co.insert_one({"_id": 3, "k": 2}, session=a)) == "ok"
        assert _code(lambda: a.commit_transaction()) == "ok"
        assert _code(lambda: co.insert_one({"_id": 4, "k": 2}, session=b)) == (112, True)
        b.abort_transaction() if b.in_transaction else None
        # 3. committed before the snapshot: DuplicateKey
        b.start_transaction()
        assert _code(lambda: co.insert_one({"_id": 5, "k": 2}, session=b)) == (11000, False)
        b.abort_transaction() if b.in_transaction else None
        # 4. a transaction that aborts gives the value back
        a.start_transaction()
        assert _code(lambda: co.insert_one({"_id": 6, "k": 3}, session=a)) == "ok"
        a.abort_transaction()
        b.start_transaction()
        assert _code(lambda: co.insert_one({"_id": 7, "k": 3}, session=b)) == "ok"
        assert _code(lambda: b.commit_transaction()) == "ok"
    finally:
        a.end_session()
        b.end_session()
    assert [co.count_documents({"k": v}) for v in (1, 2, 3)] == [1, 1, 1]


def test_plain_write_against_open_transaction(client):
    # mongod: the plain insert waits for the transaction, which commits, then gets 11000. Ours: the plain insert does not
    # wait (as for document keys), commits, and the transaction gets WriteConflict at commit. Either way exactly one wins.
    co = _fresh(client, "plain_vs_txn").c
    co.create_index("k", unique=True)
    a = client.start_session()
    a.start_transaction()
    assert _code(lambda: co.insert_one({"_id": 1, "k": 1}, session=a)) == "ok"
    res = {}
    t = threading.Thread(target=lambda: res.setdefault("plain", _code(lambda: co.insert_one({"_id": 2, "k": 1}))))
    t.start()
    time.sleep(0.5)
    res["txn"] = _code(lambda: a.commit_transaction())
    t.join()
    a.end_session()
    assert sorted([res["plain"] == "ok", res["txn"] == "ok"]) == [False, True], res
    assert res["plain"] in ("ok", (11000, False)), res
    assert res["txn"] in ("ok", (112, True)), res
    assert co.count_documents({"k": 1}) == 1


# ------------------------------------------------------------------------------------------------ SIGKILL during commits

def test_kill9_during_commits_keeps_no_duplicate():
    port = free_port()
    srv = Server(make_bucket("uniqkill"), "uniqkill", port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        c = srv.client()
        co = c.uq_kill.c
        co.create_index("k", unique=True)
        c.close()
        acked = []           # (_id, k) of acknowledged commits
        lock = threading.Lock()
        for trial in range(3):
            stop = threading.Event()

            def worker(w):
                cl = MongoClient(srv.uri, retryWrites=False, serverSelectionTimeoutMS=2000, socketTimeoutMS=5000)
                cc = cl.uq_kill.c
                n = 0
                while not stop.is_set():
                    n += 1
                    k = random.randrange(400) + trial * 1000
                    _id = f"t{trial}-w{w}-{n}"
                    try:
                        with cl.start_session() as s:
                            s.with_transaction(lambda s: cc.insert_one({"_id": _id, "k": k}, session=s))
                        with lock:
                            acked.append((_id, k))
                    except PyMongoError:
                        pass
                cl.close()

            ts = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
            for t in ts:
                t.start()
            time.sleep(1.5 + random.random() * 2)
            srv.kill9()
            stop.set()
            for t in ts:
                t.join()
            srv.proc = None
            srv.start()
            c = srv.client()
            co = c.uq_kill.c
            dups = list(co.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}}}, {"$match": {"n": {"$gt": 1}}}]))
            assert dups == [], dups
            have = {d["_id"] for d in co.find({}, {"_id": 1})}
            lost = [a for a in acked if a[0] not in have]
            assert lost == [], lost[:5]
            c.close()
        assert len(acked) > 20, len(acked)      # the trials did commit something
    finally:
        srv.term()
