"""Small security and memory fixes (session M3, branch fix-piccoli).

1. Fork window (found by the reviewer of S14). While a fork ran, the branch had no registry record yet, so the allow-list
   check said "no list, come in", and nothing kept the branch closed: a user created on the branch during the fork could
   authenticate from an address its source refuses, and a command could open (or write to) the half-made branch. Now the
   fork holds the branch's entry lock until it is complete (or rolled back), the check reads the source's list for a branch
   being forked, and at the end the fork, like a change of the list, drops the authentication of connections outside it.
   Tested at the three points of the reviewer (after the clone, after the clean marker, after the record).
2. Blocking sort, memory. A sort without a limit that the plan does not give had no memory limit (+506 MB for 200,000
   documents, reviewer of N5): now mongod's limit, `internalQueryMaxBlockingSortMemoryUsageBytes` (100 MB, settable),
   error 292 beyond it, and a clear message that this server does not sort on disk (allowDiskUse is not supported).
3. Blocking sort through an index that does not give the order (or `$or`): the candidates are read a chunk at a time
   instead of as one list of every `_id`. The results must be the same as with the whole list (the default, for few
   candidates); the servers here lower the chunk sizes (MONGO_SORT_BUFFER_IDS, MONGO_SORT_CHUNK_IDS) so that a small
   collection takes the chunked paths: point reads chunk by chunk (MONGO_NO_FETCH_SCAN) and the switch to one scan.

Without the fixes the fork tests and the memory-limit tests fail; the comparison tests pass on main too (they are the
control that nothing changed in the answers).
"""
import random
import threading
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, NetworkTimeout, OperationFailure

from lib import Server, free_port, make_bucket, wait_until
from netlib import scram_ok

PW = "Piccoli-SECRET-5e1c07"
NO_LIMITS = ["--auth-fail-max", "1000000", "--auth-fail-max-per-db", "1000000"]


# ------------------------------------------------------------------------------------------------ 1. the fork window

def _auth_server(tag, point=None):
    env = {"CTL_FAILPOINT": point} if point else None
    srv = Server(make_bucket(tag), tag, idle=300, env=env, extra=["--auth", "--trusted-networks", "127.0.0.1/32"] + NO_LIMITS).start()
    c = srv.client()
    try:
        c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])  # localhost exception
    except OperationFailure:
        pass
    c.close()
    srv.root = srv.client(username="root", password="rootpw", authSource="admin")
    return srv


def _source(srv, db, entries):
    st, r = srv.admin.post("/v1/databases", {"name": db})
    assert st == 201, r
    srv.root[db].command("createUser", "u1", pwd=PW, roles=[{"role": "readWrite", "db": db}])
    srv.root[db].c.insert_many([{"_id": i, "x": i} for i in range(50)])
    if entries is not None:
        assert srv.admin.put(f"/v1/databases/{db}/ip-allowlist", {"entries": entries})[0] == 200


@pytest.mark.parametrize("point", ["fork.after_clone", "fork.after_marker", "fork.after_meta"])
def test_user_created_on_the_branch_during_the_fork_cannot_come_in_from_outside(point):
    srv = _auth_server("fwin", point)
    try:
        _source(srv, "wsrc", ["127.0.0.2/32"])
        th = threading.Thread(target=lambda: srv.admin.post("/v1/databases/wsrc/fork", {"target": "wkid"}, timeout=60), daemon=True)
        th.start()
        wait_until(lambda: f"FAILPOINT {point} reached" in srv.log_text(), 30, what="failpoint")
        # the reviewer's case: root creates a user on the branch while the fork is stuck at this point
        srv.root["wkid"].command("createUser", "late", pwd=PW, roles=[{"role": "readWrite", "db": "wkid"}])
        # from an address the source refuses: refused, at every point (on main: let in at the first two)
        assert not scram_ok(srv.port, "127.0.0.3", "wkid", "late", PW)
        # from an address the source allows: the password is checked as usual
        assert scram_ok(srv.port, "127.0.0.2", "wkid", "late", PW)
        # and nothing opens the branch before the fork is complete: a command on it waits
        c = MongoClient(srv.uri, username="root", password="rootpw", authSource="admin", socketTimeoutMS=3000, serverSelectionTimeoutMS=5000)
        with pytest.raises((NetworkTimeout, AutoReconnect)):
            c["wkid"].c.find_one({})
        c.close()
        # the source is not affected by the list of the branch
        assert scram_ok(srv.port, "127.0.0.2", "wsrc", "u1", PW)
        assert not scram_ok(srv.port, "127.0.0.3", "wsrc", "u1", PW)
    finally:
        srv.kill9()


def test_source_without_a_list_gives_an_open_branch_during_the_fork():
    """Control: the check during the fork reads the source's list, so a source with no list stays open (same as after)."""
    srv = _auth_server("fwopen", "fork.after_clone")
    try:
        _source(srv, "osrc", None)
        threading.Thread(target=lambda: srv.admin.post("/v1/databases/osrc/fork", {"target": "okid"}, timeout=60), daemon=True).start()
        wait_until(lambda: "FAILPOINT fork.after_clone reached" in srv.log_text(), 30, what="failpoint")
        srv.root["okid"].command("createUser", "late", pwd=PW, roles=[{"role": "readWrite", "db": "okid"}])
        assert scram_ok(srv.port, "127.0.0.3", "okid", "late", PW)
    finally:
        srv.kill9()


def test_writes_to_the_branch_name_during_the_fork_are_never_lost():
    """A writer hammers the branch's name while the fork starts, 8 rounds. Either the fork wins: the branch is a complete
    copy of the source plus every acknowledged write (the writes waited for the branch's lock); or the writer wins: the
    fork is refused (409) and every acknowledged write is still there. On main the writer could create the database
    after the check that the name is free: the clone then failed with an internal error (500), and its rollback deleted
    the objects stored under that name."""
    srv = Server(make_bucket("fwrite"), "fwrite", idle=300).start()
    try:
        c = srv.client()
        assert srv.admin.post("/v1/databases", {"name": "bsrc"})[0] == 201
        c.bsrc.c.insert_many([{"_id": i, "pad": "p" * 200} for i in range(3000)])
        outcomes = []
        for rnd, delay in enumerate([0.0, 0.05, 0.15, 0.3, 0.5, 0.8, 1.1, 1.5]):
            acked, stop, go = [], [False], threading.Event()

            def writer():
                w = srv.client()
                w.admin.command("ping")
                go.wait()
                i = 100000
                while not stop[0]:
                    try:
                        w.bkid.c.insert_one({"_id": i, "w": 1})
                        acked.append(i)
                    except Exception:
                        pass
                    i += 1
                w.close()

            res = {}
            fk = threading.Thread(target=lambda: res.update(r=srv.admin.post("/v1/databases/bsrc/fork", {"target": "bkid"}, timeout=60)))
            th = threading.Thread(target=writer)
            th.start()
            time.sleep(0.5)
            fk.start()
            time.sleep(delay)
            go.set()
            fk.join(60)
            time.sleep(0.3)
            stop[0] = True
            th.join()
            st, r = res["r"]
            ids = {d["_id"] for d in c.bkid.c.find({}, {"_id": 1})}
            assert set(acked) <= ids, f"round {rnd}: {len(set(acked) - ids)} acknowledged writes lost (fork {st})"
            if st == 201:
                assert set(range(3000)) <= ids, "the branch lost documents of its source"
            else:
                assert st == 409, r
            assert c.bsrc.c.count_documents({}) == 3000, "a write meant for the branch reached the source"
            outcomes.append(st)
            assert srv.admin.delete("/v1/databases/bkid")[0] in (200, 202, 204)
        print("fork outcomes", outcomes)
    finally:
        srv.term()


def test_fork_and_rename_across_in_opposite_name_order_do_not_deadlock():
    """The fork takes the two entry locks in name order, like renameCollection across databases: a fork of "zsrc" into
    "atgt" and renames between the two, in parallel, all finish."""
    srv = Server(make_bucket("fdead"), "fdead", idle=300).start()
    try:
        c = srv.client()
        for n in range(4):
            src, tgt = f"zsrc{n}", f"atgt{n}"
            assert srv.admin.post("/v1/databases", {"name": src})[0] == 201
            c[src].c.insert_many([{"_id": i} for i in range(500)])
            c[src].r.insert_one({"_id": 1})
            res = {}

            def rename():
                try:
                    c.admin.command("renameCollection", f"{src}.r", to=f"{tgt}.r")
                    res["rename"] = "ok"
                except OperationFailure as e:
                    res["rename"] = e.code

            th = threading.Thread(target=rename)
            th.start()
            st, r = srv.admin.post(f"/v1/databases/{src}/fork", {"target": tgt}, timeout=60)
            th.join(60)
            assert not th.is_alive(), "deadlock between the fork and the rename"
            assert st in (201, 409), r
    finally:
        srv.term()


# ------------------------------------------------------------------------------------------------ 2. memory limit

@pytest.fixture(scope="module")
def plain():
    srv = Server(make_bucket("sortcap"), "sortcap", idle=300).start()
    srv.mc = srv.client()
    yield srv
    srv.mc.close()
    srv.term()


def test_blocking_sort_over_the_memory_limit_is_error_292(plain):
    c = plain.mc.cap.c
    rnd = random.Random(3)
    c.insert_many([{"_id": i, "k": rnd.random(), "g": i % 7, "pad": "x" * 80} for i in range(5000)])   # about 600 KB
    c.create_index("g")
    c.create_index("j")
    adm = plain.mc.admin
    assert adm.command("getParameter", 1, internalQueryMaxBlockingSortMemoryUsageBytes=1)["internalQueryMaxBlockingSortMemoryUsageBytes"] == 104857600
    old = adm.command("setParameter", 1, internalQueryMaxBlockingSortMemoryUsageBytes=200_000)["was"]
    try:
        assert adm.command("getParameter", 1, internalQueryMaxBlockingSortMemoryUsageBytes=1)["internalQueryMaxBlockingSortMemoryUsageBytes"] == 200_000
        for q in ({}, {"g": {"$gte": 0}}, {"$or": [{"g": 1}, {"g": {"$gte": 2}}, {"g": 0}]}):
            with pytest.raises(OperationFailure) as e:
                list(c.find(q).sort("k", 1))
            assert e.value.code == 292, e.value.details
            assert "Sort exceeded memory limit of 200000 bytes" in str(e.value)
        # allowDiskUse cannot help here: said in the message
        with pytest.raises(OperationFailure) as e:
            list(c.find({}).sort("k", -1).allow_disk_use(True))
        assert e.value.code == 292 and "allowDiskUse is not supported" in str(e.value)
        # the same in a transaction-free update path: a sort with a limit keeps only k documents, so it is fine
        assert len(list(c.find({}).sort("k", 1).limit(10))) == 10
        assert len(list(c.find({"g": {"$gte": 0}}).sort("k", 1).limit(10))) == 10
        # a limit whose documents themselves exceed the limit: also 292, as in mongod
        with pytest.raises(OperationFailure) as e:
            list(c.find({}).sort("k", 1).limit(4000))
        assert e.value.code == 292
        # a sort the plan gives (an index in order, or `_id`) is not a blocking sort: no limit
        assert len(list(c.find({}).sort("_id", 1))) == 5000
        c.update_many({}, [{"$set": {"j": "$k"}}])
        # (without a limit the planner prefers a scan and a sort; over the limit it reads the index in order instead)
        js = [d["j"] for d in c.find({}).sort("j", 1)]
        assert len(js) == 5000 and js == sorted(js)
        # a filter that keeps few documents is fine
        assert len(list(c.find({"g": 3, "k": {"$lt": 0.1}}).sort("k", 1))) > 0
    finally:
        adm.command("setParameter", 1, internalQueryMaxBlockingSortMemoryUsageBytes=old)
    # with the default limit (100 MB) the same sorts work
    assert len(list(c.find({}).sort("k", 1))) == 5000
    assert len(list(c.find({"g": {"$gte": 0}}).sort("k", 1))) == 5000


# ------------------------------------------------------------------------------------------------ 3. chunked candidates

N = 3000


def _docs():
    rnd = random.Random(11)
    out = []
    for i in range(N):
        out.append({"_id": i if i % 5 else f"s{i:05d}", "g": i % 13, "k": rnd.randrange(400), "u": rnd.random(),
                    "tags": [rnd.randrange(30) for _ in range(rnd.randrange(0, 4))], "h": rnd.choice([None, 1, "a", 2.5])})
    return out


def _load(c):
    c.insert_many(_docs())
    c.create_index("g")
    c.create_index("tags")
    c.create_index([("h", 1), ("g", -1)])


@pytest.fixture(scope="module")
def trio():
    """The same collection on four servers: default sizes (the whole list of candidates, as before); small chunks with
    point reads only; small chunks and the scan chosen at once (a small tenant: the cost rule picks it); small chunks,
    point reads for the first candidates, then the scan (forced after 120 candidates)."""
    small = {"MONGO_SORT_BUFFER_IDS": "50", "MONGO_SORT_CHUNK_IDS": "37"}
    servers = {}
    for name, env in [("ref", {}), ("points", dict(small, MONGO_NO_FETCH_SCAN="1")), ("scan", small),
                      ("switch", dict(small, MONGO_NO_FETCH_SCAN="1", MONGO_SORT_SCAN_AFTER="120"))]:
        port = free_port()
        s = Server(make_bucket(f"chunk{name}"), f"chunk{name}", idle=300, port=port, env=env,
                   extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
        s.mc = s.client()
        _load(s.mc.d.c)
        s.mc.d.command("tenantAdmin", close=True)   # into SSTs: the cost rule sees the tenant's size
        servers[name] = s
    yield servers
    for s in servers.values():
        s.mc.close()
        s.term()


QUERIES = [
    ({"g": {"$gte": 0}}, [("k", 1)]),
    ({"g": {"$in": [1, 2, 3, 7]}}, [("k", -1), ("u", 1)]),
    ({"tags": {"$in": [1, 2, 3, 4, 5, 6]}}, [("u", 1)]),              # multikey: a document comes once per matching element
    ({"tags": {"$gte": 3}}, [("k", 1), ("u", -1)]),
    ({"$or": [{"g": {"$lte": 6}}, {"tags": {"$in": [7, 8]}}, {"g": {"$gte": 4}}]}, [("u", -1)]),   # overlapping branches
    ({"h": {"$ne": None}, "g": {"$gt": 2}}, [("k", 1)]),
    ({"g": {"$gte": 0}, "k": {"$lt": 200}}, [("u", 1)]),
]


def _ids(cur):
    return [d["_id"] for d in cur]


@pytest.mark.parametrize("qi", range(len(QUERIES)))
def test_chunked_candidates_give_the_same_answers(trio, qi):
    q, sort = QUERIES[qi]
    ref = trio["ref"].mc.d.c
    decided = sort[-1][0] == "u"   # `u` is unique: the order is fully decided by the sort
    keys = lambda c_, ids: [tuple(c_.find_one({"_id": i}).get(f) for f, _ in sort) for i in ids]
    for name in ("points", "scan", "switch"):
        c = trio[name].mc.d.c
        for limit, skip in [(1, 0), (7, 0), (100, 0), (100, 250), (5000, 0), (0, 0), (0, 31)]:
            want = _ids(ref.find(q).sort(sort).skip(skip).limit(limit))
            got = _ids(c.find(q).sort(sort).skip(skip).limit(limit))
            if name == "points" or decided:
                # point reads keep plan order for documents equal in sort order: exactly the same answer
                assert got == want, (name, q, sort, limit, skip)
            else:
                # after a switch to the scan, documents equal in sort order may come in another order (mongod promises
                # none): the same sort keys in the same order, no document twice, every one a match
                assert keys(c, got) == keys(ref, want), (name, q, sort, limit, skip)
                assert len(set(got)) == len(got)
                assert all(c.count_documents({"$and": [q, {"_id": i}]}) == 1 for i in got[:50])


def test_chunked_candidates_inside_a_transaction(trio):
    """The transaction's own writes (updates, deletes, inserts) are seen, with chunks, exactly as with the whole list."""
    answers = {}
    for name, s in trio.items():
        c = s.mc.d.c
        with s.mc.start_session() as ses:
            ses.start_transaction()
            c.update_many({"g": 4}, {"$set": {"u": -1.0}}, session=ses)
            c.delete_many({"g": 5, "k": {"$lt": 300}}, session=ses)
            c.insert_many([{"_id": f"t{i}", "g": i % 13, "k": i, "u": 2 + i / 100, "tags": [i % 30]} for i in range(120)], session=ses)
            got = []
            for lim in (1, 9, 400, 0):
                got.append(_ids(c.find({"g": {"$gte": 0}}, session=ses).sort("u", 1).limit(lim)))
                got.append(_ids(c.find({"$or": [{"g": {"$lt": 5}}, {"tags": {"$in": [1, 2]}}]}, session=ses).sort("u", -1).limit(lim)))
            r = c.find_one_and_update({"g": {"$gte": 0}}, {"$set": {"picked": True}}, sort=[("u", 1)], session=ses)
            got.append([r["_id"]])
            ses.abort_transaction()
        answers[name] = got
    for name in ("points", "scan", "switch"):
        assert answers[name] == answers["ref"], name
