"""Two bugs found by the reviewers (session S14, branch fix-ip-collation).

1. Security: a fork started with a fresh registry record and no IP allow list, so the fork of a database closed to a list of
   networks was reachable from anywhere (only the password protected it). Now the fork copies its source's list (TLS is per
   listener: a fork has the same TLS mode as its source anyway). Two small fixes of the same kind ride along: the fork gets
   a copy of its source's TTL hints (`ttl-hints/{db}/`), and a change-stream resume token from before the fork (the source's)
   is refused on the fork with ChangeStreamHistoryLost (286) instead of silently skipping history.
2. Data: inside a transaction the engine did not open the collation scope that it opens outside one (cmd.rs, before route),
   so a transaction compared strings with the simple collation: the collection's default collation and a command's explicit
   collation were ignored. Each case below runs outside and inside a transaction and must give the same answer.

Without the fix every test here fails, except the "outside a transaction" collation cases, which are the control.
"""
import shlex
import time

import pytest
from pymongo import ReturnDocument
from pymongo.errors import OperationFailure

from lib import BIN, Server, free_port, list_prefix, make_bucket, wait_until
from netlib import scram_ok

PW = "Fork-SECRET-3b9d0e15"
NO_LIMITS = ["--auth-fail-max", "1000000", "--auth-fail-max-per-db", "1000000"]
EN2 = {"locale": "en", "strength": 2}   # upper case = lower case


# ------------------------------------------------------------------------------------------------ 1. fork restrictions

@pytest.fixture(scope="module")
def auth_srv():
    bucket = make_bucket("forklist")
    srv = Server(bucket, "forklist", idle=300, extra=["--auth", "--trusted-networks", "127.0.0.1/32"] + NO_LIMITS).start()
    c = srv.client()
    try:
        c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])  # localhost exception
    except OperationFailure:
        pass
    c.close()
    srv.root = srv.client(username="root", password="rootpw", authSource="admin")
    yield srv
    srv.root.close()
    srv.term()


def _db_with_user(srv, db):
    st, r = srv.admin.post("/v1/databases", {"name": db})
    assert st == 201, r
    srv.root[db].command("createUser", "u1", pwd=PW, roles=[{"role": "readWrite", "db": db}])
    srv.root[db].c.insert_one({"x": 1})


def _list(srv, db):
    st, r = srv.admin.get(f"/v1/databases/{db}/ip-allowlist")
    assert st == 200, r
    return r["entries"]


def _fork(srv, src, target, mode="clone"):
    st, r = srv.admin.post(f"/v1/databases/{src}/fork", {"target": target, "mode": mode})
    assert st == 201, r
    srv.root[target].command("createUser", "u1", pwd=PW, roles=[{"role": "readWrite", "db": target}])   # a fork has no users
    return r


@pytest.mark.parametrize("mode", ["clone", "copy"])
def test_fork_inherits_the_allow_list(auth_srv, mode):
    srv = auth_srv
    p, f = f"lp_{mode}", f"lf_{mode}"
    _db_with_user(srv, p)
    assert srv.admin.put(f"/v1/databases/{p}/ip-allowlist", {"entries": ["127.0.0.2/32", "10.0.0.0/8"]})[0] == 200
    _fork(srv, p, f, mode)
    assert _list(srv, f) == ["127.0.0.2/32", "10.0.0.0/8"]
    # reachable only from the same addresses as the source
    assert scram_ok(srv.port, "127.0.0.2", f, "u1", PW)
    assert not scram_ok(srv.port, "127.0.0.3", f, "u1", PW)
    assert not scram_ok(srv.port, "127.0.0.3", p, "u1", PW)


def test_fork_of_a_fork_and_independence(auth_srv):
    srv = auth_srv
    _db_with_user(srv, "lp_chain")
    assert srv.admin.put("/v1/databases/lp_chain/ip-allowlist", {"entries": ["127.0.0.2/32"]})[0] == 200
    _fork(srv, "lp_chain", "lc_1")
    _fork(srv, "lc_1", "lc_2")
    assert _list(srv, "lc_2") == ["127.0.0.2/32"]
    assert not scram_ok(srv.port, "127.0.0.3", "lc_2", "u1", PW)
    assert scram_ok(srv.port, "127.0.0.2", "lc_2", "u1", PW)
    # after the fork, each list is its own: changing the source does not change the fork
    assert srv.admin.put("/v1/databases/lp_chain/ip-allowlist", {"entries": ["127.0.0.4/32"]})[0] == 200
    assert _list(srv, "lc_1") == ["127.0.0.2/32"]
    # and the list survives the fork's sleep and a restart (it is in the stored record, not only in the cache)
    srv.restart()
    srv.root = srv.client(username="root", password="rootpw", authSource="admin")
    assert not scram_ok(srv.port, "127.0.0.3", "lc_1", "u1", PW)
    assert scram_ok(srv.port, "127.0.0.2", "lc_1", "u1", PW)


def test_fork_without_a_list_stays_open(auth_srv):
    srv = auth_srv
    _db_with_user(srv, "lp_open")
    _fork(srv, "lp_open", "lf_open")
    assert _list(srv, "lf_open") == []
    assert scram_ok(srv.port, "127.0.0.3", "lf_open", "u1", PW)


def _with_ttl(tmp):
    """The harness starts every server with --no-ttl; this wrapper drops that flag (and execs, so the pid is the server's)."""
    w = tmp / "mongo-server-with-ttl"
    w.write_text('#!/bin/bash\nargs=()\nfor a in "$@"; do [ "$a" = --no-ttl ] || args+=("$a"); done\n'
                 f'exec {shlex.quote(BIN)} "${{args[@]}}"\n')
    w.chmod(0o755)
    return str(w)


@pytest.fixture(scope="module")
def rs_srv(tmp_path_factory):
    port = free_port()
    bucket = make_bucket("forkttl")
    s = Server(bucket, "forkttl", idle=3, port=port, binary=_with_ttl(tmp_path_factory.mktemp("ttl")),
               extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}", "--ttl-wake-sleeping",
                      "--ttl-maintenance-secs", "1", "--ttl-min-gap-secs", "0"]).start()
    s.bucket = bucket
    s.mc = s.client()
    yield s
    s.mc.close()
    s.term()


def test_fork_copies_the_ttl_hints(rs_srv):
    srv = rs_srv
    import datetime
    assert srv.admin.post("/v1/databases", {"name": "tp"})[0] == 201
    c = srv.mc.tp.c
    c.create_index("at", expireAfterSeconds=0)
    soon = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=4)
    c.insert_many([{"_id": 1, "at": soon}, {"_id": 2, "keep": True}])
    st, r = srv.admin.post("/v1/databases/tp/fork", {"target": "tf"})   # closes the source: its hint is written
    assert st == 201, r
    src = sorted(k.split("/", 2)[2] for k, _ in list_prefix(srv.bucket, "ttl-hints/tp/"))
    dst = sorted(k.split("/", 2)[2] for k, _ in list_prefix(srv.bucket, "ttl-hints/tf/"))
    assert src and dst == src, (src, dst)
    # the maintenance pass (every second here) then wakes the sleeping fork, deletes its expired document and, at the
    # close, removes the hint (nothing else expires). Not the `ttlPass` command: it answers "db is closed" even when it
    # works, on a plain database too (an old bug, not of this branch).
    wait_until(lambda: not list_prefix(srv.bucket, "ttl-hints/tf/"), timeout=30, what="the fork's expired document swept")
    assert sorted(d["_id"] for d in srv.mc.tf.c.find()) == [2]


def test_source_resume_token_refused_on_the_fork(rs_srv):
    srv = rs_srv
    assert srv.admin.post("/v1/databases", {"name": "cp"})[0] == 201
    with srv.mc.cp.watch() as s:
        srv.mc.cp.c.insert_one({"_id": 1})
        ev = s.next()
        token = ev["_id"]
    assert ev["documentKey"] == {"_id": 1}
    srv.mc.cp.c.insert_one({"_id": 1.5})   # the source moves on after the token: resuming the fork there would skip it
    assert srv.admin.post("/v1/databases/cp/fork", {"target": "cf"})[0] == 201
    with pytest.raises(OperationFailure) as e:
        with srv.mc.cf.watch(resume_after=token) as s:
            s.try_next()
    assert e.value.code == 286, e.value.details
    with pytest.raises(OperationFailure) as e:
        with srv.mc.cf.c.watch(start_after=token) as s:
            s.try_next()
    assert e.value.code == 286, e.value.details
    # the source still resumes with its own token, and the fork's own tokens work on the fork
    with srv.mc.cp.watch(resume_after=token) as s:
        assert s.next()["documentKey"] == {"_id": 1.5}
    with srv.mc.cf.watch() as s:
        srv.mc.cf.c.insert_one({"_id": 3})
        t2 = s.next()["_id"]
    with srv.mc.cf.watch(resume_after=t2) as s:
        srv.mc.cf.c.insert_one({"_id": 4})
        assert s.next()["documentKey"] == {"_id": 4}


# ------------------------------------------------------------------------------------------------ 2. collation in transactions

def _run(srv, in_txn, fn):
    if not in_txn:
        return fn(None)
    with srv.mc.start_session() as s:
        s.start_transaction()
        r = fn(s)
        s.commit_transaction()
    return r


def _coll(srv, name, collation):
    db = srv.mc["colltxn"]
    db.drop_collection(name)
    if collation:
        db.create_collection(name, collation=collation)
    c = db[name]
    c.insert_many([{"_id": 1, "n": "a"}, {"_id": 2, "n": "B"}])
    return c


IN_TXN = [pytest.param(False, id="outside"), pytest.param(True, id="in_txn")]


@pytest.mark.parametrize("in_txn", IN_TXN)
def test_default_collation_filter(rs_srv, in_txn):
    c = _coll(rs_srv, f"f_{in_txn}", EN2)
    r = _run(rs_srv, in_txn, lambda s: c.update_one({"n": "b"}, {"$set": {"hit": 1}}, session=s))
    assert r.matched_count == 1 and r.modified_count == 1
    assert c.find_one({"hit": 1})["_id"] == 2
    # the reads of a transaction too
    assert _run(rs_srv, in_txn, lambda s: c.count_documents({"n": "b"}, session=s)) == 1
    assert _run(rs_srv, in_txn, lambda s: [d["_id"] for d in c.find({}, session=s).sort("n", 1)]) == [1, 2]


@pytest.mark.parametrize("in_txn", IN_TXN)
def test_default_collation_update_sort(rs_srv, in_txn):
    c = _coll(rs_srv, f"s_{in_txn}", EN2)
    r = _run(rs_srv, in_txn, lambda s: c.update_one({}, {"$set": {"hit": 1}}, sort={"n": 1}, session=s))
    assert r.modified_count == 1
    assert c.find_one({"hit": 1})["_id"] == 1      # "a" before "B" under en; the simple collation puts "B" first


@pytest.mark.parametrize("in_txn", IN_TXN)
def test_explicit_collation_find_one_and_update(rs_srv, in_txn):
    c = _coll(rs_srv, f"x_{in_txn}", None)       # no default: only the command's collation applies
    d = _run(rs_srv, in_txn, lambda s: c.find_one_and_update({"n": "b"}, {"$set": {"hit": 1}}, collation=EN2,
                                                             return_document=ReturnDocument.AFTER, session=s))
    assert d is not None and d["_id"] == 2
    d = _run(rs_srv, in_txn, lambda s: c.find_one_and_update({}, {"$set": {"first": 1}}, sort=[("n", 1)], collation=EN2,
                                                             return_document=ReturnDocument.AFTER, session=s))
    assert d["_id"] == 1
    # and without a collation the simple one is used, in a transaction as outside
    d = _run(rs_srv, in_txn, lambda s: c.find_one_and_update({"n": "b"}, {"$set": {"none": 1}}, session=s))
    assert d is None


@pytest.mark.parametrize("in_txn", IN_TXN)
def test_explicit_collation_on_a_collection_with_a_default(rs_srv, in_txn):
    c = _coll(rs_srv, f"o_{in_txn}", EN2)
    # an explicit simple collation overrides the collection's default
    r = _run(rs_srv, in_txn, lambda s: c.update_one({"n": "b"}, {"$set": {"hit": 1}}, collation={"locale": "simple"}, session=s))
    assert r.matched_count == 0
    r = _run(rs_srv, in_txn, lambda s: c.delete_one({"n": "A"}, session=s))
    assert r.deleted_count == 1
    assert [d["_id"] for d in c.find()] == [2]
