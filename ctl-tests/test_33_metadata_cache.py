"""The metadata cache of the engine (perf-letture, `dbs::META_CACHE`): an open database keeps its collection list, index
definitions and collection options in memory instead of reading them from storage on every command. These tests fail if
the cache is not emptied when the metadata change: every one first reads the collection (so the cache holds the old
values), then changes collections, indexes or options, then checks that the next read sees the change.

Measured with a binary whose apply_mode does not empty the cache: the tests below fail (see docs/perf-letture.md).
"""
import signal
import time

import pytest
from pymongo.errors import OperationFailure

from lib import Server, free_port, make_bucket


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("metacache"), "metacache", idle=600).start()
    s.mc = s.client()
    yield s
    s.mc.close()
    s.term()


def _hint_works(coll, index_name):
    try:
        list(coll.find({"a": 1}).hint(index_name))
        return True
    except OperationFailure as e:
        assert "hint" in str(e).lower(), e
        return False


def _warm(coll):
    """Reads that fill the cache with the collection's current metadata."""
    for _ in range(3):
        list(coll.find({"_id": 1}))
        list(coll.find({"a": 1}))


def test_create_index_is_seen_by_the_next_query(srv):
    db = srv.mc["mc_ix"]
    c = db["c"]
    c.drop()
    c.insert_many([{"_id": i, "a": i % 3} for i in range(20)])
    _warm(c)
    assert not _hint_works(c, "a_1")
    c.create_index("a")
    assert _hint_works(c, "a_1"), "the query after createIndexes does not know the new index"


def test_dropped_index_is_gone_for_the_next_query(srv):
    db = srv.mc["mc_ix"]
    c = db["d"]
    c.drop()
    c.insert_many([{"_id": i, "a": i % 3} for i in range(20)])
    c.create_index("a")
    _warm(c)
    assert _hint_works(c, "a_1")
    c.drop_index("a_1")
    assert not _hint_works(c, "a_1"), "the query after dropIndexes still uses the dropped index"


def test_index_changes_in_a_loop(srv):
    """Create and drop the same index many times, each time checked by the very next query of another client."""
    other = srv.client()
    try:
        c = srv.mc["mc_loop"]["c"]
        c2 = other["mc_loop"]["c"]
        c.drop()
        c.insert_many([{"_id": i, "a": i % 3} for i in range(10)])
        for i in range(15):
            _warm(c2)
            c.create_index("a")
            assert _hint_works(c2, "a_1"), f"round {i}: index created, not seen"
            _warm(c2)
            c.drop_index("a_1")
            assert not _hint_works(c2, "a_1"), f"round {i}: index dropped, still used"
    finally:
        other.close()


def test_dropped_collection_does_not_hide_a_view_of_the_same_name(srv):
    """The collection list decides whether a name is a collection or a view (a collection of that name wins)."""
    db = srv.mc["mc_view"]
    for n in ("x", "src", "system.views"):
        db.drop_collection(n)
    db["src"].insert_many([{"_id": i, "k": "view"} for i in range(3)])
    db["x"].insert_one({"_id": 100, "k": "collection"})
    _warm(db["x"])
    assert [d["k"] for d in db["x"].find()] == ["collection"]
    db.drop_collection("x")
    assert "x" not in db.list_collection_names()
    assert list(db["x"].find()) == [], "the dropped collection still answers"
    db.create_collection("x", viewOn="src", pipeline=[{"$match": {}}])
    got = sorted(d["_id"] for d in db["x"].find())
    assert got == [0, 1, 2], f"after the drop, the name still reads as the old collection: {got}"


def test_collection_recreated_without_its_collation(srv):
    """The collection options (default collation) are read on every find: a recreated collection must not keep the old ones."""
    db = srv.mc["mc_coll"]
    db.drop_collection("c")
    db.create_collection("c", collation={"locale": "en", "strength": 2})
    db["c"].insert_one({"_id": 1, "s": "ABC"})
    for _ in range(3):
        assert len(list(db["c"].find({"s": "abc"}))) == 1   # case-insensitive default collation
    db.drop_collection("c")
    db.create_collection("c")
    db["c"].insert_one({"_id": 1, "s": "ABC"})
    assert list(db["c"].find({"s": "abc"})) == [], "the recreated collection still has the old default collation"


def test_coll_mod_validator_is_applied_at_once(srv):
    db = srv.mc["mc_mod"]
    db.drop_collection("c")
    db.create_collection("c")
    db["c"].insert_one({"_id": 1, "n": 1})
    _warm(db["c"])
    db.command("collMod", "c", validator={"n": {"$type": "int"}})
    with pytest.raises(OperationFailure):
        db["c"].insert_one({"_id": 2, "n": "not a number"})
    db.command("collMod", "c", validator={})
    db["c"].insert_one({"_id": 3, "n": "now allowed"})


def test_rename_collection_moves_its_indexes(srv):
    db = srv.mc["mc_ren"]
    for n in ("a", "b"):
        db.drop_collection(n)
    db["a"].insert_many([{"_id": i, "a": i % 3} for i in range(10)])
    db["a"].create_index("a")
    _warm(db["a"])
    _warm(db["b"])
    assert _hint_works(db["a"], "a_1") and not _hint_works(db["b"], "a_1")
    db["a"].rename("b")
    assert "a" not in db.list_collection_names()
    assert list(db["a"].find()) == []
    assert _hint_works(db["b"], "a_1"), "the renamed collection lost its index"
    assert len(list(db["b"].find({"a": 1}))) == 3


def test_drop_database_forgets_the_metadata(srv):
    db = srv.mc["mc_dropdb"]
    db.drop_collection("c")
    db["c"].insert_many([{"_id": i, "a": i % 3} for i in range(10)])
    db["c"].create_index("a")
    _warm(db["c"])
    assert _hint_works(db["c"], "a_1")
    srv.mc.drop_database("mc_dropdb")
    assert list(db["c"].find()) == []
    db["c"].insert_one({"_id": 1, "a": 1})
    assert not _hint_works(db["c"], "a_1"), "after dropDatabase the old index is still used"


def test_reopen_after_idle_close():
    """A database closed for idleness and opened again starts with an empty cache (the stored metadata are read again)."""
    s = Server(make_bucket("metacache-idle"), "metacache-idle", idle=2).start()
    try:
        mc = s.client()
        c = mc["mc_idle"]["c"]
        c.insert_many([{"_id": i, "a": i % 3} for i in range(10)])
        c.create_index("a")
        _warm(c)
        assert _hint_works(c, "a_1")
        time.sleep(6)   # closed for idleness (idle 2 s, the reaper looks every 0.5 s)
        assert _hint_works(c, "a_1")
        c.drop_index("a_1")
        assert not _hint_works(c, "a_1")
        mc.close()
    finally:
        s.term()


def test_other_process_changes_metadata_while_this_one_is_stopped():
    """Process A caches the metadata, is stopped past its lease, process B takes the database over and creates an index,
    B stops, A resumes: A must not answer with the metadata it cached before (it lost the database in between)."""
    bucket = make_bucket("metacache-takeover")
    a = Server(bucket, "metacache-a", idle=600).start()
    b = None
    try:
        ma = a.client(retryReads=True)
        c = ma["mc_take"]["c"]
        c.insert_many([{"_id": i, "a": i % 3} for i in range(10)])
        _warm(c)
        assert not _hint_works(c, "a_1")
        a.proc.send_signal(signal.SIGSTOP)
        time.sleep(15)      # A's lease (6 s) expires
        b = Server(bucket, "metacache-b", idle=600).start()
        mb = b.client()
        deadline = time.time() + 60
        while True:
            try:
                mb["mc_take"]["c"].create_index("a")
                break
            except OperationFailure:
                if time.time() > deadline:
                    raise
                time.sleep(0.5)
        assert _hint_works(mb["mc_take"]["c"], "a_1")
        mb.close()
        b.term()
        b = None
        a.proc.send_signal(signal.SIGCONT)
        deadline = time.time() + 60
        while True:
            try:
                ok = _hint_works(c, "a_1")
                break
            except Exception:
                # the first commands may be refused while A finds out it lost the database and takes it back
                if time.time() > deadline:
                    raise
                time.sleep(0.5)
        assert ok, "A answered with the index list it cached before B created the index"
        ma.close()
    finally:
        if a.proc and a.proc.poll() is None:
            a.proc.send_signal(signal.SIGCONT)
        a.term()
        if b:
            b.term()
