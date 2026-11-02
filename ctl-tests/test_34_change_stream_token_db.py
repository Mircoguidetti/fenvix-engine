"""Change-stream resume tokens carry the database (session M2, branch fix-cs-token).

Before: a resume token was only a position in the process-wide log of events, so a token of one database was accepted on
any other database of the same process. The case found by the reviewers of S14: a token of the source taken *after* a fork,
used on the fork, skipped in silence the fork's events between the fork and that point. The same with any two databases
(a token of `cx` used on `cy`): no error, events lost.

Now the token also holds a fingerprint of its database and branch (name, and for a fork made in this process, the position
where it was born). A stream of a database refuses a token of another database, or of another branch with the same name,
with ChangeStreamFatalError (280), as MongoDB does when the resumed event is not in the stream. Tokens of the whole cluster
(`allChangesForCluster`) are a position in every database and pass; a cluster stream takes a token of any database. Tokens of
the old layout (no database in them) are still read, with the old checks. A token from before a fork stays 286 (test_30).

Without the fix the 280 cases here are accepted (the stream opens and skips events); the others are the control.
"""
import pytest
from pymongo.errors import OperationFailure

from lib import Server, free_port, make_bucket

FATAL = 280


@pytest.fixture(scope="module")
def srv():
    port = free_port()
    bucket = make_bucket("cstoken")
    s = Server(bucket, "cstoken", idle=300, port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    s.mc = s.client()
    yield s
    s.mc.close()
    s.term()


def _new(srv, db):
    assert srv.admin.post("/v1/databases", {"name": db})[0] == 201


def _event_token(stream_owner, write):
    """Open a stream on `stream_owner` (a database or a collection), do `write`, return the token of the event it sees."""
    with stream_owner.watch() as s:
        write()
        return s.next()["_id"]


def _refused(target, token, code=FATAL, how="resume_after"):
    with pytest.raises(OperationFailure) as e:
        with target.watch(**{how: token}) as s:
            s.try_next()
    assert e.value.code == code, e.value.details
    return e.value


def test_source_token_after_the_fork_refused_on_the_fork(srv):
    """The case of S14: the source's token taken after the fork would skip the fork's own events."""
    mc = srv.mc
    _new(srv, "sp")
    mc.sp.c.insert_one({"_id": 0})
    assert srv.admin.post("/v1/databases/sp/fork", {"target": "sf"})[0] == 201
    with mc.sf.watch() as fs:
        mc.sf.c.insert_one({"_id": "fork-1"})   # a fork event after the fork...
        tf = fs.next()["_id"]
    tp = _event_token(mc.sp, lambda: mc.sp.c.insert_one({"_id": 1}))   # ...then a source token past it
    e = _refused(mc.sf, tp)
    assert e.details["codeName"] == "ChangeStreamFatalError"
    _refused(mc.sf.c, tp, how="start_after")
    _refused(mc.sf.c, tp)
    # the source resumes with its own token; the fork resumes with its own and gets everything after it
    with mc.sp.watch(resume_after=tp) as s:
        mc.sp.c.insert_one({"_id": 2})
        assert s.next()["documentKey"] == {"_id": 2}
    with mc.sf.watch(resume_after=tf) as s:
        mc.sf.c.insert_one({"_id": "fork-2"})
        assert s.next()["documentKey"] == {"_id": "fork-2"}


def test_token_of_another_database_refused_without_fork(srv):
    """The reviewer's case: any two databases of the process, no fork."""
    mc = srv.mc
    _new(srv, "cx")
    _new(srv, "cy")
    t = _event_token(mc.cx, lambda: mc.cx.c.insert_one({"_id": 1}))
    _refused(mc.cy, t)
    _refused(mc.cy.c, t)
    _refused(mc.cy.c, t, how="start_after")
    # a high-water-mark token (postBatchResumeToken, no event) of cx is refused on cy too
    with mc.cx.watch() as s:
        assert s.try_next() is None
        hwm = s.resume_token
    _refused(mc.cy, hwm)
    # and cx goes on with both
    with mc.cx.watch(resume_after=t) as s:
        mc.cx.c.insert_one({"_id": 2})
        assert s.next()["documentKey"] == {"_id": 2}
    with mc.cx.watch(resume_after=hwm) as s:
        assert s.next()["documentKey"] == {"_id": 2}


def test_same_database_tokens_between_stream_kinds(srv):
    """Database and collection streams of the same database share the identity: tokens move between them."""
    mc = srv.mc
    _new(srv, "cz")
    t_db = _event_token(mc.cz, lambda: mc.cz.a.insert_one({"_id": 1}))
    with mc.cz.b.watch(resume_after=t_db) as s:
        mc.cz.b.insert_one({"_id": 2})
        assert s.next()["documentKey"] == {"_id": 2}
    t_coll = _event_token(mc.cz.a, lambda: mc.cz.a.insert_one({"_id": 3}))
    with mc.cz.watch(resume_after=t_coll) as s:
        mc.cz.b.insert_one({"_id": 4})
        assert s.next()["documentKey"] == {"_id": 4}


def test_cluster_tokens(srv):
    """A cluster stream takes a token of any database; a database stream takes a cluster token (a position in all)."""
    mc = srv.mc
    _new(srv, "ca")
    _new(srv, "cb")
    t_a = _event_token(mc.ca, lambda: mc.ca.c.insert_one({"_id": 1}))
    with mc.watch(resume_after=t_a) as s:
        mc.cb.c.insert_one({"_id": 2})
        ev = s.next()
        assert (ev["ns"]["db"], ev["documentKey"]) == ("cb", {"_id": 2})
        t_cluster_event = ev["_id"]   # an event token of a cluster stream holds the event's database (cb)
    with mc.watch() as s:
        assert s.try_next() is None
        t_cluster_hwm = s.resume_token
    mc.ca.c.insert_one({"_id": 3})
    with mc.ca.watch(resume_after=t_cluster_hwm) as s:
        assert s.next()["documentKey"] == {"_id": 3}
    with mc.cb.watch(resume_after=t_cluster_event) as s:
        mc.cb.c.insert_one({"_id": 4})
        assert s.next()["documentKey"] == {"_id": 4}
    _refused(mc.ca, t_cluster_event)   # an event of cb is not a point of ca


def test_old_layout_still_read(srv):
    """A token of the old layout (no database: the first 26 characters) is still accepted, with the old checks."""
    mc = srv.mc
    _new(srv, "co")
    t = _event_token(mc.co, lambda: mc.co.c.insert_one({"_id": 1}))
    assert len(t["_data"]) == 44 and t["_data"][26:28] == "01"
    old = {"_data": t["_data"][:26]}
    with mc.co.watch(resume_after=old) as s:
        mc.co.c.insert_one({"_id": 2})
        assert s.next()["documentKey"] == {"_id": 2}
    with mc.co.c.watch(start_after=old) as s:
        assert s.next()["documentKey"] == {"_id": 2}


def test_tokens_sort_by_position(srv):
    """Drivers and apps may compare `_data`: later events still have larger tokens, across databases too."""
    mc = srv.mc
    _new(srv, "cs")
    toks = []
    with mc.watch() as s:
        for i in range(3):
            mc.cs.c.insert_one({"_id": i})
            mc.cx.c.insert_one({"_id": f"s{i}"})
        for _ in range(6):
            toks.append(s.next()["_id"]["_data"])
    assert toks == sorted(toks) and len(set(toks)) == 6


def test_branch_identity(srv):
    """A fork of a fork, and a fork reusing the name of a dropped fork, are other branches."""
    mc = srv.mc
    _new(srv, "gp")
    mc.gp.c.insert_one({"_id": 0})
    assert srv.admin.post("/v1/databases/gp/fork", {"target": "gf"})[0] == 201
    assert srv.admin.post("/v1/databases/gf/fork", {"target": "gg"})[0] == 201
    t_gf = _event_token(mc.gf, lambda: mc.gf.c.insert_one({"_id": 1}))
    _refused(mc.gg, t_gf)
    t_gg = _event_token(mc.gg, lambda: mc.gg.c.insert_one({"_id": 2}))
    _refused(mc.gf, t_gg)
    # drop the fork and fork again under the same name: a token of the first one is not of the second
    t_old = _event_token(mc.gg, lambda: mc.gg.c.insert_one({"_id": 3}))
    mc.drop_database("gg")
    assert srv.admin.post("/v1/databases/gp/fork", {"target": "gg"})[0] == 201
    with pytest.raises(OperationFailure) as e:
        with mc.gg.watch(resume_after=t_old) as s:
            s.try_next()
    assert e.value.code in (FATAL, 286), e.value.details   # 286: it is also from before this fork was born
    t_new = _event_token(mc.gg, lambda: mc.gg.c.insert_one({"_id": 4}))
    with mc.gg.watch(resume_after=t_new) as s:
        mc.gg.c.insert_one({"_id": 5})
        assert s.next()["documentKey"] == {"_id": 5}


def test_malformed_tokens(srv):
    mc = srv.mc
    _new(srv, "cm")
    t = _event_token(mc.cm, lambda: mc.cm.c.insert_one({"_id": 1}))["_data"]
    for bad in (t[:40], t + "00", t[:26] + "07" + t[28:], t[:26] + "00" + t[28:] if t[28:] != "0" * 16 else t[:26] + "09" + t[28:]):
        _refused(mc.cm, {"_data": bad}, code=260)
