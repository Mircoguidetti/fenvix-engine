"""An open change stream that falls behind the in-memory change log (fix-query-defects P1).

Found by the review of fix-query-limits (`probes/cs_probe.py`): a change stream opened, then 12 inserts of 2 MB; the
stream delivered 5 of 12 events, then empty getMores and no error. The change log (`changes.rs`) keeps at most
`--change-log-max-mb` and drops the oldest events; only a resume checked whether its position was still held. Now an
open stream whose position was dropped fails with ChangeStreamHistoryLost (286), as mongod fails a stream whose oplog
position is gone. A stream never delivers a later event after a gap.

Each test fails when a stream delivers fewer events than were written without the error, or skips an event.
"""
import time

import pytest
from bson import SON
from pymongo.errors import OperationFailure

from lib import Server, make_bucket

LOG_MB = 4


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("csbehind"), "csbehind", idle=600, extra=["--change-log-max-mb", str(LOG_MB)]).start()
    yield s
    s.term()


def drain(d, cid, n, secs=30):
    """getMore until `n` events, the error, or `secs`: (event _ids in order, error code or None)."""
    got, t0 = [], time.time()
    while len(got) < n and time.time() - t0 < secs:
        try:
            g = d.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 500)]))
        except OperationFailure as e:
            return got, e.code
        got += [e["documentKey"]["_id"] for e in g["cursor"]["nextBatch"]]
        if g["cursor"]["id"] == 0:
            break
    return got, None


def test_a_stream_that_falls_behind_fails_and_skips_nothing(srv):
    d = srv.client().cs1
    d.create_collection("cs")
    r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]))
    cid = r["cursor"]["id"]
    n = 12
    for i in range(n):
        d.cs.insert_one({"_id": i, "p": "x" * (1 << 20)})   # each event counts about 3 MB of the 4 MB log
    got, err = drain(d, cid, n)
    print(f"delivered {got}, error {err}")
    assert got == list(range(len(got))), f"the stream skipped events: {got}"
    assert len(got) == n or err == 286, f"{len(got)} of {n} events and no ChangeStreamHistoryLost"
    assert err == 286, "the case was not reached: the log held every event"
    # the cursor is gone after the error
    with pytest.raises(OperationFailure) as e:
        d.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 100)]))
    assert e.value.code == 43, e.value


def test_a_stream_that_keeps_up_gets_every_event(srv):
    d = srv.client().cs2
    d.create_collection("cs")
    r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]))
    cid = r["cursor"]["id"]
    got = []
    for i in range(12):
        d.cs.insert_one({"_id": i, "p": "x" * (1 << 20)})
        g, err = drain(d, cid, 1, secs=5)
        assert err is None, err
        got += g
    print(f"delivered {got}")
    assert got == list(range(12))


def test_start_at_an_operation_time_no_longer_held_fails(srv):
    c = srv.client()
    d = c.cs3
    d.create_collection("cs")
    r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]))
    d.cs.insert_one({"_id": "mark"})
    g = d.command(SON([("getMore", r["cursor"]["id"]), ("collection", "cs"), ("maxTimeMS", 2000)]))
    ts = g["cursor"]["nextBatch"][0]["clusterTime"]
    for i in range(6):
        d.cs.insert_one({"_id": i, "p": "x" * (1 << 20)})   # pushes "mark" out of the 4 MB log
    r = d.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {"startAtOperationTime": ts}}]), ("cursor", {})]))
    got, err = drain(d, r["cursor"]["id"], 7, secs=10)
    print(f"start at {ts}: delivered {got}, error {err}")
    assert err == 286, f"delivered {got} without ChangeStreamHistoryLost"
