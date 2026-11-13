"""command-cpu-on-i4 (coordinator, 2026-11-13): every cursor batch carries at most 16 MiB of documents (mongod's rule,
cmd::BATCH_BYTES), whichever path builds it. The review of command-cpu saw a 60 MB tailable batch and a 90 MB first
batch of a plain find. One test per batch builder, each with documents of about 1 MB (40 of them, 40 MB, so an uncut
batch would be 2.5 times the limit): every batch holds at most 16 MiB of documents and every document arrives exactly
once, in order.

Builders: find whole answer (sorted, cmd::cursor_reply_open) and its getMore (cmd::get_more); aggregate (first batch);
the streamed natural-order find (cmd_data::find_streamed: its first batch was not cut) and its getMore
(cmd::stream_get_more); the tailable find on a capped collection (cmd_data -> capped::fetch_after, first and getMore);
a change stream's getMore (cmd_changes). Our server only: the rule is mongod's, measured by fix-query-limits."""
import os

import bson
import pytest
from bson import SON

from lib import Server, make_bucket, free_port

N = 40
DOC = 1_000_000
CAP = 16 * 1024 * 1024


@pytest.fixture(scope="module")
def srv():
    p = free_port()
    s = Server(make_bucket("bcap"), "bcap", idle=600, port=p,
               extra=["--replica-set", "rs0", "--advertise", f"localhost:{p}", "--change-log-max-mb", "256"]).start()
    yield s
    s.term()


def big(i):
    return {"_id": i, "p": "x" * DOC}


def batch_bytes(batch):
    return len(bson.encode({"b": batch})) - 4 - 1 - 2   # the array alone (as the reply carries it)


def drain(db, reply, coll, getmore_extra=()):
    """Every batch of a cursor: (sizes in bytes, the _ids in order)."""
    cur = reply["cursor"]
    sizes, ids = [], []
    batch = cur["firstBatch"]
    while True:
        sizes.append(batch_bytes(batch))
        ids += [d["_id"] for d in batch]
        if cur["id"] == 0:
            break
        r = db.command(SON([("getMore", cur["id"]), ("collection", coll)] + list(getmore_extra)))
        cur = r["cursor"]
        batch = cur["nextBatch"]
        if not batch and cur["id"] != 0 and len(ids) >= N:
            db.command("killCursors", coll, cursors=[cur["id"]])
            break
    return sizes, ids


def check(sizes, ids, want_ids=None):
    want = want_ids if want_ids is not None else list(range(N))
    assert ids == want, (len(ids), ids[:5])
    assert max(sizes) <= CAP, f"a batch of {max(sizes)} bytes (cap {CAP}): {sizes}"
    assert len([s for s in sizes if s > 0]) >= 3, f"the documents were not cut into batches: {sizes}"


def load(db, coll, **opts):
    db.drop_collection(coll)
    if opts:
        db.create_collection(coll, **opts)
    for i in range(0, N, 4):
        db[coll].insert_many([big(j) for j in range(i, min(N, i + 4))])


def test_find_whole_answer_and_its_getmore(srv):
    db = srv.client().bcap
    load(db, "w")
    sizes, ids = drain(db, db.command(SON([("find", "w"), ("sort", {"_id": 1})])), "w")
    check(sizes, ids)


def test_aggregate_first_batch(srv):
    db = srv.client().bcap
    load(db, "a")
    sizes, ids = drain(db, db.command(SON([("aggregate", "a"), ("pipeline", [{"$sort": {"_id": 1}}]), ("cursor", {})])), "a")
    check(sizes, ids)


def test_streamed_find_first_batch_and_getmore(srv):
    """The natural-order find answered page by page (find_streamed): no sort, no collation. Its first batch took the
    asked number of documents (101 by default) whatever their size: the 90 MB of the review."""
    db = srv.client().bcap
    load(db, "s")
    r = db.command(SON([("find", "s"), ("filter", {})]))
    assert batch_bytes(r["cursor"]["firstBatch"]) <= CAP, batch_bytes(r["cursor"]["firstBatch"])
    sizes, ids = drain(db, r, "s")
    check(sizes, sorted(ids))   # natural order: compare the set in _id order


def test_tailable_find_first_batch_and_getmore(srv):
    db = srv.client().bcap
    load(db, "t", capped=True, size=200 * 1024 * 1024)
    r = db.command(SON([("find", "t"), ("tailable", True)]))
    sizes, ids = drain(db, r, "t")
    check(sizes, ids)


def test_change_stream_getmore(srv):
    db = srv.client().bcap
    db.drop_collection("cs")
    db.create_collection("cs")
    r = db.command(SON([("aggregate", "cs"), ("pipeline", [{"$changeStream": {}}]), ("cursor", {})]))
    cid = r["cursor"]["id"]
    for i in range(0, N, 4):
        db.cs.insert_many([big(j) for j in range(i, min(N, i + 4))])
    sizes, ids = [], []
    for _ in range(60):
        g = db.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 500)]))
        b = g["cursor"]["nextBatch"]
        sizes.append(batch_bytes(b))
        ids += [e["documentKey"]["_id"] for e in b]
        if len(ids) >= N:
            break
    db.command("killCursors", "cs", cursors=[cid])
    check(sizes, ids)
