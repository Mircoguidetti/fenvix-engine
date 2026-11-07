"""Cursor batches hold at most 16 MiB of documents, like mongod (found by the checkpoint-history task, 2026-11-07).

Before: a `getMore` without `batchSize` returned every document left in one reply. The LangGraph MongoDB checkpointer's
`list` of a thread with 10,000 checkpoints of about 160 kB asked for 1.6 GB in one message and pymongo refused it
("Message length (1609330630) is larger than server max message size (48000000)"). After: every batch (first or next)
stops before it would pass 16 MiB, holds at least one document, and the cursor returns every document once, in order."""
import pytest

from lib import Server, free_port, make_bucket

MIB = 1 << 20


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("batchbytes"), "batchbytes", idle=600, port=free_port()).start()
    s.mc = s.client()
    yield s
    s.mc.close()
    s.term()


def test_a_cursor_of_large_documents_comes_in_batches_of_at_most_16_mib(srv):
    c = srv.mc["batchbytes"]["big"]
    c.drop()
    n = 200  # 200 documents of 1 MiB: 200 MiB, the first batch (101 by default) and the getMore must both be cut
    for i in range(0, n, 10):
        c.insert_many([{"_id": i + k, "pad": "x" * MIB} for k in range(10)])
    db = srv.mc["batchbytes"]
    r = db.command("find", "big", sort={"_id": 1})
    first = r["cursor"]["firstBatch"]
    assert 1 <= len(first) <= 16
    got = [d["_id"] for d in first]
    cid = r["cursor"]["id"]
    while cid:
        r = db.command("getMore", cid, collection="big")
        batch = r["cursor"]["nextBatch"]
        assert 1 <= len(batch) <= 16
        got += [d["_id"] for d in batch]
        cid = r["cursor"]["id"]
    assert got == list(range(n))
    # the driver's own iteration (no batchSize) sees every document once
    assert [d["_id"] for d in c.find({}, {"pad": 0}, sort=[("_id", 1)])] == list(range(n))
    assert sum(1 for _ in c.find({}, sort=[("_id", 1)])) == n


def test_a_document_larger_than_the_cap_still_comes_alone(srv):
    c = srv.mc["batchbytes"]["one"]
    c.drop()
    c.insert_many([{"_id": i, "pad": "y" * (15 * MIB)} for i in range(3)])
    assert [d["_id"] for d in c.find({}, sort=[("_id", 1)])] == [0, 1, 2]


def test_small_documents_keep_the_count_limits(srv):
    c = srv.mc["batchbytes"]["small"]
    c.drop()
    c.insert_many([{"_id": i} for i in range(500)])
    r = srv.mc["batchbytes"].command("find", "small")
    assert len(r["cursor"]["firstBatch"]) == 101
    r2 = srv.mc["batchbytes"].command("getMore", r["cursor"]["id"], collection="small")
    assert len(r2["cursor"]["nextBatch"]) == 399 and r2["cursor"]["id"] == 0
