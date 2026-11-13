"""P-command-cpu (2026-11-13): the command path's CPU fixes, seen from a client. Every answer must stay exactly as before.

  - batches of a cursor carry at most 16 MiB of documents (mongod's rule): a whole-history list of a thread whose
    checkpoints together exceed the driver's 48 MB message limit is read in full. Before the fix the getMore without a
    batchSize returned every remaining document in one message (70 MB measured on the dev bench), and the driver failed
    with "Message length ... is larger than server max message size".
"""
import pytest

from lib import Server, make_bucket


@pytest.fixture(scope="module")
def srv():
    bucket = make_bucket("cmdcpu")
    s = Server(bucket, "cmdcpu", idle=600).start()
    yield s
    s.term()


def test_batches_stay_under_16_mib(srv):
    c = srv.client()
    db = c.app
    db.checkpoints.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1)], unique=True)
    # 40 checkpoints of 1.5 MB: 60 MB in all, more than the 48 MB a driver accepts in one message
    body = b"x" * (1_500_000)
    n = 40
    for s in range(n):
        db.checkpoints.update_one({"thread_id": "t1", "checkpoint_ns": "", "checkpoint_id": f"c{s:04d}"},
                                  {"$set": {"checkpoint": body, "step": s}}, upsert=True)
    got = list(db.checkpoints.find({"thread_id": "t1", "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)]))
    assert [d["step"] for d in got] == list(range(n - 1, -1, -1))
    assert all(d["checkpoint"] == body for d in got)
    # the raw batches: the first holds at most 16 MiB, every getMore too, and together they hold every document
    r = db.command("find", "checkpoints", filter={"thread_id": "t1"}, sort={"checkpoint_id": 1})
    sizes = [len(r["cursor"]["firstBatch"])]
    cid = r["cursor"]["id"]
    while cid:
        m = db.command("getMore", cid, collection="checkpoints")
        sizes.append(len(m["cursor"]["nextBatch"]))
        cid = m["cursor"]["id"]
    assert sum(sizes) == n
    # 1.5 MB documents: 11 fit in 16 MiB (16,777,216 / 1,500,0xx), the 12th would not
    assert max(sizes) == 11, sizes
    # one document larger than half the cap still comes one per batch, never zero
    big = b"y" * 9_000_000
    for s in range(3):
        db.big.insert_one({"_id": s, "b": big})
    r = db.command("find", "big", filter={}, batchSize=10)
    first = r["cursor"]["firstBatch"]
    assert len(first) == 1 and r["cursor"]["id"] != 0
    assert len(list(db.big.find({}))) == 3
    c.close()


def test_index_reads_return_every_document_once(srv):
    """docs_by_scan (small databases read the id span in one range scan) moves each found document out instead of
    copying it; an id the scan gives twice still comes back as before. Same answers as the documents written."""
    c = srv.client()
    db = c.app2
    db.items.create_index([("tags", 1)])
    db.items.create_index([("k", 1)])
    want = {}
    for i in range(60):
        d = {"_id": i, "k": i % 7, "tags": [f"t{i % 5}", f"t{(i + 1) % 5}", "all"], "pad": "p" * (i * 50)}
        db.items.insert_one(d)
        want[i] = d
    got = list(db.items.find({"k": 3}).sort("_id", 1))
    assert got == [want[i] for i in sorted(want) if i % 7 == 3]
    # multikey: a document matching two values of the $in once, as mongod returns it
    got = list(db.items.find({"tags": {"$in": ["t1", "t2"]}}).sort("_id", 1))
    assert got == [want[i] for i in sorted(want) if {f"t{i % 5}", f"t{(i + 1) % 5}"} & {"t1", "t2"}]
    got = list(db.items.find({"tags": "all"}).hint([("tags", 1)]))
    assert sorted(d["_id"] for d in got) == sorted(want) and all(d == want[d["_id"]] for d in got)
    c.close()
