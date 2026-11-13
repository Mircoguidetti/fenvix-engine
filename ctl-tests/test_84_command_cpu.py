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


def _span_load(db):
    db.checkpoints.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1)], unique=True)
    want = {}
    for t in range(6):
        tid = f"thread-{t}"
        for s in range(30):
            cid = f"1ef{s:05d}"
            body = ("m%d " % s) * (20 + s * 5)
            db.checkpoints.update_one({"thread_id": tid, "checkpoint_ns": "", "checkpoint_id": cid},
                                      {"$set": {"checkpoint": body, "step": s}}, upsert=True)
            want[(tid, cid)] = body
            if s % 3 == 0:
                # a subgraph namespace between the thread's documents: the spans of both lists hold rows not wanted
                db.checkpoints.update_one({"thread_id": tid, "checkpoint_ns": "sub", "checkpoint_id": cid},
                                          {"$set": {"checkpoint": "sub", "step": s}}, upsert=True)
    # a sparse span: 5 documents of namespace "rare" spread over 300 of namespace "" (the scan gives up, point reads)
    for s in range(300):
        cid = f"2ef{s:05d}"
        db.checkpoints.update_one({"thread_id": "long", "checkpoint_ns": "", "checkpoint_id": cid}, {"$set": {"step": s}}, upsert=True)
        if s % 60 == 30:
            db.checkpoints.update_one({"thread_id": "long", "checkpoint_ns": "rare", "checkpoint_id": cid}, {"$set": {"step": s}}, upsert=True)
    return want


def _span_reads(db, want):
    out = []
    for t in range(6):
        tid = f"thread-{t}"
        hist = list(db.checkpoints.find({"thread_id": tid, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)]))
        assert [d["checkpoint_id"] for d in hist] == sorted((c for (tt, c) in want if tt == tid), reverse=True)
        assert all(d["checkpoint"] == want[(tid, d["checkpoint_id"])] for d in hist)
        last10 = list(db.checkpoints.find({"thread_id": tid, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=10))
        sub = list(db.checkpoints.find({"thread_id": tid, "checkpoint_ns": "sub"}, sort=[("checkpoint_id", 1)]))
        assert len(last10) == 10 and len(sub) == 10
        assert [d["step"] for d in sub] == list(range(0, 30, 3))
        out.append((hist, last10, sub))
    rare = list(db.checkpoints.find({"thread_id": "long", "checkpoint_ns": "rare"}, sort=[("checkpoint_id", 1)]))
    assert [d["step"] for d in rare] == [30, 90, 150, 210, 270]
    out.append(rare)
    return out


@pytest.mark.parametrize("span", [True, False])
def test_span_scan_of_a_customers_documents(span):
    """A customer's documents read by one span scan give the same answers as one point read each (control: the scan
    off, FENVIX_NO_SPAN_READS); the scan runs (spanScans) when on, never when off."""
    bucket = make_bucket("cmdcpuspan" + ("on" if span else "off"))
    # MONGO_NO_FETCH_SCAN: a database this small would otherwise read its id spans whole (coll.rs prefer_scan), the
    # path of small databases; the big line's database takes the point reads that the span scan replaces
    env = {"MONGO_NO_FETCH_SCAN": "1"} if span else {"MONGO_NO_FETCH_SCAN": "1", "FENVIX_NO_SPAN_READS": "1"}
    s = Server(bucket, "cmdcpuspan", idle=600, env=env, extra=["--customer-key", "checkpoints=thread_id"]).start()
    try:
        c = s.client()
        want = _span_load(c.app)
        # a fresh process: the reads below come from storage (the writer's view after a restart)
        c.close()
        s.term()
        s = Server(bucket, "cmdcpuspan", idle=600, env=env, extra=["--customer-key", "checkpoints=thread_id"]).start()
        c = s.client()
        before = c.admin.command("serverStatus")["fenvix"]["customerKeys"]
        _span_reads(c.app, want)
        after = c.admin.command("serverStatus")["fenvix"]["customerKeys"]
        scans = after["spanScans"] - before["spanScans"]
        gave_up = after["spanGaveUp"] - before["spanGaveUp"]
        if span:
            assert scans >= 18 and gave_up >= 1, after
        else:
            assert scans == 0 and gave_up == 0, after
        c.close()
    finally:
        s.term()
