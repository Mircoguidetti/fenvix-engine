"""Operation 4: export (mongodump format, streamed, resumable)."""
import datetime
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import tempfile
import threading
import time

import bson
import pytest
from bson import Binary, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from bson.json_util import loads as ejson_loads

import export_client as xc
from lib import Server, make_bucket, list_prefix, wait_until, fill, dump_db, TOKEN, OUT

MONGORESTORE = os.environ.get("MONGORESTORE", "/tmp/claude-0/mongo-img/root/usr/bin/mongorestore")


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("export")
    srv = Server(bucket, "export", idle=300).start()
    yield srv, bucket
    srv.term()


def base(srv):
    return f"http://127.0.0.1:{srv.admin_port}"


def types_docs():
    return [
        {"_id": 1, "i32": 5, "i64": Int64(2 ** 40), "dbl": 1.5, "str": "héllo ☃", "t": True, "n": None, "arr": [1, "a", [2, 3], {"x": 1}],
         "obj": {"a": {"b": {"c": 1}}}, "oid": ObjectId("65f0c0ffee0123456789abcd"), "date": datetime.datetime(2021, 5, 17, 12, 30, 45, 123000),
         "dec": Decimal128("1234.5678"), "bin": Binary(b"\x00\x01\x02\xff", 0), "uuid": Binary(bytes(range(16)), 4), "re": Regex("^a.*z$", "i"),
         "ts": Timestamp(1700000000, 7), "min": MinKey(), "max": MaxKey(), "big": "x" * 70000},
        {"_id": "string-id", "v": 2},
        {"_id": ObjectId("65f0c0ffee0123456789abce"), "v": 3},
        {"_id": {"compound": 1, "id": [1, 2]}, "v": 4},
        {"_id": 2.5, "v": 5},
    ]


def mk_source(srv, db="src"):
    c = srv.client()
    c[db]["typed"].insert_many(types_docs())
    c[db]["typed"].create_index([("i32", 1)], name="i32_1")
    c[db]["typed"].create_index([("str", 1), ("v", -1)], unique=True, sparse=True, name="str_v", partialFilterExpression={"v": {"$gte": 0}} if False else None) if False else None
    c[db]["typed"].create_index([("str", 1)], name="str_1", sparse=True)
    fill(c, db, "users", 1500)
    c[db]["users"].create_index([("grp", 1), ("n", -1)], name="grp_n")
    c[db]["users"].create_index([("name", 1)], unique=True, name="name_u")
    c[db]["users"].create_index([("tag", 1)], expireAfterSeconds=86400, name="ttl_tag")
    c[db]["a.b"].insert_many([{"_id": i} for i in range(10)])
    c[db]["with space"].insert_many([{"_id": i, "s": "x"} for i in range(10)])
    c[db].create_collection("empty")
    return c


def read_bson(path):
    return bson.decode_all(open(path, "rb").read())


def test_download_equals_source(env):
    srv, bucket = env
    c = mk_source(srv)
    st, r = srv.admin.post("/v1/databases/src/exports")
    assert st == 201, r
    assert r["id"].startswith("src.") and {f["path"] for f in r["files"]} >= {"src/typed.bson", "src/typed.metadata.json", "src/a.b.bson", "src/with space.bson", "src/empty.bson", "src/users.metadata.json"}
    dest = tempfile.mkdtemp(prefix="dump-")
    ver = xc.download(base(srv), TOKEN, r["id"], dest)
    assert all(v == "ok" for v in ver.values()), ver
    for coll in ("typed", "users", "a.b", "with space", "empty"):
        got = read_bson(f"{dest}/src/{coll}.bson")
        want = list(c["src"][coll].find({}, sort=[("_id", 1)]))
        assert len(got) == len(want)
        gotd = {repr(d["_id"]): d for d in got}
        for d in want:
            assert gotd[repr(d["_id"])] == d, (coll, d["_id"])
    assert os.path.getsize(f"{dest}/src/empty.bson") == 0
    # the metadata files: extended JSON with the indexes
    m = ejson_loads(open(f"{dest}/src/users.metadata.json").read())
    names = {i["name"]: i for i in m["indexes"]}
    assert set(names) == {"_id_", "grp_n", "name_u", "ttl_tag"}
    assert names["name_u"]["unique"] is True and names["ttl_tag"]["expireAfterSeconds"] == 86400 and dict(names["grp_n"]["key"]) == {"grp": 1, "n": -1}
    assert m["collectionName"] == "users" and "options" not in m
    shutil.rmtree(dest)


def test_snapshot_ignores_later_writes_and_survives_compaction(env):
    srv, bucket = env
    c = srv.client()
    fill(c, "snap", "c", 3000)
    c["snap"]["c"].create_index([("grp", 1)])
    ref = dump_db(c, "snap")
    st, r = srv.admin.post("/v1/databases/snap/exports")
    assert st == 201
    # everything changes afterwards
    c["snap"]["c"].delete_many({"grp": {"$lt": 5}})
    c["snap"]["c"].update_many({}, {"$set": {"changed": True}})
    c["snap"]["new"].insert_one({"_id": 1})
    c["snap"]["c"].insert_many([{"_id": 100000 + i} for i in range(500)])
    for _ in range(6):  # sessions of writes so the database is compacted
        c["snap"]["c"].insert_many([{"_id": 200000 + _ * 100 + i} for i in range(100)])
        c["snap"].command("tenantAdmin", close=True)
    dest = tempfile.mkdtemp(prefix="dump-")
    ver = xc.download(base(srv), TOKEN, r["id"], dest)
    assert all(v == "ok" for v in ver.values()), ver
    got = {repr(d["_id"]): d for d in read_bson(f"{dest}/snap/c.bson")}
    assert got == ref["c"], "the export must show the database as it was when the snapshot was taken"
    assert not os.path.exists(f"{dest}/snap/new.bson")
    shutil.rmtree(dest)


def test_resume_after_broken_transfers(env):
    srv, _ = env
    c = srv.client()
    fill(c, "res", "big", 20000)
    st, r = srv.admin.post("/v1/databases/res/exports")
    eid = r["id"]
    clean = tempfile.mkdtemp(prefix="dump-")
    xc.download(base(srv), TOKEN, eid, clean)
    want = open(f"{clean}/res/big.bson", "rb").read()
    assert len(want) > 2_000_000
    dest = tempfile.mkdtemp(prefix="dump-")
    random.seed(3)
    target = f"{dest}/res/big.bson"
    breaks = 0
    while True:
        stop = random.randint(1000, 400_000)
        done = xc.fetch(base(srv), TOKEN, eid, "res/big.bson", target, stop_after=stop, retry=False)
        if done:
            break
        breaks += 1
        assert open(target, "rb").read() == want[: os.path.getsize(target)], "a partial download must be a prefix of the file"
    print("transfer broken", breaks, "times")
    assert breaks >= 3
    assert open(target, "rb").read() == want
    assert hashlib.sha256(want).hexdigest() == next(f for f in srv.admin.get(f"/v1/exports/{eid}")[1]["files"] if f["path"] == "res/big.bson")["complete"]["sha256"]
    # the Range request of a finished file ends at once
    assert xc.fetch(base(srv), TOKEN, eid, "res/big.bson", target) is True and open(target, "rb").read() == want
    shutil.rmtree(dest); shutil.rmtree(clean)


def test_server_kill9_during_download_then_resume():
    bucket = make_bucket("exportcrash")
    srv = Server(bucket, "exc", idle=300, env={"CTL_FAILPOINT": "export.mid_stream"}).start()
    try:
        c = srv.client()
        fill(c, "kd", "big", 20000)
        st, r = srv.admin.post("/v1/databases/kd/exports")
        assert st == 201
        eid = r["id"]
        dest = tempfile.mkdtemp(prefix="dump-")
        target = f"{dest}/kd/big.bson"
        res = {}
        th = threading.Thread(target=lambda: res.update(r=xc.fetch(base(srv), TOKEN, eid, "kd/big.bson", target, retry=False)))
        th.start()
        wait_until(lambda: "FAILPOINT export.mid_stream reached" in srv.log_text(), 30, what="failpoint")
        srv.kill9()
        th.join(timeout=60)
        partial = os.path.getsize(target) if os.path.exists(target) else 0
        srv.env = {}
        srv.proc = None
        srv.start()  # same bucket: the snapshot (a checkpoint) is still there
        assert srv.admin.get(f"/v1/exports/{eid}")[0] == 200
        assert xc.fetch(base(srv), TOKEN, eid, "kd/big.bson", target) is True
        for f in rec_files(srv, eid):
            if f["kind"] == "metadata":
                xc.fetch(base(srv), TOKEN, eid, f["path"], f"{dest}/{f['path']}")
        ver = xc.verify(base(srv), TOKEN, eid, dest)
        print("killed after", partial, "bytes;", ver)
        assert ver["kd/big.bson"] == "ok"
        assert len(read_bson(target)) == 20000
        # and the source is intact and the checkpoint is not leaked once the export is deleted
        assert c["kd"]["big"].count_documents({}) == 20000 or srv.client()["kd"]["big"].count_documents({}) == 20000
        assert srv.admin.delete(f"/v1/exports/{eid}")[0] == 200
        assert not [x for x in srv.admin.get("/v1/databases/kd/checkpoints")[1]["checkpoints"] if (x["name"] or "").startswith("export:")]
    finally:
        srv.term()


def rec_files(srv, eid):
    return srv.admin.get(f"/v1/exports/{eid}")[1]["files"]


def test_streaming_uses_bounded_memory(env):
    """A 40 MB collection is exported while the server's resident memory grows by far less than that."""
    srv, _ = env
    c = srv.client()
    big = [{"_id": i, "p": "q" * 380, "n": i} for i in range(100000)]
    for i in range(0, 100000, 5000):
        c["mem"]["c"].insert_many(big[i:i + 5000])
    c["mem"].command("tenantAdmin", close=True)

    def hwm():
        for line in open(f"/proc/{srv.proc.pid}/status"):
            if line.startswith("VmHWM"):
                return int(line.split()[1]) // 1024
    # peak RSS so far includes the loading above; read the live RSS around the download instead
    def rss():
        for line in open(f"/proc/{srv.proc.pid}/status"):
            if line.startswith("VmRSS"):
                return int(line.split()[1]) // 1024
    st, r = srv.admin.post("/v1/databases/mem/exports")
    assert st == 201
    dest = tempfile.mkdtemp(prefix="dump-")
    base_rss = rss()
    peak = [base_rss]
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            peak[0] = max(peak[0], rss())
            time.sleep(0.02)

    t = threading.Thread(target=sampler)
    t.start()
    ver = xc.download(base(srv), TOKEN, r["id"], dest)
    stop.set(); t.join()
    size = os.path.getsize(f"{dest}/mem/c.bson")
    print("exported", size // 1_000_000, "MB; server RSS", base_rss, "->", peak[0], "MB")
    assert size > 40_000_000 and ver["mem/c.bson"] == "ok"
    assert peak[0] - base_rss < size // 1_000_000 // 2, f"memory grew by {peak[0] - base_rss} MB for a {size // 1_000_000} MB export"
    shutil.rmtree(dest)


def test_errors(env):
    srv, bucket = env
    a = srv.admin
    assert a.post("/v1/databases/nope/exports")[0] == 404
    assert a.post("/v1/databases/bad.name/exports")[0] == 400
    assert a.post("/v1/databases/src/exports", token=None)[0] == 401
    assert a.get("/v1/exports/src.0000000000000000")[0] == 404
    assert a.get("/v1/exports/garbage")[0] == 404
    st, r = a.post("/v1/databases/src/exports")
    eid = r["id"]
    assert a.get(f"/v1/exports/{eid}/files/src/nothing.bson")[0] == 404
    code, body, _ = a.call("GET", f"/v1/exports/{eid}/files/src/typed.bson", raw=True, headers={"Range": "bytes=-5"})
    assert code == 416
    code, body, h = a.call("GET", f"/v1/exports/{eid}/files/src/typed.bson", raw=True, headers={"Range": "bytes=10-"})
    assert code == 206 and h["Content-Range"].startswith("bytes 10-")
    full = a.call("GET", f"/v1/exports/{eid}/files/src/typed.bson", raw=True)[1]
    assert body == full[10:]
    code, _, _ = a.call("GET", f"/v1/exports/{eid}/files/src/typed.bson", raw=True, token=None)
    assert code == 401
    # expired snapshots answer 410
    import boto3
    from lib import s3
    key = f"control/exports/src/{eid}.json"
    rec = json.loads(s3().get_object(Bucket=bucket, Key=key)["Body"].read())
    rec["expires_at_ms"] = 1000
    s3().put_object(Bucket=bucket, Key=key, Body=json.dumps(rec).encode())
    assert a.get(f"/v1/exports/{eid}")[0] == 410
    assert a.delete(f"/v1/exports/{eid}")[0] == 200


def test_delete_releases_the_snapshot(env):
    srv, bucket = env
    c = srv.client()
    fill(c, "rel", "c", 200)
    st, r = srv.admin.post("/v1/databases/rel/exports")
    cps = lambda: [x for x in srv.admin.get("/v1/databases/rel/checkpoints")[1]["checkpoints"] if (x["name"] or "").startswith("export:")]
    assert len(cps()) == 1 and cps()[0]["name"] == "export:" + r["id"]
    assert srv.admin.delete(f"/v1/exports/{r['id']}")[0] == 200
    assert cps() == [] and list_prefix(bucket, "control/exports/rel/") == []
    assert srv.admin.get(f"/v1/exports/{r['id']}")[0] == 404
    # erase takes the snapshot records with it
    st, r = srv.admin.post("/v1/databases/rel/exports")
    assert srv.admin.delete("/v1/databases/rel")[0] == 200
    assert list_prefix(bucket, "control/exports/rel/") == [] and list_prefix(bucket, "tenants/rel/") == []


def test_concurrent_exports_and_writes(env):
    srv, _ = env
    c = srv.client()
    c["hot"]["c"].insert_one({"_id": 0})
    stop = threading.Event()
    last = [0]

    def writer():
        cc = srv.client()
        i = 1
        while not stop.is_set():
            cc["hot"]["c"].insert_one({"_id": i})
            last[0] = i
            i += 1

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(0.5)
    out = []

    def exp():
        out.append(srv.admin.post("/v1/databases/hot/exports"))

    ts = [threading.Thread(target=exp) for _ in range(4)]
    [x.start() for x in ts]
    [x.join() for x in ts]
    stop.set()
    t.join()
    assert all(s == 201 for s, _ in out), out
    ids = {r["id"] for _, r in out}
    assert len(ids) == 4
    for _, r in out:
        dest = tempfile.mkdtemp(prefix="dump-")
        ver = xc.download(base(srv), TOKEN, r["id"], dest)
        assert ver["hot/c.bson"] == "ok"
        got = sorted(d["_id"] for d in read_bson(f"{dest}/hot/c.bson"))
        assert got == list(range(len(got))), "an export must be a consistent prefix of the writes"
        shutil.rmtree(dest)


@pytest.mark.skipif(not os.path.exists(MONGORESTORE), reason="mongorestore binary not available")
def test_mongorestore_imports_the_dump(env):
    srv, bucket = env
    c = mk_source(srv, "mr")
    st, r = srv.admin.post("/v1/databases/mr/exports")
    assert st == 201
    dest = tempfile.mkdtemp(prefix="dump-")
    ver = xc.download(base(srv), TOKEN, r["id"], dest)
    assert all(v == "ok" for v in ver.values())
    # restore into a second, empty server (ours: another bucket), renaming the database on the way
    b2 = make_bucket("exportrestore")
    tgt = Server(b2, "exr", idle=300).start()
    try:
        cmd = [MONGORESTORE, "--uri", tgt.uri, "--dir", dest, "--nsInclude", "mr.*", "--nsFrom", "mr.*", "--nsTo", "mr_restored.*", "--stopOnError"]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        print(p.stderr[-1500:])
        assert p.returncode == 0, p.stderr
        c2 = tgt.client()
        src_d = dump_db(c, "mr")
        got_d = dump_db(c2, "mr_restored")
        assert got_d == src_d
        def idx(c_, db_, coll):
            return {i["name"]: (dict(i["key"]), i.get("unique"), i.get("expireAfterSeconds")) for i in c_[db_][coll].list_indexes()}

        for coll in src_d:
            # a multi-index createIndexes (what mongorestore sends) finishes building in the background in this
            # server (existing behaviour, not part of this change): wait for it, then compare exactly
            wait_until(lambda: idx(c, "mr", coll) == idx(c2, "mr_restored", coll), 15, what=f"indexes of {coll}")
        assert sorted(c2["mr_restored"].list_collection_names()) == sorted(c["mr"].list_collection_names())
    finally:
        tgt.term()
        shutil.rmtree(dest)


def test_snapshot_survives_compaction_and_garbage_collection(env):
    """The checkpoint of a snapshot keeps its SSTs through compaction and a GC pass that deletes others."""
    srv, bucket = env
    c = srv.client()
    fill(c, "gcx", "c", 1500)
    c["gcx"].command("tenantAdmin", close=True)
    ref = dump_db(c, "gcx")
    st, r = srv.admin.post("/v1/databases/gcx/exports")
    assert st == 201
    base_stats = c.admin.command("tenantStats")["maintenance"]
    for s in range(7):
        c["gcx"]["c"].update_many({}, {"$set": {"round": s}})
        c["gcx"]["c"].insert_many([{"_id": 50000 + s * 100 + i} for i in range(100)])
        c["gcx"].command("tenantAdmin", close=True)
        time.sleep(0.3)
    wait_until(lambda: c.admin.command("tenantStats")["maintenance"]["compactRuns"] > base_stats["compactRuns"], 60, what="compaction")
    after_compaction = {k for k, _ in list_prefix(bucket, "tenants/gcx/")}
    wait_until(lambda: after_compaction - {k for k, _ in list_prefix(bucket, "tenants/gcx/")}, 150, every=2, what="garbage collection of the database")
    time.sleep(2)
    deleted = after_compaction - {k for k, _ in list_prefix(bucket, "tenants/gcx/")}
    print("objects collected by GC while the snapshot existed:", len(deleted))
    dest = tempfile.mkdtemp(prefix="dump-")
    ver = xc.download(base(srv), TOKEN, r["id"], dest)
    assert all(v == "ok" for v in ver.values()), ver
    assert {repr(d["_id"]): d for d in read_bson(f"{dest}/gcx/c.bson")} == ref["c"]
    shutil.rmtree(dest)
