"""The export against a real MongoDB 7.0 (mongod, mongodump and mongorestore from the official mongo:7.0 image):
our dump has the same files and the same bytes as mongodump's, and mongorestore loads it into a real mongod."""
import json
import os
import shutil
import subprocess
import tempfile

import bson
import pytest
from bson.json_util import loads as ejson_loads
from pymongo import MongoClient

import export_client as xc
from lib import Server, make_bucket, fill, dump_db, TOKEN

BIN = os.environ.get("MONGO_BIN_DIR", "/tmp/claude-0/mongo-img/root/usr/bin")
REAL = os.environ.get("REAL_URI", "mongodb://127.0.0.1:27020/?directConnection=true")
MONGODUMP, MONGORESTORE = f"{BIN}/mongodump", f"{BIN}/mongorestore"

pytestmark = pytest.mark.skipif(not os.path.exists(MONGODUMP), reason="mongo tools not available")


def real_up():
    try:
        MongoClient(REAL, serverSelectionTimeoutMS=2000).admin.command("ping")
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def env():
    assert real_up(), "the reference mongod is not running"
    bucket = make_bucket("exportreal")
    srv = Server(bucket, "exr", idle=300).start()
    yield srv
    srv.term()


def populate(client, db):
    c = client[db]
    c["users"].insert_many([{"_id": i, "n": i, "grp": i % 10, "name": f"user{i}", "tag": ["x", "y", "z"][i % 3], "nested": {"k": i % 7, "arr": [1, 2, {"z": i}]}, "f": i / 3, "big": 2 ** 40 + i} for i in range(2000)])
    c["users"].create_index([("grp", 1), ("n", -1)], name="grp_n")
    c["users"].create_index([("name", 1)], unique=True, name="name_u")
    c["users"].create_index([("tag", 1)], sparse=True, name="tag_s")
    c["a.b"].insert_many([{"_id": i, "s": "é" * (i % 5)} for i in range(50)])
    c["a.b"].create_index([("s", 1)], name="s_1")
    c["empty"].insert_one({"_id": 1})
    c["empty"].delete_one({"_id": 1})


def indexes(client, db, coll):
    return {i["name"]: (dict(i["key"]), i.get("unique", False), i.get("sparse", False), i.get("expireAfterSeconds")) for i in client[db][coll].list_indexes()}


def test_same_files_and_bytes_as_mongodump(env):
    srv = env
    real = MongoClient(REAL)
    real.drop_database("xp_real")
    populate(real, "xp_real")
    populate(srv.client(), "xp_ours")
    d_real = tempfile.mkdtemp(prefix="real-")
    p = subprocess.run([MONGODUMP, "--uri", REAL, "--db", "xp_real", "--out", d_real], capture_output=True, text=True, timeout=300)
    assert p.returncode == 0, p.stderr
    st, r = srv.admin.post("/v1/databases/xp_ours/exports")
    assert st == 201
    d_ours = tempfile.mkdtemp(prefix="ours-")
    ver = xc.download(f"http://127.0.0.1:{srv.admin_port}", TOKEN, r["id"], d_ours)
    assert all(v == "ok" for v in ver.values())
    real_files = sorted(os.listdir(f"{d_real}/xp_real"))
    ours_files = sorted(os.listdir(f"{d_ours}/xp_ours"))
    print("mongodump files:", real_files, "\nexport files:   ", ours_files)
    # mongodump also writes `prelude.json`; everything mongorestore reads is present in both
    assert [f for f in real_files if f != "prelude.json"] == ours_files or set(ours_files) <= set(real_files)
    for coll in ("users", "a.b", "empty"):
        rb = open(f"{d_real}/xp_real/{coll}.bson", "rb").read()
        ob = open(f"{d_ours}/xp_ours/{coll}.bson", "rb").read()
        assert rb == ob, f"{coll}.bson differs from mongodump's ({len(rb)} vs {len(ob)} bytes)"
        rm = ejson_loads(open(f"{d_real}/xp_real/{coll}.metadata.json").read())
        om = ejson_loads(open(f"{d_ours}/xp_ours/{coll}.metadata.json").read())
        assert rm["collectionName"] == om["collectionName"] and rm["type"] == om["type"] and rm.get("options", {}) == om.get("options", {})
        assert set(rm) == set(om), (set(rm), set(om))
        rix = {i["name"]: dict(i) for i in rm["indexes"]}
        oix = {i["name"]: dict(i) for i in om["indexes"]}
        assert set(rix) == set(oix), (coll, rix.keys(), oix.keys())
        for n in rix:
            assert {k: (dict(v) if isinstance(v, dict) or hasattr(v, "items") else v) for k, v in rix[n].items()} == {k: (dict(v) if isinstance(v, dict) or hasattr(v, "items") else v) for k, v in oix[n].items()}, (coll, n, rix[n], oix[n])
    shutil.rmtree(d_real); shutil.rmtree(d_ours)


def test_mongorestore_into_a_real_mongod(env):
    srv = env
    c = srv.client()
    populate(c, "xp_src")
    st, r = srv.admin.post("/v1/databases/xp_src/exports")
    d = tempfile.mkdtemp(prefix="ours-")
    assert all(v == "ok" for v in xc.download(f"http://127.0.0.1:{srv.admin_port}", TOKEN, r["id"], d).values())
    real = MongoClient(REAL)
    real.drop_database("xp_back")
    p = subprocess.run([MONGORESTORE, "--uri", REAL, "--dir", d, "--nsInclude", "xp_src.*", "--nsFrom", "xp_src.*", "--nsTo", "xp_back.*", "--stopOnError"], capture_output=True, text=True, timeout=300)
    print(p.stderr[-400:])
    assert p.returncode == 0, p.stderr
    assert dump_db(real, "xp_back") == dump_db(c, "xp_src")
    for coll in ("users", "a.b"):
        assert indexes(real, "xp_back", coll) == indexes(c, "xp_src", coll), coll
    assert sorted(real["xp_back"].list_collection_names()) == sorted(c["xp_src"].list_collection_names())
    shutil.rmtree(d)
