"""audit-fencing round 2, item 24: a prefix per incarnation of a database name.

An erase or a dropDatabase moves the name to a new incarnation before it deletes its objects, so a late write of a
process that lost the old incarnation (a WAL PUT SlateDB retries, a run upload, a version create) lands under a prefix
the new database never reads. Incarnation 0 lives at tenants/{db}/, incarnation k >= 1 at tenants/{db}/i{k}/.

Covered here, through the server: erase then the same name again, dropDatabase then the same name again, a late object
planted under the old incarnation's prefix (as a late WAL PUT would land) is neither read nor left (the next process
sweeps it), and a fork and a restore of the new incarnation work.
"""
import time

import pytest
from pymongo import MongoClient

from lib import Server, make_bucket, list_prefix, s3, dump_db, wait_until


def now_ms():
    return int(time.time() * 1000)


def objs(bucket, db):
    return [k for k, _ in list_prefix(bucket, f"tenants/{db}/")]


def test_erase_drop_recreate_fork_restore_and_sweep():
    bucket = make_bucket("incarn")
    extra = ["--retention-interval-secs", "2", "--retention-prune-secs", "3"]
    srv = Server(bucket, "incarn", idle=300, extra=extra).start()
    try:
        c = srv.client()
        # incarnation 0
        c["inc"]["c"].insert_many([{"_id": i, "gen": 0} for i in range(200)])
        c["inc"].command("tenantAdmin", close=True)
        old = objs(bucket, "inc")
        assert old and not any("/i1/" in k for k in old), old[:5]
        wal0 = sorted(k for k in old if "/wal/" in k)
        assert wal0, "setup: incarnation 0 has WAL objects"
        body = s3().get_object(Bucket=bucket, Key=wal0[-1])["Body"].read()
        # erase: the name moves to incarnation 1
        st, r = srv.admin.delete("/v1/databases/inc")
        assert st == 200, r
        assert objs(bucket, "inc") == []
        assert [k for k, _ in list_prefix(bucket, "") if "inc/" in k or k.endswith("/inc")] == []
        # a late WAL write of the erased incarnation lands after the erase (as a PUT SlateDB retried would)
        late = f"tenants/inc/wal/{99:020}.sst"
        s3().put_object(Bucket=bucket, Key=late, Body=body)
        # the same name again: incarnation 1, which reads nothing of incarnation 0
        c["inc"]["c"].insert_many([{"_id": i, "gen": 1} for i in range(50)])
        got = dump_db(c, "inc")
        assert sorted(d["_id"] for d in got["c"].values()) == list(range(50)), "rows of the erased incarnation came back"
        assert all(d["gen"] == 1 for d in got["c"].values())
        now = objs(bucket, "inc")
        assert any(k.startswith("tenants/inc/i1/") for k in now), now[:10]
        # a new process takes the lease of incarnation 1 and sweeps the old prefix (the planted late object)
        srv.term()
        srv = Server(bucket, "incarn2", idle=300, extra=extra).start()
        c = srv.client()
        assert c["inc"]["c"].count_documents({}) == 50
        wait_until(lambda: late not in objs(bucket, "inc"), timeout=30, what="the late object of incarnation 0 swept")
        assert [k for k in objs(bucket, "inc") if not k.startswith("tenants/inc/i1/") and not k.endswith("/_lease")] == []
        # dropDatabase: incarnation 2
        c.drop_database("inc")
        c["inc"]["c"].insert_many([{"_id": 1000 + i, "gen": 2} for i in range(30)])
        got = dump_db(c, "inc")
        assert sorted(d["_id"] for d in got["c"].values()) == [1000 + i for i in range(30)]
        assert any(k.startswith("tenants/inc/i2/") for k in objs(bucket, "inc"))
        # a fork of incarnation 2
        c["inc"].command("tenantAdmin", close=True)
        st, r = srv.admin.post("/v1/databases/inc/fork", {"target": "incf"})
        assert st == 201, r
        assert sorted(d["_id"] for d in dump_db(c, "incf")["c"].values()) == [1000 + i for i in range(30)]
        # a restore of incarnation 2
        st, r = srv.admin.put("/v1/databases/inc/retention", {"window_secs": 3600, "interval_secs": 2})
        assert st == 200, r
        time.sleep(2.5)
        t = now_ms()
        want = dump_db(c, "inc")
        time.sleep(2.5)
        c["inc"]["c"].insert_many([{"_id": 5000 + i, "gen": 2} for i in range(10)])
        c["inc"]["c"].delete_many({"_id": {"$lt": 1010}})
        time.sleep(2.5)
        st, r = srv.admin.post("/v1/databases/inc/restore", {"to": t})
        assert st == 200, r
        assert dump_db(c, "inc") == want, "the restore of incarnation 2 gave another state"
        assert [k for k in objs(bucket, "inc") if not k.startswith("tenants/inc/i2/") and not k.endswith("/_lease")] == []
    finally:
        srv.term()


def test_erased_name_is_not_listed():
    """Item 5 of P-audit-fencing-2 (GitHub CI 7657d2f, test_05 erase.after_manifests): once an erase reports finished,
    listDatabases must not list the name. An opening of an absent name takes its lease (tenants/{db}/_lease) before it
    learns the database is absent, and a late write of the erased incarnation can land under the old prefix: neither is
    a database. Before the fix listDatabases listed any name with any object under tenants/{db}/."""
    bucket = make_bucket("unlisted")
    srv = Server(bucket, "unlisted", idle=300).start()
    try:
        c = srv.client()
        c["victim"]["c"].insert_many([{"_id": i} for i in range(100)])
        c["keep"]["c"].insert_one({"_id": 1})
        c["victim"].command("tenantAdmin", close=True)
        wal = sorted(k for k in objs(bucket, "victim") if "/wal/" in k)
        body = s3().get_object(Bucket=bucket, Key=wal[-1])["Body"].read()
        st, r = srv.admin.delete("/v1/databases/victim")
        assert st == 200, r
        listed = lambda: [d["name"] for d in srv.client().list_databases()]
        assert "victim" not in listed() and "keep" in listed()
        # what an opening of the absent name leaves for a moment: its lease
        import bson
        s3().put_object(Bucket=bucket, Key="tenants/victim/_lease", Body=bson.encode({"holder": "gone", "expiresMs": bson.Int64(1), "seq": bson.Int64(1)}))
        # a late write of the erased incarnation
        s3().put_object(Bucket=bucket, Key=f"tenants/victim/wal/{99:020}.sst", Body=body)
        assert "victim" not in listed(), "an erased name is listed (a lease or a late write of the erased incarnation)"
        assert srv.admin.get("/v1/databases/victim")[0] == 404
        assert c["victim"]["c"].find_one({}) is None
        assert "victim" not in listed()
        # the same name created again is listed
        c["victim"]["c"].insert_one({"_id": "new"})
        assert "victim" in listed()
        assert [d["_id"] for d in c["victim"]["c"].find({})] == ["new"]
    finally:
        srv.term()
