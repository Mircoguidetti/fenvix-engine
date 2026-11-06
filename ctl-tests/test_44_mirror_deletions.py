"""gp3 floor prototype (the SST mirror, --mirror-dir), point 4 of its contract (guarantee 6: deletions complete, also on
the waiter's disks).

1. After dropDatabase none of the dropped database's bytes remain in the mirror files (a marker string, in an indexed
   field and in the documents, is searched in every file of the mirror directory), the other database keeps its own,
   and the dropped database reads empty.
2. After the control-layer erase (DELETE /v1/databases/{db}), the same.
3. A restart keeps the mirror (one file per SST), and the kept database is read from it: no S3 GET for its SSTs.
"""
import os
import shutil
import tempfile

from lib import Server, make_bucket, wait_until


def fen(c):
    return c.admin.command("serverStatus")["fenvix"]["waiter"]


def count_in_dir(d, needle):
    n = 0
    for root, _, files in os.walk(d):
        for f in files:
            with open(os.path.join(root, f), "rb") as fh:
                n += fh.read().count(needle)
    return n


def files_in(d):
    return sum(len(fs) for _, _, fs in os.walk(d))


def fill(c, db, mark, n=3000):
    coll = c[db]["c"]
    coll.create_index("cust")
    docs = [{"_id": i, "cust": f"{mark}{i:05d}", "pad": "x" * 400} for i in range(n)]
    for i in range(0, n, 500):
        coll.insert_many(docs[i:i + 500])
    c[db].command("tenantAdmin", close=True)          # flushed to SSTs and closed


def read_all(c, db, mark, n=3000):
    coll = c[db]["c"]
    for i in range(0, n, 7):
        assert coll.find_one({"cust": f"{mark}{i:05d}"})["_id"] == i


def start(tag, mirror, bucket=None):
    bucket = bucket or make_bucket(tag)
    srv = Server(bucket, tag, idle=600, extra=["--waiter-tools", "--counter-ram-mb", "1", "--read-ahead-docs", "0",
                                                 "--mirror-dir", mirror]).start()
    return srv, bucket


def mirrored(c, db, mark):
    """Open the database and wait until the maps loop has filled the mirror with its SSTs."""
    read_all(c, db, mark)
    wait_until(lambda: fen(c)["mirror"]["files"] > 0 and fen(c)["mirror"]["filling"] == 0 and fen(c)["maps"]["busy"] == 0, what="mirror filled")


def test_drop_and_erase_leave_no_bytes_in_the_mirror():
    mirror = tempfile.mkdtemp(prefix="ctl-mirror-")
    srv, bucket = start("mdrop", mirror)
    try:
        c = srv.client()
        for db, mark in (("ma", "SECRETMA"), ("mb", "KEEPMB"), ("mc", "SECRETMC")):
            fill(c, db, mark)
            mirrored(c, db, mark)
        wait_until(lambda: all(count_in_dir(mirror, m.encode()) > 0 for m in ("SECRETMA", "KEEPMB", "SECRETMC")), what="all three in the mirror")
        a0, b0, c0 = (count_in_dir(mirror, m) for m in (b"SECRETMA", b"KEEPMB", b"SECRETMC"))
        w0 = fen(c)["mirror"]
        print("before:", {k: w0[k] for k in ("files", "bytes", "indexBytes", "pushed", "filled")}, "| markers ma", a0, "mb", b0, "mc", c0)
        assert a0 > 0 and b0 > 0 and c0 > 0

        # 1. dropDatabase
        c.drop_database("ma")
        wait_until(lambda: count_in_dir(mirror, b"SECRETMA") == 0, timeout=10, what="dropped database gone from the mirror")
        assert not os.path.exists(os.path.join(mirror, "tenants", "ma")), "the dropped database's directory stays"
        assert count_in_dir(mirror, b"KEEPMB") == b0, "the drop touched the other database's files"
        assert c["ma"]["c"].count_documents({}) == 0
        w1 = fen(c)["mirror"]
        print("after the drop:", {k: w1[k] for k in ("files", "scrubbedFiles", "scrubbedBytes")})
        assert w1["scrubbedFiles"] > 0 and w1["scrubbedBytes"] > 0

        # 2. control-layer erase
        c["mc"].command("tenantAdmin", close=True)
        st, r = srv.admin.delete("/v1/databases/mc")
        assert st == 200 and r["erased"], r
        wait_until(lambda: count_in_dir(mirror, b"SECRETMC") == 0, timeout=10, what="erased database gone from the mirror")
        assert not os.path.exists(os.path.join(mirror, "tenants", "mc")), "the erased database's directory stays"
        assert count_in_dir(mirror, b"KEEPMB") == b0, "the erase touched the other database's files"
        print("after the erase: markers ma", count_in_dir(mirror, b"SECRETMA"), "mc", count_in_dir(mirror, b"SECRETMC"), "mb", count_in_dir(mirror, b"KEEPMB"))
        read_all(c, "mb", "KEEPMB")
    finally:
        srv.term()

    # 3. restart: the mirror is kept, the kept database is read from it, the dropped ones do not come back
    kept = files_in(mirror)
    srv, _ = start("mdrop2", mirror, bucket)
    try:
        c = srv.client()
        wait_until(lambda: fen(c)["mirror"]["files"] == kept, timeout=10, what="mirror kept across the restart")
        before = fen(c)["mirror"]["misses"]
        read_all(c, "mb", "KEEPMB")
        wait_until(lambda: fen(c)["maps"]["busy"] == 0, what="maps loaded")
        w = fen(c)["mirror"]
        print("after the restart:", {k: w[k] for k in ("files", "hits", "misses", "filled")})
        assert w["hits"] > 0
        assert w["misses"] == before, "an SST of the kept database was read from S3 after the restart"
        assert count_in_dir(mirror, b"SECRETMA") == 0 and count_in_dir(mirror, b"SECRETMC") == 0
    finally:
        srv.term()
        shutil.rmtree(mirror, ignore_errors=True)
