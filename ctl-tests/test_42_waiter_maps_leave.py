"""The waiter's tools (--waiter-tools), reviewer R-T1 correction 1, end to end through the server.

1. After dropDatabase the dropped database's maps and blocks leave the counter within seconds: its SSTs are no longer
   mapped, and none of its keys (index keys and documents, here a marker string in an indexed field) stays in the
   counter's disk floor files. The other database keeps its own.
2. The maps of a database closed for long (--maps-idle-secs) are released: they leave the RAM.
"""
import os
import shutil
import tempfile
import time

from lib import Server, make_bucket, wait_until


def waiter(c):
    return c.admin.command("serverStatus")["fenvix"]["waiter"]


def count_on_disk(d, needle):
    n = 0
    for f in os.listdir(d):
        with open(os.path.join(d, f), "rb") as fh:
            n += fh.read().count(needle)
    return n


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


def start(tag, extra=()):
    disk = tempfile.mkdtemp(prefix=f"ctl-{tag}-disk-")
    bucket = make_bucket(tag)
    # 1 MiB of counter RAM: most of what is read goes down to the disk floor
    srv = Server(bucket, tag, idle=600, extra=["--waiter-tools", "--counter-ram-mb", "1", "--counter-disk-dir", disk,
                                                 "--counter-disk-gb", "1", "--read-ahead-docs", "0"] + list(extra)).start()
    return srv, disk


def test_drop_database_leaves_the_counter():
    srv, disk = start("wdrop")
    try:
        c = srv.client()
        fill(c, "wa", "SECRETWA")
        fill(c, "wb", "KEEPWB")
        read_all(c, "wa", "SECRETWA")
        read_all(c, "wb", "KEEPWB")
        wait_until(lambda: waiter(c)["maps"]["busy"] == 0 and waiter(c)["maps"]["sstsMapped"] > 0, what="maps loaded")
        w0 = waiter(c)
        a0, b0 = count_on_disk(disk, b"SECRETWA"), count_on_disk(disk, b"KEEPWB")
        print("before the drop:", {k: w0["counter"][k] for k in ("mapsRamBytes", "diskBytes", "diskEntries")}, "| SSTs mapped",
              w0["maps"]["sstsMapped"], "| marker of wa on disk", a0, "| marker of wb on disk", b0)
        assert a0 > 0 and b0 > 0, "the test must start with both databases on the disk floor"

        t0 = time.time()
        c.drop_database("wa")
        wait_until(lambda: count_on_disk(disk, b"SECRETWA") == 0, timeout=5, what="the dropped database's keys gone from the disk floor")
        took = time.time() - t0
        w1 = waiter(c)
        b1 = count_on_disk(disk, b"KEEPWB")
        print(f"after the drop ({took:.2f} s):", {k: w1["counter"][k] for k in ("mapsRamBytes", "droppedEntries", "droppedDiskBytes", "mapsForgotten")},
              "| SSTs mapped", w1["maps"]["sstsMapped"], "| marker of wb on disk", b1)
        assert w1["maps"]["droppedDbs"] >= 1 and w1["counter"]["droppedEntries"] > 0 and w1["counter"]["droppedDiskBytes"] > 0
        assert w1["maps"]["sstsMapped"] < w0["maps"]["sstsMapped"], "the dropped database's SSTs are still mapped"
        assert b1 == b0, "the drop touched the other database's entries"
        # the dropped database reads empty, the other one is whole
        assert c["wa"]["c"].count_documents({}) == 0
        read_all(c, "wb", "KEEPWB")
        time.sleep(2)
        assert count_on_disk(disk, b"SECRETWA") == 0, "the dropped database came back on the disk floor"
    finally:
        srv.term()
        shutil.rmtree(disk, ignore_errors=True)


def test_maps_of_a_long_closed_database_are_released():
    srv, disk = start("widle", ["--maps-idle-secs", "2"])
    try:
        c = srv.client()
        fill(c, "wc", "IDLEWC", 1500)
        read_all(c, "wc", "IDLEWC", 1500)
        wait_until(lambda: waiter(c)["maps"]["busy"] == 0 and waiter(c)["maps"]["sstsMapped"] > 0, what="maps loaded")
        w0 = waiter(c)
        assert w0["counter"]["mapsRamBytes"] > 0
        c["wc"].command("tenantAdmin", close=True)
        w1 = wait_until(lambda: (lambda w: w if w["maps"]["releasedDbs"] >= 1 else None)(waiter(c)), timeout=15, what="the closed database's maps released")
        print("before:", w0["counter"]["mapsRamBytes"], "maps bytes in RAM,", w0["maps"]["sstsMapped"], "SSTs mapped | after:",
              w1["counter"]["mapsRamBytes"], "bytes,", w1["maps"]["sstsMapped"], "SSTs, released", w1["counter"]["mapsReleased"])
        assert w1["counter"]["mapsReleased"] > 0
        assert w1["counter"]["mapsRamBytes"] < w0["counter"]["mapsRamBytes"]
        assert w1["maps"]["sstsMapped"] == 0
        # it opens again: right answers, maps warmed again
        read_all(c, "wc", "IDLEWC", 1500)
        wait_until(lambda: waiter(c)["maps"]["sstsMapped"] > 0, what="maps warmed again")
    finally:
        srv.term()
        shutil.rmtree(disk, ignore_errors=True)
