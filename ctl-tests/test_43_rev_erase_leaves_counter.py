"""Review of waiter 0d74e0c (guarantee 6, deletions complete also on caches and disks).

The control-layer erase (DELETE /v1/databases/{db}, the deletion that returns a "proof") deletes every object on S3
but never tells the waiter's counter: the erased database's documents and index keys stay in the counter's RAM and on
its disk floor, and its SSTs stay mapped. dropDatabase got a hook (correction 1); erase did not.
"""
import shutil

from lib import wait_until
from test_42_waiter_maps_leave import count_on_disk, fill, read_all, start, waiter


def test_erase_leaves_the_counter():
    srv, disk = start("werase")
    try:
        c = srv.client()
        fill(c, "we", "SECRETWE")
        read_all(c, "we", "SECRETWE")
        wait_until(lambda: waiter(c)["maps"]["busy"] == 0 and waiter(c)["maps"]["sstsMapped"] > 0, what="maps loaded")
        c["we"].command("tenantAdmin", close=True)
        assert count_on_disk(disk, b"SECRETWE") > 0, "the test must start with the database on the disk floor"
        st, r = srv.admin.delete("/v1/databases/we")
        assert st == 200 and r["erased"], r
        try:
            wait_until(lambda: count_on_disk(disk, b"SECRETWE") == 0, timeout=5, what="erased database gone from the disk floor")
        except Exception:
            pass
        left = count_on_disk(disk, b"SECRETWE")
        w = waiter(c)
        print("after the erase: marker on disk", left, "| droppedDbs", w["maps"]["droppedDbs"], "| SSTs mapped", w["maps"]["sstsMapped"])
        assert left == 0, f"the erased database's keys stay on the waiter's disk floor ({left} copies)"
    finally:
        srv.term()
        shutil.rmtree(disk, ignore_errors=True)
