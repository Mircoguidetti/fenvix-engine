"""A read command longer than the serving window (fix-long-commands).

Found by the integration-4 owner: right after a store outage, an unfiltered find over about 700,000 documents was refused
with F7 ("this process's serving lease expired; it begins no new read") at every retry. The tenant's F7 deadline
(`serving_until`) was set only when a command opened the database: every step of one command compared itself with the
lease as it was at the command's start, so a command longer than the window (the lease ttl minus a sixth, counted from the
last renewal before it) failed, however often the lease was renewed meanwhile; and a retry started again from zero.

Here: lease ttl 3 s (a window of 2.5 s at most), the database closed so the reads are cold, and every GET of the database
delayed (fault injection), so an unfiltered find lasts longer than the window. Fails when the find is refused at every
attempt, or when it does not return every document exactly once.
"""
import os
import tempfile
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket

N = int(os.environ.get("T55_DOCS", "100000"))
DELAY_MS = int(os.environ.get("T55_DELAY_MS", "60"))


def setup_reader(holder):
    """A server (lease ttl 3 s, holder `holder`) whose database d holds N documents, published as versions of our runs
    and closed when idle, then woken by a small read; every GET of d delayed by DELAY_MS from then on."""
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    srv = Server(make_bucket("longcmd"), "longcmd", idle=3, holder=holder, extra=["--lease-ttl-secs", "3"],
                 env={"MONGO_TEST_FAULTS_FILE": f.name, "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_E1_WAIT_MS": "0", "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}).start()
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=300000, retryReads=False)
    for s in range(0, N, 5000):
        c.d.t.insert_many([{"_id": i, "pad": "x" * 200} for i in range(s, min(N, s + 5000))])
    deadline = time.time() + 60
    while time.time() < deadline and "LEASE writer_close tenant=d " not in srv.log_text():
        time.sleep(0.5)
    time.sleep(2)
    with open(f.name, "w") as fh:
        fh.write(f"delay_get tenants/d/ {DELAY_MS}\n")
    c.d.t.find_one({"_id": 0})
    return srv, c, f.name


def test_a_long_find_is_still_refused_when_the_lease_is_lost():
    """The other side of F7: when the lease does lapse during a long find, the find is refused, never answered."""
    import threading
    srv, c, ff = setup_reader("L1")
    try:
        def lapse():
            time.sleep(1.0)
            with open(ff, "a") as fh:
                fh.write("delay_put processes/L1/_lease 4500\n")
        th = threading.Thread(target=lapse)
        th.start()
        t0 = time.time()
        try:
            n = len(list(c.d.t.find({}, {"_id": 1}).batch_size(10_000)))
            out = f"answered {n} documents"
        except PyMongoError as e:
            out = f"refused after {time.time() - t0:.1f} s: {getattr(e, 'code', None)} {str(e)[:100]}"
        th.join()
        lost = srv.log_text().count("LEASE lost")
        print(f"lease lost {lost} times during the find; the find: {out}")
        # refused by F7 or by the read's ownership re-check (`ReadView`), both retryable (134); never answered
        assert out.startswith("refused") and " 134 " in out and ("F7" in out or "ownership lost" in out), out
    finally:
        srv.term()
        os.unlink(ff)


def test_unfiltered_find_longer_than_the_serving_window():
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    # the writer publishes versions of our runs (compactor, small knobs) and closes when idle: the find then reads through
    # them as a reader, and every step of it goes through the F7 check
    srv = Server(make_bucket("longcmd"), "longcmd", idle=3, extra=["--lease-ttl-secs", "3"],
                 env={"MONGO_TEST_FAULTS_FILE": f.name, "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_E1_WAIT_MS": "0", "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}).start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=300000, retryReads=False)
        for s in range(0, N, 5000):
            c.d.t.insert_many([{"_id": i, "pad": "x" * 200} for i in range(s, min(N, s + 5000))])
        deadline = time.time() + 60
        while time.time() < deadline and "LEASE writer_close tenant=d " not in srv.log_text():
            time.sleep(0.5)
        time.sleep(2)
        with open(f.name, "w") as fh:
            fh.write(f"delay_get tenants/d/ {DELAY_MS}\n")
        c.d.t.find_one({"_id": 0})   # wakes the database (the long find is not the command that opens it)
        attempts, ok = [], None
        for a in range(4):
            t0 = time.time()
            try:
                ids = [d["_id"] for d in c.d.t.find({}, {"_id": 1}).batch_size(10_000)]
                attempts.append(f"attempt {a}: {len(ids)} documents in {time.time() - t0:.1f} s")
                ok = ids
                break
            except PyMongoError as e:
                attempts.append(f"attempt {a}: refused after {time.time() - t0:.1f} s: {getattr(e, 'code', None)} {str(e)[:110]}")
        print("\n".join(attempts))
        print(f"F7 refusals in the server log: {srv.log_text().count('it begins no new read (F7)')}")
        with open(f.name, "w") as fh:
            fh.write("")
        assert any("refused" not in x for x in attempts), "the find was refused at every attempt"
        assert sorted(ok) == list(range(N)), f"{len(ok)} documents, {len(set(ok))} distinct"
        assert any(float(x.split(" in ")[1].split(" s")[0]) > 3.0 for x in attempts if "refused" not in x), \
            "the find was not longer than the serving window: the test did not reach the case"
    finally:
        srv.term()
        os.unlink(f.name)
