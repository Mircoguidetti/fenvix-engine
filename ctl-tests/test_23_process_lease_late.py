"""Process lease (`--process-lease`, the default): renewals that reach S3 late (review B2).

Fault injection (`MONGO_TEST_FAULTS_FILE`, test-only) delays the PUTs of P1's own process lease
(`processes/{holder}/_lease`); nothing is frozen (no SIGSTOP), the process keeps running.

1. Renewals that land 3 s late with a 4 s ttl: between two late landings the stored lease is expired for long enough that
   P2 takes the database over. P1 must then stop serving it: from P2's first write on, no read through P1 may return the
   old value and no write through P1 may be acknowledged. Before the fix, a late renewal that still succeeded made the lease
   valid again (same generation) and kept it valid, so P1 went on serving while P2 owned the database.
2. Renewals that are slow but land before the deadline (1 s late): P1 must keep its databases. Before the fix a new renewal
   was started every ttl/12 while one was still in flight; the slower one then failed its condition and P1 dropped every
   database it owned.
"""
import os
import tempfile
import threading
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure, AutoReconnect, NetworkTimeout

from lib import Server, make_bucket

TTL = 4


def client(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000, retryWrites=False, retryReads=False)


def faults_file():
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    return f.name


def test_late_renewals_never_let_the_old_owner_serve_after_a_takeover():
    bucket = make_bucket("plate")
    ff = faults_file()
    p1 = Server(bucket, "plate1", idle=600, holder="P1", extra=["--lease-ttl-secs", str(TTL)], env={"MONGO_TEST_FAULTS_FILE": ff}).start()
    p2 = Server(bucket, "plate2", idle=600, holder="P2", extra=["--lease-ttl-secs", str(TTL)]).start()
    c1, c2 = client(p1), client(p2)
    violations, p2_wrote_at, stop = [], [None], threading.Event()
    try:
        c1["d"]["c"].insert_one({"_id": 1, "v": 1})
        assert c1["d"]["c"].find_one({"_id": 1})["v"] == 1
        with open(ff, "w") as f:
            f.write("delay_put processes/P1/_lease 3000\n")

        def through_p1():
            n = 0
            while not stop.is_set():
                n += 1
                try:
                    d = c1["d"]["c"].find_one({"_id": 1})
                    if p2_wrote_at[0] and d and d.get("v") == 1:
                        violations.append(f"P1 served the old value {time.time() - p2_wrote_at[0]:.1f} s after P2's write")
                except (OperationFailure, AutoReconnect, NetworkTimeout):
                    pass
                try:
                    c1["d"]["c"].insert_one({"_id": f"p1-{n}", "v": 3})
                    if p2_wrote_at[0]:
                        violations.append(f"P1 acknowledged a write {time.time() - p2_wrote_at[0]:.1f} s after P2's write")
                except (OperationFailure, AutoReconnect, NetworkTimeout):
                    pass
                time.sleep(0.05)

        t = threading.Thread(target=through_p1)
        t.start()
        deadline = time.time() + 40
        while time.time() < deadline and p2_wrote_at[0] is None:
            try:
                c2["d"]["c"].update_one({"_id": 1}, {"$set": {"v": 2}})
                p2_wrote_at[0] = time.time()
            except OperationFailure:
                time.sleep(0.2)
        assert p2_wrote_at[0], "P2 never took the database over"
        time.sleep(15)       # P1 keeps renewing late all this time
        stop.set()
        t.join()
        print("violations:", len(violations), violations[:5])
        assert not violations, f"{len(violations)} reads/writes served by P1 after P2 took over: {violations[:3]}"
        assert c2["d"]["c"].find_one({"_id": 1})["v"] == 2
    finally:
        stop.set()
        p1.term()
        p2.term()
        os.unlink(ff)


def test_slow_renewals_that_land_in_time_keep_the_databases():
    bucket = make_bucket("pslow")
    ff = faults_file()
    p1 = Server(bucket, "pslow1", idle=600, holder="S1", extra=["--lease-ttl-secs", str(TTL)], env={"MONGO_TEST_FAULTS_FILE": ff}).start()
    c1 = client(p1)
    try:
        c1["d"]["c"].insert_one({"_id": 1, "v": 1})
        with open(ff, "w") as f:
            f.write("delay_put processes/S1/_lease 1000\n")
        errors = 0
        t0 = time.time()
        while time.time() - t0 < 15:
            try:
                c1["d"]["c"].find_one({"_id": 1})
            except OperationFailure:
                errors += 1
            time.sleep(0.05)
        log = p1.log_text()
        lost = log.count("LEASE lost tenant=d")
        print("refused reads:", errors, "databases dropped:", lost)
        assert lost == 0 and errors == 0, f"slow renewals made P1 drop its database ({lost} times, {errors} refused reads)"
    finally:
        p1.term()
        os.unlink(ff)


def test_takeover_under_clock_skew_revokes_the_old_process_lease():
    """P2's wall clock runs 20 s ahead (clock skew between machines, test-only offset): P2 sees P1's live process lease as
    expired and takes the database over while P1 keeps renewing normally. The takeover revokes P1's process lease, so P1's
    next renewal fails its condition and P1 drops the database: reads through P1 may return the old value only until then
    (at most about one renewal interval, ttl/3), never for the rest of the run. Before the fix nothing ever told P1: its
    renewals kept succeeding and it served the old value (and its own memtable) for as long as it ran."""
    bucket = make_bucket("pskew")
    p1 = Server(bucket, "pskew1", idle=600, holder="K1", extra=["--lease-ttl-secs", str(TTL)]).start()
    p2 = Server(bucket, "pskew2", idle=600, holder="K2", extra=["--lease-ttl-secs", str(TTL)], env={"MONGO_TEST_WALL_CLOCK_OFFSET_MS": "20000"}).start()
    c1, c2 = client(p1), client(p2)
    try:
        c1["d"]["c"].insert_one({"_id": 1, "v": 1})
        assert c1["d"]["c"].find_one({"_id": 1})["v"] == 1
        deadline = time.time() + 20
        wrote = None
        while time.time() < deadline and wrote is None:
            try:
                c2["d"]["c"].update_one({"_id": 1}, {"$set": {"v": 2}})
                wrote = time.time()
            except OperationFailure:
                time.sleep(0.2)
        assert wrote, "P2 never took the database over"
        stale_after = []
        while time.time() - wrote < 12:
            try:
                d = c1["d"]["c"].find_one({"_id": 1})
                if d and d.get("v") == 1:
                    stale_after.append(round(time.time() - wrote, 2))
            except (OperationFailure, AutoReconnect, NetworkTimeout):
                pass
            time.sleep(0.05)
        late = [x for x in stale_after if x > TTL]
        print(f"old value served by P1 {len(stale_after)} times after P2's write, the last {max(stale_after, default=0)} s after; later than ttl: {len(late)}")
        assert not late, f"P1 still served the old value {late[-1]} s after P2 took the database over"
    finally:
        p1.term()
        p2.term()
