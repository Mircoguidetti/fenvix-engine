"""One lease per process (`--process-lease`, on by default since round3): safety with two processes on the same bucket.

In this mode a process renews only its own lease (`processes/{holder}/_lease`); a database it opens gets an owner record
(`tenants/{db}/_lease`) that names the process lease and its generation, and is valid while that process lease is.
What is checked here, each time with two engine processes P1 and P2 sharing one bucket:
 1. while P1 owns a database, P2 is refused (code 134, retryable) for reads and writes;
 2. P1 frozen (SIGSTOP) past the ttl: P2 takes the database over; P1 resumed believes it still owns it, and its writes are
    refused (its process lease lapsed: every database of the old generation is abandoned), so nothing of P1 lands after the
    takeover; the negative control (`--unsafe-skip-ownership-checks` on P1) shows the write then DOES land, i.e. this check
    is what prevents it;
 3. P1 killed (SIGKILL): P2 is refused until P1's process lease expires, then takes over and reads every write P1 acknowledged;
 4. the same holder id restarting gets its databases back at once;
 5. 40 threads writing one new database through both processes: every acknowledged write is there at the end (two writers
    at once would fence each other and lose acknowledged writes);
 6. cost: open idle databases cost no lease request (renewals are per process, not per database);
 7. mixed modes on one bucket (one process per-database leases, the other per-process): each refuses the other's live records.
"""
import os
import signal
import threading
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure, AutoReconnect

from lib import Server, make_bucket

TTL = 4


def client(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000, retryWrites=False, retryReads=False)


def refused(fn):
    try:
        fn()
        return False
    except OperationFailure as e:
        return e.code == 134
    except AutoReconnect:
        return False


def pair(tag, extra1=(), extra2=()):
    b = make_bucket(tag)
    flags = ["--lease-ttl-secs", str(TTL)]
    p1 = Server(b, f"{tag}1", idle=600, extra=flags + list(extra1)).start()
    p2 = Server(b, f"{tag}2", idle=600, extra=flags + list(extra2)).start()
    return b, p1, p2


def stats(srv):
    return client(srv).admin.command("tenantStats")


def test_other_process_is_refused_while_owner_is_alive():
    b, p1, p2 = pair("pl1")
    try:
        c1, c2 = client(p1), client(p2)
        c1.d.t.insert_one({"_id": 1})
        assert refused(lambda: c2.d.t.insert_one({"_id": 2}))
        assert refused(lambda: c2.d.t.find_one({"_id": 1}))
        # still refused after several ttls: the owner renews its process lease, not the database's record
        time.sleep(TTL * 2.5)
        assert refused(lambda: c2.d.t.insert_one({"_id": 3}))
        assert c1.d.t.count_documents({}) == 1
    finally:
        p1.term(); p2.term()


def frozen_owner(unsafe):
    b, p1, p2 = pair("pl2u" if unsafe else "pl2", extra1=["--unsafe-skip-ownership-checks"] if unsafe else [])
    try:
        c1, c2 = client(p1), client(p2)
        c1.d.t.insert_one({"_id": "p1-a"})
        os.kill(p1.proc.pid, signal.SIGSTOP)
        try:
            deadline = time.time() + TTL * 6
            took = False
            while time.time() < deadline:
                try:
                    c2.d.t.insert_one({"_id": "p2-a"})
                    took = True
                    break
                except OperationFailure as e:
                    assert e.code == 134
                    time.sleep(0.5)
            assert took, "P2 never took the database over after P1 stopped renewing"
        finally:
            os.kill(p1.proc.pid, signal.SIGCONT)
        # P1, resumed, still believes it owns the database: a read and a write through it
        try:
            late_read = "served " + ",".join(sorted(d["_id"] for d in c1.d.t.find()))
        except OperationFailure as e:
            late_read = f"refused {e.code}"
        except AutoReconnect as e:
            late_read = f"error {e}"
        try:
            c1.d.t.insert_one({"_id": "p1-late"})
            late = "accepted"
        except OperationFailure as e:
            late = f"refused {e.code}"
        except AutoReconnect as e:
            late = f"error {e}"
        time.sleep(1)
        # P2 restarts so it reads the store, not its memory
        p2.restart()
        docs = sorted(d["_id"] for d in client(p2).d.t.find())
        return late, late_read, docs
    finally:
        p1.term(); p2.term()


def test_frozen_owner_cannot_read_or_write_after_takeover():
    late, late_read, docs = frozen_owner(unsafe=False)
    assert late_read.startswith("refused 134"), late_read
    assert late.startswith("refused 134"), late
    assert docs == ["p1-a", "p2-a"], docs


def test_negative_control_without_ownership_checks_the_frozen_owner_serves_stale_data():
    late, late_read, docs = frozen_owner(unsafe=True)
    # With the ownership checks off the resumed process goes on serving the database as if it still owned it: it answers a
    # read from its own stale view, without the write P2 made meanwhile. (Its late write is stopped anyway by slatedb's
    # writer fencing, the second line of defence, so it does not land.) This is what the process-lease check prevents.
    assert late_read == "served p1-a", late_read
    assert "p1-late" not in docs, docs


def test_killed_owner_taken_over_with_every_acknowledged_write():
    b, p1, p2 = pair("pl3")
    try:
        c1, c2 = client(p1), client(p2)
        acked = []
        for i in range(200):
            c1.d.t.insert_one({"_id": i})
            acked.append(i)
        p1.kill9()
        t0 = time.time()
        assert refused(lambda: c2.d.t.find_one({}))
        while True:
            try:
                n = c2.d.t.count_documents({})
                break
            except OperationFailure as e:
                assert e.code == 134
                assert time.time() - t0 < TTL * 4
                time.sleep(0.3)
        waited = time.time() - t0
        assert n == len(acked)
        assert waited >= TTL * 0.5, waited  # refused until P1's process lease expired
    finally:
        p1.term(); p2.term()


def test_same_holder_restart_gets_databases_back_at_once():
    b = make_bucket("pl4")
    p1 = Server(b, "pl4", idle=600, holder="same-holder", extra=["--lease-ttl-secs", "30"]).start()
    try:
        client(p1).d.t.insert_one({"_id": 1})
        p1.kill9()
        p1.proc = None
        p1.start()
        t0 = time.time()
        assert client(p1).d.t.count_documents({}) == 1
        assert time.time() - t0 < 10  # not the 30 s ttl
    finally:
        p1.term()


def test_racing_writers_lose_no_acknowledged_write():
    b, p1, p2 = pair("pl5")
    try:
        acked, lock = [], threading.Lock()

        def worker(w):
            c = client(p1 if w % 2 else p2)
            for i in range(25):
                try:
                    c.race.t.insert_one({"_id": f"{w}-{i}"})
                    with lock:
                        acked.append(f"{w}-{i}")
                except (OperationFailure, AutoReconnect):
                    pass
        ts = [threading.Thread(target=worker, args=(w,)) for w in range(40)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        p1.term(); p2.term()
        p3 = Server(b, "pl5c", idle=600).start()
        try:
            have = {d["_id"] for d in client(p3).race.t.find()}
        finally:
            p3.term()
        assert acked, "no write succeeded"
        assert set(acked) <= have, f"{len(set(acked) - have)} acknowledged writes lost"
    finally:
        p1.term(); p2.term()


def test_idle_open_databases_cost_no_lease_request():
    b = make_bucket("pl6")
    p = Server(b, "pl6", idle=600, extra=["--lease-ttl-secs", "6"]).start()
    try:
        c = client(p)
        for i in range(20):
            c[f"db{i}"].t.insert_one({"_id": 1})
        s0 = stats(p)
        time.sleep(12)
        s1 = stats(p)
        renewals = s1["leases"]["renewals"] - s0["leases"]["renewals"]
        lease_puts = s1["putByCategory"]["lease"] - s0["putByCategory"]["lease"]
        assert s1["leasesHeld"] >= 20
        # one process lease renewed every ttl/3 = 2 s: ~6 in 12 s whatever the number of databases (per-database leases: ~120)
        assert renewals <= 8, renewals
        assert lease_puts <= 8, lease_puts
    finally:
        p.term()


@pytest.mark.parametrize("first,second", [("true", "false"), ("false", "true")])
def test_mixed_modes_refuse_each_other(first, second):
    b, p1, p2 = pair(f"pl7{first[0]}", extra1=["--process-lease", first], extra2=["--process-lease", second])
    try:
        c1, c2 = client(p1), client(p2)
        c1.d.t.insert_one({"_id": 1})
        assert refused(lambda: c2.d.t.insert_one({"_id": 2}))
        assert refused(lambda: c2.d.t.find_one({}))
        time.sleep(TTL * 2)
        assert refused(lambda: c2.d.t.insert_one({"_id": 3}))
    finally:
        p1.term(); p2.term()
