"""fix-erase-fork-usage-c (2026-11-12): no usage record of an erased database is written after its erase, whatever was
in flight; a usage record of a real database is never dropped. From the review of fix-erase-fork-usage (U1-U7).

U1 reads racing an erase: after the erase, no usage record of the erased database may appear.
U2 erase, then the same name created again by an insert: the NEW database's usage must be kept (it is billed).
U3 a fork to a name that an insert creates a moment later: the target is a real database, its usage must be kept.
U4 two processes: process B looked at a database (its usage record exists), process A erases it, then B is asked about
   the name again (404): nothing may be left under the erased name.
U5 a database created through the admin API that so far has only storage requests (describe, durability): it exists, its
   usage must be written (billed), not dropped.
U7 404 probes on a name racing its creation: every command of the new database is counted.
U8 two processes: process B keeps asking about a database while process A erases it: nothing is left under the name.
U9 billing guards: a database closed in this process and described (storage requests only) keeps its record; a database
   served by another process and described here keeps the record of this process.
"""
import threading
import time

from lib import Server, make_bucket, list_prefix

FLUSH = ["--usage-flush-secs", "1"]


def usage_objs(bucket, name):
    return [k for k, _ in list_prefix(bucket, f"control/usage/{name}/")]


def start(tag, bucket=None, holder=None):
    b = bucket or make_bucket(tag)
    return b, Server(b, tag, idle=300, extra=FLUSH, holder=holder).start()


def test_u1_reads_racing_an_erase_leave_nothing():
    b, s = start("ru1")
    try:
        left = {}
        for i in range(6):
            db = f"race{i}"
            c = s.client()
            c[db].t.insert_many([{"_id": j, "v": "x" * 100} for j in range(200)])
            stop = threading.Event()

            def reader():
                cr = s.client()
                while not stop.is_set():
                    try:
                        cr[db].t.find_one({"_id": 5})
                    except Exception:
                        pass

            ts = [threading.Thread(target=reader) for _ in range(3)]
            for t in ts:
                t.start()
            time.sleep(0.5)
            st, r = s.admin.delete(f"/v1/databases/{db}")
            assert st == 200, r
            stop.set()
            for t in ts:
                t.join()
            time.sleep(4)
            o = usage_objs(b, db)
            if o:
                left[db] = o
        assert not left, f"usage records of erased databases written after their erase: {left}"
    finally:
        s.term()


def test_u2_a_new_database_with_an_erased_name_keeps_its_usage():
    b, s = start("ru2")
    try:
        c = s.client()
        c.again.t.insert_many([{"_id": j} for j in range(100)])
        time.sleep(2.5)
        st, r = s.admin.delete("/v1/databases/again")
        assert st == 200, r
        # created again at once, then read and written
        c.again.t.insert_many([{"_id": j, "n": 1} for j in range(100)])
        for j in range(20):
            c.again.t.find_one({"_id": j})
        time.sleep(4)
        o = usage_objs(b, "again")
        assert o, "the new database named like the erased one has no usage record (not billed)"
        st, u = s.admin.get("/v1/databases/again/usage")
        assert st == 200, u
        print("U2 usage of the new database:", str(u)[:400])
    finally:
        s.term()


def test_u3_a_fork_target_created_a_moment_later_keeps_its_usage():
    b, s = start("ru3")
    try:
        c = s.client()
        results = []
        for i in range(4):
            src, tgt = f"fs{i}", f"ft{i}"
            c[src].t.insert_many([{"_id": j, "v": "x" * 100} for j in range(300)])
            res = {}
            t1 = threading.Thread(target=lambda: res.update(f=s.admin.post(f"/v1/databases/{src}/fork", {"target": tgt})[0]))
            t1.start()
            time.sleep(0.05 * i)
            try:
                c[tgt].t.insert_one({"_id": "made-by-insert"})
                res["ins"] = "ok"
            except Exception as e:
                res["ins"] = type(e).__name__
            t1.join()
            results.append(res)
        time.sleep(4)
        missing = [f"ft{i}" for i in range(4) if not usage_objs(b, f"ft{i}")]
        print("U3", results)
        assert not missing, f"fork targets that exist (fork or insert) without a usage record: {missing}"
    finally:
        s.term()


def test_u4_two_processes():
    b = make_bucket("ru4")
    _, a = start("ru4a", b, holder="ru4a")
    _, p = start("ru4b", b, holder="ru4b")
    try:
        a.client().two.t.insert_many([{"_id": j} for j in range(50)])
        # process B describes it: storage requests only, B's flusher writes (and may remember) the name
        for _ in range(3):
            st, _r = p.admin.get("/v1/databases/two")
        time.sleep(3)
        st, r = a.admin.delete("/v1/databases/two")
        assert st == 200, r
        time.sleep(1)
        before = usage_objs(b, "two")
        # B is asked about it again: 404, and its flusher runs
        for _ in range(3):
            st, _r = p.admin.get("/v1/databases/two")
        print("U4 B's answer after the erase:", st, "objects right after the erase:", before)
        time.sleep(4)
        left = usage_objs(b, "two")
        assert not left, f"usage records of the erased database written by the other process: {left}"
    finally:
        a.term()
        p.term()


def test_u5_a_created_database_with_storage_requests_only_is_billed():
    b, s = start("ru5")
    try:
        st, r = s.admin.post("/v1/databases", {"name": "fresh"})
        assert st in (200, 201), r
        for _ in range(5):
            s.admin.get("/v1/databases/fresh")
            s.admin.get("/v1/databases/fresh/durability")
        time.sleep(4)
        tenant = [k for k, _ in list_prefix(b, "tenants/fresh/")]
        o = usage_objs(b, "fresh")
        print("U5 objects under tenants/fresh/:", tenant[:6], "usage records:", o)
        assert o, "a database created through the API has no usage record (its requests are not billed)"
    finally:
        s.term()


def _commands(u):
    tot = 0
    for d in u.get("days", []):
        tot += d.get("requests", {}).get("commands", 0)
    return tot


def test_u7_a_404_probe_racing_the_creation_keeps_the_new_databases_commands():
    """Admin GETs of a name answer 404 while a client creates that database by its first insert and keeps running
    commands on it: every command of the new database must be counted."""
    b, s = start("ru7")
    try:
        lost = []
        for i in range(5):
            db = f"born{i}"
            stop = threading.Event()

            def poller():
                while not stop.is_set():
                    s.admin.get(f"/v1/databases/{db}")

            ts = [threading.Thread(target=poller) for _ in range(2)]
            for t in ts:
                t.start()
            c = s.client()
            sent = 0
            for j in range(60):
                c[db].t.insert_one({"_id": j})
                sent += 1
            stop.set()
            for t in ts:
                t.join()
            for j in range(5):
                c[db].t.find_one({"_id": j})
                sent += 1
            time.sleep(3)
            st, u = s.admin.get(f"/v1/databases/{db}/usage")
            got = _commands(u)
            if got < sent:
                lost.append((db, sent, got))
        print("U7 (database, commands sent, commands counted):", lost)
        assert not lost, f"commands of a new database not counted while 404 probes ran on its name: {lost}"
    finally:
        s.term()


def test_u8_two_processes_racing_an_erase():
    b = make_bucket("ru8")
    _, a = start("ru8a", b, holder="ru8a")
    _, p = start("ru8b", b, holder="ru8b")
    try:
        left = {}
        for i in range(4):
            db = f"two{i}"
            a.client()[db].t.insert_many([{"_id": j} for j in range(50)])
            stop = threading.Event()

            def asker():
                while not stop.is_set():
                    p.admin.get(f"/v1/databases/{db}")
                    p.admin.get(f"/v1/databases/{db}/durability")

            ts = [threading.Thread(target=asker) for _ in range(2)]
            for t in ts:
                t.start()
            time.sleep(1.5)
            st, r = a.admin.delete(f"/v1/databases/{db}")
            assert st == 200, r
            time.sleep(0.5)
            stop.set()
            for t in ts:
                t.join()
            time.sleep(4)
            o = usage_objs(b, db)
            if o:
                left[db] = o
        assert not left, f"usage records of erased databases written after their erase (two processes): {left}"
    finally:
        a.term()
        p.term()


def test_u9_real_databases_keep_their_usage_records():
    b = make_bucket("ru9")
    _, a = start("ru9a", b, holder="ru9a")
    _, p = start("ru9b", b, holder="ru9b")
    try:
        # served by A, described by B: B's record of a real database is written
        a.client().served.t.insert_many([{"_id": j} for j in range(50)])
        for _ in range(3):
            st, _r = p.admin.get("/v1/databases/served")
            assert st == 200
        time.sleep(4)
        by_b = [o for o in usage_objs(b, "served") if "/ru9b-" in o]
        assert by_b, f"process B's usage record of an existing database was dropped: {usage_objs(b, 'served')}"
        # closed in A (a clean restart closes every database), then described in A: the record is written
        a.client().closed.t.insert_many([{"_id": j} for j in range(50)])
        time.sleep(3)
        a.restart()
        before = set(usage_objs(b, "closed"))
        for _ in range(3):
            st, _r = a.admin.get("/v1/databases/closed")
            assert st == 200
        time.sleep(4)
        new = set(usage_objs(b, "closed")) - before
        assert new, "a closed database described after a restart has no new usage record (its requests not billed)"
    finally:
        a.term()
        p.term()
