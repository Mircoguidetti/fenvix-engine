"""An insert in flight across an erase (the test_05 "lost id", 2026-11-12), deterministic: no timing luck.

Group commit (on by default): an insert stages its batch under the database's entry lock, releases the lock, and replies
only once the batch is durable. So an erase can take the lock between an insert's staging and its reply.

- test_a_insert_staged_before_the_erase_is_erased_with_it: every WAL write of the database is delayed (fault file), so an
  insert X sits staged in the old writer, waiting for durability, while the erase is sent. X was written into the database
  that is being erased: after the erase, X must be gone and nothing may be left under the name, whatever X's reply was (ok
  or an error). Its reply time is recorded against the erase reply.
- test_b_inserts_waiting_behind_the_erase_land_in_the_new_database: the erase is paused while it holds the entry lock
  (failpoint `erase.before_proof_two`); five inserts are sent during the pause and wait behind it. Each must be
  acknowledged after the erase answered, and each must be in the re-created database (rule 8: a write acknowledged after
  the erase, on the new database, may never be missing), also after a restart.
"""
import os
import tempfile
import threading
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket, list_prefix, live_prefix, wait_until, fill


def erase(srv, db, tries=20):
    """DELETE /v1/databases/<db>; a 503 close_pending (the old writer still closing) is retried, as a client would."""
    for _ in range(tries):
        st, r = srv.admin.delete(f"/v1/databases/{db}")
        if st != 503:
            return st, r
        time.sleep(0.5)
    return st, r


def test_a_insert_staged_before_the_erase_is_erased_with_it():
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("e65a")
    srv = Server(bucket, "e65a", idle=300, env={"MONGO_TEST_FAULTS_FILE": ff.name}).start()
    try:
        c = srv.client()
        fill(c, "infla", "c", 200)
        out = {}
        with open(ff.name, "w") as fh:
            fh.write("delay_put tenants/infla/wal/ 1500\n")

        def insert_x():
            cx = MongoClient(srv.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=60000)
            t0 = time.time()
            try:
                cx.infla.c.insert_one({"_id": "X"})
                out["x"] = ("ok", t0, time.time())
            except PyMongoError as e:
                out["x"] = (f"error {getattr(e, 'code', None)} {str(e)[:120]}", t0, time.time())
        th = threading.Thread(target=insert_x)
        th.start()
        time.sleep(0.3)                     # X is staged and waits for its WAL write (1.5 s)
        t_e0 = time.time()
        st, r = erase(srv, "infla")
        t_done = time.time()
        th.join(timeout=60)
        with open(ff.name, "w") as fh:
            fh.write("")
        assert st == 200, (st, r)
        kind, ts, ta = out["x"]
        time.sleep(3)                       # any write still in flight lands now
        left = live_prefix(bucket, "tenants/infla/")
        found = c.infla.c.find_one({"_id": "X"})
        print(f"X: {kind}, sent {ts - t_done:+.3f} s, answered {ta - t_done:+.3f} s relative to the erase reply (erase took {t_done - t_e0:.3f} s); "
              f"X now: {found}; objects under tenants/infla/: {left[:5]}")
        assert found is None, "X was staged into the erased database: it must be gone with it"
        assert left == [], f"objects left under the erased name: {left[:5]}"
    finally:
        srv.term()
        os.unlink(ff.name)


def test_b_inserts_waiting_behind_the_erase_land_in_the_new_database():
    bucket = make_bucket("e65b")
    srv = Server(bucket, "e65b", idle=300, env={"CTL_FAILPOINT": "erase.before_proof_two", "CTL_FAILPOINT_PAUSE_MS": "2000"}).start()
    try:
        c = srv.client()
        fill(c, "inflb", "c", 200)
        res = {}
        th = threading.Thread(target=lambda: res.update(r=srv.admin.delete("/v1/databases/inflb"), t=time.time()))
        th.start()
        wait_until(lambda: "FAILPOINT erase.before_proof_two reached" in srv.log_text(), 30, what="failpoint")
        acks = {}

        def insert_y(k):
            cy = MongoClient(srv.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=60000)
            try:
                cy.inflb.c.insert_one({"_id": f"Y{k}"})
                acks[k] = ("ok", time.time())
            except PyMongoError as e:
                acks[k] = (f"error {getattr(e, 'code', None)} {str(e)[:100]}", time.time())
        ys = [threading.Thread(target=insert_y, args=(k,)) for k in range(5)]
        for t in ys:
            t.start()
        th.join(timeout=60)
        for t in ys:
            t.join(timeout=60)
        st, r = res["r"]
        assert st == 200, (st, r)
        oks = sorted(k for k, v in acks.items() if v[0] == "ok")
        errs = {k: v[0] for k, v in acks.items() if v[0] != "ok"}
        after = all(acks[k][1] >= res["t"] - 0.05 for k in oks)
        ids = sorted(d["_id"] for d in c.inflb.c.find({}))
        srv.restart()
        ids2 = sorted(d["_id"] for d in srv.client().inflb.c.find({}))
        print(f"inserts sent during the erase: acked {oks}, errors {errs}; all acked after the erase reply: {after}; "
              f"new database: {ids}; after a restart: {ids2}")
        assert ids == [f"Y{k}" for k in oks] and ids2 == ids, "an insert acknowledged after the erase is missing from the re-created database"
        assert after
    finally:
        srv.term()


def test_c_insert_staged_into_an_opening_abandoned_by_a_lease_loss():
    """An insert X stages its batch and waits for its WAL write (delayed); meanwhile the process lease lapses (its
    renewal delayed 4 s, ttl 3 s; its WAL write 12 s) and the database is abandoned. X must never be acknowledged unless it is durable: after
    the abandon either the client got an error, or X is there after a restart (kill -9, then a fresh read)."""
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("e65c")
    srv = Server(bucket, "e65c", idle=300, holder="H65", extra=["--lease-ttl-secs", "3"], env={"MONGO_TEST_FAULTS_FILE": ff.name}).start()
    try:
        c = srv.client()
        fill(c, "inflc", "c", 200)
        out = {}
        with open(ff.name, "w") as fh:
            fh.write("delay_put tenants/inflc/wal/ 12000\ndelay_put processes/H65/_lease 4000\n")

        def insert_x():
            cx = MongoClient(srv.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=60000, retryWrites=False)
            t0 = time.time()
            try:
                cx.inflc.c.insert_one({"_id": "X"})
                out["x"] = ("ok", time.time() - t0)
            except PyMongoError as e:
                out["x"] = (f"error {getattr(e, 'code', None)} {str(e)[:150]}", time.time() - t0)
        th = threading.Thread(target=insert_x)
        th.start()
        th.join(timeout=60)
        with open(ff.name, "w") as fh:
            fh.write("")
        log = srv.log_text()
        lost = log.count("LEASE lost") + log.count("process_lease_lapsed")
        kind, took = out["x"]
        time.sleep(2)
        srv.kill9()
        srv.proc = None
        srv.start()
        found = None
        for _ in range(60):
            try:
                found = srv.client().inflc.c.find_one({"_id": "X"})
                break
            except PyMongoError:
                time.sleep(0.5)
        n = srv.client().inflc.c.count_documents({})
        print(f"X: {kind} after {took:.1f} s; lease lost/lapsed lines {lost}; after kill -9 and restart X is {'present' if found else 'MISSING'}; documents {n}")
        assert lost > 0, "the lease never lapsed: the test did not reach the case"
        assert n >= 200, "acknowledged documents of the fill are missing"
        if kind == "ok":
            assert found is not None, "RULE 8: X was acknowledged but is missing after a restart"
    finally:
        srv.term()
        os.unlink(ff.name)


def test_c2_insert_staged_into_an_opening_taken_over_by_another_process():
    """As C, but a second process P2 takes the database over while X waits, and writes to it. X (staged in P1's opening)
    must be either refused, or durable and visible to P2 and after both restart; P2's acknowledged writes must all stay."""
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("e65c2")
    p1 = Server(bucket, "e65c2a", idle=300, holder="P65a", extra=["--lease-ttl-secs", "3"], env={"MONGO_TEST_FAULTS_FILE": ff.name}).start()
    p2 = Server(bucket, "e65c2b", idle=300, holder="P65b", extra=["--lease-ttl-secs", "3"]).start()
    try:
        c1 = p1.client()
        fill(c1, "inflc2", "c", 200)
        out = {}
        with open(ff.name, "w") as fh:
            fh.write("delay_put tenants/inflc2/wal/ 15000\ndelay_put processes/P65a/_lease 4000\n")

        def insert_x():
            cx = MongoClient(p1.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=60000, retryWrites=False)
            t0 = time.time()
            try:
                cx.inflc2.c.insert_one({"_id": "X"})
                out["x"] = ("ok", time.time() - t0)
            except PyMongoError as e:
                out["x"] = (f"error {getattr(e, 'code', None)} {str(e)[:150]}", time.time() - t0)
        th = threading.Thread(target=insert_x)
        th.start()
        c2 = MongoClient(p2.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=60000, retryWrites=False)
        p2_acked, t0 = [], time.time()
        while th.is_alive() and time.time() - t0 < 40:
            k = len(p2_acked)
            try:
                c2.inflc2.c.insert_one({"_id": f"P2-{k}"})
                p2_acked.append(f"P2-{k}")
            except PyMongoError:
                time.sleep(0.3)
        th.join(timeout=60)
        with open(ff.name, "w") as fh:
            fh.write("")
        kind, took = out["x"]
        x_on_p2 = None
        try:
            x_on_p2 = c2.inflc2.c.find_one({"_id": "X"})
        except PyMongoError as e:
            x_on_p2 = f"read error {str(e)[:80]}"
        p1.kill9()
        p2.term()
        p2.proc = None
        p2.start()
        ids = set()
        for _ in range(60):
            try:
                ids = set(d["_id"] for d in p2.client().inflc2.c.find({}))
                break
            except PyMongoError:
                time.sleep(0.5)
        missing_p2 = [i for i in p2_acked if i not in ids]
        print(f"X: {kind} after {took:.1f} s; P2 acknowledged {len(p2_acked)} writes during X's wait; X seen by P2: {x_on_p2}; "
              f"after restarts X {'present' if 'X' in ids else 'absent'}, P2's acknowledged writes missing {missing_p2}; documents {len(ids)}")
        assert p2_acked, "P2 never took the database over: the test did not reach the case"
        assert not missing_p2, "RULE 8: acknowledged writes of the new owner are missing"
        assert len([i for i in ids if isinstance(i, int)]) == 200, "acknowledged documents of the fill are missing"
        if kind == "ok":
            assert "X" in ids, "RULE 8: X was acknowledged but is missing after the takeover and the restarts"
    finally:
        p1.term()
        p2.term()
        os.unlink(ff.name)
