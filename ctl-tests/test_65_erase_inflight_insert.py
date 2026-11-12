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

from lib import Server, make_bucket, list_prefix, wait_until, fill


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
        left = list_prefix(bucket, "tenants/infla/")
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
