"""Review of c2-fix-outage-loss (fenvix-brain prompts/R-review-c2-fix-outage-loss.md).

The fix refuses the write of a command whose database was abandoned (lease lost) and opened again after the command began
to use it (`dbs::Reopened`). The pin is cleared at every `Dbs::lock`, so a transaction, whose statements and commit are
separate commands, is not covered: its insert statements take natural-order numbers from one opening, and its commit can
run on the next opening.

1. test_txn_commit_after_a_reopen_keeps_a_concurrent_insert_in_scans (one process): a transaction inserts "A" (it takes
   natural-order number n from opening 1), the lease is lost and the database abandoned, a plain insert of "B" reopens it
   (opening 2 reloads the counter from storage, where n is not written, and gives "B" number n too), then the transaction
   commits (no statement after the reopen, so nothing reads through the old snapshot). Fails when an acknowledged document
   is missing from a collection scan, now or after a restart.
2. test_txn_commit_after_a_takeover_loses_no_acknowledged_update (two processes): a transaction on P1 increments a counter,
   P1 loses its lease, P2 takes the database over and increments the same counter (acknowledged), P2 closes it, then the
   transaction commits on P1. Fails when the final counter does not count every acknowledged increment (P2's increment
   overwritten by a commit computed on P1's old opening; mongod aborts such a transaction with a write conflict).
3. test_mixed_writes_spanning_lease_losses: ordered and unordered insertMany, update with upsert ($inc), findAndModify
   ($inc), and transactions inserting, all while the process lease renewals are delayed past the lease. Every acknowledged
   insert must be found by `_id`, by a collection scan and by a secondary index, and every counter must equal the number of
   acknowledged increments (a refused one applied, or an acknowledged one lost, both fail), now and after a restart.
"""
import os
import tempfile
import threading
import time

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import PyMongoError, BulkWriteError

from lib import Server, make_bucket, free_port

TTL = 2


def faults_file():
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    return f.name


def rs_server(bucket, tag, holder, ff=None, idle=600):
    port = free_port()
    env = {"MONGO_TEST_FAULTS_FILE": ff} if ff else {}
    return Server(bucket, tag, idle=idle, holder=holder, port=port, env=env,
                  extra=["--lease-ttl-secs", str(TTL), "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()


def client(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000, retryWrites=False, retryReads=False)


def lose_lease(srv, ff, holder):
    """Delay the process-lease renewals until the log says the lease was lost, then let them through again."""
    lost0 = srv.log_text().count("LEASE lost")
    with open(ff, "w") as fh:
        fh.write(f"delay_put processes/{holder}/_lease {TTL * 1000 + 1500}\n")
    deadline = time.time() + 30
    while srv.log_text().count("LEASE lost") == lost0 and time.time() < deadline:
        time.sleep(0.1)
    with open(ff, "w") as fh:
        fh.write("")
    assert srv.log_text().count("LEASE lost") > lost0, "the lease was not lost: the test did not reach the case"


def scan_ids(col):
    return [d["_id"] for d in col.find({}, {"_id": 1}).hint([("$natural", 1)])]


def retry_until_ok(fn, what, secs=40):
    deadline, last = time.time() + secs, None
    while time.time() < deadline:
        try:
            return fn()
        except PyMongoError as e:
            last = e
            time.sleep(0.2)
    raise AssertionError(f"{what} never succeeded: {last}")


def test_txn_commit_after_a_reopen_keeps_a_concurrent_insert_in_scans():
    bucket = make_bucket("rvtxnnat")
    ff = faults_file()
    srv = rs_server(bucket, "rvtxnnat", "T1", ff)
    acked = []
    try:
        c = client(srv)
        col = c.d.t
        col.insert_many([{"_id": f"pre{i}"} for i in range(10)])
        acked += [f"pre{i}" for i in range(10)]
        s = c.start_session()
        s.start_transaction()
        col.insert_one({"_id": "A"}, session=s)
        acq0 = srv.log_text().count("LEASE acquired")
        lose_lease(srv, ff, "T1")
        time.sleep(TTL + 1)
        # a plain insert opens the database again (opening 2)
        retry_until_ok(lambda: col.insert_one({"_id": "B"}), "the insert of B")
        acked.append("B")
        reopened = srv.log_text().count("LEASE acquired") > acq0
        commit = "committed"
        try:
            s.commit_transaction()
            acked.append("A")
        except PyMongoError as e:
            commit = f"refused: {type(e).__name__} {getattr(e, 'code', None)} {str(e)[:160]}"
        s.end_session()
        st = c.admin.command("tenantStats")["maintenance"]
        scanned = scan_ids(col)
        by_id = [i for i in acked if col.find_one({"_id": i}) is not None]
        print(f"reopened {reopened}; commit {commit}; reopenedWritesRefused {st.get('reopenedWritesRefused')}")
        print(f"acknowledged {acked}\nscanned {scanned}\nfound by _id {len(by_id)} of {len(acked)}")
        assert reopened, "the database was not opened again: the test did not reach the case"
        missing = [i for i in acked if i not in scanned]
        missing_by_id = [i for i in acked if i not in by_id]
        twice = len(scanned) - len(set(scanned))
    finally:
        srv.term()
    srv2 = Server(bucket, "rvtxnnat2", idle=600).start()
    try:
        c2 = srv2.client()
        scanned2 = scan_ids(c2.d.t)
        missing2 = [i for i in acked if i not in scanned2]
        by_id2 = [i for i in acked if c2.d.t.find_one({"_id": i}) is not None]
        print(f"after a restart: scanned {scanned2}; found by _id {len(by_id2)} of {len(acked)}")
    finally:
        srv2.term()
        os.unlink(ff)
    assert not missing_by_id, f"acknowledged documents missing by _id: {missing_by_id}"
    assert twice == 0, f"documents scanned twice: {scanned}"
    assert not missing, f"acknowledged documents missing from a collection scan: {missing} (commit {commit})"
    assert not missing2, f"after a restart: acknowledged documents missing from a collection scan: {missing2}"


def test_txn_commit_after_a_takeover_loses_no_acknowledged_update():
    bucket = make_bucket("rvtxnlu")
    ff = faults_file()
    p1 = rs_server(bucket, "rvtxnlu1", "P1", ff)
    p2 = rs_server(bucket, "rvtxnlu2", "P2", idle=2)
    try:
        c1, c2 = client(p1), client(p2)
        c1.d.c.insert_one({"_id": 1, "n": 0})
        acked_incs = 0
        s = c1.start_session()
        s.start_transaction()
        assert c1.d.c.find_one({"_id": 1}, session=s)["n"] == 0
        c1.d.c.update_one({"_id": 1}, {"$inc": {"n": 1}}, session=s)
        lose_lease(p1, ff, "P1")
        # P2 takes the database over and increments the counter (acknowledged)
        retry_until_ok(lambda: c2.d.c.update_one({"_id": 1}, {"$inc": {"n": 1}}), "P2's increment")
        acked_incs += 1
        p2_saw = c2.d.c.find_one({"_id": 1})["n"]
        # P2 goes idle and closes the database (releases its lease): P1 can open it again
        deadline = time.time() + 30
        while time.time() < deadline and "LEASE writer_close tenant=d " not in p2.log_text():
            time.sleep(0.2)
        assert "LEASE writer_close tenant=d " in p2.log_text(), "P2 never closed the database"
        time.sleep(1)
        commit = "committed"
        try:
            s.commit_transaction()
            acked_incs += 1
        except PyMongoError as e:
            commit = f"refused: {type(e).__name__} {getattr(e, 'code', None)} {str(e)[:160]}"
        s.end_session()
        final = retry_until_ok(lambda: c1.d.c.find_one({"_id": 1})["n"], "P1's read")
        print(f"P2 saw n={p2_saw} after its increment; commit on P1: {commit}; final n={final}; acknowledged increments {acked_incs}")
        assert p2_saw == 1, f"P2's increment did not land on the stored counter (n={p2_saw})"
        assert final == acked_incs, f"final counter {final}, acknowledged increments {acked_incs} (commit {commit})"
    finally:
        p1.term()
        p2.term()
        os.unlink(ff)


ROUNDS = int(os.environ.get("T52R_ROUNDS", "5"))


def test_mixed_writes_spanning_lease_losses():
    bucket = make_bucket("rvmixed")
    ff = faults_file()
    srv = rs_server(bucket, "rvmixed", "M1", ff)
    lock = threading.Lock()
    acked = set()          # inserted _ids acknowledged
    incs = {}              # counter _id -> acknowledged increments
    errors = []
    stop = threading.Event()
    try:
        c = client(srv)
        c.d.t.create_index("k")
        c.d.t.insert_one({"_id": "seed", "k": -1})
        acked.add("seed")
        c.d.cnt.insert_one({"_id": "seed"})

        def note_err(e):
            with lock:
                errors.append(f"{type(e).__name__} {getattr(e, 'code', None)} {str(e)[:120]}")

        def ins(ordered, tag):
            cl = client(srv)
            b = 0
            while not stop.is_set():
                ids = [f"{tag}-{b}-{i}" for i in range(1000)]
                b += 1
                time.sleep(0.2)
                try:
                    cl.d.t.insert_many([{"_id": x, "k": b, "f": "x" * 40} for x in ids], ordered=ordered)
                    with lock:
                        acked.update(ids)
                except BulkWriteError as e:
                    # partly applied: the documents before the first error (ordered) or all but the failed ones
                    det = e.details
                    bad = {we["index"] for we in det.get("writeErrors", [])}
                    ok = [x for i, x in enumerate(ids) if i not in bad and (not ordered or i < min(bad, default=len(ids)))]
                    if det.get("nInserted") == len(ok):
                        with lock:
                            acked.update(ok)
                    note_err(e)
                except PyMongoError as e:
                    note_err(e)
            cl.close()

        def upserts(tag):
            cl = client(srv)
            n = 0
            while not stop.is_set():
                key = f"{tag}-{n % 50}"
                n += 1
                try:
                    if n % 2:
                        cl.d.cnt.update_one({"_id": key}, {"$inc": {"v": 1}}, upsert=True)
                    else:
                        cl.d.cnt.find_one_and_update({"_id": key}, {"$inc": {"v": 1}}, upsert=True, return_document=ReturnDocument.AFTER)
                    with lock:
                        incs[key] = incs.get(key, 0) + 1
                except PyMongoError as e:
                    note_err(e)
            cl.close()

        def txns(tag):
            cl = client(srv)
            n = 0
            while not stop.is_set():
                n += 1
                ids = [f"{tag}-{n}-{i}" for i in range(20)]
                try:
                    with cl.start_session() as s:
                        s.start_transaction()
                        for x in ids[:10]:
                            cl.d.t.insert_one({"_id": x, "k": n}, session=s)
                        time.sleep(0.3)
                        cl.d.t.insert_many([{"_id": x, "k": n} for x in ids[10:]], session=s)
                        time.sleep(0.3)
                        s.commit_transaction()
                    with lock:
                        acked.update(ids)
                except PyMongoError as e:
                    note_err(e)
            cl.close()

        ths = [threading.Thread(target=ins, args=(True, "o")), threading.Thread(target=ins, args=(False, "u")),
               threading.Thread(target=upserts, args=("c1",)), threading.Thread(target=upserts, args=("c2",)),
               threading.Thread(target=txns, args=("x1",)), threading.Thread(target=txns, args=("x2",))]
        for t in ths:
            t.start()
        try:
            for r in range(ROUNDS):
                time.sleep(1.5)
                with open(ff, "w") as fh:
                    fh.write(f"delay_put processes/M1/_lease {TTL * 1000 + 1500}\n")
                time.sleep(TTL + 1.0)
                with open(ff, "w") as fh:
                    fh.write("")
                time.sleep(TTL + 2)
        finally:
            stop.set()
            for t in ths:
                t.join(150)
        losses = srv.log_text().count("LEASE lost")
        st = c.admin.command("tenantStats")["maintenance"]
        kinds = {}
        for e in errors:
            k = e.split(" ")[0] + " " + e.split(" ")[1]
            kinds[k] = kinds.get(k, 0) + 1
        print(f"lease losses {losses}; reopenedWritesRefused {st.get('reopenedWritesRefused')}; errors {kinds}; samples {errors[:3]}")
        problems = check(c, acked, incs, "now")
        assert losses > 0, "no lease loss: the test did not reach the case"
    finally:
        stop.set()
        srv.term()
    srv2 = Server(bucket, "rvmixed2", idle=600).start()
    try:
        problems += retry_until_ok(lambda: check(srv2.client(), acked, incs, "after a restart"), "the read after a restart", 300)
    finally:
        srv2.term()
        os.unlink(ff)
    assert not problems, "\n".join(problems)


def check(c, acked, incs, when):
    t = c.d.t
    scanned = scan_ids(t)
    sset = set(scanned)
    by_index = set(d["_id"] for d in t.find({"k": {"$gte": -1}}, {"_id": 1}).hint("k_1"))
    stored = set(d["_id"] for d in t.find({}, {"_id": 1}).hint("_id_"))
    out = []
    print(f"{when}: acknowledged inserts {len(acked)}, scanned {len(scanned)}, by _id index {len(stored)}, by k index {len(by_index)}")
    if len(scanned) != len(sset):
        out.append(f"{when}: {len(scanned) - len(sset)} documents scanned twice")
    for name, have in (("collection scan", sset), ("_id index", stored), ("k index", by_index)):
        miss = sorted(acked - have)
        if miss:
            out.append(f"{when}: {len(miss)} acknowledged inserts missing from the {name}, e.g. {miss[:5]}")
    if sset != stored:
        out.append(f"{when}: scan and _id index differ: only in scan {sorted(sset - stored)[:5]}, only by _id {sorted(stored - sset)[:5]}")
    vals = {d["_id"]: d.get("v", 0) for d in c.d.cnt.find({})}
    bad = {k: (vals.get(k, 0), n) for k, n in incs.items() if vals.get(k, 0) != n}
    if bad:
        out.append(f"{when}: {len(bad)} counters differ from their acknowledged increments (stored, acknowledged), e.g. {list(bad.items())[:5]}")
    print("\n".join(out) or f"{when}: OK")
    return out
