"""A database abandoned (lease lost) and opened again while one write command runs (c2-fix-outage-loss).

Found by the review of c2-fix-clone-uploads (ctl test_31 with sealed runs on: acknowledged inserts missing after a store
outage past the lease). The inserts were not lost from storage: the documents were stored, but missing from collection
scans. An insert takes a natural-order number for each document (`natural.rs`, kept in memory per opening). When one of the
command's reads finds the lease invalid, the database is abandoned and opened again inside the same command; the reopen
forgets the numbers, so the rest of the batch takes the same numbers again, and each later document's `nat/` entry
overwrites an earlier one's. The fix refuses the write of a command whose database was reopened after the command had
begun to use it (`dbs::Reopened`, retryable, nothing applied).

test_insert_spanning_a_lease_loss: fault injection (`MONGO_TEST_FAULTS_FILE`, test-only) delays the renewals of the
process lease for a few seconds while large insert commands run, so the lease is lost and taken back during a command.
Fails when a document whose insert was acknowledged is missing from a collection scan (now, and after a restart), when two
documents share a natural-order number, or when no command spanned a lease loss (the test did not reach the case).
"""
import os
import tempfile
import threading
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket, free_port

TTL = 2
BATCH = 5000
ROUNDS = int(os.environ.get("T52_ROUNDS", "6"))


# integration-4 (coordinator's decision, 2026-11-12): with sealed runs on, one read command longer than the F7 serving
# window (about 1-1.7 s with this 2 s lease) is refused at every try: the serving deadline is set when a command enters
# and never renewed inside it (product defect, its own fix task). Unthrottled, the merged engine acknowledges 4-5 times
# more documents than c2-fix-outage-loss alone (635,000-755,000 against about 150,000), so the in-process scan is such a
# command: when refused it is recorded "not verified in-process (F7, long command)" and the restart read-back decides.
# The throttled variant holds the loader near the branch's count (THROTTLED_DOCS_PER_S over the 51 s of the rounds,
# about 150,000 documents) and its in-process read-back is mandatory.
THROTTLED_DOCS_PER_S = 3000


def _scan_ids(col, mandatory, secs=60):
    """A collection scan's _ids. Refused with code 134 for `secs`: None when not mandatory (F7, long command)."""
    deadline = time.time() + secs
    while True:
        try:
            return [d["_id"] for d in col.find({}, {"_id": 1}).hint([("$natural", 1)])]
        except PyMongoError as e:
            if getattr(e, "code", None) != 134 or time.time() > deadline:
                if mandatory or getattr(e, "code", None) != 134:
                    raise
                print(f"not verified in-process (F7, long command): {str(e)[:160]}")
                return None
            time.sleep(0.5)


def test_insert_spanning_a_lease_loss():
    _insert_spanning(throttled=False)


def test_insert_spanning_a_lease_loss_throttled():
    _insert_spanning(throttled=True)


def _insert_spanning(throttled):
    bucket = make_bucket("reopen")
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    ff = f.name
    srv = Server(bucket, "reopen", idle=600, holder="R1", extra=["--lease-ttl-secs", str(TTL)], env={"MONGO_TEST_FAULTS_FILE": ff}).start()
    acked, refused, losses = [], [], 0
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000, retryWrites=False)
        col = c.d.t
        col.insert_one({"_id": -1})
        acked.append(-1)
        stop, nxt, lock = threading.Event(), [0], threading.Lock()

        def loader():
            cl = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000, retryWrites=False)
            t0, sent = time.time(), 0
            while not stop.is_set():
                if throttled:
                    wait = t0 + sent / THROTTLED_DOCS_PER_S - time.time()
                    if wait > 0:
                        time.sleep(wait)
                    sent += BATCH
                with lock:
                    b = nxt[0]
                    nxt[0] += 1
                ids = list(range(b * BATCH, (b + 1) * BATCH))
                try:
                    cl.d.t.insert_many([{"_id": i, "f": "x" * 50} for i in ids], ordered=False)
                    with lock:
                        acked.extend(ids)
                except PyMongoError as e:
                    with lock:
                        refused.append(str(e)[:200])
            cl.close()
        th = threading.Thread(target=loader)
        th.start()
        try:
            for r in range(ROUNDS):
                time.sleep(1.5)
                with open(ff, "w") as fh:
                    fh.write(f"delay_put processes/R1/_lease {TTL * 1000 + 1500}\n")
                time.sleep(TTL + 1.0)
                with open(ff, "w") as fh:
                    fh.write("")
                time.sleep(TTL + 2)
        finally:
            stop.set()
            th.join(150)
        losses = srv.log_text().count("LEASE lost")
        print(f"batches {nxt[0]}, acknowledged {len(acked)} documents, refused {len(refused)} batches {refused[:2]}; lease losses {losses}")
        st = c.admin.command("tenantStats")["maintenance"]
        print(f"reopenedWritesRefused {st.get('reopenedWritesRefused')}")
        assert losses > 0, "no lease loss happened while an insert ran: the test did not reach the case"
        scanned = _scan_ids(col, mandatory=throttled)
        if scanned is not None:
            dup = len(scanned) - len(set(scanned))
            missing = sorted(set(acked) - set(scanned))
            print(f"acknowledged {len(acked)}, scanned in-process {len(scanned)}, missing from the scan {len(missing)}")
            assert dup == 0, f"{dup} documents scanned twice"
            assert not missing, f"{len(missing)} acknowledged documents missing from a collection scan, e.g. {missing[:5]}"
            extra = set(scanned) - set(acked)
            assert not extra, f"{len(extra)} documents of refused inserts were applied, e.g. {sorted(extra)[:5]}"
    finally:
        srv.term()
    srv2 = Server(bucket, "reopen2", idle=600).start()
    try:
        scanned = [d["_id"] for d in srv2.client().d.t.find({}, {"_id": 1}).hint([("$natural", 1)])]
        missing = sorted(set(acked) - set(scanned))
        extra = set(scanned) - set(acked)
        print(f"after a restart: acknowledged {len(acked)}, scanned {len(scanned)}, missing {len(missing)}, "
              f"twice {len(scanned) - len(set(scanned))}, refused but stored {len(extra)}")
        assert not missing, f"after a restart: {len(missing)} acknowledged documents missing from a collection scan, e.g. {missing[:5]}"
        assert len(scanned) == len(set(scanned)), "after a restart: documents scanned twice"
        assert not extra, f"after a restart: {len(extra)} documents of refused inserts were applied, e.g. {sorted(extra)[:5]}"
    finally:
        srv2.term()


def test_transaction_spanning_a_lease_loss():
    """A transaction inserts, the lease is lost and taken back (the database is abandoned and opened again), the same
    transaction inserts again and commits, or is refused and run again. A guard, not a reproduction: integration-3 already
    refuses the statement after the reopen (it reads through the old opening's snapshot: "db is closed"), so its natural-
    order numbers never come from two openings. Fails when a committed document is missing from a collection scan, when
    the transaction never commits, or when the lease was not lost between the two statements."""
    from pymongo.errors import PyMongoError as E
    bucket = make_bucket("reopentx")
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    ff = f.name
    port = free_port()
    srv = Server(bucket, "reopentx", idle=600, holder="T1", port=port, env={"MONGO_TEST_FAULTS_FILE": ff},
                 extra=["--lease-ttl-secs", str(TTL), "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        c = srv.client()
        col = c.d.t
        col.insert_many([{"_id": f"pre{i}"} for i in range(10)])
        committed = [f"pre{i}" for i in range(10)]
        outcomes = []
        for attempt in range(3):
            with c.start_session() as s:
                try:
                    s.start_transaction()
                    col.insert_one({"_id": "a"}, session=s)
                    if attempt == 0:
                        lost0 = srv.log_text().count("LEASE lost")
                        with open(ff, "w") as fh:
                            fh.write(f"delay_put processes/T1/_lease {TTL * 1000 + 1500}\n")
                        deadline = time.time() + 30
                        while srv.log_text().count("LEASE lost") == lost0 and time.time() < deadline:
                            time.sleep(0.1)
                        with open(ff, "w") as fh:
                            fh.write("")
                        assert srv.log_text().count("LEASE lost") > lost0, "the lease was not lost: the test did not reach the case"
                        time.sleep(TTL + 1)
                    col.insert_one({"_id": "b"}, session=s)
                    s.commit_transaction()
                    outcomes.append(f"attempt {attempt}: committed")
                    committed += ["a", "b"]
                    break
                except E as e:
                    outcomes.append(f"attempt {attempt}: {type(e).__name__} {getattr(e, 'code', None)} {str(e)[:120]}")
                    try:
                        s.abort_transaction()
                    except E:
                        pass
        print("\n".join(outcomes))
        scanned = [d["_id"] for d in col.find({}, {"_id": 1}).hint([("$natural", 1)])]
        missing = sorted(set(committed) - set(scanned))
        print(f"scanned {scanned}")
        assert "a" in committed, f"the transaction never committed: {outcomes}"
        assert not missing, f"committed documents missing from a collection scan: {missing} ({outcomes})"
        assert len(scanned) == len(set(scanned)) == len(committed), (scanned, committed)
    finally:
        srv.term()


import pytest  # noqa: E402


@pytest.mark.skipif(not os.environ.get("R52_OLD_BIN"), reason="needs R52_OLD_BIN, a binary that writes duplicate natural-order numbers (integration-3)")
def test_validate_repair_renumbers_a_damaged_collection():
    """Fix round, review defect 4. R52_OLD_BIN (integration-3) damages a collection the way the review found (a transaction
    commits after a reopen; a plain insert in between took its natural-order number again), with a secondary index. This
    binary's `validate` must report it, and `validate {repair: true}` must bring every document back into collection
    scans, keep the healthy documents in their insertion order, renumber the index entries of the repaired ones, and leave
    `validate {full: true}` clean, also after a restart."""
    old = os.environ["R52_OLD_BIN"]
    bucket = make_bucket("repairnat")
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    ff = f.name
    port = free_port()
    srv = Server(bucket, "repairnat", idle=600, holder="T1", port=port, env={"MONGO_TEST_FAULTS_FILE": ff}, binary=old,
                 extra=["--lease-ttl-secs", str(TTL), "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        c = srv.client()
        col = c.d.t
        col.create_index([("k", 1)])
        col.insert_many([{"_id": f"pre{i}", "k": 1} for i in range(10)])
        s = c.start_session()
        s.start_transaction()
        col.insert_one({"_id": "A", "k": 1}, session=s)
        lost0 = srv.log_text().count("LEASE lost")
        with open(ff, "w") as fh:
            fh.write(f"delay_put processes/T1/_lease {TTL * 1000 + 1500}\n")
        deadline = time.time() + 30
        while srv.log_text().count("LEASE lost") == lost0 and time.time() < deadline:
            time.sleep(0.1)
        with open(ff, "w") as fh:
            fh.write("")
        time.sleep(TTL + 1)
        deadline = time.time() + 40
        while True:
            try:
                col.insert_one({"_id": "B", "k": 1})
                break
            except PyMongoError:
                assert time.time() < deadline, "the insert of B never succeeded"
                time.sleep(0.2)
        s.commit_transaction()
        s.end_session()
        col.insert_many([{"_id": f"post{i}", "k": 1} for i in range(5)])
        before = [d["_id"] for d in col.find({}, {"_id": 1}).hint([("$natural", 1)])]
    finally:
        srv.term()
        os.unlink(ff)
    assert "B" not in before, f"the old binary did not damage the collection: the test did not reach the case ({before})"
    every = [f"pre{i}" for i in range(10)] + ["A", "B"] + [f"post{i}" for i in range(5)]
    new = Server(bucket, "repairnat2", idle=600).start()
    try:
        c = new.client()
        v = c.d.command("validate", "t", full=True)
        print(f"before the repair: valid={v['valid']} errors={v['errors']}")
        assert not v["valid"], "validate did not find the damage"
        r = c.d.command("validate", "t", repair=True)
        print(f"repair: valid={r['valid']} repaired={r['repaired']} naturalOrderRepaired={r.get('naturalOrderRepaired')} errors={r['errors']}")
        scan = [d["_id"] for d in c.d.t.find({}, {"_id": 1}).hint([("$natural", 1)])]
        by_k = [d["_id"] for d in c.d.t.find({"k": 1}, {"_id": 1}).hint([("k", 1)])]
        v2 = c.d.command("validate", "t", full=True)
        print(f"after the repair: scan {scan}\nindex k order {by_k}\nvalidate valid={v2['valid']} errors={v2['errors']}")
        assert sorted(scan, key=str) == sorted(every, key=str) and len(scan) == len(every), f"scan after the repair: {scan}"
        healthy = [i for i in before]
        assert [i for i in scan if i in healthy] == healthy, "the repair changed the order of healthy documents"
        assert sorted(by_k, key=str) == sorted(every, key=str), f"index k after the repair: {by_k}"
        assert v2["valid"], f"validate after the repair: {v2['errors']}"
    finally:
        new.term()
    again = Server(bucket, "repairnat3", idle=600).start()
    try:
        scan = [d["_id"] for d in again.client().d.t.find({}, {"_id": 1}).hint([("$natural", 1)])]
        assert len(scan) == len(every) and set(scan) == set(every), f"after a restart: {scan}"
    finally:
        again.term()


def _damage_with_old_binary(old, bucket, pre):
    """R52_OLD_BIN (integration-3) writes `pre` documents, then "A" in a transaction that commits after a reopen while a
    plain insert of "B" took A's natural-order number again: B is stored but missing from scans. Returns the scan."""
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    ff = f.name
    port = free_port()
    srv = Server(bucket, "damage", idle=600, holder="T1", port=port, env={"MONGO_TEST_FAULTS_FILE": ff}, binary=old,
                 extra=["--lease-ttl-secs", str(TTL), "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        c = srv.client()
        col = c.d.t
        col.insert_many([{"_id": f"pre{i}"} for i in range(pre)])
        s = c.start_session()
        s.start_transaction()
        col.insert_one({"_id": "A"}, session=s)
        lost0 = srv.log_text().count("LEASE lost")
        with open(ff, "w") as fh:
            fh.write(f"delay_put processes/T1/_lease {TTL * 1000 + 1500}\n")
        deadline = time.time() + 30
        while srv.log_text().count("LEASE lost") == lost0 and time.time() < deadline:
            time.sleep(0.1)
        with open(ff, "w") as fh:
            fh.write("")
        time.sleep(TTL + 1)
        deadline = time.time() + 40
        while True:
            try:
                col.insert_one({"_id": "B"})
                break
            except PyMongoError:
                assert time.time() < deadline, "the insert of B never succeeded"
                time.sleep(0.2)
        s.commit_transaction()
        s.end_session()
        return [d["_id"] for d in col.find({}, {"_id": 1}).hint([("$natural", 1)])]
    finally:
        srv.term()
        os.unlink(ff)


@pytest.mark.skipif(not os.environ.get("R52_OLD_BIN"), reason="needs R52_OLD_BIN, a binary that writes duplicate natural-order numbers (integration-3)")
def test_delete_many_in_a_damaged_collection_keeps_the_healthy_document():
    """Fix round 2: the delete guard reads the natural-order entries of a whole write set at once (range reads over dense
    clusters of numbers). One transaction deletes 200 documents and the hidden B (B's number is A's, and its entry names
    A), so B's entry is checked inside a range-read cluster at the commit. (A plain deleteMany walks natural order and
    never reaches a hidden document.) Fails when A vanishes from collection scans, or when a deleted document stays."""
    bucket = make_bucket("delmany")
    before = _damage_with_old_binary(os.environ["R52_OLD_BIN"], bucket, 200)
    assert "B" not in before and "A" in before, f"the old binary did not damage the collection: the test did not reach the case ({before[-3:]})"
    port = free_port()
    new = Server(bucket, "delmany2", idle=600, port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        c = new.client()
        col = c.d.t
        assert col.find_one({"_id": "B"}) is not None, "B is not stored: the test did not reach the case"
        with c.start_session() as s:
            s.start_transaction()
            n = col.delete_many({"_id": {"$in": [f"pre{i}" for i in range(200)]}}, session=s).deleted_count
            n += col.delete_one({"_id": "B"}, session=s).deleted_count
            s.commit_transaction()
        scan = [d["_id"] for d in col.find({}, {"_id": 1}).hint([("$natural", 1)])]
        print(f"deleted {n} in one transaction; scan after: {scan}; A by _id {col.find_one({'_id': 'A'}) is not None}; B by _id {col.find_one({'_id': 'B'}) is not None}")
        assert n == 201, n
        assert scan == ["A"], f"scan after the deletes: {scan}"
    finally:
        new.term()
