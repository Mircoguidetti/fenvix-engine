"""P-fix-slow-store (2026-11-13): the defects that a slow or failing store turns into outages.

Item 1, the clean close on a slow store. On SIGTERM every database's close waited on the store step by step (a memtable
flush with the compactor running, its absorption, the compactor's stop, the final flush and SlateDB's close, the marker,
the lease); only the process-wide `--shutdown-timeout-secs` (25 s) bounded it, and the process then exited with leases
and usage counters not written. MEASURED on integration-5-stack 359f09b: 8.7 s at 200 ms per store request, 25.0 s at
1 s per request. Now one deadline (`--close-budget-secs`, 8 s) is shared by every step of every close; what does not fit
is cut where cutting is safe (an unclean close: no marker, the WAL replayed by the next owner).

- test_sigterm_on_a_slow_store_ends_within_the_budget: every store request delayed 1 s, SIGTERM. The process must exit
  within the budget plus the bounded steps after it (14 s); on the base it waits the whole 25 s.
- test_sigterm_with_the_store_not_answering_ends_within_the_budget: every PUT held without an answer, SIGTERM: the same
  bound.
Both then restart on a healthy store and read back every acknowledged insert and update (rule 8), and require no
incomplete multipart upload under the databases once their next owner has opened them.

The proxy is tools/lease-stall/pathproxy.py (a forwarder that delays or holds requests): deterministic, no timing luck.
"""
import os
import signal
import sys
import threading
import time

import pytest
from pymongo import MongoClient

from lib import Server, make_bucket, MINIO, s3, list_prefix

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lease-stall"))
from pathproxy import PathProxy  # noqa: E402

DBS = ["slowa", "slowb", "slowc"]
BUDGET_S = 8           # --close-budget-secs (the default)
EXIT_BOUND_S = 14      # the budget, the fallback close's 2 s grace, then 1 s for each of the steps after the closes


def proxy():
    host, port = MINIO.split("//")[1].split(":")
    return PathProxy((host, int(port)))


def incomplete_uploads(bucket, prefix="tenants/"):
    out, c, kw = [], s3(), {"Bucket": bucket}
    while True:
        r = c.list_multipart_uploads(**kw)
        out += [(u["Key"], u["UploadId"]) for u in r.get("Uploads", []) if u["Key"].startswith(prefix)]
        if not r.get("IsTruncated"):
            return out
        kw["KeyMarker"], kw["UploadIdMarker"] = r["NextKeyMarker"], r["NextUploadIdMarker"]


def load(srv):
    """Inserts and $inc updates on three databases, every one acknowledged: returns what must be read back."""
    c = srv.client()
    acked = {}
    for d in DBS:
        ids = []
        for i in range(30):
            docs = [{"_id": i * 100 + k, "v": "x" * 400, "n": 0} for k in range(100)]
            c[d].t.insert_many(docs)
            ids += [x["_id"] for x in docs]
        for k in range(20):
            c[d].t.update_one({"_id": k}, {"$inc": {"n": 1}})
        acked[d] = ids
    return acked


def writers(srv, stop, acked):
    """Writes during the close, each acknowledged id recorded (the close must keep them too)."""
    def run(d):
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=2000, socketTimeoutMS=4000, retryWrites=False)
        i = 1_000_000
        while not stop.is_set():
            try:
                c[d].t.insert_one({"_id": i, "late": True})
                acked[d].append(i)
            except Exception:
                pass
            i += 1
    ths = [threading.Thread(target=run, args=(d,), daemon=True) for d in DBS]
    for t in ths:
        t.start()
    return ths


def read_back(bucket, acked, tag):
    srv = Server(bucket, tag, idle=600).start()
    try:
        c = srv.client()
        deadline = time.time() + 60   # the stopped process's leases (6 s) may still be running out
        for d in DBS:
            while True:
                try:
                    have = {x["_id"] for x in c[d].t.find({}, {"_id": 1})}
                    break
                except Exception:
                    if time.time() > deadline:
                        raise
                    time.sleep(1)
            missing = [i for i in acked[d] if i not in have]
            assert not missing, f"{d}: {len(missing)} acknowledged inserts missing, e.g. {missing[:5]}"
            n = [x["n"] for x in c[d].t.find({"_id": {"$lt": 20}}, {"n": 1})]
            assert len(n) == 20 and all(v == 1 for v in n), f"{d}: acknowledged updates lost: {n}"
            print(f"{d}: {len(acked[d])} acknowledged inserts read back, 20 updates")
        # every database is opened by its next writer (one write each): then no upload of the stopped process may be left
        # (item 2 aborts, at a writer's open, those of an older writer epoch)
        for d in DBS:
            c[d].t.insert_one({"_id": "next-owner"})
        t0 = time.time()
        while incomplete_uploads(bucket) and time.time() - t0 < 20:
            time.sleep(1)
        left = incomplete_uploads(bucket)
        assert not left, f"incomplete uploads left: {left[:3]}"
    finally:
        srv.kill9()


def sigterm_case(slow_rule, tag):
    px = proxy()
    bucket = make_bucket(tag)
    srv = Server(bucket, tag, idle=600, endpoint=px.endpoint).start()
    try:
        acked = load(srv)
        time.sleep(2)
        stop = threading.Event()
        ths = writers(srv, stop, acked)
        time.sleep(1)
        slow_rule(px)
        t0 = time.time()
        srv.proc.send_signal(signal.SIGTERM)
        try:
            srv.proc.wait(60)
            took = time.time() - t0
        except Exception:
            took = None
        stop.set()
        for t in ths:
            t.join(10)
        print(f"SIGTERM to exit: {took if took is None else round(took, 1)} s (bound {EXIT_BOUND_S} s)")
        print("\n".join(l for l in srv.log_text().splitlines() if "TIMING shutdown" in l or "left behind" in l))
    finally:
        px.release()
        srv.kill9()
        px.close()
    assert took is not None and took <= EXIT_BOUND_S, f"the clean close took {took} s on a slow store (bound {EXIT_BOUND_S} s)"
    read_back(bucket, acked, tag + "2")


def test_sigterm_on_a_slow_store_ends_within_the_budget():
    sigterm_case(lambda px: px.slow(("GET", "PUT", "POST", "DELETE", "HEAD"), "/", 1.0), "slowclose")


def test_sigterm_with_the_store_not_answering_ends_within_the_budget():
    sigterm_case(lambda px: px.hold("PUT", "/"), "deadclose")


# ---- item 2: uploads of older writers, aborted whatever their age, never a live writer's

CRASH_DB = "crashloop"


def run_uploads(bucket, db=CRASH_DB):
    """Incomplete run uploads of `db` with the writer epoch their key names: [(key, epoch)]."""
    out = []
    for k, _ in incomplete_uploads(bucket, f"tenants/{db}/fx/run/"):
        e = k.rsplit("/", 1)[-1].split("-")
        out.append((k, int(e[1]) if len(e) == 3 else 0))
    return out


def write_until(srv, cond, start, timeout=60):
    """Insert batches into CRASH_DB until cond() (retrying while the previous owner's lease runs out); returns the
    acknowledged ids."""
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000)
    acked, i, t0 = [], start, time.time()
    while time.time() - t0 < timeout:
        docs = [{"_id": i + k, "v": "y" * 300} for k in range(50)]
        try:
            c[CRASH_DB].t.insert_many(docs)
            acked += [d["_id"] for d in docs]
            i += 50
        except Exception:
            time.sleep(0.5)
            continue
        if cond():
            return acked
    raise AssertionError("condition not reached")


def test_crash_loop_leaves_no_older_writers_upload():
    px = proxy()
    bucket = make_bucket("crashloop")
    acked, epochs = [], []
    try:
        # three processes in a row, each killed (-9) while its run uploads are held open by the proxy
        for n in range(3):
            srv = Server(bucket, f"crash{n}", idle=600, endpoint=px.endpoint).start()
            before = {k for k, _ in run_uploads(bucket)}
            px.hold("PUT", "/fx/run/")
            acked += write_until(srv, lambda: any(k not in before for k, _ in run_uploads(bucket)), 10_000 * (n + 1))
            mine = [(k, e) for k, e in run_uploads(bucket) if k not in before]
            epochs.append(max(e for _, e in mine))
            srv.kill9()
            px.release()
            print(f"process {n}: killed with {len(mine)} run uploads open (writer epoch {epochs[-1]}); "
                  f"{len(run_uploads(bucket))} open in all")
        # the next owner opens the database: every upload of an older writer must go, whatever its age (minutes here,
        # the startup sweep's age rule is an hour)
        live = Server(bucket, "crashlive", idle=600, endpoint=px.endpoint).start()
        acked += write_until(live, lambda: True, 90_000)
        t0 = time.time()
        while time.time() - t0 < 15:
            left = run_uploads(bucket)
            if not left:
                break
            time.sleep(0.5)
        left = run_uploads(bucket)
        print(f"after the next owner's open: {len(left)} run uploads open: {left[:3]}")
        assert not left, f"uploads of older writers left after the next owner opened: {left[:3]}"

        # never a live writer's upload: hold the live writer's run uploads, then start another process on the bucket
        # (its startup sweep lists everything); the live upload must stay and complete
        px.hold("PUT", "/fx/run/")
        before = set()
        acked += write_until(live, lambda: bool(run_uploads(bucket)), 100_000)
        held = run_uploads(bucket)
        other = Server(bucket, "crashother", idle=600).start()
        t0 = time.time()
        while "unfinished multipart uploads" not in other.log_text() and time.time() - t0 < 20:
            time.sleep(0.2)
        assert "unfinished multipart uploads" in other.log_text(), "the other process's startup sweep did not run"
        still = run_uploads(bucket)
        print(f"live writer's uploads {held} -> after another process's startup sweep {still}")
        assert set(held) <= set(still), f"a live writer's upload was aborted: {set(held) - set(still)}"
        other.kill9()
        px.release()
        # the live writer goes on: its uploads complete, every acknowledged write reads back
        c = live.client()
        have = {x["_id"] for x in c[CRASH_DB].t.find({}, {"_id": 1})}
        missing = [i for i in acked if i not in have]
        assert not missing, f"{len(missing)} acknowledged inserts missing, e.g. {missing[:5]}"
        t0 = time.time()
        while run_uploads(bucket) and time.time() - t0 < 30:
            time.sleep(0.5)
        assert not run_uploads(bucket), f"the live writer's uploads did not complete: {run_uploads(bucket)}"
        live.kill9()
    finally:
        px.release()
        px.close()


# ---- item 3: the server-side write deadline

from pymongo.errors import OperationFailure, PyMongoError  # noqa: E402

DEADLINE_MS = 2000


def deadline_server(tag, px):
    # a replica set member, as in production: drivers send a write's session and transaction number (what makes it a
    # retryable write) only to one
    # (a binary without the flag, the base of this fix, runs as it is: the control)
    import subprocess
    from lib import free_port, BIN
    port = free_port()
    has = "--write-deadline-ms" in subprocess.run([BIN, "--help"], capture_output=True, text=True).stdout
    return Server(make_bucket(tag), tag, idle=600, endpoint=px.endpoint, port=port,
                  extra=(["--write-deadline-ms", str(DEADLINE_MS)] if has else []) + ["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()


def test_a_write_the_store_does_not_take_gets_a_retryable_error_in_time():
    px = proxy()
    srv = deadline_server("wdeadline", px)
    try:
        # the driver's defaults (retryable writes on): the first attempt gets the labelled error, the driver retries
        # once, the retry waits for the original (still held) and gets the same error: about two deadlines in all
        c = MongoClient(srv.uri, socketTimeoutMS=30000)
        c.wdl.t.insert_one({"_id": 0, "n": 0})
        px.hold("PUT", "/tenants/wdl/wal/")
        t0 = time.time()
        err = None
        try:
            c.wdl.t.insert_one({"_id": 1})
        except PyMongoError as e:
            err = e
        took = time.time() - t0
        print(f"insert with the WAL PUT held: {type(err).__name__} after {took:.1f} s: {getattr(err, 'details', err)}")
        assert isinstance(err, OperationFailure), f"no server error within the deadline: {err!r} after {took:.1f} s"
        assert err.code == 262 and err.has_error_label("RetryableWriteError"), f"wrong error: {err.details}"
        assert took < 2 * DEADLINE_MS / 1000 + 2, f"answered after {took:.1f} s (deadline {DEADLINE_MS} ms, one retry)"
        log = [l for l in srv.log_text().splitlines() if l.startswith("WRITE_DEADLINE ")]
        assert len(log) == 2 and all("retryable=true" in l for l in log), log
        # an unacknowledged write is not a lie either way: once the store answers, the database is consistent and every
        # acknowledged write is there
        px.release()
        c2 = MongoClient(srv.uri)
        c2.wdl.t.insert_one({"_id": 2})
        have = {x["_id"] for x in c2.wdl.t.find({})}
        assert {0, 2} <= have, have
        print(f"after the store answers again: ids {sorted(have)} (1 may or may not be there: it was never acknowledged)")
    finally:
        px.release()
        srv.kill9()
        px.close()


def test_a_retried_write_is_applied_once():
    """The driver's retry of a write answered by the deadline must not apply it twice: the original goes on, and the
    retry answers from the session's record once the original is durable."""
    px = proxy()
    srv = deadline_server("wdeadonce", px)
    try:
        c = MongoClient(srv.uri, retryWrites=True, socketTimeoutMS=60000)
        c.wdo.t.insert_one({"_id": 0, "n": 0})
        px.hold("PUT", "/tenants/wdo/wal/")
        res = {}

        def inc():
            t = time.time()
            try:
                r = c.wdo.t.update_one({"_id": 0}, {"$inc": {"n": 1}})
                res["ok"] = (r.modified_count, round(time.time() - t, 1))
            except PyMongoError as e:
                res["err"] = (repr(e)[:200], round(time.time() - t, 1))

        th = threading.Thread(target=inc)
        th.start()
        time.sleep(DEADLINE_MS / 1000 * 1.5)   # the first attempt got the deadline's error; the driver's retry waits
        px.release()
        th.join(60)
        time.sleep(1)   # an original that was answered by the deadline ends in its own task
        n = c.wdo.t.find_one({"_id": 0})["n"]
        print(f"$inc with the WAL held {DEADLINE_MS * 1.5:.0f} ms: {res}; n = {n}; log: "
              f"{[l for l in srv.log_text().splitlines() if 'WRITE_DEADLINE' in l]}")
        assert n in (0, 1), f"the write was applied {n} times"
        if "ok" in res:
            assert n == 1, f"acknowledged but n = {n}"
    finally:
        px.release()
        srv.kill9()
        px.close()


# ---- item 5: a write answered with a definite error must not be applied

def _faults_file():
    import tempfile
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    return f.name


def _inc_across_a_lease_loss(retry_writes, tag):
    """One $inc whose staged batch is held before its flush (test knob FENVIX_TEST_COMMIT_WAIT_DELAY_MS, databases named
    inc5*); meanwhile the process lease lapses and the database is abandoned: the abandon flushes the WAL (the batch is
    then durable) and closes SlateDB. The held write then finds SlateDB closed. Returns (the client's outcome, whether its
    error carried RetryableWriteError, the counter's value)."""
    from lib import free_port
    ff = _faults_file()
    port, holder = free_port(), f"H{tag}"
    srv = Server(make_bucket(tag), tag, idle=600, holder=holder, port=port,
                 env={"MONGO_TEST_FAULTS_FILE": ff, "FENVIX_TEST_COMMIT_WAIT_DELAY_MS": "12000",
                      **({"FENVIX_TEST_CONTROL_OUTCOME_DEFINITE": "1"} if os.environ.get("CONTROL_ITEM5") == "1" else {})},
                 extra=["--lease-ttl-secs", "2", "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        c = MongoClient(srv.uri, retryWrites=retry_writes, socketTimeoutMS=120000)
        c.inc5.cnt.insert_one({"_id": "k", "v": 0})
        res = {}

        def inc():
            try:
                c.inc5.cnt.update_one({"_id": "k"}, {"$inc": {"v": 1}})
                res["ok"] = True
            except PyMongoError as e:
                res["err"] = e

        n0 = srv.log_text().count("TEST commit wait held")
        th = threading.Thread(target=inc)
        th.start()
        t0 = time.time()
        while srv.log_text().count("TEST commit wait held") == n0 and time.time() - t0 < 20:
            time.sleep(0.02)
        lost0 = srv.log_text().count("LEASE lost")
        open(ff, "w").write(f"delay_put processes/{holder}/_lease 3500\n")
        t0 = time.time()
        while srv.log_text().count("LEASE lost") == lost0 and time.time() - t0 < 30:
            time.sleep(0.05)
        assert srv.log_text().count("LEASE lost") > lost0, "setup: the lease was not lost"
        open(ff, "w").write("")
        th.join(120)
        v = None
        t0 = time.time()
        while time.time() - t0 < 60:
            try:
                v = MongoClient(srv.uri).inc5.cnt.find_one({"_id": "k"})["v"]
                break
            except PyMongoError:
                time.sleep(0.5)
        err = res.get("err")
        label = bool(err is not None and getattr(err, "has_error_label", lambda _l: False)("RetryableWriteError"))
        outcome = "ok" if res.get("ok") else f"{type(err).__name__} code {getattr(err, 'code', None)} labelled {label}: {str(err)[:140]}"
        print(f"retryWrites={retry_writes}: client {outcome}; stored v = {v}")
        return res, label, v
    finally:
        srv.kill9()
        os.unlink(ff)


def test_a_write_answered_with_a_definite_error_was_not_applied():
    res, label, v = _inc_across_a_lease_loss(False, "inc5def")
    assert v in (0, 1), v
    if "err" in res and not label:
        assert v == 0, "the client saw a definite error (no RetryableWriteError label) and the increment was applied"
    if res.get("ok"):
        assert v == 1


def test_a_retried_write_across_a_lease_loss_is_applied_once():
    res, label, v = _inc_across_a_lease_loss(True, "inc5once")
    assert v in (0, 1), f"the increment was applied {v} times"
    if res.get("ok"):
        assert v == 1, "acknowledged but not applied"
    if "err" in res and not label:
        assert v == 0, "a definite error, and the increment was applied"


# ---- item 6: an erase is never left half done by an upload listing

def _populate(c, db):
    for i in range(5):
        c[db].t.insert_many([{"_id": i * 100 + k, "pad": "p" * 300} for k in range(100)])


def test_an_upload_the_listing_shows_but_the_store_does_not_have_does_not_block_an_erase():
    """A listing that shows an upload the store has no record of (ListParts: NoSuchUpload) is a ghost: the erase must
    end complete. The proxy adds the ghost to every ListMultipartUploads answer; the store never had it."""
    px = proxy()
    bucket = make_bucket("ghosterase")
    srv = Server(bucket, "ghosterase", idle=600, endpoint=px.endpoint).start()
    try:
        _populate(srv.client(), "ghostdb")
        px.ghosts = [("tenants/ghostdb/fx/run/g0/00000000000000000011-1-eca5621d", "R2hvc3RVcGxvYWRJZA")]
        st, body = srv.admin.delete("/v1/databases/ghostdb")
        print(f"erase with a ghost upload listed: {st} {str(body)[:200]}")
        assert st == 200, f"the erase did not complete: {st} {body}"
        assert srv.admin.get("/v1/databases/ghostdb")[0] == 404
    finally:
        px.ghosts = []
        srv.kill9()
        px.close()


def test_an_erase_left_incomplete_by_an_upload_is_finished_by_retries():
    """A real upload whose aborts the store refuses: the erase fails and its intent stays (the data is gone). After a
    restart, the recovery fails too while the refusal lasts; once aborts go through, the erase must end complete without
    another restart or request (before, it stayed half done until the next restart)."""
    px = proxy()
    bucket = make_bucket("stuckerase")
    srv = Server(bucket, "stuckerase", idle=600, endpoint=px.endpoint).start()
    try:
        _populate(srv.client(), "stuckdb")
        key = "tenants/stuckdb/fx/run/g0/00000000000000000011-1-0badc0de"
        up = s3().create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        s3().upload_part(Bucket=bucket, Key=key, UploadId=up, PartNumber=1, Body=b"x" * 1024)
        px.refuse(("DELETE",), "/fx/run/g0/00000000000000000011-1-0badc0de", 500)
        st, body = srv.admin.delete("/v1/databases/stuckdb")
        print(f"erase while the upload's abort is refused: {st} {str(body)[:160]}")
        assert st != 200
        srv.kill9()
        srv.proc = None
        srv.start()
        time.sleep(3)
        assert list_prefix(bucket, "control/ops/stuckdb.json"), "setup: the erase's intent is not recorded"
        px.release()
        t0 = time.time()
        while list_prefix(bucket, "control/ops/stuckdb.json") and time.time() - t0 < 60:
            time.sleep(1)
        took = time.time() - t0
        left = incomplete_uploads(bucket, "tenants/stuckdb/")
        print(f"after the refusal ended: intent {'gone' if not list_prefix(bucket, 'control/ops/stuckdb.json') else 'still there'} after {took:.0f} s; uploads left {left}")
        assert not list_prefix(bucket, "control/ops/stuckdb.json"), "the interrupted erase was never finished"
        assert not left
        assert srv.admin.get("/v1/databases/stuckdb")[0] == 404
    finally:
        px.release()
        srv.kill9()
        px.close()


# ---- item 4: the cold start of a new database

def test_the_first_insert_into_a_new_database_probes_the_store_once():
    """The first insert of a new database read its catalog under several locks, and each lock probed the store again: a
    lease GET and PUT, the clean marker, LISTs, then a lease discard because the database did not exist yet; then the
    write created it. MEASURED at 100 ms per store request: 3.9 s and 3 lease PUTs with 2 discards on the base. Now the
    "absent" answer and its lease are kept for a short hold, so the database is probed once and created with the lease
    already held. Every request is delayed 100 ms by the proxy, so the counts and the time are deterministic."""
    from lib import free_port
    px = proxy()
    port = free_port()
    srv = Server(make_bucket("coldstart"), "coldstart", idle=600, endpoint=px.endpoint, port=port,
                 extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        c = MongoClient(srv.uri)
        c.warm.t.insert_one({"_id": 0})
        time.sleep(1)
        px.slow(("GET", "PUT", "POST", "DELETE", "HEAD"), "/", 0.1)
        px.journal = []
        t0 = time.time()
        c.newdb.t.insert_one({"_id": 1})
        took = time.time() - t0
        j = list(px.journal)
        px.journal = None
        lease_puts = sum(1 for _, m, p, _s in j if m == "PUT" and p.endswith("/tenants/newdb/_lease"))
        discards = sum(1 for _, m, p, _s in j if m in ("DELETE", "DELETE*") and p.endswith("/tenants/newdb/_lease"))
        print(f"first insert into a new database at 100 ms per request: {took:.2f} s, {len(j)} store requests, "
              f"{lease_puts} lease PUTs, {discards} lease discards")
        assert lease_puts == 1 and discards == 0, f"the new database was probed {lease_puts} times ({discards} discards)"
        assert took < 3.2, f"the first insert took {took:.2f} s"
        assert c.newdb.t.find_one({"_id": 1}) == {"_id": 1}
        # a name only read, never written: its lease is given up once the hold is over (the reaper), and it stays absent
        px.release()
        assert c.nosuchdb.t.find_one({}) is None
        bucket = srv.bucket
        t0 = time.time()
        while list_prefix(bucket, "tenants/nosuchdb/") and time.time() - t0 < 15:
            time.sleep(0.5)
        print(f"lease of a name only read: {'given up' if not list_prefix(bucket, 'tenants/nosuchdb/') else 'still there'} after {time.time() - t0:.1f} s")
        assert not list_prefix(bucket, "tenants/nosuchdb/"), "the absent hold kept its lease"
        assert "nosuchdb" not in c.list_database_names()
    finally:
        px.release()
        srv.kill9()
        px.close()
