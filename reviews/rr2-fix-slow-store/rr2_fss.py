"""R-rolling (second reviewer), fix-slow-store: two probes on a --store local server (no MinIO in this sandbox).

usage: python3 rr2_fss.py <mongo-server binary> <case> [extra server args...]
  case deadline_unlabelled : item 3. A non-retryable insert (retryWrites=false) answered with an error, then applied.
  case fork_in_hold        : item 4. A read of a missing name, then a fork onto that name in the same process, then reads.
Prints a verdict line "RESULT <case> HOLE|OK ..." and exits 1 on HOLE.
"""
import json, os, shutil, socket, subprocess, sys, tempfile, time, urllib.request

import pymongo
from pymongo.errors import OperationFailure

BIN, CASE, EXTRA = sys.argv[1], sys.argv[2], sys.argv[3:]
TOKEN = "rr2-token-0123456789abcdef"


def port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def start(env_extra, args):
    d = tempfile.mkdtemp(prefix="rr2-")
    p, ap = port(), port()
    log = open(os.path.join(d, "server.log"), "wb")
    env = dict(os.environ, MONGO_ADMIN_TOKEN=TOKEN, **env_extra)
    cmd = [BIN, "--listen", f"127.0.0.1:{p}", "--admin-listen", f"127.0.0.1:{ap}", "--store", "local", "--local-dir", os.path.join(d, "store"),
           "--holder", "rr2", "--no-ttl"] + args + EXTRA
    os.makedirs(os.path.join(d, "store"))
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
    for _ in range(300):
        try:
            socket.create_connection(("127.0.0.1", p), 0.2).close(); break
        except OSError:
            if proc.poll() is not None:
                raise SystemExit(open(os.path.join(d, "server.log")).read()[-3000:])
            time.sleep(0.1)
    return proc, p, ap, d


def admin(ap, method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{ap}{path}", data=json.dumps(body).encode() if body is not None else None, method=method,
                                 headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def deadline_unlabelled():
    # the item-5 test knob holds the staged batch of an inc5* database 3 s before its WAL flush: a slow store, deterministic
    proc, p, ap, d = start({"FENVIX_TEST_COMMIT_WAIT_DELAY_MS": "3000"}, [] if "--write-deadline-ms" in EXTRA else ["--write-deadline-ms", "1000"])
    try:
        c = pymongo.MongoClient(f"mongodb://127.0.0.1:{p}/?directConnection=true", retryWrites=False, serverSelectionTimeoutMS=20000, socketTimeoutMS=60000)
        try:
            c.inc5rr.t.insert_one({"_id": 0, "warm": 1})  # the database exists (its creation is not what is measured)
        except OperationFailure as e:
            print(f"warm-up insert: code {e.code} (held too)")
        time.sleep(5)
        print(f"warm-up doc after 5 s: {c.inc5rr.t.find_one({'_id': 0})}")
        t0 = time.time()
        err = None
        try:
            c.inc5rr.t.insert_one({"_id": 1, "v": "answered-as-failed?"})
        except OperationFailure as e:
            err = e
        ms = int((time.time() - t0) * 1000)
        if err is None:
            print(f"RESULT deadline_unlabelled OK: insert acknowledged after {ms} ms (no error, no lie)")
            return 0
        labels = [l for l in ("RetryableWriteError", "UnknownTransactionCommitResult") if err.has_error_label(l)]
        print(f"insert answered after {ms} ms: code {err.code} labels {labels} msg {str(err)[:200]!r}")
        time.sleep(5)
        doc = c.inc5rr.t.find_one({"_id": 1})
        print(f"read 5 s later: {doc}")
        if doc is not None and not labels:
            print("RESULT deadline_unlabelled HOLE: an error without the RetryableWriteError label (definite, test_52r's rule) and the write is applied")
            return 1
        print("RESULT deadline_unlabelled OK: " + ("labelled (unknown outcome)" if labels else "not applied"))
        return 0
    finally:
        proc.terminate(); proc.wait(30)


def fork_in_hold():
    proc, p, ap, d = start({}, [])
    try:
        c = pymongo.MongoClient(f"mongodb://127.0.0.1:{p}/?directConnection=true", serverSelectionTimeoutMS=20000)
        c.src.users.insert_many([{"_id": i, "n": i} for i in range(50)])
        assert c.src.users.count_documents({}) == 50
        # 1. a read of a name that does not exist (any driver probing, a dashboard): starts item 4's absent hold
        assert c.br.users.find_one({}) is None
        # 2. a fork onto that name, in the same process
        st, r = admin(ap, "POST", "/v1/databases/src/fork", {"target": "br", "mode": os.environ.get("RR2_MODE", "clone")})
        print(f"fork: {st} {str(r)[:160]}")
        assert st == 201, r
        # 3. reads of the branch right after the fork answered 201
        n_now = c.br.users.count_documents({})
        names = c.list_database_names()
        print(f"right after the fork: br.users count {n_now}; listDatabases has br: {'br' in names}")
        # 4. a write in the same window, then everything that must be there
        c.br.users.insert_one({"_id": 1000, "n": "after-fork"})
        n_after = c.br.users.count_documents({})
        print(f"after one insert: br.users count {n_after} (expected 51); find _id 1000: {c.br.users.find_one({'_id': 1000})}; collections {c.br.list_collection_names()}")
        time.sleep(6)
        n_late = c.br.users.count_documents({})
        print(f"6 s later: br.users count {n_late} (expected 51)")
        proc.terminate(); proc.wait(30)
        proc2 = subprocess.Popen(proc.args, stdout=open(os.path.join(d, "server2.log"), "wb"), stderr=subprocess.STDOUT, env=dict(os.environ, MONGO_ADMIN_TOKEN=TOKEN))
        try:
            for _ in range(300):
                try:
                    socket.create_connection(("127.0.0.1", p), 0.2).close(); break
                except OSError:
                    time.sleep(0.1)
            c2 = pymongo.MongoClient(f"mongodb://127.0.0.1:{p}/?directConnection=true", serverSelectionTimeoutMS=20000)
            n_restart = c2.br.users.count_documents({})
            ids = sorted(d["_id"] for d in c2.br.users.find({}, {"_id": 1}))
            byid = [i for i in list(range(50)) + [1000] if c2.br.users.find_one({"_id": i}) is None]
            print(f"after a restart: br.users count {n_restart} (expected 51); full scan {len(ids)} ids, missing from the scan {sorted(set(list(range(50)) + [1000]) - set(ids))}; missing by _id lookup {byid}; estimated {c2.br.users.estimated_document_count()}; find_one _id 0 returns {c2.br.users.find_one({'_id': 0})}")
        finally:
            proc2.terminate(); proc2.wait(30)
        bad = n_now != 50 or n_after != 51 or n_late != 51 or n_restart != 51
        print(f"RESULT fork_in_hold {'HOLE' if bad else 'OK'}: now={n_now} after_insert={n_after} late={n_late} restart={n_restart} (log {d})")
        return 1 if bad else 0
    finally:
        if proc.poll() is None:
            proc.terminate(); proc.wait(30)


sys.exit({"deadline_unlabelled": deadline_unlabelled, "fork_in_hold": fork_in_hold}[CASE]())
