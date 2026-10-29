#!/usr/bin/env python3
"""aws-test3: dormant databases of the final engine (commit 41ed169) on real S3 (eu-central-1), run on a GitHub Actions
runner with the OIDC role. No EC2, no other AWS service than S3 (plus STS to renew the role session).

Part A (checks on real S3, all must pass before part B):
  A1 credentials of the Actions environment, --region eu-central-1, no --endpoint
  A2 conditional writes / leases / fencing with two engine processes (different holders) on the same database
  A3 a full cycle: create, 200 documents, rest, reopen, read everything back
Part B: N = 5, 10, 50, 100, 1000, 2000 databases (each step adds databases to the previous ones), production flags
  (--idle-secs 60 --retention-window-secs 604800 --wal-gc-mode bulk --auth --tls-mode requireTLS), per step:
  bytes and objects in the bucket, S3 requests per type and phase, a 30-minute rest with every database dormant,
  cold wakes of a sample (>= 50, all if N <= 50) and a second request, engine start-up requests (does it depend on N?).

Spend limit: a monitor thread prices every request counted (engine counters + the runner's own aws calls) and the
storage, every 10 s; above the cap it stops the engine and the run. Requests are counted by the engine's CountingStore,
which sits above object_store's retry layer: retried requests are NOT counted (stated in the report).

Credentials: the role session lasts 1 h. The engine reads its credentials once, at start, so it is restarted (clean
shutdown, databases closed) with renewed credentials before every phase that could outlive them. Renewal = new GitHub
OIDC token + sts:AssumeRoleWithWebIdentity, two attempts, then STOP (never retried forever).

Nothing secret is printed: no account id, no ARN, no role name, no key, no token (they are masked with ::add-mask::
as soon as they are known). Results go to results-aws-test3/ (no account id).

    T3_LOCAL=1 driver.py   -> dry check against --store local (no S3, no leases, tiny sizes), never used on the runner
"""
import csv
import hashlib
import json
import math
import os
import random
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

LOCAL = os.environ.get("T3_LOCAL") == "1"
R = "eu-central-1"
RUN = os.environ.get("GITHUB_RUN_ID", "local")
B = os.environ.get("T3_BUCKET", f"fenvix-test-t3-{RUN}")
OUT = os.environ.get("T3_OUT", "results-aws-test3")
BIN = os.environ["ENGINE_BIN"]
CERT = os.environ.get("T3_CERT_DIR", "t3-cert")
CAP = float(os.environ.get("T3_CAP_USD", "5.0"))
JOB_DEADLINE = float(os.environ.get("T3_DEADLINE_EPOCH", str(time.time() + 5 * 3600)))
STEPS = [2, 3] if LOCAL else [5, 10, 50, 100, 1000, 2000]
REST_SECS = 20 if LOCAL else 30 * 60
IDLE = 3 if LOCAL else 60
WAKE_SAMPLE = 50
PARALLEL = 16
PROD = ["--idle-secs", str(IDLE), "--retention-window-secs", "604800", "--wal-gc-mode", "bulk"]
AUTH = os.environ.get("T3_AUTH", "1") == "1"  # with --auth no user can run tenantStats (it needs anyAction): see the report
os.makedirs(OUT, exist_ok=True)
PRICES = json.load(open(os.path.join(OUT, "prices.json")))
P_PUT, P_GET, P_GB = PRICES["put_tier_usd_per_request"], PRICES["get_tier_usd_per_request"], PRICES["storage_usd_per_gb_month"]
T0 = time.time()
ROOT_PW = os.urandom(12).hex()
print(f"::add-mask::{ROOT_PW}", flush=True)


class Stop(Exception):
    pass


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def save(name, obj):
    with open(os.path.join(OUT, name), "w") as f:
        json.dump(obj, f, indent=1, default=str)


# ---------------------------------------------------------------- credentials (role session = 1 h)
class Creds:
    def __init__(self):
        self.at = time.time()  # the workflow logged in right before this step

    def age(self):
        return time.time() - self.at

    def renew(self, force=False, max_age=15 * 60):
        if LOCAL or (not force and self.age() < max_age):
            return
        for attempt in (1, 2):
            try:
                req = urllib.request.Request(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com",
                                             headers={"Authorization": "bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
                tok = json.load(urllib.request.urlopen(req, timeout=30))["value"]
                print(f"::add-mask::{tok}", flush=True)
                env = {k: v for k, v in os.environ.items() if k not in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")}
                r = subprocess.run(["aws", "sts", "assume-role-with-web-identity", "--role-arn", os.environ["T3_ROLE"],
                                    "--role-session-name", f"t3-{RUN}-{int(time.time())}", "--web-identity-token", tok,
                                    "--duration-seconds", "3600", "--output", "json"], capture_output=True, text=True, env=env, timeout=60)
                if r.returncode:
                    raise RuntimeError("assume-role failed (output not printed: it can contain the account id)")
                c = json.loads(r.stdout)["Credentials"]
                for v in (c["SecretAccessKey"], c["SessionToken"], c["AccessKeyId"]):
                    print(f"::add-mask::{v}", flush=True)
                os.environ.update(AWS_ACCESS_KEY_ID=c["AccessKeyId"], AWS_SECRET_ACCESS_KEY=c["SecretAccessKey"], AWS_SESSION_TOKEN=c["SessionToken"])
                self.at = time.time()
                log("credentials renewed")
                return
            except Exception as e:
                log(f"credential renewal attempt {attempt} failed: {type(e).__name__}")
                time.sleep(5)
        raise Stop("credential renewal failed twice")


CREDS = Creds()


# ---------------------------------------------------------------- the runner's own S3 calls (counted and priced too)
class Runner:
    put = get = lst = delete = 0
    lock = threading.Lock()


def aws(args, kind, n=1, check=True):
    """One aws cli call; kind = put|get|list|delete (how S3 bills it), n = requests it makes."""
    with Runner.lock:
        setattr(Runner, {"put": "put", "get": "get", "list": "lst", "delete": "delete"}[kind], getattr(Runner, {"put": "put", "get": "get", "list": "lst", "delete": "delete"}[kind]) + n)
    r = subprocess.run(["aws"] + args, capture_output=True, text=True, timeout=600)
    if check and r.returncode:
        err = r.stderr.strip().splitlines()[-1] if r.stderr.strip() else ""
        raise Stop(f"aws {' '.join(args[:2])} failed: {err[:200]}")
    return r


def listing():
    """Every object of the bucket: [(key, size)]. In local mode, the files of the local store."""
    if LOCAL:
        base = os.environ["T3_LOCAL_DIR"]
        out = []
        for d, _, fs in os.walk(base):
            for f in fs:
                p = os.path.join(d, f)
                out.append((os.path.relpath(p, base), os.path.getsize(p)))
        return out
    r = aws(["s3api", "list-objects-v2", "--bucket", B, "--output", "json", "--query", "Contents[].[Key,Size]"], "list", n=1)
    rows = json.loads(r.stdout or "null") or []
    extra_pages = max(0, math.ceil(len(rows) / 1000) - 1)  # the cli pages by itself: one LIST per 1,000 keys
    with Runner.lock:
        Runner.lst += extra_pages
    return [(k, s) for k, s in rows]


# ---------------------------------------------------------------- engine processes and their request counters
FIELDS = ("put", "copy", "list", "get", "head", "delete", "billedPutTier", "billedGetTier", "putBytes", "getBytes")


class Totals:
    """Requests of every engine process this run started (a restart resets an engine's own counters)."""
    done = {k: 0 for k in FIELDS}
    live = []
    lock = threading.Lock()

    @classmethod
    def now(cls):
        t = dict(cls.done)
        for e in list(cls.live):
            s = e.last
            for k in FIELDS:
                t[k] += s.get(k, 0)
        return t


def diff(a, b):
    return {k: b[k] - a[k] for k in a}


class Engine:
    def __init__(self, name, port, holder, prod=True, extra=()):
        self.name, self.port, self.holder, self.prod, self.extra = name, port, holder, prod, list(extra)
        self.proc, self.last, self.started, self.sc = None, {k: 0 for k in FIELDS}, None, None
        self.logf = os.path.join(OUT, "engine-logs", f"{name}.log")
        os.makedirs(os.path.dirname(self.logf), exist_ok=True)

    def cmd(self):
        c = [BIN, "--listen", f"127.0.0.1:{self.port}", "--bucket", B, "--holder", self.holder]
        if LOCAL:
            c += ["--store", "local", "--local-dir", os.environ["T3_LOCAL_DIR"], "--no-leases"]
        else:
            c += ["--region", R]  # no --endpoint: AWS itself
        if self.prod:
            c += PROD + (["--auth"] if AUTH else []) + ["--tls-mode", "requireTLS", "--tls-certificate-key-file", os.path.join(CERT, "server.pem")]
        return c + self.extra

    def start(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_ENDPOINT") and k != "AWS_ALLOW_HTTP"}
        self.proc = subprocess.Popen(self.cmd(), stdout=open(self.logf, "ab"), stderr=subprocess.STDOUT, env=env)
        self.last = {k: 0 for k in FIELDS}
        for _ in range(300):
            if self.proc.poll() is not None:
                raise Stop(f"engine {self.name} exited at start (code {self.proc.returncode}); see engine-logs/{self.name}.log")
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            raise Stop(f"engine {self.name} did not open its port")
        self.started = time.time()
        with Totals.lock:
            Totals.live.append(self)
        if self.prod and AUTH:
            self.ensure_root()
        return self

    def ensure_root(self):
        c = MongoClient(self.uri(False), serverSelectionTimeoutMS=30000)
        try:
            c.admin.command("createUser", "root", pwd=ROOT_PW, roles=[{"role": "root", "db": "admin"}])
        except OperationFailure:
            pass  # created by an earlier start: users live in the bucket
        finally:
            c.close()

    def uri(self, auth=True):
        q = "directConnection=true"
        if self.prod:
            q += f"&tls=true&tlsCAFile={os.path.join(CERT, 'ca.pem')}"
            if auth and AUTH:
                return f"mongodb://root:{ROOT_PW}@127.0.0.1:{self.port}/?{q}&authSource=admin"
        return f"mongodb://127.0.0.1:{self.port}/?{q}"

    def client(self, **kw):
        kw.setdefault("serverSelectionTimeoutMS", 30000)
        kw.setdefault("socketTimeoutMS", 300000)
        return MongoClient(self.uri(), **kw)

    def stats(self, c=None):
        # One persistent, already authenticated connection for the counters: a new connection per call would authenticate
        # each time, read the users from the `admin` database and keep that database from ever going to rest.
        if c is None:
            if self.sc is None:
                self.sc = self.client(maxPoolSize=2)
            c = self.sc
        try:
            s = c.admin.command("tenantStats")
        except PyMongoError:
            if c is self.sc:
                self.sc.close()
                self.sc = None
            raise
        r = s["requests"]
        self.last = {k: int(r.get(k, 0)) for k in FIELDS}
        return s

    def stop(self, timeout=180):
        if not self.proc:
            return
        try:
            self.stats()
        except Exception:
            pass
        if self.sc is not None:
            self.sc.close()
            self.sc = None
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        with Totals.lock:
            for k in FIELDS:
                Totals.done[k] += self.last[k]
            if self in Totals.live:
                Totals.live.remove(self)
        self.proc = None


# ---------------------------------------------------------------- spend monitor
STATE = {"bytes": 0, "stop": None}


def cost_now():
    t = Totals.now()
    put = t["billedPutTier"] + Runner.put + Runner.lst
    get = t["billedGetTier"] + Runner.get
    hours = (time.time() - T0) / 3600
    storage = STATE["bytes"] / 2 ** 30 * P_GB * (hours + 24) / 730  # + 24 h: the bucket could outlive the run by a day (lifecycle)
    return put * P_PUT + get * P_GET + storage, put, get


def monitor(engines_ref):
    f = open(os.path.join(OUT, "cost_live.csv"), "w")
    f.write("secs,cost_usd,put_tier,get_tier\n")
    n = 0
    while not STATE.get("done"):
        time.sleep(10)
        for e in list(engines_ref):
            if e.proc and e.proc.poll() is None:
                try:
                    e.stats()
                except Exception:
                    pass
        c, put, get = cost_now()
        f.write(f"{int(time.time() - T0)},{c:.6f},{put},{get}\n")
        f.flush()
        n += 1
        if n % 6 == 0:
            log(f"spend so far {c:.4f} USD (cap {CAP})")
        if c > CAP:
            STATE["stop"] = f"spend estimate {c:.4f} USD above the cap {CAP} USD"
            log("STOP:", STATE["stop"])
            for e in list(engines_ref):
                try:
                    e.proc.kill()
                except Exception:
                    pass
            return


def check_stop():
    if STATE["stop"]:
        raise Stop(STATE["stop"])
    if time.time() > JOB_DEADLINE:
        raise Stop("job time budget exhausted")


# ---------------------------------------------------------------- data
NAMES = ["Rossi", "Bianchi", "Ferrari", "Esposito", "Romano", "Colombo", "Ricci", "Marino", "Greco", "Bruno"]


def customer(i, rnd):
    return {"_id": i, "name": f"{rnd.choice(NAMES)} {i}", "email": f"user{i}@example.com", "tier": rnd.choice(["free", "pro", "team"]),
            "address": {"street": f"Via Roma {rnd.randint(1, 200)}", "city": rnd.choice(["Milano", "Roma", "Torino", "Napoli"]), "zip": f"{rnd.randint(10000, 99999)}"},
            "tags": rnd.sample(["vip", "new", "eu", "b2b", "trial", "churn-risk"], 2), "created": 1700000000 + i * 3600,
            "notes": "".join(rnd.choice("abcdefghijklmnopqrstuvwxyz ") for _ in range(600))}


def order(i, rnd, ncust):
    items = [{"sku": f"SKU-{rnd.randint(1000, 9999)}", "qty": rnd.randint(1, 5), "price": round(rnd.uniform(3, 300), 2)} for _ in range(rnd.randint(2, 5))]
    return {"_id": i, "customer_id": rnd.randrange(ncust), "status": rnd.choice(["new", "paid", "shipped", "returned"]), "items": items,
            "total": round(sum(x["qty"] * x["price"] for x in items), 2), "ts": 1700000000 + i * 60,
            "shipping": {"carrier": rnd.choice(["DHL", "UPS", "BRT"]), "tracking": "%016x" % rnd.getrandbits(64)},
            "comment": "".join(rnd.choice("abcdefghijklmnopqrstuvwxyz ") for _ in range(450))}


def docs_for(db):
    rnd = random.Random(db)
    return [customer(i, rnd) for i in range(50)], [order(i, rnd, 50) for i in range(150)]  # 200 documents, ~1 KB each


def fingerprint(c, db):
    h = hashlib.sha256()
    n = 0
    for coll in ("customers", "orders"):
        for d in c[db][coll].find({}, sort=[("_id", 1)]):
            h.update(json.dumps(d, sort_keys=True, default=str).encode())
            n += 1
    return n, h.hexdigest()


def expected_fingerprint(db):
    cu, od = docs_for(db)
    h = hashlib.sha256()
    for d in cu + od:
        h.update(json.dumps(d, sort_keys=True, default=str).encode())
    return len(cu) + len(od), h.hexdigest()


def wait_dormant(e, timeout=900):
    t0 = time.time()
    while time.time() - t0 < timeout:
        check_stop()
        s = e.stats()
        if int(s["openReaders"]) + int(s["openWriters"]) == 0 and int(s["leasesHeld"]) == 0:
            return time.time() - t0
        time.sleep(2)
    raise Stop("databases did not go to rest within 15 minutes")


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(math.ceil(len(xs) * q)) - 1)] if xs else None


# ---------------------------------------------------------------- part A
def part_a(engines):
    res = {}
    # A1 + A3: production engine, credentials of the environment, --region, no --endpoint; full cycle with 200 documents
    e = Engine("a1-a3", 27100, "t3-a", prod=True)
    engines.append(e)
    e.start()
    c = e.client()
    db = "cycle"
    cu, od = docs_for(db)
    c[db].customers.insert_many(cu)
    c[db].orders.insert_many(od)
    c[db].orders.create_index("customer_id")
    res["A1_credentials"] = {"ok": True, "how": "engine started with --region eu-central-1, no --endpoint, credentials only from the Actions environment (OIDC role session); 200 documents written and acknowledged"}
    c.close()
    dormant = wait_dormant(e)
    c = e.client()
    n1, h1 = fingerprint(c, db)
    c.close()
    e.stop()
    CREDS.renew(force=True)
    e.start()
    c = e.client()
    n2, h2 = fingerprint(c, db)
    c.close()
    e.stop()
    want = expected_fingerprint(db)
    res["A3_full_cycle"] = {"documents_expected": want[0], "after_rest": n1, "after_rest_and_engine_restart": n2,
                            "identical_after_rest": (n1, h1) == want, "identical_after_restart": (n2, h2) == want,
                            "seconds_to_rest": round(dormant, 1), "ok": (n1, h1) == want and (n2, h2) == want}
    save("partA.json", res)
    if not res["A3_full_cycle"]["ok"]:
        raise Stop("A3: data lost or changed")
    # A2: two processes, different holders, same database; short lease to see the takeover within the test
    if LOCAL:
        res["A2_leases"] = {"skipped": "local store has no conditional writes across processes"}
        save("partA.json", res)
        return res
    lease = ["--lease-ttl-secs", "10", "--idle-secs", "300"]
    p1 = Engine("a2-p1", 27101, "t3-holder-1", prod=False, extra=lease)
    p2 = Engine("a2-p2", 27102, "t3-holder-2", prod=False, extra=lease)
    engines += [p1, p2]
    p1.start()
    p2.start()
    c1 = MongoClient(p1.uri(), serverSelectionTimeoutMS=20000, retryWrites=False, retryReads=False, socketTimeoutMS=60000)
    c2 = MongoClient(p2.uri(), serverSelectionTimeoutMS=20000, retryWrites=False, retryReads=False, socketTimeoutMS=60000)
    steps = []

    def tryop(who, label, fn):
        t = time.time()
        try:
            fn()
            steps.append({"t": round(t - t_start, 1), "who": who, "op": label, "result": "ok"})
            return True
        except PyMongoError as ex:
            steps.append({"t": round(t - t_start, 1), "who": who, "op": label, "result": "refused",
                          "code": getattr(ex, "code", None), "error": str(ex)[:300]})
            return False

    t_start = time.time()
    tryop("P1", "insert p1-1", lambda: c1.leasedb.c.insert_one({"_id": "p1-1"}))
    second_refused = not tryop("P2", "insert p2-1 while P1 holds the lease", lambda: c2.leasedb.c.insert_one({"_id": "p2-1"}))
    tryop("P2", "read while P1 holds the lease", lambda: c2.leasedb.c.find_one({}))
    os.kill(p1.proc.pid, signal.SIGSTOP)
    steps.append({"t": round(time.time() - t_start, 1), "who": "P1", "op": "SIGSTOP (P1 frozen: no renewals)"})
    took = None
    for _ in range(40):
        time.sleep(3)
        if tryop("P2", "insert p2-2 (waiting for the lease to expire)", lambda: c2.leasedb.c.insert_one({"_id": "p2-2"})):
            took = time.time() - t_start
            break
    os.kill(p1.proc.pid, signal.SIGCONT)
    steps.append({"t": round(time.time() - t_start, 1), "who": "P1", "op": "SIGCONT (P1 resumes, believes it may still own the database)"})
    fenced = not tryop("P1", "insert p1-2 after losing the lease", lambda: c1.leasedb.c.insert_one({"_id": "p1-2"}))
    time.sleep(2)
    fenced2 = not tryop("P1", "insert p1-3, 2 s later", lambda: c1.leasedb.c.insert_one({"_id": "p1-3"}))
    seen = sorted(d["_id"] for d in c2.leasedb.c.find({}))
    steps.append({"t": round(time.time() - t_start, 1), "who": "P2", "op": "read all", "result": seen})
    c1.close()
    c2.close()
    s1, s2 = p1.stats(), p2.stats()
    p1.stop()
    p2.stop()
    ok = second_refused and took is not None and fenced and fenced2 and seen == ["p1-1", "p2-2"]
    res["A2_leases"] = {"ok": ok, "second_refused_while_first_holds": second_refused, "takeover_after_secs": round(took, 1) if took else None,
                        "first_fenced_after_losing_lease": fenced and fenced2, "documents_finally_present": seen,
                        "lease_ttl_secs": 10, "p1_lease_stats": s1["leases"], "p2_lease_stats": s2["leases"], "timeline": steps}
    save("partA.json", res)
    if not ok:
        raise Stop("A2: lease / fencing behaviour not as required (see partA.json)")
    return res


# ---------------------------------------------------------------- part B
def create_db(c, db):
    c[db].create_collection("customers")
    c[db].create_collection("orders")
    c[db].orders.create_index("customer_id")


def fill_db(c, db):
    cu, od = docs_for(db)
    c[db].customers.insert_many(cu)
    c[db].orders.insert_many(od)


def phase(name, fn, rows, step):
    check_stop()
    a = Totals.now()
    t = time.time()
    out = fn()
    b = Totals.now()
    rows.append({"N": step, "phase": name, "secs": round(time.time() - t, 1), **{k: v for k, v in diff(a, b).items()}})
    return out


def objects_by_db(step):
    rows = listing()
    STATE["bytes"] = sum(s for _, s in rows)
    per = {}
    other = {"objects": 0, "bytes": 0}
    for k, s in rows:
        parts = k.split("/")
        if parts[0] == "tenants" and len(parts) > 2 and parts[1].startswith("d"):
            x = per.setdefault(parts[1], {"objects": 0, "bytes": 0})
        else:
            x = other
        x["objects"] += 1
        x["bytes"] += s
    with open(os.path.join(OUT, f"objects_by_db_N{step}.csv"), "w") as f:
        w = csv.writer(f)
        w.writerow(["db", "objects", "bytes"])
        for db in sorted(per):
            w.writerow([db, per[db]["objects"], per[db]["bytes"]])
    vals = list(per.values())
    return {"total_objects": len(rows), "total_bytes": STATE["bytes"], "databases_listed": len(per),
            "objects_per_db_median": statistics.median([v["objects"] for v in vals]) if vals else None,
            "bytes_per_db_median": statistics.median([v["bytes"] for v in vals]) if vals else None,
            "objects_per_db_max": max([v["objects"] for v in vals]) if vals else None,
            "bytes_per_db_max": max([v["bytes"] for v in vals]) if vals else None,
            "other_prefixes": other}


def part_b(engines):
    names = [f"d{i:05d}" for i in range(STEPS[-1])]
    have = 0
    summary, phases, wakes_rows, rest_rows = [], [], [], []
    e = None
    for step in STEPS:
        check_stop()
        log(f"=== N = {step}")
        # fresh engine (and fresh credentials) for every step; start-up requests measured with no client for 60 s
        if e:
            e.stop()
        CREDS.renew(force=True)
        e = Engine(f"b-N{step}", 27200, "t3-b", prod=True)
        engines.append(e)
        a = Totals.now()
        e.start()
        time.sleep(5 if LOCAL else 60)
        e.stats()
        startup = diff(a, Totals.now())
        new = names[have:step]
        cw = e.client(maxPoolSize=PARALLEL * 2)  # one client (one authentication) shared by the threads
        with ThreadPoolExecutor(PARALLEL) as ex:
            phase("create", lambda: list(ex.map(lambda d: create_db(cw, d), new)), phases, step)
            phase("write", lambda: list(ex.map(lambda d: fill_db(cw, d), new)), phases, step)
        cw.close()
        have = step
        phase("to_rest", lambda: wait_dormant(e), phases, step)
        objs = objects_by_db(step)
        if CREDS.age() > 10 * 60:  # the rest + the wakes must fit in the session: restart with fresh credentials
            e.stop()
            CREDS.renew(force=True)
            e.start()
            time.sleep(5 if LOCAL else 60)
            wait_dormant(e)
        # rest: every database dormant, REST_SECS, per-minute samples
        a = Totals.now()
        t = time.time()
        while time.time() - t < REST_SECS:
            check_stop()
            time.sleep(min(60, REST_SECS))
            e.stats()
            rest_rows.append({"N": step, "secs": round(time.time() - t), **diff(a, Totals.now())})
        rest = diff(a, Totals.now())
        phases.append({"N": step, "phase": "rest", "secs": round(time.time() - t, 1), **rest})
        # cold wakes, then a second request on the same database
        sample = names[:step] if step <= WAKE_SAMPLE else random.Random(step).sample(names[:step], WAKE_SAMPLE)
        c = e.client(maxPoolSize=2)
        c.admin.command("ping")  # authenticate now: the users read from `admin` must not be counted in the first wake
        e.stats()
        w1, w2 = [], []
        a = Totals.now()
        for db in sample:
            check_stop()
            q0 = Totals.now()
            t1 = time.perf_counter()
            c[db].orders.find_one({"customer_id": 7})
            l1 = (time.perf_counter() - t1) * 1000
            e.stats()
            q1 = Totals.now()
            t2 = time.perf_counter()
            c[db].orders.find_one({"customer_id": 8})
            l2 = (time.perf_counter() - t2) * 1000
            e.stats()
            q2 = Totals.now()
            d1, d2 = diff(q0, q1), diff(q1, q2)
            w1.append((l1, d1))
            w2.append((l2, d2))
            wakes_rows.append({"N": step, "db": db, "first_ms": round(l1, 1), "second_ms": round(l2, 1),
                               **{f"first_{k}": d1[k] for k in ("get", "list", "head", "put", "delete")},
                               **{f"second_{k}": d2[k] for k in ("get", "list", "head", "put", "delete")}})
        c.close()
        phases.append({"N": step, "phase": "wake", "secs": 0, **diff(a, Totals.now())})
        l1s, l2s = [x[0] for x in w1], [x[0] for x in w2]
        per_wake = {k: statistics.median([x[1][k] for x in w1]) for k in ("get", "list", "head", "put", "delete")}
        summary.append({"N": step, "startup_requests_60s": startup, "objects": objs, "rest_secs": REST_SECS, "rest_requests": rest,
                        "wake_sample": len(sample), "first_ms_p50": round(pct(l1s, .5), 1), "first_ms_p95": round(pct(l1s, .95), 1),
                        "first_ms_max": round(max(l1s), 1), "second_ms_p50": round(pct(l2s, .5), 1), "second_ms_p95": round(pct(l2s, .95), 1),
                        "second_ms_max": round(max(l2s), 1), "requests_per_first_wake_median": per_wake,
                        "spend_so_far_usd": round(cost_now()[0], 6)})
        save("partB_summary.json", summary)
        for name, rows in (("partB_phases.csv", phases), ("partB_wakes.csv", wakes_rows), ("partB_rest_minutes.csv", rest_rows)):
            if rows:
                with open(os.path.join(OUT, name), "w") as f:
                    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                    w.writeheader()
                    w.writerows(rows)
        log(f"N={step} done: {objs['total_objects']} objects, {objs['total_bytes']} bytes, rest requests {rest['billedPutTier']}+{rest['billedGetTier']}, "
            f"wake p50 {summary[-1]['first_ms_p50']} ms, spend {summary[-1]['spend_so_far_usd']} USD")
        # will the next step fit in the time left? (create+write+rest+wakes, generous)
        nxt = STEPS[STEPS.index(step) + 1] if step != STEPS[-1] else None
        if nxt and time.time() + (REST_SECS + 1200 + (nxt - step) * 1.0) > JOB_DEADLINE:
            raise Stop(f"not enough job time left for N={nxt}")
    if e:
        e.stop()
    return summary


def main():
    engines = []
    meta = {"bucket_name_pattern": "fenvix-test-t3-<run id>", "region": R, "engine_commit": os.environ.get("ENGINE_COMMIT"),
            "engine_sha256": os.environ.get("ENGINE_SHA256"), "engine_patch": os.environ.get("ENGINE_PATCH"), "steps": STEPS, "rest_secs": REST_SECS, "cap_usd": CAP,
            "prices": PRICES, "local_dry_check": LOCAL, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    th = threading.Thread(target=monitor, args=(engines,), daemon=True)
    th.start()
    status = "ok"
    try:
        part_a(engines)
        part_b(engines)
    except Stop as s:
        status = f"stopped: {s}"
        log("STOP:", s)
    except Exception as ex:
        status = f"error: {type(ex).__name__}: {str(ex)[:300]}"
        log("ERROR:", status)
    finally:
        for e in engines:
            try:
                e.stop(60)
            except Exception:
                pass
        STATE["done"] = True
        c, put, get = cost_now()
        meta.update(status=status, finished_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), spend_estimate_usd=round(c, 6),
                    put_tier_requests=put, get_tier_requests=get, engine_requests_total=Totals.now(),
                    runner_requests={"put": Runner.put, "get": Runner.get, "list": Runner.lst, "delete": Runner.delete},
                    last_bucket_bytes=STATE["bytes"], duration_secs=round(time.time() - T0))
        save("run.json", meta)
        log("finished:", status, f"spend estimate {c:.4f} USD")
    sys.exit(0 if status == "ok" else 3)


if __name__ == "__main__":
    main()
