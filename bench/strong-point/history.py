"""P-B1-strong-point (PLAN B1-B2, 2026-11-13): one customer's whole history read cold, ours against mongod and Postgres,
with the database at three sizes larger than every system's memory cap.

The data: a message store, one row per message, read one customer at a time, newest first.
  - document: {_id, userId, convId, role, ts, text, tokens}; text 800-3,200 characters of English-like words (a slice
    of a fixed word pool, Zipf weights, so it compresses like text); about 2.1 KB of BSON on average;
  - index {userId: 1, ts: -1} (Postgres: (user_id, ts DESC)), created before the load, as an app would;
  - measured customers: 200 with exactly 50 messages, 200 with 500, 200 with 5,000 ("h50", "h500", "h5000"), all
    written during the first size's load, spread over the whole of it and interleaved with every other customer's
    writes (the way a busy database receives them); they never write again, so their histories stay the same
    while the database grows around them;
  - filler customers: uniform over 100,000 ids during size 1, then 200,000 and 300,000 (new customers arrive);
  - the plan is a fixed list of (slot -> customer); a slot's document is a pure function of (seed, slot), so every
    system receives the same documents in the same order and every answer can be checked without storing it.

The reads (one at a time, one client, after a restart of every container of the line and a drop of the host's
page cache):
  - warm-up: the newest 10 of 300 other filler customers (the database is in service; index tops and SST metadata
    come back the way they would for any request), recorded, not reported;
  - "whole[cold]": find({userId}).sort({ts: -1}), every document, the first request for that customer since the
    restart; then at once "whole[warm]" (the same read again) and "newest10[warm]";
  - "newest10[cold]": find(...).sort({ts: -1}).limit(10) on 200 filler customers never read since the restart;
  - the order of all tasks is one fixed shuffle, the same for every system and size.
Timed region: the request until the last document is in the client, documents kept raw (RawBSONDocument; Postgres
rows as raw text), so the client's decoding is not in it. Every answer is decoded and checked after the timer
(sha256 over id, customer, ts and text of every document in order) against the plan: a wrong or missing document is
a STOP (exit 3, file STOP). Client CPU (process time) is measured inside every timed region.

Per call, outside the timed region (the probe of bench-big's bigmix.py): every container's CPU (cgroup) and bytes
read from the disk (/proc/<pid>/io), and on ours serverStatus().fenvix (S3 GETs and bytes, caches, background reads).

Usage (BENCH_PW in the environment):
  history.py plan  --state DIR [--sizes-gb 5,10,15]
  history.py run   --state DIR --line fenvix-log|mongo-single|postgres-single|mongo-rs3 --out DIR [--sizes 1,2,3]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import multiprocessing as mp
import os
import random
import shutil
import subprocess
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import lines  # noqa: E402  (the bench's line runner: same containers, limits, durability checks, restart)
import targets  # noqa: E402

DB, COLL = "app_demo", "messages"
T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
T0_S = int(T0.timestamp())
CLASSES = {"h50": 50, "h500": 500, "h5000": 5000}
SAVE_BASE = 50_000_000      # customers of the B2 save runs (never read by the measured phases)
CG = "/sys/fs/cgroup"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# ----------------------------------------------------------------------------------------------------------------------
# the plan and the documents
# ----------------------------------------------------------------------------------------------------------------------

POOL = None
POOL_SEED, POOL_LEN = 7, 64 << 20
TEXT_MIN, TEXT_MAX = 800, 3200


def pool() -> str:
    """64 MiB of English-like text: words of 2-9 letters from a 4,000-word vocabulary, Zipf weights (as cold_customer.py)."""
    global POOL
    if POOL is None:
        r = random.Random(POOL_SEED)
        letters = "abcdefghijklmnopqrstuvwxyz"
        words = ["".join(r.choice(letters) for _ in range(r.randint(2, 9))) for _ in range(4000)]
        rn = np.random.default_rng(POOL_SEED)
        w = 1.0 / np.arange(1, 4001)
        idx = rn.choice(4000, size=POOL_LEN // 5, p=w / w.sum())
        s = " ".join(words[i] for i in idx)
        POOL = s[:POOL_LEN]
    return POOL


def mix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return x ^ (x >> 31)


def doc_parts(n: int, u: int) -> dict:
    """The document of slot n (customer u): a pure function of n."""
    h = mix64(n * 2654435761 + 12345)
    ln = TEXT_MIN + h % (TEXT_MAX - TEXT_MIN + 1)
    off = (h >> 16) % (POOL_LEN - ln)
    ts_ms = n * 10
    oid = (T0_S + ts_ms // 1000).to_bytes(4, "big") + n.to_bytes(8, "big")
    return {"oid": oid, "u": int(u), "conv": int(u) * 16 + (h >> 48) % 16, "role": "user" if n % 2 else "assistant",
            "ts_ms": ts_ms, "off": off, "len": ln}


def mongo_doc(n: int, u: int) -> dict:
    from bson import ObjectId
    p = doc_parts(n, u)
    text = pool()[p["off"]:p["off"] + p["len"]]
    return {"_id": ObjectId(p["oid"]), "userId": p["u"], "convId": p["conv"], "role": p["role"],
            "ts": T0 + dt.timedelta(milliseconds=p["ts_ms"]), "text": text, "tokens": p["len"] // 4}


def doc_digest_line(oid_hex: str, u: int, ts_ms: int, text: str) -> bytes:
    return f"{oid_hex}|{u}|{ts_ms}|".encode() + text.encode() + b"\n"


def expected_hash(slots: list[int], u: int) -> str:
    """sha256 of the customer's documents newest first (slots in descending order)."""
    h = hashlib.sha256()
    for n in sorted(slots, reverse=True):
        p = doc_parts(n, u)
        h.update(doc_digest_line(p["oid"].hex(), u, p["ts_ms"], pool()[p["off"]:p["off"] + p["len"]]))
    return h.hexdigest()


def avg_bson_bytes(sample=2000) -> float:
    import bson
    return sum(len(bson.encode(mongo_doc(n, 1234))) for n in range(sample)) / sample


def make_plan(state: str, sizes_gb: list[float], per_class: int, seed: int, fillers: list[int]) -> dict:
    os.makedirs(state, exist_ok=True)
    rng = np.random.default_rng(seed)
    avg = avg_bson_bytes()
    n_tot = [int(g * 1e9 / avg) for g in sizes_gb]
    # The measured customers are odd ids at random inside the filler range of size 1, and no filler ever uses them: in
    # the key order of every system a measured customer's neighbours are filler customers (the first plan gave them
    # adjacent ids, so on our line a block fetched for one measured customer also held the next one: its "cold" read
    # found its data already fetched; that run was discarded).
    odd = rng.choice(np.arange(1, fillers[0], 2), size=len(CLASSES) * per_class, replace=False)
    measured = {c: np.sort(odd[i * per_class:(i + 1) * per_class]) for i, c in enumerate(CLASSES)}
    reserved = np.sort(odd)
    m_slots = np.concatenate([np.repeat(ids, CLASSES[c]) for c, ids in measured.items()])

    def filler(hi, cnt):
        a = rng.integers(0, hi, cnt)
        while True:
            bad = np.isin(a, reserved)
            if not bad.any():
                return a
            a[bad] = rng.integers(0, hi, int(bad.sum()))

    parts, prev = [], 0
    for k, n in enumerate(n_tot):
        cnt = n - prev
        if k == 0:
            f = filler(fillers[0], cnt - len(m_slots))
            a = np.concatenate([m_slots, f])
            a = a[rng.permutation(len(a))]
        else:
            a = filler(fillers[min(k, 2)], cnt)
        parts.append(a.astype(np.int64))
        np.save(os.path.join(state, f"slots_{k + 1}.npy"), parts[-1])
        prev = n
    size1 = parts[0]
    # filler customers with at least 10 messages at size 1: the newest-10 set and the warm-up set
    counts = np.bincount(size1[size1 < fillers[0]], minlength=fillers[0])
    counts[reserved] = 0
    ok = np.nonzero(counts >= 10)[0]
    pick = rng.choice(ok, size=500, replace=False)
    plan = {"seed": seed, "sizes_gb_logical": sizes_gb, "avg_bson_bytes": round(avg, 1), "slots_total": n_tot,
            "per_class": per_class, "classes": CLASSES, "measured_ids": {c: [int(x) for x in v] for c, v in measured.items()},
            "newest_ids": [int(x) for x in pick[:200]], "warmup_ids": [int(x) for x in pick[200:]],
            "filler_customers": fillers, "text": [TEXT_MIN, TEXT_MAX], "pool": [POOL_SEED, POOL_LEN], "t0": T0.isoformat()}
    json.dump(plan, open(os.path.join(state, "plan.json"), "w"), indent=1)
    return plan


class Plan:
    def __init__(self, state: str):
        self.state = state
        self.p = json.load(open(os.path.join(state, "plan.json")))
        self.parts = [np.load(os.path.join(state, f"slots_{k + 1}.npy")) for k in range(len(self.p["slots_total"]))]
        self.offsets = [0] + list(self.p["slots_total"])

    def slots_of(self, u: int, size: int) -> list[int]:
        out = []
        for k in range(size):
            idx = np.nonzero(self.parts[k] == u)[0]
            out += [int(i) + self.offsets[k] for i in idx]
        return out

    def expected(self, ids: list[int], size: int) -> dict:
        """{customer: (count, hash of the whole history, hash of the newest 10)}"""
        allslots = np.concatenate(self.parts[:size])
        want = np.isin(allslots, np.array(ids))
        idx = np.nonzero(want)[0]
        by = {u: [] for u in ids}
        for i in idx:
            by[int(allslots[i])].append(int(i))
        out = {}
        for u, s in by.items():
            s.sort(reverse=True)
            out[u] = (len(s), expected_hash(s, u), expected_hash(s[:10], u))
        return out


# ----------------------------------------------------------------------------------------------------------------------
# targets: connect, schema, load, read
# ----------------------------------------------------------------------------------------------------------------------

def target_of(line: str) -> str:
    return lines.LINES[line]["target"]


def mongo_coll(uri: str, raw=False, w=None):
    from bson.codec_options import CodecOptions
    from bson.raw_bson import RawBSONDocument
    from pymongo import MongoClient
    kw = {"w": w} if w is not None else {}
    c = MongoClient(uri, serverSelectionTimeoutMS=30000, socketTimeoutMS=600000, **kw)
    co = CodecOptions(document_class=RawBSONDocument, tz_aware=True) if raw else CodecOptions(tz_aware=True)
    return c, c[DB].get_collection(COLL, codec_options=co)


PG_DDL = ["CREATE TABLE IF NOT EXISTS messages (id bytea PRIMARY KEY, user_id bigint NOT NULL, conv_id bigint NOT NULL, "
          "role text NOT NULL, ts timestamptz NOT NULL, body text NOT NULL, tokens int NOT NULL)",
          "CREATE INDEX IF NOT EXISTS messages_user_ts ON messages (user_id, ts DESC)"]
PG_COLS = "id, user_id, conv_id, role, ts, body, tokens"


def pg_raw_conn(uri: str):
    """A connection whose rows come back as the server's raw text (no type conversion in the timed region)."""
    import psycopg
    from psycopg.adapt import Loader

    class Raw(Loader):
        def load(self, data):
            return bytes(data)

    c = psycopg.connect(uri, autocommit=True)
    for t in ("bytea", "int8", "int4", "text", "timestamptz"):
        c.adapters.register_loader(t, Raw)
    return c


def schema(line: str, uri: str) -> dict:
    t = target_of(line)
    if t == "postgres":
        import psycopg
        with psycopg.connect(uri, autocommit=True) as c:
            for q in PG_DDL:
                c.execute(q)
        return {"ddl": PG_DDL}
    c, coll = mongo_coll(uri)
    c[DB].create_collection(COLL) if COLL not in c[DB].list_collection_names() else None
    coll.create_index([("userId", 1), ("ts", -1)], name="userId_1_ts_-1")
    info = [x for x in c[DB].command("listCollections")["cursor"]["firstBatch"] if x["name"] == COLL]
    return {"index": "userId_1_ts_-1", "collection": json.loads(json.dumps(info, default=str)),
            "indexes": json.loads(json.dumps(list(coll.list_indexes()), default=str))}


_W = {}


def _load_init(line: str, uri: str, state: str):
    _W["line"], _W["uri"], _W["plan"] = line, uri, Plan(state)
    pool()
    if target_of(line) == "postgres":
        import psycopg
        _W["pg"] = psycopg.connect(uri, autocommit=True)
    else:
        _W["c"], _W["coll"] = mongo_coll(uri)


def _load_chunk(job):
    size, lo, hi = job
    pl = _W["plan"]
    arr = pl.parts[size - 1][lo:hi]
    base = pl.offsets[size - 1]
    for attempt in range(5):
        try:
            if target_of(_W["line"]) == "postgres":
                with _W["pg"].cursor() as cur:
                    with cur.copy(f"COPY messages ({PG_COLS}) FROM STDIN") as cp:
                        for i, u in enumerate(arr):
                            n = base + lo + i
                            p = doc_parts(n, int(u))
                            cp.write_row((p["oid"], p["u"], p["conv"], p["role"], T0 + dt.timedelta(milliseconds=p["ts_ms"]),
                                          pool()[p["off"]:p["off"] + p["len"]], p["len"] // 4))
            else:
                docs = [mongo_doc(base + lo + i, int(u)) for i, u in enumerate(arr)]
                _W["coll"].insert_many(docs, ordered=True)
            return hi - lo
        except Exception as e:  # noqa: BLE001 - a retried chunk must not double-insert: refuse to guess, fail loudly
            raise RuntimeError(f"chunk {size}:{lo}-{hi} failed: {e!r}") from e
    return 0


def load(line: str, uri: str, state: str, size: int, workers: int, chunk: int, min_free_gib: float) -> dict:
    pl = Plan(state)
    n = len(pl.parts[size - 1])
    jobs = [(size, lo, min(n, lo + chunk)) for lo in range(0, n, chunk)]
    t0, done, last = time.time(), 0, time.time()
    with mp.get_context("fork").Pool(workers, initializer=_load_init, initargs=(line, uri, state)) as p:
        for k in p.imap(_load_chunk, jobs, chunksize=1):
            done += k
            if time.time() - last > 60:
                last = time.time()
                free = shutil.disk_usage("/").free / 2**30
                log(f"load size {size}: {done:,}/{n:,} ({done / max(1, time.time() - t0):,.0f} docs/s), disk free {free:.1f} GiB")
                if free < min_free_gib:
                    p.terminate()
                    raise RuntimeError(f"STOP: disk free {free:.1f} GiB < {min_free_gib}")
    return {"size": size, "docs": n, "seconds": round(time.time() - t0, 1), "docs_per_s": round(n / (time.time() - t0), 1)}


# ----------------------------------------------------------------------------------------------------------------------
# per-call probe (bigmix.py's Probe, copied: cgroup CPU and disk bytes of every container; our server's counters)
# ----------------------------------------------------------------------------------------------------------------------

def _flat(d, prefix="") -> dict:
    out = {}
    for k, v in (d or {}).items():
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            out[prefix + k] = v
        elif isinstance(v, dict):
            out |= _flat(v, prefix + k + ".")
    return out


class Probe:
    SERVER_KEYS = ("s3", "cache", "sealedRuns", "backgroundReads", "firstCourse", "waiter", "compactor", "compaction", "customerKeys")

    def __init__(self, containers: dict[str, str], uri: str | None = None):
        self.containers = containers
        self.client = None
        if uri:
            from pymongo import MongoClient
            self.client = MongoClient(uri, serverSelectionTimeoutMS=10000)

    def _cgroup(self) -> dict:
        out = {}
        for n, cid in self.containers.items():
            try:
                out[f"cpu_ns.{n}"] = int(open(f"{CG}/cpuacct/docker/{cid}/cpuacct.usage").read())
            except OSError:
                continue
            try:
                tot = 0
                for pid in open(f"{CG}/cpuacct/docker/{cid}/cgroup.procs").read().split():
                    try:
                        for ln in open(f"/proc/{pid}/io"):
                            if ln.startswith("read_bytes:"):
                                tot += int(ln.split()[1])
                    except OSError:
                        continue
                out[f"rd_bytes.{n}"] = tot
            except OSError:
                pass
        return out

    def _server(self) -> dict:
        if not self.client:
            return {}
        try:
            f = self.client.admin.command("serverStatus").get("fenvix", {})
        except Exception:  # noqa: BLE001
            return {}
        return _flat({k: f.get(k) for k in self.SERVER_KEYS if k in f}, "srv.")

    def before(self) -> dict:
        return self._server() | self._cgroup()

    def after(self) -> dict:
        return self._cgroup() | self._server()

    @staticmethod
    def delta(a: dict, b: dict) -> dict:
        return {k: v - a[k] for k, v in b.items() if k in a and isinstance(v, (int, float)) and v - a[k]}


# ----------------------------------------------------------------------------------------------------------------------
# reads
# ----------------------------------------------------------------------------------------------------------------------

class Reader:
    def __init__(self, line: str, uri: str):
        self.t = target_of(line)
        if self.t == "postgres":
            self.pg = pg_raw_conn(uri)
        else:
            self.c, self.coll = mongo_coll(uri, raw=True)

    def read(self, u: int, limit: int | None):
        """Timed: the request until every document is in the client, raw. Returns (seconds, client cpu s, raw rows)."""
        if self.t == "postgres":
            q = f"SELECT {PG_COLS} FROM messages WHERE user_id = %s ORDER BY ts DESC" + (f" LIMIT {limit}" if limit else "")
            c0, t0 = time.process_time(), time.perf_counter()
            with self.pg.cursor() as cur:
                cur.execute(q, (u,), prepare=False)
                rows = cur.fetchall()
            t1, c1 = time.perf_counter(), time.process_time()
            return t1 - t0, c1 - c0, rows
        c0, t0 = time.process_time(), time.perf_counter()
        cur = self.coll.find({"userId": u}, sort=[("ts", -1)], limit=limit or 0)
        rows = list(cur)
        t1, c1 = time.perf_counter(), time.process_time()
        return t1 - t0, c1 - c0, rows

    def digest(self, rows) -> tuple[int, str, int]:
        """(count, sha256, bytes on the wire for the documents) of a raw answer, after the timer."""
        h, nbytes = hashlib.sha256(), 0
        if self.t == "postgres":
            for r in rows:
                oid = bytes.fromhex(r[0].decode()[2:])
                ts = dt.datetime.fromisoformat(r[4].decode())
                ts_ms = int(round((ts - T0).total_seconds() * 1000))
                nbytes += sum(len(x) for x in r)
                h.update(doc_digest_line(oid.hex(), int(r[1]), ts_ms, r[5].decode()))
            return len(rows), h.hexdigest(), nbytes
        import bson
        for r in rows:
            d = bson.decode(r.raw, codec_options=bson.CodecOptions(tz_aware=True))
            ts_ms = int(round((d["ts"] - T0).total_seconds() * 1000))
            nbytes += len(r.raw)
            h.update(doc_digest_line(str(d["_id"]), int(d["userId"]), ts_ms, d["text"]))
        return len(rows), h.hexdigest(), nbytes


def pct(xs, q):
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))]


def container_ids(line: str) -> dict:
    return {n: lines.sh(["docker", "inspect", "-f", "{{.Id}}", n]).stdout.strip() for n in lines.all_containers(line)}


def server_container(line: str) -> str:
    return {"fenvix-log": "cook", "fenvix-s3": "cook-nolog", "mongo-single": "mongo-single", "mongo-rs3": "mongo-a",
            "postgres-single": "pg-single", "postgres-sync": "pg-primary"}[line]


def measure(line: str, uri: str, pl: Plan, size: int, round_name: str, out: str, seed: int, stop_file: str) -> dict:
    """One measured round: restart (cold), warm-up, then the shuffled tasks. Rows in <out>/rows-<size>-<round>.jsonl."""
    t_start = time.time()
    lines.restart(line)
    ids = container_ids(line)
    probe = Probe(ids, uri if target_of(line) == "fenvix" else None)
    srv = server_container(line)
    rd = Reader(line, uri)
    P = pl.p
    measured = {c: P["measured_ids"][c] for c in CLASSES}
    allids = [u for v in measured.values() for u in v] + P["newest_ids"]
    log(f"{line} size {size} {round_name}: expected answers for {len(allids)} customers")
    exp = pl.expected(allids, size)
    for c, us in measured.items():
        bad = [u for u in us if exp[u][0] != CLASSES[c]]
        if bad:
            raise RuntimeError(f"plan error: {c} customers with other counts: {bad[:5]}")
    tasks = [("H", c, u) for c, us in measured.items() for u in us] + [("N", "newest", u) for u in P["newest_ids"]]
    random.Random(seed).shuffle(tasks)
    rows_path = os.path.join(out, f"rows-{size}-{round_name}.jsonl")
    wrong = []
    agg = {}
    f = open(rows_path, "w")

    def one(op, cls, u, limit, want):
        b = probe.before()
        secs, ccpu, rows = rd.read(u, limit)
        a = probe.after()
        n, h, nb = rd.digest(rows)
        ok = (n, h) == want
        res = Probe.delta(b, a)
        row = {"op": op, "cls": cls, "u": u, "ms": round(secs * 1000, 3), "client_cpu_ms": round(ccpu * 1000, 3), "n": n,
               "bytes": nb, "ok": ok, "res": res}
        f.write(json.dumps(row) + "\n")
        if not ok:
            wrong.append(row | {"want_n": want[0]})
            open(stop_file, "a").write(f"{time.strftime('%FT%TZ', time.gmtime())} WRONG READ {line} size {size} {round_name} "
                                       f"{op} {cls} customer {u}: got {n} docs hash {h[:16]}, want {want[0]} hash {want[1][:16]}\n")
        k = f"{op}|{cls}"
        g = agg.setdefault(k, {"ms": [], "cpu": [], "gets": [], "get_bytes": [], "srv_cpu_ms": [], "disk_bytes": [], "bg_gets": []})
        g["ms"].append(secs * 1000)
        g["cpu"].append(ccpu * 1000)
        g["srv_cpu_ms"].append(res.get(f"cpu_ns.{srv}", 0) / 1e6)
        g["disk_bytes"].append(sum(v for kk, v in res.items() if kk.startswith("rd_bytes.")))
        if target_of(line) == "fenvix":
            g["gets"].append(res.get("srv.s3.get", 0) + res.get("srv.s3.getRange", 0))
            g["get_bytes"].append(res.get("srv.s3.getBytes", 0))
            g["bg_gets"].append(res.get("srv.backgroundReads.gets", 0))

    # warm-up: the newest 10 of 300 other customers
    wexp = pl.expected(P["warmup_ids"], size)
    for u in P["warmup_ids"]:
        one("warmup", "newest10", u, 10, (min(10, wexp[u][0]), wexp[u][2]))
    for kind, cls, u in tasks:
        cnt, hw, h10 = exp[u]
        if kind == "H":
            one("whole_cold", cls, u, None, (cnt, hw))
            one("whole_warm", cls, u, None, (cnt, hw))
            one("newest10_warm", cls, u, 10, (min(10, cnt), h10))
        else:
            one("newest10_cold", "newest", u, 10, (min(10, cnt), h10))
            one("newest10_warm", "newest", u, 10, (min(10, cnt), h10))
        if wrong:
            break
    f.close()
    summ = {"line": line, "size": size, "round": round_name, "seconds": round(time.time() - t_start, 1), "wrong_reads": len(wrong),
            "wrong": wrong[:5], "ops": {}}
    for k, g in sorted(agg.items()):
        d = {"n": len(g["ms"]), "p50_ms": round(pct(g["ms"], .5), 2), "p90_ms": round(pct(g["ms"], .9), 2),
             "p99_ms": round(pct(g["ms"], .99), 2), "max_ms": round(max(g["ms"]), 2), "mean_ms": round(sum(g["ms"]) / len(g["ms"]), 2),
             "client_cpu_share": round(sum(g["cpu"]) / max(1e-9, sum(g["ms"])), 3),
             "server_cpu_ms_p50": round(pct(g["srv_cpu_ms"], .5), 2), "disk_read_bytes_p50": pct(g["disk_bytes"], .5),
             "disk_read_bytes_mean": round(sum(g["disk_bytes"]) / len(g["disk_bytes"]))}
        if g["gets"]:
            d |= {"s3_gets_p50": pct(g["gets"], .5), "s3_gets_p99": pct(g["gets"], .99), "s3_gets_mean": round(sum(g["gets"]) / len(g["gets"]), 2),
                  "s3_get_bytes_p50": pct(g["get_bytes"], .5), "s3_get_bytes_mean": round(sum(g["get_bytes"]) / len(g["get_bytes"])),
                  "background_gets_total": sum(g["bg_gets"])}
        d["valid"] = d["client_cpu_share"] < 0.8
        summ["ops"][k] = d
    json.dump(summ, open(os.path.join(out, f"summary-{size}-{round_name}.json"), "w"), indent=1)
    for k, d in summ["ops"].items():
        if not k.startswith("warmup"):
            log(f"  {k:28s} n={d['n']:4d} p50 {d['p50_ms']:9.2f} p99 {d['p99_ms']:9.2f} ms  client {d['client_cpu_share']:.2f}"
                + (f"  GETs p50 {d['s3_gets_p50']} bytes p50 {d['s3_get_bytes_p50']}" if "s3_gets_p50" in d else "")
                + f"  disk p50 {d['disk_read_bytes_p50']}")
    if wrong:
        log(f"STOP: {len(wrong)} wrong read(s): {wrong[:2]}")
    return summ


# ----------------------------------------------------------------------------------------------------------------------
# sizes, settle, verification
# ----------------------------------------------------------------------------------------------------------------------

def stored_sizes(line: str, uri: str) -> dict:
    t = target_of(line)
    try:
        if t == "postgres":
            import psycopg
            with psycopg.connect(uri, autocommit=True) as c:
                r = c.execute("SELECT pg_total_relation_size('messages'), pg_relation_size('messages'), pg_indexes_size('messages'),"
                              " pg_total_relation_size('messages') - pg_relation_size('messages') - pg_indexes_size('messages'),"
                              " (SELECT count(*) FROM messages), pg_database_size(current_database())").fetchone()
            return dict(zip(("total", "heap", "indexes", "toast_and_maps", "rows", "database"), map(int, r)))
        c, _ = mongo_coll(uri)
        if t == "mongo":
            c.admin.command("fsync")
            s = c[DB].command("collStats", COLL)
            return {k: s.get(k) for k in ("count", "size", "storageSize", "totalIndexSize", "avgObjSize")} | {
                "indexSizes": s.get("indexSizes")}
        s3 = targets.s3_prefix_bytes({"endpoint": lines.S3["endpoint"], "bucket": lines.S3["bucket"], "region": lines.S3["region"],
                                      "key": lines.S3["key"], "secret": lines.S3["secret"]}, f"tenants/{DB}/", levels=2)
        return {"s3_bytes_total": s3["total"], "s3_objects": s3["objects"], "s3_by_kind": s3["by_kind"],
                "s3_objects_by_kind": s3["count_by_kind"]}
    except Exception as e:  # noqa: BLE001 - recorded
        return {"error": repr(e)}


def count_all(line: str, uri: str) -> dict:
    t0 = time.time()
    try:
        if target_of(line) == "postgres":
            import psycopg
            with psycopg.connect(uri, autocommit=True) as c:
                n = c.execute("SELECT count(*) FROM messages").fetchone()[0]
        else:
            _, coll = mongo_coll(uri)
            n = coll.count_documents({}, maxTimeMS=3_600_000)
        return {"count": int(n), "seconds": round(time.time() - t0, 1)}
    except Exception as e:  # noqa: BLE001
        return {"error": repr(e), "seconds": round(time.time() - t0, 1)}


def verify_sample(line: str, uri: str, pl: Plan, size: int, n: int, seed: int) -> dict:
    """Untimed: the whole history of n random filler customers, checked against the plan (acknowledged writes)."""
    r = random.Random(seed + size)
    ids = r.sample(range(pl.p["filler_customers"][min(size - 1, 2)]), n)
    exp = pl.expected(ids, size)
    rd = Reader(line, uri)
    bad = []
    for u in ids:
        _, _, rows = rd.read(u, None)
        cnt, h, _ = rd.digest(rows)
        if (cnt, h) != exp[u][:2]:
            bad.append({"u": u, "got": cnt, "want": exp[u][0]})
    return {"customers": n, "documents": sum(e[0] for e in exp.values()), "wrong": bad}


def settle(line: str, uri: str) -> dict | None:
    t = target_of(line)
    if t == "fenvix":
        tg = targets.Target("fenvix", uri, "", {"endpoint": lines.S3["endpoint"], "bucket": lines.S3["bucket"], "region": lines.S3["region"],
                                                 "key": lines.S3["key"], "secret": lines.S3["secret"]})
        r = targets.wait_compaction_idle(tg, cap_s=int(os.environ.get("SETTLE_CAP_S", "5400")))
        if r:
            r.pop("polls", None)
        return r
    if t == "postgres":
        import psycopg
        t0 = time.time()
        with psycopg.connect(uri, autocommit=True) as c:
            c.execute("VACUUM ANALYZE messages")
            c.execute("CHECKPOINT")
        return {"vacuum_analyze_checkpoint_s": round(time.time() - t0, 1)}
    c, _ = mongo_coll(uri)
    c.admin.command("fsync")
    return {"fsync": True}


# ----------------------------------------------------------------------------------------------------------------------
# B2: our server with compaction and GC off, and saves with and without compaction
# ----------------------------------------------------------------------------------------------------------------------

# B2 "off": no background compaction or GC loop (and no L0 guard), no compactor, no publish by the server itself
# (--sealed-runs 0: without the compactor the server would otherwise publish runs every 2 write batches under the lock)
OFF_FLAGS = "--no-maintenance --no-sealed-compactor --sealed-runs 0 --wal-gc-secs 0 --sst-gc-secs 0"


def recreate_cook(extra: str) -> None:
    """Our cook again with BASE flags plus `extra` (the spare and the log copies keep running)."""
    base = os.environ.get("FXBENCH_SERVER_FLAGS_BASE", "")
    os.environ["FXBENCH_SERVER_FLAGS"] = (base + " " + extra).strip()
    # a clean stop (SIGTERM, then docker's RESTART_T) closes the databases and releases the leases; `rm -f` would leave
    # them held for the lease's ttl and the new process's first writes fail with TenantLeaseHeld (the pilot)
    lines.sh(["docker", "stop", "-t", lines.RESTART_T, "cook"], timeout=600)
    lines.sh(["docker", "rm", "cook"])
    lines.run_ours("cook", lines.server_cmd("cook", True))
    lines.wait_until("our server answers", lambda: lines.ours_ready("fenvix-log"), 600)
    _, coll = mongo_coll(lines.uri_of("fenvix-log"))
    lines.wait_until("our server takes a write", lambda: coll.database["b2_probe"].insert_one({"t": time.time()}).acknowledged, 300, 2)
    log(f"cook recreated with flags: {os.environ['FXBENCH_SERVER_FLAGS']}")


def saves(uri: str, out: str, tag: str, seconds: int, rate: float, bg_docs_per_s: float, seed: int, slot_base: int) -> dict:
    """B2: timed single-document saves (insert_one, the server's acknowledgement), one client paced at `rate` per s,
    while a second process writes `bg_docs_per_s` documents per second in batches of 100 (work for the compactor).
    Every save and background document is read back afterwards: zero acknowledged writes lost."""
    stop = mp.get_context("fork").Event()

    def bg(stop_ev, base):
        _, coll = mongo_coll(uri)
        k = 0
        while not stop_ev.is_set():
            t0 = time.time()
            docs = [mongo_doc(base + k + i, SAVE_BASE + 1000 + (k + i) % 1000) for i in range(100)]
            coll.insert_many(docs)
            k += 100
            time.sleep(max(0, 100 / bg_docs_per_s - (time.time() - t0)))
        open(os.path.join(out, f"bg-{tag}.count"), "w").write(str(k))

    p = mp.get_context("fork").Process(target=bg, args=(stop, slot_base + 10_000_000))
    p.start()
    _, coll = mongo_coll(uri)
    probe_c, _ = mongo_coll(uri)
    ms, k, t_end = [], 0, time.time() + seconds
    srv0 = probe_c.admin.command("serverStatus").get("fenvix", {})
    while time.time() < t_end:
        t0 = time.perf_counter()
        coll.insert_one(mongo_doc(slot_base + k, SAVE_BASE + k % 100))
        ms.append((time.perf_counter() - t0) * 1000)
        k += 1
        time.sleep(max(0, 1 / rate - (time.perf_counter() - t0)))
    srv1 = probe_c.admin.command("serverStatus").get("fenvix", {})
    stop.set()
    p.join()
    nbg = int(open(os.path.join(out, f"bg-{tag}.count")).read())
    # read back every acknowledged save and background document
    def in_range(lo_slot, n, ulo, uhi):
        return coll.count_documents({"userId": {"$gte": ulo, "$lt": uhi},
                                     "ts": {"$gte": T0 + dt.timedelta(milliseconds=lo_slot * 10),
                                            "$lt": T0 + dt.timedelta(milliseconds=(lo_slot + n) * 10)}})
    got = in_range(slot_base, len(ms), SAVE_BASE, SAVE_BASE + 100)
    gotbg = in_range(slot_base + 10_000_000, nbg, SAVE_BASE + 1000, SAVE_BASE + 2000)
    d = _flat({k2: srv1.get(k2) for k2 in ("compactor", "compaction", "s3")})
    d0 = _flat({k2: srv0.get(k2) for k2 in ("compactor", "compaction", "s3")})
    res = {"tag": tag, "saves": len(ms), "p50_ms": round(pct(ms, .5), 2), "p90_ms": round(pct(ms, .9), 2), "p99_ms": round(pct(ms, .99), 2),
           "max_ms": round(max(ms), 2), "rate_per_s": rate, "background_docs": nbg, "background_docs_per_s_target": bg_docs_per_s,
           "read_back_saves": got, "read_back_background": gotbg, "lost": (len(ms) - got) + (nbg - gotbg),
           "server_counters_delta": {kk: d[kk] - d0.get(kk, 0) for kk in d if isinstance(d[kk], (int, float)) and d[kk] - d0.get(kk, 0)}}
    json.dump({"summary": res, "ms": [round(x, 3) for x in ms]}, open(os.path.join(out, f"saves-{tag}.json"), "w"))
    log(f"saves {tag}: n={len(ms)} p50 {res['p50_ms']} p99 {res['p99_ms']} max {res['max_ms']} ms; lost {res['lost']}; "
        f"compactor delta {{{', '.join(f'{k2}: {v}' for k2, v in res['server_counters_delta'].items() if k2.startswith('compact'))}}}")
    return res


# ----------------------------------------------------------------------------------------------------------------------
# the driver
# ----------------------------------------------------------------------------------------------------------------------

def run(a) -> int:
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    os.makedirs(os.path.join(out, "logs"), exist_ok=True)
    stop_file = os.path.join(out, "STOP")
    if os.path.exists(stop_file):
        log(f"refused: {stop_file} exists")
        return 3
    pl = Plan(a.state)
    line = a.line
    lines.set_big(a.mem)
    os.environ["FXBENCH_SERVER_FLAGS_BASE"] = a.server_flags
    os.environ["FXBENCH_SERVER_FLAGS"] = a.server_flags
    uri = lines.uri_of(line)
    os.environ.setdefault("NO_PROXY", "*")
    sizes = [int(x) for x in a.sizes.split(",")]
    res_path = os.path.join(out, "result.json")
    res = json.load(open(res_path)) if os.path.exists(res_path) and a.resume else {"line": line, "sizes": {}}
    if not a.resume:
        lines.down()
        lines.up(line)
        chk = lines.check_line(line)
        res["check_before"] = chk
        res["settings"] = {"limits": lines.LIMITS, "mem": a.mem, "server_flags": a.server_flags, "plan": pl.p,
                           "binaries_sha256": lines.bin_sha() if target_of(line) == "fenvix" else None,
                           "server_command": lines.sh(["docker", "inspect", "-f", "{{json .Config.Cmd}}", server_container(line)]).stdout.strip(),
                           "disk_free_gib_start": round(shutil.disk_usage("/").free / 2**30, 1)}
        if line == "mongo-single" or line == "mongo-rs3":
            res["settings"]["mongod"] = lines.mongo_settings(server_container(line))
        if target_of(line) == "postgres":
            res["settings"]["postgres"] = lines.pg_settings(lines.IP[server_container(line)])
        res["schema"] = schema(line, uri)
        json.dump(res, open(res_path, "w"), indent=1, default=str)
    sampler = lines.Sampler(line, os.path.join(out, "cpu_mem.jsonl"), min_free_gib=a.min_free_gib)
    sampler.client_pid = os.getpid()
    sampler.start()
    rc = 0
    try:
        for size in sizes:
            r = res["sizes"].setdefault(str(size), {})
            if "load" not in r:
                log(f"{line}: load size {size}")
                r["load"] = load(line, uri, a.state, size, a.workers, a.chunk, a.min_free_gib)
                log(f"{line}: load size {size} done: {r['load']}")
                r["health_after_load"] = lines.health(line)
                json.dump(res, open(res_path, "w"), indent=1, default=str)
                if not r["health_after_load"]["ok"]:
                    log(f"line not healthy after the load: {r['health_after_load']['not_healthy']}")
                    return 5
                if line == "fenvix-log":
                    q = (r["health_after_load"]["check"].get("quaderno") or {})
                    if q.get("failed") != 0:
                        log(f"the 3-zone log refused batches during the load (S3-WAL fallback acknowledged them): {q}")
                        return 5
            if "settle" not in r:
                r["settle"] = settle(line, uri)
                r["stored"] = stored_sizes(line, uri)
                r["count"] = count_all(line, uri) if a.count else None
                r["verify_sample"] = verify_sample(line, uri, pl, size, a.verify, a.seed)
                if r["verify_sample"]["wrong"]:
                    open(stop_file, "a").write(f"lost or wrong documents after load size {size}: {r['verify_sample']['wrong'][:5]}\n")
                    log(f"STOP: {r['verify_sample']['wrong'][:5]}")
                    json.dump(res, open(res_path, "w"), indent=1, default=str)
                    return 3
                log(f"{line}: size {size} stored {r['stored']}, settle {json.dumps(r['settle'], default=str)[:300]}")
                json.dump(res, open(res_path, "w"), indent=1, default=str)
            rounds = ["A", "B"] if target_of(line) == "fenvix" else ["A"]
            if target_of(line) == "fenvix" and a.b2:
                rounds += ["B2off"]
            for rn in rounds:
                if rn in r.get("rounds", {}):
                    continue
                if target_of(line) == "fenvix":
                    lines.set_delay(0 if rn == "A" else a.delay_ms)
                    if rn == "B2off":
                        recreate_cook(OFF_FLAGS)
                s = measure(line, uri, pl, size, rn, out, a.seed, stop_file)
                if rn == "B2off":
                    recreate_cook("")
                r.setdefault("rounds", {})[rn] = {k: v for k, v in s.items() if k != "wrong"} | {"wrong": s["wrong"]}
                r["health_after_" + rn] = lines.health(line)
                json.dump(res, open(res_path, "w"), indent=1, default=str)
                if s["wrong_reads"]:
                    return 3
            if target_of(line) == "fenvix" and a.b2_saves and "saves" not in r:
                lines.set_delay(a.delay_ms)
                base = 900_000_000 + size * 1_000_000_00
                r["saves"] = {}
                r["saves"]["on"] = saves(uri, out, f"{size}-on", a.save_secs, a.save_rate, a.save_bg, a.seed, base)
                recreate_cook(OFF_FLAGS)
                r["saves"]["off"] = saves(uri, out, f"{size}-off", a.save_secs, a.save_rate, a.save_bg, a.seed, base + 50_000_000)
                recreate_cook("")
                json.dump(res, open(res_path, "w"), indent=1, default=str)
                if r["saves"]["on"]["lost"] or r["saves"]["off"]["lost"]:
                    open(stop_file, "a").write(f"saves lost at size {size}: {r['saves']}\n")
                    return 3
                # the saves left compaction work: settle again before the next size
                r["settle_after_saves"] = settle(line, uri)
                json.dump(res, open(res_path, "w"), indent=1, default=str)
            if sampler.disk_low.is_set():
                log("STOP: disk low")
                rc = 6
                break
    finally:
        sampler.stop.set()
        sampler.join()
        res["cpu_mem"] = lines.summarize_samples(os.path.join(out, "cpu_mem.jsonl"))
        res["members_end"] = lines.members_state(line)
        res["disk_free_gib_end"] = round(shutil.disk_usage("/").free / 2**30, 1)
        json.dump(res, open(res_path, "w"), indent=1, default=str)
        import gzip
        for n in lines.all_containers(line):
            txt = lines.docker_logs(n)
            if txt:
                with gzip.open(os.path.join(out, "logs", f"{n}.log.gz"), "wt") as fz:
                    fz.write(txt)
        if a.down:
            lines.down()
    return rc


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--state", required=True)
    p.add_argument("--sizes-gb", default="5,10,15")
    p.add_argument("--per-class", type=int, default=200)
    p.add_argument("--seed", type=int, default=11)
    p.add_argument("--fillers", default="100000,200000,300000")
    r = sub.add_parser("run")
    r.add_argument("--state", required=True)
    r.add_argument("--line", required=True, choices=sorted(lines.LINES))
    r.add_argument("--out", required=True)
    r.add_argument("--sizes", default="1,2,3")
    r.add_argument("--mem", default="512m")
    r.add_argument("--server-flags", default="")
    r.add_argument("--workers", type=int, default=3)
    r.add_argument("--chunk", type=int, default=500)
    r.add_argument("--min-free-gib", type=float, default=2.5)
    r.add_argument("--delay-ms", type=int, default=20)
    r.add_argument("--verify", type=int, default=300)
    r.add_argument("--count", action="store_true")
    r.add_argument("--b2", action="store_true")
    r.add_argument("--b2-saves", action="store_true")
    r.add_argument("--save-secs", type=int, default=120)
    r.add_argument("--save-rate", type=float, default=50)
    r.add_argument("--save-bg", type=float, default=100)
    r.add_argument("--seed", type=int, default=11)
    r.add_argument("--resume", action="store_true")
    r.add_argument("--down", action="store_true")
    a = ap.parse_args()
    if a.cmd == "plan":
        pl = make_plan(a.state, [float(x) for x in a.sizes_gb.split(",")], a.per_class, a.seed, [int(x) for x in a.fillers.split(",")])
        print(json.dumps({k: v for k, v in pl.items() if not k.endswith("_ids")}, indent=1))
        return
    sys.exit(run(a))


if __name__ == "__main__":
    main()
