"""Compaction 2, step 8: a database written by today's engine moves to compaction 2 while it serves writes, through 50
takeovers by new processes (kill -9, then a server with another holder takes the database over), with 0 acknowledged
writes missing; then it is rolled back to release E0 (sealed runs off: the first open rolls it back), migrated again,
committed, and a release without sealed runs refuses to open it.

The writer records only acknowledged writes. A write whose answer was lost in a kill is uncertain: its document may hold
the old or the new value (or be absent for an insert), never anything else. Every acknowledged write must be read back
exactly, after every phase.
"""
import os
import random
import threading
import time

import pytest
from pymongo.errors import PyMongoError

from lib import Server, make_bucket, list_prefix, wait_until

TAKEOVERS = int(os.environ.get("C2S8_TAKEOVERS", "50"))
E1 = ["--sealed-runs", "0", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
      "--sealed-group-mb", "1", "--sealed-migration-poll-ms", "200", "--sealed-gc-grace-ms", "150000", "--lease-ttl-secs", "3"]
E1_ENV = {"FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_TEST_MIGRATION_GROUPS": "1", "FENVIX_SEALED_TEST_MIGRATION_DELAY_MS": "700"}
E0 = ["--lease-ttl-secs", "3"]
DB = "mig"


class Writer:
    """Inserts and updates in DB.c, recording what was acknowledged."""

    def __init__(self, uri_fn):
        self.uri_fn = uri_fn
        self.acked = {}        # _id -> v acknowledged last
        self.uncertain = {}    # _id -> set of values a lost answer may have left (None: absent)
        self.stop = False
        self.n = 0
        self.errors = 0
        self.lock = threading.Lock()
        self.t = threading.Thread(target=self.run, daemon=True)

    def run(self):
        from pymongo import MongoClient
        rng = random.Random(8)
        c = None
        while not self.stop:
            try:
                if c is None:
                    c = MongoClient(self.uri_fn(), serverSelectionTimeoutMS=3000, socketTimeoutMS=8000, retryWrites=False)
                coll = c[DB]["c"]
                with self.lock:
                    ids = list(self.acked)
                if ids and rng.random() < 0.5:
                    i = rng.choice(ids)
                    v = self.n
                    try:
                        coll.update_one({"_id": i}, {"$set": {"v": v}})
                    except PyMongoError:
                        with self.lock:
                            self.uncertain.setdefault(i, set()).add(v)
                        raise
                    with self.lock:
                        self.acked[i] = v
                        self.uncertain.pop(i, None)
                else:
                    i = f"w{self.n}"
                    try:
                        coll.insert_one({"_id": i, "v": self.n, "pad": "p" * rng.randint(10, 300)})
                    except PyMongoError:
                        with self.lock:
                            self.uncertain.setdefault(i, set()).update({None, self.n})
                        raise
                    with self.lock:
                        self.acked[i] = self.n
                self.n += 1
            except PyMongoError:
                self.errors += 1
                try:
                    if c is not None:
                        c.close()
                except Exception:
                    pass
                c = None
                time.sleep(0.2)

    def check(self, client, what):
        with self.lock:
            acked, unc = dict(self.acked), {k: set(v) for k, v in self.uncertain.items()}
        got = {d["_id"]: d.get("v") for d in client[DB]["c"].find({}, {"v": 1})}
        missing = [i for i in acked if i not in got and None not in unc.get(i, set())]
        wrong = [i for i, v in acked.items() if i in got and got[i] != v and got[i] not in unc.get(i, set())]
        assert not missing and not wrong, f"{what}: {len(missing)} acknowledged documents missing, {len(wrong)} with a wrong value (of {len(acked)}); e.g. {missing[:3]} {wrong[:3]}"
        return len(acked)


def migration(srv):
    st, r = srv.admin.get(f"/v1/databases/{DB}/migration")
    assert st == 200, r
    return r["migration"]


def preload(c, n_docs=40000):
    """Today's engine: inserts, updates and deletes over about 20 MB."""
    rng = random.Random(1)
    coll = c[DB]["c"]
    for b in range(0, n_docs, 2000):
        coll.insert_many([{"_id": f"p{i}", "v": i, "pad": "x" * rng.randint(200, 800)} for i in range(b, b + 2000)])
    coll.update_many({"v": {"$lt": 5000}}, {"$set": {"pad": "updated"}})
    coll.delete_many({"v": {"$gte": 35000, "$lt": 36000}})
    c[DB]["other"].insert_many([{"_id": i, "k": i % 7} for i in range(3000)])
    c[DB]["other"].create_index("k")
    return {d["_id"]: d["v"] for d in coll.find({}, {"v": 1})}


def test_migration_with_takeovers_rollback_and_commit():
    bucket = make_bucket("c2s8")
    gen = [0]

    def server(extra, env, tag):
        gen[0] += 1
        s = Server(bucket, f"c2s8{tag}{gen[0]}", idle=600, extra=extra, env=env, holder=f"h{gen[0]}")
        s.start()
        return s

    # 1. today's engine (sealed runs off)
    s = server(E0, {}, "e0")
    c = s.client()
    pre = preload(c)
    c.close()
    s.term()

    # 2. release E1 with sealed runs: the migration begins and the writer runs; 50 takeovers
    cur = {"srv": server(E1, E1_ENV, "e1")}
    w = Writer(lambda: cur["srv"].uri)
    for k, v in pre.items():
        w.acked[k] = v
    w.t.start()
    st, r = cur["srv"].admin.post(f"/v1/databases/{DB}/migration", {})
    assert st == 200, r
    phases = []
    rng = random.Random(51)
    for i in range(TAKEOVERS):
        time.sleep(rng.uniform(0.3, 1.5))
        try:
            phases.append(migration(cur["srv"])["phase"])
        except Exception:
            phases.append("unknown")
        cur["srv"].kill9()
        cur["srv"] = server(E1, E1_ENV, "e1")  # another process, another holder: a takeover
    print(f"takeovers: {len(phases)}; phase at the kill: " + ", ".join(f"{p} {phases.count(p)}" for p in sorted(set(phases))))
    # the migration finishes on the last process
    m = wait_until(lambda: (x := migration(cur["srv"])) and x["phase"] == "published" and x, timeout=300, every=1, what="the migration's publish")
    time.sleep(2)
    w.stop = True
    w.t.join()
    c = cur["srv"].client()
    n = w.check(c, "after the migration and 50 takeovers")
    st, status = cur["srv"].admin.get("/v1/health")
    fx = c.admin.command("serverStatus")["fenvix"]
    print(f"published version {m['version']} after {sum(1 for p in phases if p == 'building')} takeovers while building; "
          f"{n} acknowledged documents all read back; writer errors {w.errors}; migration counters {fx.get('sealedRuns', {}).get('migration')}")
    assert phases.count("building") >= min(10, TAKEOVERS), f"too few takeovers landed while building: {phases}"
    assert c[DB]["other"].count_documents({"k": 3}) == len([i for i in range(3000) if i % 7 == 3])
    c.close()

    # 3. rollback: the operator deploys E0 (sealed runs off); its first open rolls the migration back
    cur["srv"].term()
    s0 = server(E0, {}, "e0")
    c = s0.client()
    w.check(c, "on E0 after the rollback")
    c[DB]["c"].insert_one({"_id": "e0-write", "v": -1})
    w.acked["e0-write"] = -1
    c.close()
    s0.term()

    # 4. E1 again: a second generation, published, then committed
    s1 = server(E1, E1_ENV, "e1")
    m = migration(s1)
    assert m["phase"] == "rolled_back", m
    st, r = s1.admin.post(f"/v1/databases/{DB}/migration", {})
    assert st == 200, r
    st, r = s1.admin.post(f"/v1/databases/{DB}/migration/run", {"budget_ms": 300000, "groups": 50})
    assert st == 200 and r["migration"]["phase"] == "published" and r["migration"]["generation"] == 2, r
    c = s1.client()
    w.check(c, "generation 2 published")
    st, r = s1.admin.post(f"/v1/databases/{DB}/migration/commit", {})
    assert st == 409, f"a commit before the window ends needs force: {st} {r}"
    st, r = s1.admin.post(f"/v1/databases/{DB}/migration/commit", {"force": True})
    assert st == 200 and r["migration"]["phase"] == "committed", r
    c[DB]["c"].insert_one({"_id": "after-commit", "v": -2})
    w.acked["after-commit"] = -2
    w.check(c, "committed")
    st, r = s1.admin.post(f"/v1/databases/{DB}/migration/rollback", {})
    assert st == 409 and r["error"]["code"] == "rollback_refused", (st, r)
    c.close()
    s1.term()

    # 5. a release without sealed runs refuses the committed database
    s0 = server(E0, {}, "e0")
    c = s0.client()
    with pytest.raises(PyMongoError) as e:
        list(c[DB]["c"].find({}).limit(1))
    assert "committed" in str(e.value), e.value
    c.close()
    s0.term()
    s1 = server(E1, E1_ENV, "e1")
    c = s1.client()
    w.check(c, "E1 after the refusal")
    c.close()
    s1.term()
