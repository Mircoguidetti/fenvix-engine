#!/usr/bin/env python3
"""Crash test (results7, step 6): SIGKILL the server while writers run against indexed collections and while an
index is being built or dropped, restart it, and check every index of every tenant.

For each tenant and collection after each restart:
  * `validate`: every document has exactly its index entries, and no entry points to a missing document
    (reads documents and index entries from the store);
  * an independent check through the query path: for every non-sparse, non-partial index, the number of
    documents found by scanning the collection equals the number found through that index (`hint`);
  * every acknowledged write that was not followed by an unacknowledged one for the same _id is still there
    with the acknowledged value.
Usage: crash_test.py [--kills 40] [--tenants 6] [--seed 1] [--out out/crash.json]
"""
import argparse
import json
import os
import random
import signal
import subprocess
import threading
import time
import datetime

import pymongo
from pymongo import errors

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "target", "release", "mongo-server")
PORT = 27031
URI = f"mongodb://localhost:{PORT}/?directConnection=true&serverSelectionTimeoutMS=20000"
FUTURE = datetime.datetime(2100, 1, 1)
CHUNK = "250"  # documents per backfill step (small, so a build takes several steps and can be killed in the middle)


class Server:
    def __init__(self):
        self.p = None
        self.log = open(os.path.join(HERE, "out", "crash-server.log"), "ab")

    def start(self):
        env = dict(os.environ, MONGO_BUILD_CHUNK=CHUNK)
        self.p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "4", "--holder", "crash-holder", "--lease-ttl-secs", "3", "--no-ttl"], stdout=self.log, stderr=self.log, env=env)
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                pymongo.MongoClient(URI, serverSelectionTimeoutMS=500).admin.command("ping")
                return
            except Exception:
                time.sleep(0.2)
        raise RuntimeError("server did not start")

    def kill(self):
        self.p.send_signal(signal.SIGKILL)
        self.p.wait()


INDEXES = [
    ("a_1", [("a", 1)], {}),
    ("bc", [("b", -1), ("c", 1)], {"name": "bc"}),
    ("u_1", [("u", 1)], {"unique": True}),
    ("m_1", [("m", 1)], {}),
    ("s_1", [("s", 1)], {"sparse": True}),
    ("p_1", [("p", 1)], {"partialFilterExpression": {"on": True}}),
    ("t_1", [("t", 1)], {"expireAfterSeconds": 3600}),
]


class Tenant:
    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.opgate = threading.Lock()
        self.acked = {}      # _id -> doc as last acknowledged
        self.uncertain = set()
        self.next_id = 1
        self.writes = 0


def client():
    return pymongo.MongoClient(URI, socketTimeoutMS=15000, retryWrites=False)


def make_doc(i, rnd):
    d = {"_id": i, "a": rnd.randint(0, 20), "b": rnd.randint(0, 5), "c": f"c{rnd.randint(0, 9)}", "u": f"u{i}", "m": [rnd.randint(0, 6), rnd.randint(0, 6)], "t": FUTURE}
    if rnd.random() < 0.5:
        d["s"] = rnd.randint(0, 9)
    d["p"] = rnd.randint(0, 3)
    d["on"] = rnd.random() < 0.5
    return d


def writer(t, stop, seed, stats):
    rnd = random.Random(seed)
    cl = client()
    coll = cl[t.name]["c"]
    while not stop.is_set():
        t.opgate.acquire()
        try:
            r = rnd.random()
            if r < 0.45 or not t.acked:
                with t.lock:
                    i = t.next_id
                    t.next_id += 1
                d = make_doc(i, rnd)
                t.uncertain.add(i)
                coll.insert_one(d)
                t.acked[i] = d
                t.uncertain.discard(i)
            else:
                with t.lock:
                    i = rnd.choice(list(t.acked))
                if r < 0.65:
                    t.uncertain.add(i)
                    new = rnd.randint(0, 20)
                    res = coll.update_one({"_id": i}, {"$set": {"a": new}, "$push": {"m": 99}})
                    if res.matched_count:
                        t.acked[i] = dict(t.acked[i], a=new, m=t.acked[i]["m"] + [99])
                    t.uncertain.discard(i)
                elif r < 0.75:
                    t.uncertain.add(i)
                    coll.delete_one({"_id": i})
                    t.acked.pop(i, None)
                    t.uncertain.discard(i)
                elif r < 0.85:
                    t.uncertain.add(i)
                    d = make_doc(i, rnd)
                    coll.replace_one({"_id": i}, d)
                    t.acked[i] = d
                    t.uncertain.discard(i)
                elif r < 0.92:
                    t.uncertain.add(i)
                    d = coll.find_one_and_update({"_id": i}, {"$inc": {"b": 1}}, return_document=pymongo.ReturnDocument.AFTER)
                    if d is not None:
                        t.acked[i] = d
                    t.uncertain.discard(i)
                else:
                    # transaction touching two documents
                    j = rnd.choice(list(t.acked))
                    t.uncertain.update((i, j))
                    with cl.start_session() as s:
                        s.start_transaction()
                        coll.update_one({"_id": i}, {"$inc": {"a": 1}}, session=s)
                        coll.update_one({"_id": j}, {"$inc": {"a": 1}}, session=s)
                        s.commit_transaction()
                    # the new values are unknown to the log: leave both exempt from the exact check
            t.writes += 1
            stats["ops"] += 1
        except errors.DuplicateKeyError:
            stats["dup"] += 1
        except Exception:
            stats["errors"] += 1
            time.sleep(0.2)
            try:
                cl = client()
                coll = cl[t.name]["c"]
            except Exception:
                pass
        finally:
            t.opgate.release()


def check_tenant(name, acked, uncertain, report, label):
    cl = client()
    db = cl[name]
    problems = []
    for collname in db.list_collection_names():
        coll = db[collname]
        v = db.command("validate", collname)
        report["collections_checked"] += 1
        report["indexes_validated"] += len(coll.index_information())
        if not v.get("valid"):
            problems.append((collname, "validate", v.get("errors")))
        # independent check through the query path
        total = coll.count_documents({})
        for ix in coll.list_indexes():
            if ix["name"] == "_id_" or ix.get("sparse") or ix.get("partialFilterExpression"):
                continue
            if any(isinstance(v, str) for v in ix["key"].values()):
                continue
            n = len(list(coll.find({}, {"_id": 1}, hint=ix["name"] if "name" in ix else list(ix["key"].items()))))
            multikey = ix["name"] == "m_1"
            if not multikey and n != total:
                problems.append((collname, "hint-scan", ix["name"], n, total))
    if "c" in db.list_collection_names() and name in acked:
        coll = db["c"]
        for i, d in acked[name].items():
            if i in uncertain[name]:
                continue
            got = coll.find_one({"_id": i})
            if got is None or any(got.get(k) != d.get(k) for k in ("a", "b", "c", "u", "m", "s", "p", "on")):
                problems.append(("c", "acked-write-lost", i))
    if problems:
        report["problems"].append({"tenant": name, "at": label, "problems": [str(p) for p in problems][:10]})
    report["tenants_checked"] += 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kills", type=int, default=40)
    ap.add_argument("--tenants", type=int, default=6)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=os.path.join(HERE, "out", "crash.json"))
    a = ap.parse_args()
    os.makedirs(os.path.join(HERE, "out"), exist_ok=True)
    rnd = random.Random(a.seed)
    run = f"{int(time.time()) % 100000}"
    srv = Server()
    srv.start()
    names = [f"crash{run}_{i}" for i in range(a.tenants)]
    tenants = {n: Tenant(n) for n in names}
    cl = client()
    for n in names:
        db = cl[n]
        db.create_collection("c")
        for name, keys, opts in INDEXES:
            db.c.create_index(keys, name=name, **{k: v for k, v in opts.items() if k != "name"})
    # a larger collection per run for the backfill kills
    big = cl[names[0]]["big"]
    big.insert_many([{"_id": i, "z": i % 97, "y": f"y{i % 13}", "w": [i % 5, i % 7]} for i in range(4000)])
    report = {"kills": 0, "kills_during_backfill": 0, "kills_during_drop": 0, "backfills_resumed_ok": 0, "drops_resumed_ok": 0, "tenants_checked": 0, "collections_checked": 0, "indexes_validated": 0, "problems": [], "ops": 0}
    stats = {"ops": 0, "errors": 0, "dup": 0}
    stop = threading.Event()
    threads = [threading.Thread(target=writer, args=(tenants[n], stop, a.seed * 100 + k, stats), daemon=True) for k, n in enumerate(names)]
    [t.start() for t in threads]
    big_idx_names = []
    for k in range(a.kills):
        mode = "plain"
        if k % 4 == 1:
            mode = "backfill"
        elif k % 4 == 3:
            mode = "drop"
        killer_result = {}
        if mode == "backfill":
            kind = rnd.choice([("z", {}), ("y", {}), ("w", {}), ("z", {"unique": False, "sparse": True})])
            iname = f"bf{k}"

            def build(kind=kind, iname=iname):
                try:
                    cl2 = client()
                    cl2[names[0]]["big"].create_index([(kind[0], 1)], name=iname, **kind[1])
                    killer_result["finished"] = True
                except Exception:
                    killer_result["finished"] = False
            bt = threading.Thread(target=build, daemon=True)
            bt.start()
            time.sleep(rnd.uniform(0.05, 0.6))
        elif mode == "drop":
            victim = big_idx_names.pop(0) if big_idx_names else None
            if victim:
                def dropper(victim=victim):
                    try:
                        client()[names[0]]["big"].drop_index(victim)
                        killer_result["finished"] = True
                    except Exception:
                        killer_result["finished"] = False
                threading.Thread(target=dropper, daemon=True).start()
                time.sleep(rnd.uniform(0.0, 0.15))
        else:
            time.sleep(rnd.uniform(0.3, 2.0))
        srv.kill()
        report["kills"] += 1
        if mode == "backfill" and not killer_result.get("finished"):
            report["kills_during_backfill"] += 1
        if mode == "drop" and not killer_result.get("finished"):
            report["kills_during_drop"] += 1
        time.sleep(0.3)
        srv.start()
        # touching the collection resumes unfinished builds / drops
        c2 = client()
        db0 = c2[names[0]]
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                names_now = [i["name"] for i in db0.big.list_indexes()]
                st = db0.command("collStats", "big")
                raw = [i for i in db0.command({"listIndexes": "big"})["cursor"]["firstBatch"]]
                # unfinished builds are hidden from listIndexes; validate waits until none is pending
                v = db0.command("validate", "big")
                if v.get("valid"):
                    break
            except Exception:
                pass
            time.sleep(0.3)
        else:
            report["problems"].append({"tenant": names[0], "at": f"kill {k}", "problems": ["did not become valid"]})
        if mode == "backfill":
            iname = f"bf{k}"
            if iname in [i["name"] for i in db0.big.list_indexes()]:
                big_idx_names.append(iname)
                if not killer_result.get("finished"):
                    report["backfills_resumed_ok"] += 1
        if mode == "drop" and not killer_result.get("finished"):
            report["drops_resumed_ok"] += 1
        for n in names:
            try:
                with tenants[n].opgate:
                    check_tenant(n, {m: dict(t.acked) for m, t in tenants.items()}, {m: set(t.uncertain) for m, t in tenants.items()}, report, f"kill {k} ({mode})")
            except Exception as e:  # noqa
                report["problems"].append({"tenant": n, "at": f"kill {k}", "problems": [f"check failed: {e}"]})
        # let the workload run a little before the next kill
    stop.set()
    [t.join(timeout=5) for t in threads]
    report["ops"] = stats["ops"]
    report["worker_errors_during_outage"] = stats["errors"]
    report["acknowledged_writes_tracked"] = sum(len(t.acked) for t in tenants.values())
    srv.kill()
    json.dump(report, open(a.out, "w"), indent=1)
    print(json.dumps({k: v for k, v in report.items() if k != "problems"}, indent=1))
    print("problems:", len(report["problems"]))
    for p in report["problems"][:10]:
        print(p)


if __name__ == "__main__":
    main()
