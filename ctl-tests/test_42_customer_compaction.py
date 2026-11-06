"""Per-customer compaction, step 1 (Owner · Compaction, task compaction-1): customer-first keys and the tiered compaction.

Every server here runs with `--customer-key cc.m=u` (collection cc.m stores its documents customer first), 1 MiB
memtables (so L0 SSTs come every MB and the compaction passes run all the time, with the tiered scheduler, T = 4, and the
separate worker that writes 64 KiB lz4 blocks), and a fast SST cleanup.

test_index_matches_documents_during_compaction (contract point 4): writers insert, update, move documents between
customers and delete them, while readers read each customer through two indexes and by `_id`, and compare with an exact
model (one lock per customer, so a read sees no write of its customer in flight). Fails on any difference, if fewer than
3 compaction passes ran during the reads, or if the compacted SSTs are not lz4.

test_sigkill_mid_compaction (contract point 3): inserts while compaction runs, SIGKILL while a pass is running, restart:
every acknowledged insert must be there (by `_id` through the locator, by the customer index, by a collection scan), and
every SST left in the bucket by the killed pass that is not in the manifest must be deleted by the cleanup (no orphan
left forever, rule 24 of docs/tla-lavaggio/REGOLE-PER-IL-CODICE.md).
"""
import random
import threading
import time

from pymongo import ASCENDING

from lib import Server, make_bucket, list_prefix, wait_until

FLAGS = ["--customer-key", "cc.m=u", "--memtable-max-mb", "1", "--sst-gc-secs", "2", "--sst-gc-min-unref-secs", "8", "--writer-poll-secs", "1"]
CUSTOMERS = 40
PAD = "p" * 700


def passes(c):
    return c.admin.command("serverStatus")["fenvix"]["compaction"]["passes"]


def running(c):
    return c.admin.command("serverStatus")["fenvix"]["compaction"]["running"]


def sst_ids(c, db):
    info = c.admin.command("fenvixSstInfo", db)
    ssts = list(info["l0"]) + [s for r in info["runs"] for s in r["ssts"]]
    return info, {s["id"] for s in ssts}


def test_index_matches_documents_during_compaction():
    bucket = make_bucket("custcomp")
    srv = Server(bucket, "custcomp", idle=600, extra=FLAGS).start()
    try:
        c = srv.client()
        m = c.cc.m
        m.create_index([("u", ASCENDING), ("k", ASCENDING)])
        m.create_index([("k", ASCENDING)])
        model = {u: {} for u in range(CUSTOMERS)}
        locks = [threading.Lock() for _ in range(CUSTOMERS)]
        errors, stats = [], {"reads": 0, "writes": 0, "moves": 0}
        next_id = [0]
        idlock = threading.Lock()
        stop = threading.Event()

        def new_id():
            with idlock:
                next_id[0] += 1
                return next_id[0]

        def writer(seed):
            r = random.Random(seed)
            cl = srv.client()
            col = cl.cc.m
            while not stop.is_set() and not errors:
                u = r.randrange(CUSTOMERS)
                op = r.random()
                try:
                    if op < 0.55 or not model[u]:
                        with locks[u]:
                            docs = [{"_id": new_id(), "u": u, "k": r.randrange(1000), "pad": PAD} for _ in range(r.randint(1, 20))]
                            col.insert_many(docs)
                            for d in docs:
                                model[u][d["_id"]] = d
                    elif op < 0.75:
                        with locks[u]:
                            i = r.choice(list(model[u]))
                            k = r.randrange(1000)
                            col.update_one({"_id": i}, {"$set": {"k": k}})
                            model[u][i] = dict(model[u][i], k=k)
                    elif op < 0.9:
                        v = r.randrange(CUSTOMERS)
                        if v == u:
                            continue
                        a, b = sorted((u, v))
                        with locks[a], locks[b]:
                            if not model[u]:
                                continue
                            i = r.choice(list(model[u]))
                            col.update_one({"_id": i}, {"$set": {"u": v}})
                            d = model[u].pop(i)
                            model[v][i] = dict(d, u=v)
                            stats["moves"] += 1
                    else:
                        with locks[u]:
                            i = r.choice(list(model[u]))
                            col.delete_one({"_id": i})
                            del model[u][i]
                    stats["writes"] += 1
                except Exception as e:
                    errors.append(f"write: {e!r}"[:300])

        def check(col, u, r):
            want = model[u]
            got = {d["_id"]: d for d in col.find({"u": u}).hint([("u", 1), ("k", 1)])}
            if got != want:
                errors.append(f"customer {u} through {{u,k}}: {len(got)} documents, model {len(want)}; "
                              f"missing {sorted(set(want) - set(got))[:5]}, extra {sorted(set(got) - set(want))[:5]}")
                return
            lo = r.randrange(1000)
            got = {d["_id"] for d in col.find({"u": u, "k": {"$gte": lo}}).hint([("k", 1)])}
            exp = {i for i, d in want.items() if d["k"] >= lo}
            if got != exp:
                errors.append(f"customer {u} through {{k}} >= {lo}: {len(got)} documents, model {len(exp)}")
                return
            for i in r.sample(list(want), min(3, len(want))):
                d = col.find_one({"_id": i})
                if d != want[i]:
                    errors.append(f"_id {i} of customer {u}: {d} against {want[i]}")
                    return
            # index entries never point at a document of another customer
            for d in col.find({"u": u}, {"u": 1}).hint([("u", 1), ("k", 1)]):
                if d["u"] != u:
                    errors.append(f"index entry of customer {u} gives a document of customer {d['u']}")
                    return

        def reader(seed):
            r = random.Random(seed)
            cl = srv.client()
            col = cl.cc.m
            while not stop.is_set() and not errors:
                u = r.randrange(CUSTOMERS)
                with locks[u]:
                    try:
                        check(col, u, r)
                    except Exception as e:
                        errors.append(f"read: {e!r}"[:300])
                stats["reads"] += 1

        p0 = passes(c)
        ts = [threading.Thread(target=writer, args=(s,)) for s in range(4)] + [threading.Thread(target=reader, args=(100 + s,)) for s in range(2)]
        for t in ts:
            t.start()
        t0 = time.time()
        while time.time() - t0 < 75 and not errors:
            time.sleep(1)
        stop.set()
        for t in ts:
            t.join(120)
        during = passes(c) - p0
        assert not errors, errors[:5]
        assert during >= 3, f"only {during} compaction passes ran during the reads (reads {stats['reads']}, writes {stats['writes']})"
        assert stats["moves"] > 0 and stats["reads"] > 50, stats
        # after the compaction settles: every customer once more, and the compacted output is lz4
        wait_until(lambda: running(c) == 0, timeout=120, what="compaction settles")
        r = random.Random(1)
        for u in range(CUSTOMERS):
            check(c.cc.m, u, r)
        assert not errors, errors[:5]
        info, _ = sst_ids(c, "cc")
        run_ssts = [s for run in info["runs"] for s in run["ssts"]]
        assert run_ssts, info
        assert all(s["codec"] == "lz4" for s in run_ssts), [s["codec"] for s in run_ssts]
        assert all(s["codec"] == "lz4" for s in info["l0"]), [s["codec"] for s in info["l0"]]
        print(f"reads {stats['reads']}, writes {stats['writes']}, moves {stats['moves']}, compaction passes during the reads {during}, "
              f"runs now {len(info['runs'])}, documents {sum(len(v) for v in model.values())}")
    finally:
        srv.term()


def test_sigkill_mid_compaction():
    bucket = make_bucket("custkill")
    srv = Server(bucket, "custkill", idle=600, extra=FLAGS).start()
    acked = {}
    try:
        c = srv.client()
        c.cc.m.create_index([("u", ASCENDING), ("k", ASCENDING)])
        r = random.Random(5)
        next_id, killed_in_pass = 0, False
        t0 = time.time()
        # write until a pass is running (and at least 6 MB are in), then SIGKILL right away
        while time.time() - t0 < 180:
            docs = []
            for _ in range(200):
                next_id += 1
                docs.append({"_id": next_id, "u": r.randrange(CUSTOMERS), "k": r.randrange(1000), "pad": PAD})
            c.cc.m.insert_many(docs)
            for d in docs:
                acked[d["_id"]] = d
            if next_id > 8000 and running(c) > 0:
                # the moment that matters: an SST is in the bucket and not in the manifest (a pass's output not yet
                # published, or a flush)
                for _ in range(200):
                    _, live = sst_ids(c, "cc")
                    names = {k.rsplit("/", 1)[1][:-4] for k, _ in list_prefix(bucket, "tenants/cc/compacted/") if k.endswith(".sst")}
                    if names - live:
                        break
                    if running(c) == 0:
                        break
                if names - live:
                    srv.kill9()
                    killed_in_pass = True
                    break
        assert killed_in_pass, "never saw an SST outside the manifest while a pass was running"
        left = {k for k, _ in list_prefix(bucket, "tenants/cc/compacted/") if k.endswith(".sst")}
        srv.start()
        c = srv.client()
        col = c.cc.m
        # every acknowledged insert is there: by _id (the locator), through the customer index, and by a collection scan
        ids = sorted(acked)
        found = set()
        for i in range(0, len(ids), 500):
            found |= {d["_id"] for d in col.find({"_id": {"$in": ids[i:i + 500]}}, {"_id": 1})}
        assert found == set(ids), f"lost {len(set(ids) - found)} acknowledged inserts, e.g. {sorted(set(ids) - found)[:5]}"
        for u in range(CUSTOMERS):
            want = {i for i, d in acked.items() if d["u"] == u}
            via_ix = {d["_id"] for d in col.find({"u": u}, {"_id": 1}).hint([("u", 1), ("k", 1)])}
            via_scan = {d["_id"] for d in col.find({"u": u}, {"_id": 1}).hint({"$natural": 1})}
            assert want <= via_ix and via_ix == via_scan, f"customer {u}: acked {len(want)}, index {len(via_ix)}, scan {len(via_scan)}"
        for i in r.sample(ids, 50):
            assert col.find_one({"_id": i}) == acked[i]
        _, live0 = sst_ids(c, "cc")
        orphans_at_restart = {k.rsplit("/", 1)[1][:-4] for k in left} - live0
        # a write opens the writer again (its compaction passes and SST cleanup run while it is open)
        col.insert_one({"_id": -1, "u": 0, "k": 0, "pad": PAD})
        # no orphan forever: what the killed pass left outside the manifest is deleted by the cleanup
        def no_orphans():
            if running(c) > 0:
                return False
            _, live = sst_ids(c, "cc")
            now = {k for k, _ in list_prefix(bucket, "tenants/cc/compacted/") if k.endswith(".sst")}
            names = {k.rsplit("/", 1)[1][:-4] for k in now}
            orphans_now = {n for n in names if n not in live}
            left_orphans = {k.rsplit("/", 1)[1][:-4] for k in left} - live
            return not orphans_now and not (left_orphans & names)
        wait_until(no_orphans, timeout=150, every=2, what="the SSTs outside the manifest are deleted")
        print(f"acked {len(acked)} inserts, all there after SIGKILL in a pass; {len(left)} SSTs in the bucket at the kill, "
              f"{len(orphans_at_restart)} of them outside the manifest after the restart, all deleted since")
    finally:
        srv.term()


def test_reads_while_documents_move():
    """Reads by `_id` and through an index never miss a document whose customer changes meanwhile (the reader then finds
    it again through the locator). Found by the judge's open-GC kill test run with `--customer-key c=v` (every update
    moves the document): one `find_one` out of 61,640 returned nothing before the fix."""
    bucket = make_bucket("custmove")
    srv = Server(bucket, "custmove", idle=600, extra=FLAGS).start()
    try:
        c = srv.client()
        col = c.cc.m
        col.create_index([("k", ASCENDING)])
        n = 400
        col.insert_many([{"_id": i, "u": i % CUSTOMERS, "k": i, "pad": PAD} for i in range(n)])
        stop = threading.Event()
        viol = {"reads": 0, "missing": 0, "missing_ix": 0, "moves": 0}

        def mover(seed):
            r = random.Random(seed)
            cl = srv.client()
            while not stop.is_set():
                cl.cc.m.update_one({"_id": r.randrange(n)}, {"$set": {"u": r.randrange(CUSTOMERS)}})
                viol["moves"] += 1

        def reader(seed):
            r = random.Random(seed)
            cl = srv.client()
            while not stop.is_set():
                i = r.randrange(n)
                if cl.cc.m.find_one({"_id": i}) is None:
                    viol["missing"] += 1
                # through the {k} index: the entry gives the old place, the document moved
                if cl.cc.m.find_one({"k": i}, hint=[("k", 1)]) is None:
                    viol["missing_ix"] += 1
                viol["reads"] += 1

        ts = [threading.Thread(target=mover, args=(s,)) for s in range(4)] + [threading.Thread(target=reader, args=(50 + s,)) for s in range(4)]
        for t in ts:
            t.start()
        time.sleep(30)
        stop.set()
        for t in ts:
            t.join(60)
        print(viol)
        assert viol["reads"] > 1000 and viol["moves"] > 1000, viol
        assert viol["missing"] == 0 and viol["missing_ix"] == 0, viol
        assert col.count_documents({}) == n
    finally:
        srv.term()
