#!/usr/bin/env python3
"""Copied from fenvix-engine `first-course-engine` bench/first-course/stress.py for compaction 2 step 2 (layout v2). Changes:
the first-course counters are optional (this branch has no first course, so both forms of every query take the normal
path), and `--ref-uri` repeats every write on a reference mongod and also compares every answer with mongod's
(sort-key sequence equal; ids equal except among documents tied on the sort key at the limit's edge).

First course: exactness under hostile writes (opening.rs). Every first query of a visit that the server may answer
from an opening is compared with the same query run again at once with the customer equality written as
`{$in: [user]}` (same answer; the first course never answers that form, so it takes the normal path; nothing is written
in between).

Between visits: inserts, text updates, timestamp updates (the entry moves inside the user's index range), deletes,
documents moved to another user (customer field changed), documents whose customer field is an array, a few
inserts with the same timestamp; queries of several shapes (newest N with N from 1 to 30, a page below a timestamp,
an equality plus a range on a second index, a filter on a field outside every index, ascending order, no sort).
The server runs with tiny openings (`--first-course-max-kb 16`) so trims happen all the time.

Usage: stress.py --uri URI --rounds 60 --users 40 [--expect-wrong]
  --expect-wrong: the negative control (server started with FENVIX_FIRST_COURSE_NEGATIVE_CONTROL=1) must find
  wrong answers, otherwise the test cannot see them.
"""
import argparse
import datetime as dt
import random
import sys
import time

from pymongo import MongoClient

DB, COLL = "fcstress", "m"


def fc(admin):
    return admin.command("serverStatus").get("fenvix", {}).get("firstCourse", {})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--rounds", type=int, default=60)
    ap.add_argument("--users", type=int, default=40)
    ap.add_argument("--idle", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=5)
    ap.add_argument("--expect-wrong", action="store_true")
    ap.add_argument("--ref-uri", default=None)
    # negative control of the mongod comparison: every Nth write is not repeated on mongod; differences must be found
    ap.add_argument("--skip-ref-every", type=int, default=0)
    a = ap.parse_args()
    r = random.Random(a.seed)
    c = MongoClient(a.uri)
    c.drop_database(DB)
    db = c[DB]
    db.create_collection(COLL, customerKey="u")
    ours = db[COLL]
    cols = [ours]
    if a.ref_uri:
        rc = MongoClient(a.ref_uri)
        rc.drop_database(DB)
        cols.append(rc[DB][COLL])
    for col in cols:
        col.create_index([("u", 1), ("ts", -1)])
        col.create_index([("u", 1), ("k", 1)])
        col.create_index([("k", 1)])

    class Both:
        """Writes go to both servers; reads to ours."""
        def __getattr__(self, name):
            f = getattr(ours, name)
            if name in ("find", "find_one"):
                return f
            def both(*args, **kw):
                out = f(*args, **kw)
                both.n = getattr(both, "n", 0) + 1
                Both.n = getattr(Both, "n", 0) + 1
                if a.skip_ref_every and Both.n % a.skip_ref_every == 0:
                    return out
                for other in cols[1:]:
                    getattr(other, name)(*args, **kw)
                return out
            return both
    m = Both()
    t0 = dt.datetime(2026, 1, 1)
    nid = [0]

    def new_doc(u, ts=None):
        nid[0] += 1
        return {"_id": nid[0], "u": u, "ts": ts or (t0 + dt.timedelta(seconds=nid[0])), "k": r.randint(0, 50), "x": r.randint(0, 9), "pad": "p" * r.randint(10, 300)}

    m.insert_many([new_doc(r.randrange(a.users)) for _ in range(3000)])
    users = list(range(a.users))

    def query(u, ref=False, m=ours):
        # the reference read uses {$in: [u]}: same answer, and the first course never answers it (only an equality
        # on the customer field starts a session), so it always takes the normal path
        uf = {"$in": [u]} if ref else u
        shape = r.randrange(7)
        query.shape = shape
        if shape == 0:
            return list(m.find({"u": uf}).sort("ts", -1).limit(r.randint(1, 30)))
        if shape == 1:
            cut = t0 + dt.timedelta(seconds=r.randint(0, nid[0]))
            return list(m.find({"u": uf, "ts": {"$lt": cut}}).sort("ts", -1).limit(20))
        if shape == 2:
            return list(m.find({"u": uf, "k": {"$gte": r.randint(0, 50)}}).sort("k", 1).limit(15))
        if shape == 3:
            return list(m.find({"u": uf, "x": r.randint(0, 9)}).sort("ts", -1).limit(10))
        if shape == 4:
            return list(m.find({"u": uf}).sort("ts", 1).limit(r.randint(1, 25)))
        if shape == 5:
            return sorted(m.find({"u": uf}), key=lambda d: d["_id"])
        return list(m.find({"u": {"$eq": u} if not ref else uf}).sort([("ts", -1), ("_id", 1)]).limit(12))

    checks = wrong = 0
    wrong_ref = [0]
    sort_key = {0: ("ts",), 1: ("ts",), 2: ("k",), 3: ("ts",), 4: ("ts",), 5: ("_id",), 6: ("ts", "_id")}

    def same_as_mongod(shape, a_, b_):
        key = lambda d: tuple(str(d.get(f)) for f in sort_key[shape])
        if [key(d) for d in a_] != [key(d) for d in b_]:
            return False
        if not a_:
            return True
        edge = key(a_[-1])
        ids = lambda xs: sorted(str(d["_id"]) for d in xs if key(d) != edge)
        return ids(a_) == ids(b_)
    before = fc(c.admin)
    for rnd in range(a.rounds):
        # writes between visits
        for _ in range(r.randint(20, 120)):
            op = r.randrange(9)
            u = r.choice(users)
            if op <= 2:
                m.insert_one(new_doc(u))
            elif op == 3:
                d = m.find_one({"u": u}, sort=[("ts", -1)])
                if d:
                    m.update_one({"_id": d["_id"]}, {"$set": {"pad": "q" * r.randint(10, 300), "x": r.randint(0, 9)}})
            elif op == 4:
                d = m.find_one({"u": u}, sort=[("ts", -1)])
                if d:
                    m.update_one({"_id": d["_id"]}, {"$set": {"ts": t0 + dt.timedelta(seconds=r.randint(0, nid[0]))}})
            elif op == 5:
                d = m.find_one({"u": u}, sort=[("ts", -1)])
                if d:
                    m.delete_one({"_id": d["_id"]})
            elif op == 6:
                d = m.find_one({"u": u}, sort=[("ts", -1)])
                if d:
                    m.update_one({"_id": d["_id"]}, {"$set": {"u": r.choice(users), "k": r.randint(0, 50)}})
            elif op == 7:
                # an array as customer value: the document lives outside both users' ranges (only users 0-2, so the
                # other users' openings can be kept)
                m.insert_one(new_doc([u % 3, (u + 1) % 3]))
            else:
                ts = t0 + dt.timedelta(seconds=r.randint(0, nid[0]))
                m.insert_many([new_doc(u, ts) for _ in range(3)])
        time.sleep(a.idle + 0.2)
        # a visit of some users: first query (maybe from the opening), then the same at once (normal path)
        for u in r.sample(users, k=min(len(users), r.randint(5, 25))):
          for _ in range(3):  # the first three queries of the visit (the variant answers all three)
            st = r.getstate()
            got = query(u)
            r.setstate(st)
            r2 = r.getstate()
            ref = query(u, ref=True)
            if len(cols) > 1:
                r.setstate(st)
                mongod = query(u, m=cols[1])
                r.setstate(r2)
                if not same_as_mongod(query.shape, got, mongod):
                    wrong_ref[0] += 1
                    if wrong_ref[0] <= 3:
                        print(f"DIFFERS FROM MONGOD round {rnd} user {u} shape {query.shape}: ours {[d['_id'] for d in got]} mongod {[d['_id'] for d in mongod]}", flush=True)
            checks += 1
            if got != ref:
                wrong += 1
                if not a.expect_wrong and wrong <= 3:
                    print(f"WRONG round {rnd} user {u}: got {[d['_id'] for d in got]} normal {[d['_id'] for d in ref]}", flush=True)
    after = fc(c.admin)
    d = {k: after[k] - before[k] for k in after if isinstance(after[k], int)}
    if not d and a.skip_ref_every:
        ok = wrong_ref[0] > 0
        print(f"checks {checks}, differing from mongod {wrong_ref[0]}", flush=True)
        print("NEGATIVE CONTROL " + ("PASS: differences found" if ok else "FAIL: none found"), flush=True)
        sys.exit(0 if ok else 1)
    if not d:
        ok = wrong == 0 and wrong_ref[0] == 0 and checks > 0
        print(f"checks {checks}, wrong {wrong}, differing from mongod {wrong_ref[0] if len(cols) > 1 else 'not compared'} (no first course on this server)", flush=True)
        print("STRESS " + ("PASS" if ok else "FAIL"), flush=True)
        sys.exit(0 if ok else 1)
    print(f"checks {checks}, wrong {wrong}; served queries {d['servedQueries']}, partial {d['partialQueries']}, missed {d['missedQueries']}, "
          f"no opening {d['noOpening']}, kept {d['kept']}, unkeepable {d['unkeepable']}, trimmed {d['trimmed']}, dropped {d['droppedTooBig']}, "
          f"writes applied {d['writesApplied']}, forgotten {d['forgotten']}; reads served from openings {d['servedReads']}, missed {d['missedReads']}", flush=True)
    if a.expect_wrong:
        ok = wrong > 0
        print("NEGATIVE CONTROL " + ("PASS: wrong answers found" if ok else "FAIL: no wrong answer found"), flush=True)
    else:
        ok = wrong == 0 and d["servedQueries"] + d["partialQueries"] > checks // 10
        print("STRESS " + ("PASS" if ok else "FAIL"), flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
