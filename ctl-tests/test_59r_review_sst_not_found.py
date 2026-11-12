"""Reviewer's tests for fix-sst-not-found (a reader whose SSTs were compacted and collected answers 134 and is reopened).

Same setting as the owner's test_56: 1 MB memtables and caches, a burst of inserts, a close (L0 over the threshold, so a
compaction pass of the closed database is queued), one read that opens the reader, then 40 s for the pass and the
collectors. Then:

- test_write_path_through_a_stale_reader: the first command after the wait is a write (update_many {$inc: {c: 1}},
  retryable writes on): every document must end with c == 1 exactly (applied once, nothing skipped, nothing twice).
- test_repeated_cycles_never_short_and_always_end: 4 cycles of (burst, close, read, 40 s, reads): every answer exact
  (find, count, $group sums), every read ends (bounded tries), with the driver's own retryable reads (default).
- test_two_processes: P1 writes and closes (its maintenance compacts and collects the closed database); P2 opens the
  reader; P2's reads after the wait must answer exactly.
"""
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket

EXTRA = ["--memtable-max-mb", "1", "--block-cache-mb", "1", "--meta-cache-mb", "1"]


def burst(c, start, n=50_000, coll="t"):
    for s in range(start, start + n, 5000):
        c.d[coll].insert_many([{"_id": s + i, "g": (s + i) % 5, "p": "x" * 200} for i in range(5000)])


def exact_reads(c, n, ids_sum, tries=40):
    """find, count and $group through the database; each retried on 134 at most `tries` times. Returns (tries used, problems)."""
    used, problems = 0, []
    for what in ("find", "count", "group"):
        for t in range(tries):
            try:
                if what == "find":
                    ids = [d["_id"] for d in c.d.t.find({}, {"_id": 1})]
                    if len(ids) != n or len(set(ids)) != n or sum(ids) != ids_sum:
                        problems.append(f"find returned {len(ids)} ({len(set(ids))} distinct) of {n}")
                elif what == "count":
                    k = c.d.t.count_documents({})
                    if k != n:
                        problems.append(f"count {k} of {n}")
                else:
                    g = list(c.d.t.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}, "s": {"$sum": "$_id"}}}]))
                    if not g or g[0]["n"] != n or g[0]["s"] != ids_sum:
                        problems.append(f"group {g}")
                break
            except PyMongoError as e:
                if getattr(e, "code", None) != 134:
                    problems.append(f"{what}: {getattr(e, 'code', None)} {str(e)[:150]}")
                    break
                used += 1
                time.sleep(0.2)
        else:
            problems.append(f"{what}: refused {tries} times")
    return used, problems


def test_write_path_through_a_stale_reader():
    srv = Server(make_bucket("r59w"), "r59w", idle=600, extra=EXTRA).start()
    try:
        c = srv.client(retryWrites=True)
        burst(c, 0)
        n = 50_000
        c.d.command("tenantAdmin", close=True)
        assert c.d.t.count_documents({}) == n
        time.sleep(40)
        out = None
        for t in range(10):
            try:
                r = c.d.t.update_many({}, {"$inc": {"c": 1}})
                out = f"try {t}: matched {r.matched_count} modified {r.modified_count}"
                break
            except PyMongoError as e:
                out = f"try {t}: {getattr(e, 'code', None)} {str(e)[:150]}"
                if getattr(e, "code", None) != 134:
                    break
        vals = {}
        for d in c.d.t.find({}, {"c": 1}):
            vals[d.get("c")] = vals.get(d.get("c"), 0) + 1
        st = c.admin.command("tenantStats").get("maintenance", {})
        print(f"update_many: {out}; values of c: {vals}; staleReaderErrors {st.get('staleReaderErrors')}")
        assert vals == {1: n}, vals
    finally:
        srv.term()


def test_repeated_cycles_never_short_and_always_end():
    srv = Server(make_bucket("r59c"), "r59c", idle=600, extra=EXTRA).start()
    try:
        c = srv.client()          # the driver's defaults: retryable reads on
        n, lines, problems = 0, [], []
        for cycle in range(4):
            burst(c, n)
            n += 50_000
            c.d.command("tenantAdmin", close=True)
            c.d.t.find_one({"_id": 0})          # opens the reader
            time.sleep(40)
            used, p = exact_reads(c, n, n * (n - 1) // 2)
            st = c.admin.command("tenantStats").get("maintenance", {})
            lines.append(f"cycle {cycle}: {n} documents, 134 seen by the app {used}, problems {p}, staleReaderErrors {st.get('staleReaderErrors')}")
            problems += p
        print("\n".join(lines))
        assert not problems, problems
    finally:
        srv.term()


def test_two_processes():
    bucket = make_bucket("r59p")
    p1 = Server(bucket, "r59p1", idle=600, holder="S1", extra=EXTRA).start()
    p2 = Server(bucket, "r59p2", idle=600, holder="S2", extra=EXTRA).start()
    try:
        c1 = p1.client()
        c2 = p2.client()
        burst(c1, 0)
        n = 50_000
        c1.d.command("tenantAdmin", close=True)   # P1 releases the database; its maintenance compacts and collects it
        deadline = time.time() + 30
        while True:
            try:
                assert c2.d.t.count_documents({}) == n   # P2 opens the reader on the manifest of now
                break
            except PyMongoError:
                assert time.time() < deadline, "P2 could not open the database"
                time.sleep(0.5)
        time.sleep(40)
        used, problems = exact_reads(c2, n, n * (n - 1) // 2)
        st1 = c1.admin.command("tenantStats").get("maintenance", {})
        st2 = c2.admin.command("tenantStats").get("maintenance", {})
        print(f"P2 reads: 134 seen by the app {used}, problems {problems}; staleReaderErrors P1 {st1.get('staleReaderErrors')} P2 {st2.get('staleReaderErrors')}")
        assert not problems, problems
    finally:
        p1.term()
        p2.term()
