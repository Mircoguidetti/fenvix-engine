"""An operation that finds a database absent leaves no object under its name (fix-erase-fork-usage).

Found by the review of perf-erase: ctl test_05 `test_concurrent_erase_and_fork_are_serialised` fails about 1 in 5. When the
erase wins the race, the fork then finds its source gone (404), but on its way there it touched objects under the
target's name (its intent record, the target's lease, the existence checks), and usage accounting counts every request by
object path. Nothing forgot those counters, so the flusher wrote `control/usage/{target}/{day}/...json` for a database
that does not exist. The race is not needed: an erase followed by a fork of the erased source always does it. Every other
operation that answers "database ... does not exist" after touching the name did the same (an erase or a describe of a
name that never existed, a usage query, a list of restore points, an allow list, a durability setting).

Each test waits past the usage flush (1 s here) and lists every prefix of the absent names: none may hold an object. A
database that exists still gets its usage record (control).
"""
import time

from lib import Server, make_bucket, list_prefix, fill

PREFIXES = lambda db: [f"tenants/{db}/", f"ttl-hints/{db}/", f"control/dbs/{db}/", f"control/dbs/{db}.json", f"control/pins/{db}",
                       f"control/usage/{db}/", f"control/forks/{db}/", f"control/forks/{db}.json", f"control/exports/{db}/",
                       f"control/ops/{db}.json", f"control/retention/{db}", f"control/staging/{db}"]


def objects_of(bucket, db):
    out = []
    for p in PREFIXES(db):
        out += [k for k, _ in list_prefix(bucket, p)]
    return sorted(set(out))


def server(tag):
    bucket = make_bucket(tag)
    return bucket, Server(bucket, tag, idle=300, extra=["--usage-flush-secs", "1"]).start()


def test_fork_of_an_erased_source_leaves_nothing_under_the_target():
    bucket, srv = server("absfork")
    try:
        fill(srv.client(), "src", "c", 200)
        st, r = srv.admin.delete("/v1/databases/src")
        assert st == 200, r
        st, r = srv.admin.post("/v1/databases/src/fork", {"target": "src_k"})
        print(f"fork after the erase: {st} {r}")
        assert st == 404, (st, r)
        time.sleep(3)          # past the usage flush
        left = {db: objects_of(bucket, db) for db in ("src", "src_k")}
        print(f"objects left: {left}")
        assert left == {"src": [], "src_k": []}, left
    finally:
        srv.term()


def test_operations_on_absent_names_leave_nothing():
    bucket, srv = server("absops")
    try:
        fill(srv.client(), "real", "c", 50)
        calls = [
            ("erase", lambda: srv.admin.delete("/v1/databases/ghost1")),
            ("fork from a source that never existed", lambda: srv.admin.post("/v1/databases/ghost2/fork", {"target": "ghost2_k"})),
            ("describe", lambda: srv.admin.get("/v1/databases/ghost3")),
            ("usage", lambda: srv.admin.get("/v1/databases/ghost4/usage")),
            ("restore points", lambda: srv.admin.get("/v1/databases/ghost5/restore-points")),
            ("allow list", lambda: srv.admin.get("/v1/databases/ghost6/ip-allowlist")),
            ("durability", lambda: srv.admin.get("/v1/databases/ghost7/durability")),
            ("checkpoints", lambda: srv.admin.get("/v1/databases/ghost8/checkpoints")),
            ("erasures", lambda: srv.admin.get("/v1/databases/ghost9/erasures")),
            ("quota", lambda: srv.admin.get("/v1/databases/ghost10/quota")),
            ("keep-awake off", lambda: srv.admin.delete("/v1/databases/ghost11/keep-awake")),
        ]
        for what, f in calls:
            st, r = f()
            print(f"{what}: {st} {str(r)[:120]}")
        time.sleep(3)
        names = ["ghost1", "ghost2", "ghost2_k", "ghost3", "ghost4", "ghost5", "ghost6", "ghost7", "ghost8", "ghost9", "ghost10", "ghost11"]
        left = {n: objects_of(bucket, n) for n in names if objects_of(bucket, n)}
        real = [k for k, _ in list_prefix(bucket, "control/usage/real/")]
        print(f"objects left under absent names: {left}; usage records of the existing database: {len(real)}")
        assert real, "the existing database has no usage record: the flush did not run (the test did not reach the case)"
        assert not left, left
    finally:
        srv.term()


def test_reads_of_an_absent_database_leave_nothing_and_a_dropped_one_keeps_its_usage():
    """A Mongo read of an erased database and of one never created leaves nothing under the name; a dropped database
    (dropDatabase keeps its usage records, unlike an erase) keeps them after a later read and a usage query of it."""
    bucket, srv = server("absreads")
    try:
        c = srv.client()
        fill(c, "gone", "c", 100)
        fill(c, "dropped", "c", 100)
        time.sleep(3)
        st, r = srv.admin.delete("/v1/databases/gone")
        assert st == 200, r
        for db in ("gone", "never"):
            assert c[db]["c"].find_one({}) is None
            assert c[db]["c"].count_documents({}) == 0
            c[db].list_collection_names()
        before = [k for k, _ in list_prefix(bucket, "control/usage/dropped/")]
        c.drop_database("dropped")
        assert c["dropped"]["c"].find_one({}) is None
        st, r = srv.admin.get("/v1/databases/dropped/usage")
        print(f"usage query of the dropped database: {st}")
        time.sleep(3)
        # integration-6 (known failures): since fix-slow-store (item 4) a read of an absent name keeps its lease for the
        # absent hold (5 s, `ABSENT_HOLD` in dbs.rs): `tenants/{name}/_lease` exists until it ends (MEASURED on fea18a34:
        # present at 3 s, gone at 11 s). Wait for the names to empty (at most 12 s), then check again after 3 s more.
        deadline = time.time() + 12
        while time.time() < deadline and any(objects_of(bucket, db) for db in ("gone", "never")):
            time.sleep(0.5)
        time.sleep(3)
        left = {db: objects_of(bucket, db) for db in ("gone", "never")}
        kept = [k for k, _ in list_prefix(bucket, "control/usage/dropped/")]
        print(f"objects left under absent names: {left}; usage records of the dropped database before {before}, after {kept}")
        assert left == {"gone": [], "never": []}, left
        assert before and kept == before, f"the dropped database's usage records changed: {before} -> {kept}"
    finally:
        srv.term()
