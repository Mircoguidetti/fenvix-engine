"""Should-fix items of the round3 review, each with a test that fails without its fix.

1. The fair queue's per-database counters are created only for databases that exist (opened or written) and only for
   commands that passed authentication: neither unauthenticated clients nor commands naming arbitrary databases can grow
   the map (before the fix every command created an entry first, an unbounded memory growth).
2. Erase forgets the database like a drop: its counters go, and its WAL cleanup state too. A database created again with
   the same name starts its WAL ids at 1; before the fix the floor of the erased one stayed and the hot WAL cleanup of the
   new database did nothing (its WAL grew without bound).
3. `--writer-poll-secs` must be below `--sst-gc-min-unref-secs` (a reader polling slower than the SST cleanup reads SSTs
   that were already deleted): the server refuses to start otherwise.
4. `--unsafe-skip-ownership-checks` needs `MONGO_ALLOW_UNSAFE_TEST_FLAGS=1`.
"""
import os
import subprocess
import time
import uuid

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, BIN, MINIO

DOC = "x" * 4000


def test_unauthenticated_commands_create_no_counters():
    bucket = make_bucket("fq-auth")
    srv = Server(bucket, "fqauth", extra=["--auth"]).start()
    try:
        MongoClient(srv.uri, serverSelectionTimeoutMS=5000).admin.command("createUser", "root", pwd="pw", roles=["root"])
        anon = MongoClient(srv.uri, serverSelectionTimeoutMS=5000)
        names = [f"junk{uuid.uuid4().hex[:10]}" for _ in range(200)]
        for n in names:
            try:
                anon[n]["c"].find_one({})
            except OperationFailure:
                pass
        st, m = srv.admin.get("/v1/metrics/databases")
        grown = [n for n in names if n in m["databases"]]
        print("counters created by unauthenticated commands:", len(grown))
        assert not grown, f"{len(grown)} per-database entries created by unauthenticated commands"
    finally:
        srv.term()


def test_commands_on_databases_that_do_not_exist_create_no_counters():
    bucket = make_bucket("fq-ghost")
    srv = Server(bucket, "fqghost").start()
    try:
        c = srv.client()
        c["real"]["c"].insert_one({"_id": 1})
        names = [f"ghost{uuid.uuid4().hex[:10]}" for _ in range(200)]
        for n in names:
            c[n]["c"].find_one({})
        st, m = srv.admin.get("/v1/metrics/databases")
        grown = [n for n in names if n in m["databases"]]
        print("counters created for databases that do not exist:", len(grown), "| the real one is there:", "real" in m["databases"])
        assert "real" in m["databases"]
        assert not grown, f"{len(grown)} per-database entries created for databases that do not exist"
    finally:
        srv.term()


def test_erase_forgets_counters_and_wal_state():
    bucket = make_bucket("fq-erase")
    srv = Server(bucket, "fqerase", idle=600, extra=["--memtable-max-mb", "1", "--wal-gc-secs", "1"]).start()
    try:
        c = srv.client()

        def fill(n):
            for i in range(n):
                c["r"]["c"].insert_one({"_id": i, "p": DOC})

        def deleted():
            return c.admin.command("tenantStats")["walGc"]["deleted"]
        fill(1800)                     # several memtable flushes: the hot cleanup deletes WAL and remembers a high floor
        time.sleep(4)
        d1 = deleted()
        st, m = srv.admin.get("/v1/metrics/databases")
        assert "r" in m["databases"]
        st, r = srv.admin.delete("/v1/databases/r")
        assert st == 200, r
        st, m = srv.admin.get("/v1/metrics/databases")
        counters_left = "r" in m["databases"]
        fill(900)                      # the new database "r": one memtable flush, a boundary below the old floor
        time.sleep(5)
        d2 = deleted()
        print(f"WAL objects deleted: before the erase {d1}, after recreating {d2 - d1} | counters left after the erase: {counters_left}")
        assert d1 > 0, "the first incarnation's WAL was never collected (the test needs it)"
        assert not counters_left, "the erased database kept its counters"
        assert d2 > d1, "the recreated database's WAL is never collected (the erased one's floor stayed)"
    finally:
        srv.term()


def _start_fails(extra, env=None):
    try:
        p = subprocess.run([BIN, "--listen", "127.0.0.1:0", "--bucket", "x", "--endpoint", MINIO, "--no-ttl"] + extra,
                           env=dict(os.environ, **(env or {})), capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        return False, "started (still running after 15 s)"
    return p.returncode != 0, p.stderr or ""


def test_writer_poll_must_be_below_the_sst_cleanup_age():
    failed, err = _start_fails(["--writer-poll-secs", "200", "--sst-gc-min-unref-secs", "120"])
    print("refused:", failed)
    assert failed and "writer-poll-secs" in err, "a writer poll slower than the SST cleanup age was accepted"


def test_unsafe_flag_needs_the_environment_variable():
    failed, err = _start_fails(["--unsafe-skip-ownership-checks"], env={"MONGO_ALLOW_UNSAFE_TEST_FLAGS": ""})
    print("refused without the variable:", failed)
    assert failed and "MONGO_ALLOW_UNSAFE_TEST_FLAGS" in err, "--unsafe-skip-ownership-checks accepted without the environment variable"


def test_rollback_to_per_database_leases_after_a_process_mode_crash():
    """A process in per-process mode (the default) owns a database and is killed; a process started with
    `--process-lease false` (a rollback) must take the database over once the dead process's lease expired. Before the fix
    the owner record (written with an expiry far in the future, the process lease decides) locked the database for ever
    for a per-database-mode process."""
    from lib import Server as S
    bucket = make_bucket("rollback")
    p1 = S(bucket, "rb1", idle=600, holder="R1", extra=["--lease-ttl-secs", "4"]).start()
    p1.client()["d"]["c"].insert_one({"_id": 1})
    p1.kill9()
    p2 = S(bucket, "rb2", idle=600, holder="R2", extra=["--lease-ttl-secs", "4", "--process-lease", "false"]).start()
    c2 = MongoClient(p2.uri, serverSelectionTimeoutMS=5000, retryReads=False)
    took, t0 = None, time.time()
    try:
        while time.time() - t0 < 25 and took is None:
            try:
                assert c2["d"]["c"].find_one({"_id": 1}) == {"_id": 1}
                took = time.time() - t0
            except OperationFailure as e:
                assert e.code == 134, e
                time.sleep(0.5)
        print("per-database-mode process took the database over after", None if took is None else round(took, 1), "s")
        assert took is not None, "after a rollback to --process-lease false the database stayed locked"
    finally:
        p2.term()
