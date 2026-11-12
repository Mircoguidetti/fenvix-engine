"""fix-erase-fork-usage (2026-11-12): no usage record of a database that does not exist.

test_05_erase.py::test_concurrent_erase_and_fork_are_serialised failed about 1 run in 5 (7/40 and 9/40): when a fork
waited behind an erase of its source, the fork found the source gone and answered 404, but on the way it had read the
records of its target (registry record, lease) and of its source; those reads were metered, and the usage flusher later
wrote `control/usage/{target}/{day}/...` for a database that never existed. Every request that reads a database's
records and then answers 404 did the same (test_43's refused fork leaves the same files, on integration-3 too).

Fix (`ctl/usage.rs`): the flusher does not write a record that holds storage requests only (no command, no activity)
for a database that does not exist; it checks the store, unmetered, under the database's operation lock.

These tests are deterministic (no race needed): the erase runs to the end, then the fork of the erased source; requests
on missing databases; a refused fork. Each must leave no object under the names involved after several flushes.
Control: a database that exists keeps its usage records.
"""
import time

from lib import Server, make_bucket, list_prefix

FLUSH = ["--usage-flush-secs", "1"]


def objects_of(bucket, names):
    out = []
    for n in names:
        for prefix in (f"control/usage/{n}/", f"control/dbs/{n}.json", f"tenants/{n}/", f"control/forks/{n}", f"control/staging/{n}"):
            out += [k for k, _ in list_prefix(bucket, prefix)]
    return out


def test_fork_of_an_erased_source_leaves_no_object_of_either_name():
    b = make_bucket("efuerase")
    s = Server(b, "efuerase", idle=300, extra=FLUSH).start()
    try:
        c = s.client()
        c.src.t.insert_many([{"_id": i, "v": "x" * 100} for i in range(500)])
        time.sleep(2.5)  # the source's own usage records exist before the erase (the erase must remove them)
        st, r = s.admin.delete("/v1/databases/src")
        assert st == 200, r
        st, r = s.admin.post("/v1/databases/src/fork", {"target": "src_k"})
        assert st == 404, (st, r)
        time.sleep(4)  # several flushes
        left = objects_of(b, ["src", "src_k"])
        assert left == [], f"objects left of the erased source or of the fork's target: {left}"
    finally:
        s.term()


def test_requests_on_missing_databases_leave_no_object():
    b = make_bucket("efumissing")
    s = Server(b, "efumissing", idle=300, extra=FLUSH).start()
    try:
        calls = [
            ("POST", "/v1/databases/never_was/fork", {"target": "nf_k"}),
            ("GET", "/v1/databases/ghost_a", None),
            ("GET", "/v1/databases/ghost_b/usage", None),
            ("DELETE", "/v1/databases/ghost_c", None),
            ("GET", "/v1/databases/ghost_d/durability", None),
            ("GET", "/v1/databases/ghost_e/restore-points", None),
            ("PUT", "/v1/databases/ghost_f/keep-awake", {}),
            ("GET", "/v1/databases/ghost_g/ip-allowlist", None),
            ("POST", "/v1/databases/ghost_h/exports", {}),
            ("PUT", "/v1/databases/ghost_i/retention", {"window_secs": 60}),
        ]
        for method, path, body in calls:
            st, r = s.admin.call(method, path, body)
            assert st in (404, 409), f"{method} {path}: {st} {r}"
        time.sleep(4)
        names = ["never_was", "nf_k"] + [f"ghost_{x}" for x in "abcdefghi"]
        left = objects_of(b, names)
        assert left == [], f"objects of databases that do not exist: {left}"
    finally:
        s.term()


def test_control_an_existing_database_keeps_its_usage_records():
    b = make_bucket("efureal")
    s = Server(b, "efureal", idle=3, extra=FLUSH).start()
    try:
        c = s.client()
        c.real.t.insert_one({"_id": 1})
        time.sleep(3)
        assert [k for k, _ in list_prefix(b, "control/usage/real/")], "a database that exists must keep its usage records"
        # only storage requests (the database closes after 3 s idle and the closing reads/writes are metered, no command)
        n0 = len(list_prefix(b, "control/usage/real/"))
        time.sleep(8)
        assert len(list_prefix(b, "control/usage/real/")) >= n0
        st, r = s.admin.get("/v1/databases/real/usage")
        assert st == 200, r
    finally:
        s.term()
