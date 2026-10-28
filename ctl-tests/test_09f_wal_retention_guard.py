"""Negative control for the correction in `gc_tenant_wal` (src/lifecycle.rs): the cleanup of an open writer's WAL must not delete WAL
objects that are still inside the retention window of the database, because a point-in-time restore replays them.

What the control shows (measured with this file):
 * `--wal-gc-mode slatedb` (the pass that uses slatedb's own garbage collector with an object age limit): WITHOUT the correction the pass
   deletes WAL objects the restore needs and every restore fails with `retention_gap` ("WAL object N needed ... is missing"); WITH it
   the same history restores exactly.
 * the default `--wal-gc-mode bulk`: both pass. Its incremental pass already skips every database that has a checkpoint, and the restore
   points are checkpoints, so that mode was protected before the correction. This is pinned too, so that nobody reads the correction as
   "the default mode was broken".
The correction is switched off for the control with MONGO_TEST_DISABLE_WAL_RETENTION_GUARD=1 (a test switch in gc_tenant_wal)."""
import random
import time

import pytest

from lib import Server, make_bucket, dump_db, list_prefix


def now_ms():
    return int(time.time() * 1000)


def history_and_restores(mode_flags, guard_off):
    bucket = make_bucket("walguard")
    env = {"MONGO_TEST_DISABLE_WAL_RETENTION_GUARD": "1"} if guard_off else {}
    srv = Server(bucket, "walguard", idle=300, env=env,
                 extra=["--retention-interval-secs", "600", "--retention-prune-secs", "30", "--wal-gc-secs", "2", "--l0-sst-bytes", "262144", *mode_flags]).start()
    try:
        assert srv.admin.post("/v1/databases", {"name": "w"})[0] == 201
        assert srv.admin.put("/v1/databases/w/retention", {"window_secs": 3600, "interval_secs": 600})[0] == 200
        c = srv.client()
        pad = "x" * 900
        states = []
        for b in range(7):
            c.w.t.insert_many([{"_id": b * 1000 + i, "b": b, "pad": pad} for i in range(600)])
            if b % 2:
                c.w.t.update_many({"b": {"$lt": b}}, {"$inc": {"u": 1}})
            time.sleep(0.1)
            states.append((now_ms(), dump_db(c, "w")))
            time.sleep(3.2)                                   # several cleanup passes (every 2 s) between two states
        order = list(range(len(states)))
        random.Random(5).shuffle(order)
        failures = []
        for i in order:
            t, want = states[i]
            st, r = srv.admin.post("/v1/databases/w/restore", {"to": t})
            if st != 200:
                failures.append((i, st, r["error"]["code"] if isinstance(r, dict) and "error" in r else str(r)[:80]))
            elif dump_db(c, "w") != want:
                failures.append((i, "different data"))
        return len(states), failures
    finally:
        srv.term()


def test_with_the_correction_slatedb_mode_restores_every_state():
    n, failures = history_and_restores(["--wal-gc-mode", "slatedb"], guard_off=False)
    assert failures == [], failures


def test_negative_control_without_the_correction_slatedb_mode_loses_the_history():
    n, failures = history_and_restores(["--wal-gc-mode", "slatedb"], guard_off=True)
    assert failures, "with the correction switched off every restore still worked: this test no longer shows that the correction is needed"
    assert any(f[1:] == (409, "retention_gap") or f[1] == "different data" for f in failures), failures
    print(f"negative control: {len(failures)} of {n} restores failed without the correction: {failures[:3]}")


@pytest.mark.parametrize("guard_off", [False, True])
def test_default_bulk_mode_was_protected_by_the_checkpoints_already(guard_off):
    n, failures = history_and_restores([], guard_off=guard_off)
    assert failures == [], failures
