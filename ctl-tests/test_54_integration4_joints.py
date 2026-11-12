"""integration-4: the joints between the merged fixes that are not textual (P-integration-4, output 1 point 2).

A. c2-fix-long-reads (a read step renews its state under the shared lock) against c2-fix-clone-uploads (a writer's close
   flushes, lets the compactor absorb the last flush, then stops it; the upgrade of a reader to a writer skips the old
   pass's L0 wait when a compactor owns the database). The merge put `absorb_renewal()` before the clone-uploads guard in
   `upgrade_inner`. Here: a long find on a database that woke as a reader; between its getMores, writes upgrade the
   tenant to a writer (its compactor starts); after the find, the database closes when idle (flush, absorb, stop). The
   find must return every original id once (no duplicate, nothing missing, no refusal); afterwards a read of the closed
   database returns every original and every new id.
B. c2-fix-hangs (the compactor's requests are maintenance: counted in the database's points, never drawn from its quota)
   against c2-fix-clone-uploads (the close's absorption reads each L0 SST whole, and the compactor's jobs are cancelled at
   stop). The merge applied the maintenance store in `maybe_start_fxc()`, where the clone-uploads fix moved the start.
   Here: a database under a quota closes with L0 SSTs to absorb. The close's reads must be counted in its points but
   leave its quota bucket positive. Tested where it lives, in the engine: `src/fx/i4_joint_tests.rs`
   (`i4_compactor_requests_are_maintenance`: every request of the compactor started as `maybe_start_fxc()` starts it,
   12 whole-L0 GETs included, carries the maintenance mark; the control on the plain store carries none). A ctl version
   with `tenantAdmin close` measured a command, inside which every request is charged to the database by design.
C. The ballast generator's runs (V2K keys, today's run format) against the current compactor: covered by the 10 GB proof
   on the final head (ballast_check.py, 0 wrong answers against mongod, before and after compaction) and the unit tests
   of `fx::tests_gen`; not repeated here.
"""
import time

from lib import Server, make_bucket

T_PIN_MS = 200
N = 60_000


def test_A_upgrade_mid_scan_then_close():
    s = Server(make_bucket("i4ja"), "i4ja", idle=3, env={"FENVIX_SEALED_TPIN_MS": str(T_PIN_MS)}).start()
    try:
        c = s.client()
        for b in range(N // 5000):
            c.d.w.insert_many([{"_id": b * 5000 + i, "p": "x" * 100} for i in range(5000)])
        c.close()
        time.sleep(8)                     # the writer closes when idle (3 s); the next read wakes it as a reader
        c = s.client()
        cur = c.d.w.find({"_id": {"$lt": N}}, {"_id": 1}, batch_size=2000)
        got, new, t0 = [], [], time.time()
        for k, doc in enumerate(cur):
            got.append(doc["_id"])
            if k % 6000 == 5999:          # between getMores: a write upgrades the reader to a writer
                _id = N + len(new)
                c.d.w.insert_one({"_id": _id, "p": "new"})
                new.append(_id)
                time.sleep(T_PIN_MS / 1000)   # the find's state ages past T_pin/2: its next step renews
        dt = time.time() - t0
        assert new, "no write happened during the find"
        assert len(got) == len(set(got)), f"{len(got) - len(set(got))} ids returned twice"
        assert sorted(got) == list(range(N)), f"{len(got)} of {N} ids"
        assert dt * 1000 >= 3 * T_PIN_MS, f"the find lasted {dt:.2f} s, under 3 T_pin: it proves nothing"
        before = c.admin.command("serverStatus")["fenvix"]["compactor"]
        c.close()
        time.sleep(10)                    # idle close: flush, absorb the last flush, stop the compactor
        c = s.client()
        after = c.admin.command("serverStatus")["fenvix"]["compactor"]
        all_ids = sorted(d["_id"] for d in c.d.w.find({}, {"_id": 1}))
        assert all_ids == list(range(N)) + new, f"{len(all_ids)} ids after the close, {N + len(new)} expected"
        print(f"find of {N} ids in {dt:.2f} s with {len(new)} upgrades between getMores; compactor before {before}, "
              f"after the close {after}")
        assert after["started"] >= 1, "the compactor never started: the joint was not exercised"
    finally:
        s.term()

