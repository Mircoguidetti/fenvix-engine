"""NOT DISCRIMINATING (reviewer, 2026-11-13): this test passes with and without the change it was written for, so it proves
nothing about it. Kept for the record only; do not count it as a check.

Reviewer's test for perf-latency-on-i4, joint 2 (`Compactor::stop`): after a database closes, its compactor makes no
request of the store at all, neither a read (c2-fix-clone-uploads: a stopped compactor's absorption read every L0 SST it
had planned) nor a write (perf-latency: nothing writes after `stop` returns).

test_51's closed-database check passes when no job happens to be running at the close. Here every GET of the database is
answered 2 s late (fault file), so the compactor's absorption is reading L0 SSTs when the close comes; the requests to the
database after the close are counted by a proxy. Rerun with each of the two stop mechanisms removed, this must fail.
"""
import os
import tempfile
import time

from pymongo import UpdateOne

from lib import Server, make_bucket
from proxy import CountingProxy

ENV = {"FENVIX_SEALED_RUNS": "7", "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
       "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}


def test_no_request_after_a_close_with_jobs_in_flight():
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("r69")
    proxy = CountingProxy()
    env = dict(ENV, MONGO_TEST_FAULTS_FILE=ff.name)
    srv = Server(bucket, "r69", idle=600, endpoint=proxy.endpoint, extra=["--l0-sst-bytes", "262144", "--compaction-tiers", "0"], env=env).start()
    rounds = []
    try:
        c = srv.client()
        col = c["d"]["c"]
        for rnd in range(3):
            for b in range(8):
                col.bulk_write([UpdateOne({"_id": rnd * 3000 + i}, {"$set": {"v": b, "pad": "x" * 200}}, upsert=True) for i in range(3000) if (i + b) % 3 == 0])
            st = c.admin.command("serverStatus")["fenvix"]["compactor"]
            with open(ff.name, "w") as fh:
                fh.write("delay_get tenants/d/ 2000\n")      # the compactor's L0 reads are in flight at the close
            time.sleep(0.3)
            c["d"].command("tenantAdmin", close=True)
            proxy.reset()
            time.sleep(8)
            snap = proxy.snapshot()
            with open(ff.name, "w") as fh:
                fh.write("")
            after = {k: v for k, v in snap["by_prefix"].items() if k.startswith("tenants/d")}
            rounds.append((rnd, st.get("started"), st.get("absorbs"), after))
            c["d"].c.find_one({})                              # opens it again for the next round
        print("\n".join(f"round {r}: compactors started {s}, absorbs {a}; requests to the closed database in 8 s: {x}" for r, s, a, x in rounds))
        assert rounds[-1][1] and rounds[-1][1] > 0, "no compactor ran: the test did not exercise it"
        bad = [x for _, _, _, x in rounds if x]
        assert not bad, f"requests to a closed database after its close: {bad}"
    finally:
        srv.term()
        proxy.stop()
        os.unlink(ff.name)
