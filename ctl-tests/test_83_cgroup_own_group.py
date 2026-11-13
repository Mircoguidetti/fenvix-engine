"""Fix round of big-speed (2026-11-13), review error 3: a memory limit on the process's own cgroup, without its own
cgroup namespace (a systemd unit's `MemoryMax=`, `docker --cgroupns=host`, a child group), is seen.

A server is started inside a 1 GiB child group of this test's own group, WITHOUT `--memory-limit-mb`. Before the fix the
budget read only the files at the mount's root and took the whole machine (the reviewer MEASURED "limit 16086 MiB
(memtotal), caches 7531 MiB" under the 1 GiB group). Pass: serverStatus fenvix.memoryBudget says 1 GiB from the
process's own group, with 256 MiB of caches.
"""
import os

import pytest

from lib import Server, make_bucket
from memcg import MemCg, Unavailable, wrapper


def test_limit_of_the_own_group_is_seen():
    try:
        cg = MemCg(f"t83-{os.getpid()}", 1 << 30)
    except Unavailable as e:
        pytest.fail(f"no memory cgroup could be set up here (the test needs one): {e}")
    # the budget is fixed at start: the launcher joins the group before it runs the server
    s = Server(make_bucket("t83"), "t83", idle=60, binary=wrapper(cg)).start()
    mb = s.client().admin.command("serverStatus")["fenvix"]["memoryBudget"]
    line = [l for l in s.log_text().splitlines() if l.startswith("memory budget: limit")]
    print(f"T83 {line[:1]} -> limitBytes {mb['limitBytes']} source {mb['source']} cachesBytes {mb['cachesBytes']}", flush=True)
    s.term()
    assert mb["limitBytes"] == 1 << 30, mb
    assert mb["source"] in ("cgroup1-own", "cgroup2-own"), mb
    assert mb["cachesBytes"] == 256 << 20, mb
