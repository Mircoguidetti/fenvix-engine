"""Fix round of big-speed (2026-11-13), review error 1: one read of a big customer must not take the server down.

The reviewer's `bigbox.py` as a ctl test. One customer's documents are moved into our runs; a cold server then runs in a
1 GiB cgroup (the kernel kills it past the limit, as Docker's `--memory 1g` does) with `--memory-limit-mb 1024`, and
reads that customer. Before the fix the box's warm-up fetched and held the customer's whole range: a 1.5 GiB customer
OOM-killed the server (exit -9, the cgroup's oom_kill counter +1, MEASURED by the reviewer on 13d531b).

Two customers, both above the customer's allowance (`--customer-box-max-mb`, the runs' cache / 8: 12 MiB at 1 GiB):
  - `big`: CTL81_MIB MiB (default 1536, the reviewer's size) of compressible documents ('y' * 256 KiB, as in
    `bigbox.py`): few stored bytes, many decoded ones. The warm-up must stop at the allowance of decoded bytes
    (serverStatus fenvix.customerBox.capped);
  - `rnd`: 64 MiB of random documents: more stored bytes than the allowance, so the box is not warmed at all (tooBig).

Pass: with the box on (the default) and with it off (control), the server stays up, the cgroup records no OOM kill, every
answer is right, and the box's counters show the cap at work.
"""
import os
import threading
import time

import pytest

from lib import Server, make_bucket
from memcg import MemCg, Unavailable, rss_mib, wrapper

MIB = int(os.environ.get("CTL81_MIB", "1536"))
RND_MIB = 64
X = ["--no-sealed-compactor", "--sealed-runs", "1000000", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000",
     "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait", "--memory-limit-mb", "1024"]
ENV = {"FENVIX_TEST_DROP": "1"}


@pytest.fixture(scope="module")
def bucket():
    b = make_bucket("t81")
    s = Server(b, "t81load", idle=600, extra=X, env=ENV).start()
    db = s.client().app
    blob = "y" * (256 * 1024)
    n = MIB * 4
    for k in range(0, n, 16):
        db.checkpoints.insert_many([{"_id": f"big-{j:06d}", "thread_id": "big", "v": j, "b": blob + str(j)} for j in range(k, min(n, k + 16))])
    for k in range(0, RND_MIB * 4, 16):
        db.checkpoints.insert_many([{"_id": f"rnd-{j:06d}", "thread_id": "rnd", "v": j, "b": os.urandom(256 * 1024)} for j in range(k, min(RND_MIB * 4, k + 16))])
    db.checkpoints.insert_many([{"_id": f"small-{j}", "thread_id": "small", "v": j} for j in range(10)])
    st, r = s.admin.post("/v1/test/databases/app/publish-drop", {})
    assert st == 200, (st, r)
    s.term()
    return b


@pytest.mark.parametrize("box", ["on", "off"])
def test_big_customer_read_under_1gib(bucket, box):
    try:
        cg = MemCg(f"t81-{box}-{os.getpid()}", 1 << 30)
    except Unavailable as e:
        pytest.fail(f"no memory cgroup could be set up here (the test needs one): {e}")
    x = X + ([] if box == "on" else ["--customer-box", "false"])
    s = Server(bucket, f"t81{box}", idle=600, extra=x, env=ENV, binary=wrapper(cg)).start()
    pid = s.proc.pid
    peak, stop = [0], [False]

    def watch():
        while not stop[0]:
            r = rss_mib(pid)
            if r < 0:
                return
            peak[0] = max(peak[0], r)
            time.sleep(0.05)

    threading.Thread(target=watch, daemon=True).start()
    c = s.client()
    db = c.app
    res = {}
    try:
        d = db.checkpoints.find_one({"thread_id": "big", "_id": "big-000005"})
        res["big_one"] = bool(d) and d["v"] == 5 and d["b"].endswith("5") and len(d["b"]) == 256 * 1024 + 1
        last = list(db.checkpoints.find({"thread_id": "big", "_id": {"$gte": f"big-{MIB * 4 - 3:06d}"}}))
        res["big_last"] = sorted(x["v"] for x in last) == [MIB * 4 - 3, MIB * 4 - 2, MIB * 4 - 1]
        d = db.checkpoints.find_one({"thread_id": "rnd", "_id": "rnd-000100"})
        res["rnd_one"] = bool(d) and d["v"] == 100 and len(d["b"]) == 256 * 1024
        res["small"] = sorted(x["v"] for x in db.checkpoints.find({"thread_id": "small"})) == list(range(10))
        cb = c.admin.command("serverStatus")["fenvix"]["customerBox"]
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        cb = {}
    time.sleep(1)
    stop[0] = True
    alive = s.proc.poll() is None
    ooms = cg.new_oom_kills()
    print(f"T81 box={box} {MIB} MiB customer: {res}, alive {alive} (exit {s.proc.returncode}), RSS peak {peak[0]} MiB, "
          f"cgroup peak {cg.peak_mib()} MiB, new OOM kills {ooms}, box tooBig {cb.get('tooBig')} capped {cb.get('capped')} "
          f"waves {cb.get('waves')} warms {cb.get('warms')} max/customer {cb.get('maxBytesPerCustomer')}", flush=True)
    s.term()
    assert alive and ooms == 0, f"the server was killed (OOM kills {ooms}, exit {s.proc.returncode})"
    assert res == {"big_one": True, "big_last": True, "rnd_one": True, "small": True}, res
    if box == "on":
        assert cb["capped"] >= 1, "the compressible big customer's warm-up did not stop at the allowance"
        assert cb["tooBig"] >= 1, "the random customer's box was warmed although above the allowance"
        assert cb["maxBytesPerCustomer"] <= 16 << 20, cb["maxBytesPerCustomer"]
