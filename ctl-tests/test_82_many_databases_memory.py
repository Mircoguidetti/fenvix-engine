"""Fix round of big-speed (2026-11-13), review error 2: the memory budget holds across many databases.

The reviewer's `manydb.py` as a ctl test. A server in a 1 GiB cgroup (the kernel kills it past the limit) with
`--memory-limit-mb 1024`; 16 client threads write 6 MiB into each of 200 databases (512 KiB documents). Before the fix
each database's memtables were bounded (4 x 8 MiB) but not their sum: OOM-killed (MEASURED by the reviewer on 13d531b;
outside the cgroup 1,403 MiB resident at the peak). The fix: one memtable budget for the process (`memgov.rs`, 128 MiB
at 1 GiB; writes wait past 192 MiB).

Then rule 8: a second wave of writes into 40 more databases is cut by kill -9 at a random moment; after a restart every
acknowledged document of both waves is read back, whole, from a server outside the cgroup.

Control (`CTL82_CONTROL=1`): the same first wave with `--memtables-total-mb 0` (no process-wide budget, the code before
the fix) must be OOM-killed; it shows this test sees the defect.
"""
import hashlib
import os
import random
import threading
import time

import pytest
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, make_bucket
from memcg import MemCg, Unavailable, rss_mib, wrapper

N = int(os.environ.get("CTL82_DBS", "200"))
MB = int(os.environ.get("CTL82_MIB", "6"))
DOC = 512 * 1024
BLOB = os.urandom(DOC)
SUM = hashlib.sha256(BLOB).hexdigest()
X = ["--memory-limit-mb", "1024"]


def write_wave(port, names, per_db, acked, stop=None):
    from pymongo import MongoClient
    c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", maxPoolSize=64, serverSelectionTimeoutMS=20000, socketTimeoutMS=120000)
    errs = []

    def work(part):
        for name in part:
            db = c[name]
            for k in range(per_db):
                if stop and stop.is_set():
                    return
                try:
                    db.t.insert_one({"_id": k, "b": BLOB})
                except PyMongoError as e:
                    errs.append(f"{name}/{k}: {type(e).__name__}")
                    if stop:
                        return
                    continue
                acked.append((name, k))

    parts = [names[j::16] for j in range(16)]
    ts = [threading.Thread(target=work, args=(p,)) for p in parts]
    for t in ts:
        t.start()
    return c, ts, errs


def run_first_wave(bucket, tag, extra):
    try:
        cg = MemCg(f"t82-{tag}-{os.getpid()}", 1 << 30)
    except Unavailable as e:
        pytest.fail(f"no memory cgroup could be set up here (the test needs one): {e}")
    s = Server(bucket, tag, idle=600, extra=X + extra, binary=wrapper(cg)).start()
    acked = []
    names = [f"agent{i:04d}" for i in range(N)]
    t0 = time.time()
    dead = threading.Event()
    c, ts, errs = write_wave(s.port, names, MB * 2, acked, dead)
    peak = 0
    while any(t.is_alive() for t in ts):
        r = rss_mib(s.proc.pid)
        if r < 0 or s.proc.poll() is not None:
            dead.set()       # the server is gone (killed): the writers stop instead of retrying a dead server
            break
        peak = max(peak, r)
        time.sleep(0.5)
    for t in ts:
        t.join(timeout=300)
    for _ in range(10):
        peak = max(peak, rss_mib(s.proc.pid))
        time.sleep(0.5)
    alive = s.proc.poll() is None
    mb = {}
    if alive:
        mb = c.admin.command("serverStatus")["fenvix"]["memoryBudget"]
    ooms = cg.new_oom_kills()
    print(f"T82 {tag}: {N} databases x {MB} MiB in {time.time() - t0:.0f} s, acknowledged {len(acked)} of {N * MB * 2}, "
          f"errors {len(errs)}, alive {alive} (exit {s.proc.returncode}), RSS peak {peak} MiB, cgroup peak {cg.peak_mib()} MiB, "
          f"new OOM kills {ooms}, memtables peak {mb.get('memtablesPeak', 0) >> 20} MiB (budget {mb.get('memtablesBytes', 0) >> 20}, "
          f"hard {mb.get('memtablesHardBytes', 0) >> 20}), budget flushes {mb.get('memtableBudgetFlushes')}, waits {mb.get('memtableBudgetWaits')}, "
          f"wait timeouts {mb.get('memtableBudgetWaitTimeouts')}", flush=True)
    return s, cg, acked, errs, alive, ooms, mb, peak


def check_all(port, acked):
    from pymongo import MongoClient
    c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=30000, socketTimeoutMS=300000)
    by_db = {}
    for name, k in acked:
        by_db.setdefault(name, set()).add(k)
    lost, wrong = [], []
    for name, ks in sorted(by_db.items()):
        got = {}
        # the killed process's lease runs out first (6 s): until then the database answers TenantLeaseHeld (retryable)
        for attempt in range(120):
            try:
                got = {d["_id"]: hashlib.sha256(d["b"]).hexdigest() for d in c[name].t.find({})}
                break
            except OperationFailure as e:
                if e.code != 134 or attempt == 119:
                    raise
                time.sleep(0.5)
        for k in ks:
            if k not in got:
                lost.append((name, k))
            elif got[k] != SUM:
                wrong.append((name, k))
    return lost, wrong


def test_many_databases_under_1gib_and_kill9():
    bucket = make_bucket("t82")
    s, cg, acked, errs, alive, ooms, mb, peak = run_first_wave(bucket, "t82", [])
    assert alive and ooms == 0, f"the server was killed (OOM kills {ooms}, exit {s.proc.returncode})"
    assert not errs, errs[:5]
    assert len(acked) == N * MB * 2
    assert mb["memtablesBytes"] == 128 << 20
    assert mb["memtablesPeak"] <= mb["memtablesHardBytes"] + (32 << 20), mb
    # rule 8: a second wave cut by kill -9 at a random moment
    acked2 = []
    stop = threading.Event()
    names2 = [f"late{i:04d}" for i in range(40)]
    c2, ts2, _ = write_wave(s.port, names2, MB * 2, acked2, stop)
    # kill -9 after a random number of acknowledged writes of the second wave (writes are in flight then)
    target = random.randint(20, 200)
    t_wait = time.time()
    while len(acked2) < target and time.time() - t_wait < 300 and any(t.is_alive() for t in ts2):
        time.sleep(0.01)
    s.kill9()
    stop.set()
    for t in ts2:
        t.join(timeout=120)
    print(f"T82 kill -9 after {len(acked2)} acknowledged writes of the second wave (target {target}, {time.time() - t_wait:.1f} s)", flush=True)
    r = Server(bucket, "t82r", idle=600, extra=X).start()
    lost, wrong = check_all(r.port, acked + acked2)
    print(f"T82 after the restart: {len(acked) + len(acked2)} acknowledged documents checked, lost {len(lost)}, wrong {len(wrong)}", flush=True)
    r.term()
    assert not lost, f"acknowledged writes lost: {lost[:10]}"
    assert not wrong, f"wrong documents: {wrong[:10]}"
    assert len(acked2) > 0, "the kill came before any write of the second wave"


@pytest.mark.skipif(os.environ.get("CTL82_CONTROL") != "1", reason="control run only with CTL82_CONTROL=1")
def test_control_without_the_process_budget_is_oom_killed():
    bucket = make_bucket("t82c")
    s, cg, acked, errs, alive, ooms, mb, peak = run_first_wave(bucket, "t82c", ["--memtables-total-mb", "0"])
    s.kill9()
    assert (not alive) or ooms > 0, "the control (no process-wide memtable budget) was not OOM-killed: the test does not see the defect here"
