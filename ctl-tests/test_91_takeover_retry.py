"""fix-takeover-retry (2026-11-13, RULE 8): acknowledged writes lost across a takeover with the replicated log on.

What step 9 saw (CI runs 43440029598, 43449760948, 43460387918; sessions/2026-11-11-ci-public-tests/results/
step9-takeovers-rule8-investigation.md): every writer lost one contiguous run of inserts that the spare B acknowledged
just after takeover 1, when the restarted process A's background retry of its kept records finished while B was still
serving, and B was then killed before its next upload.

The mechanism, in order (this test makes each step happen on purpose):
  1. B holds database DB and acknowledges W1; B's uploads are held (fault `hang_put /wal/`), so W1 is only in the log.
  2. A starts (a restart): it reads W1 from the log; its replay is refused (B holds the lease) and it keeps the records.
     The failpoint `hold_kept_records` stops A right there.
  3. B's uploads go on: W1 reaches object storage (B's marker covers it). Then they are held again.
  4. B acknowledges W2: in the log only.
  5. A goes on. The old code wrote its kept records to the log again: copies at new positions, AFTER W2.
  6. A's retry finds W1 in object storage (B's marker) and told the log "stored up to" its highest position, which is a
     copy's, after W2: the log dropped W2 although no process had stored it.
  7. B is killed (W2's upload never finished); A takes the database over: W2 is in neither the log nor object storage.

The fix: a process that does not hold the database's lease writes nothing to the log for it (no copies, no epoch record,
no drop record) and never confirms a position past the records it read and proved are in object storage.

test_takeover_retry_keeps_writes: every acknowledged write is there after the takeover, and after a clean restart.
test_control_old_retry_loses: the same with the old behaviour (`MONGO_TEST_KEPT_RETRY_OLD=1` on A): W2 is lost. This is
the proof that the test sees the defect (on the base, where the variable does not exist, the first test fails the same way).

Needs MinIO (as every ctl test) and the log's binary `pq` (proto/quaderno): PQ_BIN, or proto/quaderno/target/release/pq,
built here once if missing.
"""
import os
import re
import shutil
import subprocess
import tempfile
import time

import pytest

from lib import Server, make_bucket, free_port, wait_until, ROOT

DB = "tkr"
W = 120          # inserts per phase (one record each)


def pq_bin():
    p = os.environ.get("PQ_BIN") or os.path.join(ROOT, "proto", "quaderno", "target", "release", "pq")
    if not os.path.exists(p):
        subprocess.run(["cargo", "build", "--release", "--manifest-path", os.path.join(ROOT, "proto", "quaderno", "Cargo.toml"), "--bin", "pq"],
                       check=True, timeout=1800)
    assert os.path.exists(p), f"the log's binary is missing: {p}"
    return p


class Log:
    """Three copies of the replicated log on this machine (the order of proto/quaderno/scripts/cluster.py, no proxy)."""

    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="tkrlog")
        self.peer = [free_port() for _ in range(3)]
        self.cli = [free_port() for _ in range(3)]
        self.procs = []

    def start(self):
        pq = pq_bin()
        for i in range(3):
            peers = ",".join(f"{j}=127.0.0.1:{self.peer[j]}" for j in range(3) if j != i)
            cmd = [pq, "replica", "--id", str(i), "--dir", os.path.join(self.dir, f"r{i}"), "--peer-listen", f"127.0.0.1:{self.peer[i]}",
                   "--client-listen", f"127.0.0.1:{self.cli[i]}", "--peers", peers, "--mem-cap-mb", "256"]
            self.procs.append(subprocess.Popen(cmd, stdout=open(os.path.join(self.dir, f"r{i}.log"), "ab"), stderr=subprocess.STDOUT))
        return self

    @property
    def addrs(self):
        return ",".join(f"127.0.0.1:{p}" for p in self.cli)

    def stop(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
                p.wait()
        shutil.rmtree(self.dir, ignore_errors=True)


def engine(bucket, tag, log, faults, env=None):
    kept = tempfile.mkdtemp(prefix=f"{tag}kept")
    e = {"MONGO_TEST_FAULTS_FILE": faults}
    e.update(env or {})
    return Server(bucket, tag, idle=600, env=e,
                  extra=["--quaderno", log.addrs, "--quaderno-id", "tk", "--local-log-dir", kept, "--local-log-upload-secs", "1"])


def put_rules(path, *lines):
    # a new file each time (mtime changes: the engine reads the rules again)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write("".join(l + "\n" for l in lines))
    os.replace(tmp, path)


def insert(srv, ids):
    c = srv.client()
    try:
        for i in ids:
            c[DB]["t"].insert_one({"_id": i, "v": "x" * 40})
    finally:
        c.close()


def count_after(lines_from, srv, what, timeout=60):
    return wait_until(lambda: re.search(what, srv.log_text()[lines_from:]), timeout, what=what)


def read_all(srv, timeout=120):
    """Every _id of the database, through a takeover (the lease of a killed process expires first)."""
    def once():
        c = srv.client()
        try:
            return sorted(d["_id"] for d in c[DB]["t"].find({}, {"_id": 1}))
        finally:
            c.close()
    return wait_until(once, timeout, every=0.5, what="the database to answer after the takeover")


def run(old):
    log = Log().start()
    bucket = make_bucket("tkr")
    fa, fb = tempfile.mktemp(prefix="tkrfa"), tempfile.mktemp(prefix="tkrfb")
    put_rules(fa)
    put_rules(fb)
    b = engine(bucket, "tkrb", log, fb)
    a = engine(bucket, "tkra", log, fa, env={"MONGO_TEST_KEPT_RETRY_OLD": "1"} if old else None)
    try:
        b.start()
        w0 = [f"w0-{i}" for i in range(W)]
        insert(b, w0)
        time.sleep(3)                                   # W0 uploaded and confirmed to the log (uploads every 1 s)
        # 1. B's uploads held: W1 only in the log
        put_rules(fb, "hang_put /wal/", "log_put /wal/")
        time.sleep(1.5)
        w1 = [f"w1-{i}" for i in range(W)]
        insert(b, w1)
        wait_until(lambda: "FAULT hang_put" in b.log_text(), 30, what="B's upload of W1 to be held")
        # 2. A starts, reads W1, its replay is refused (B holds the lease), and it stops before keeping the records
        put_rules(fa, "hold_kept_records 1")
        a.start(wait=False)
        wait_until(lambda: f"FAULT hold_kept_records {DB}" in a.log_text(), 60, what="A to stop at hold_kept_records")
        m = re.search(r"restart read (\d+) records", a.log_text())
        assert m and int(m.group(1)) >= W, f"A did not read W1 from the log: {m and m.group(0)}"
        assert re.search(rf"replay of database {DB} failed \(tenant {DB} is owned by server process", a.log_text()), "A's replay was not refused by B's lease"
        # 3. B's uploads go on: W1 reaches object storage; then held again
        mark = len(b.log_text())
        put_rules(fb, "log_put /wal/")
        wait_until(lambda: re.search(r"PUTLOG ok \S*/wal/", b.log_text()[mark:]), 30, what="B's held upload to finish")
        time.sleep(4)                                   # every 1 s an upload: all of W1 is stored and confirmed by B
        put_rules(fb, "hang_put /wal/", "log_put /wal/")
        time.sleep(1.5)
        # 4. B acknowledges W2: in the log only
        mark = len(b.log_text())
        w2 = [f"w2-{i}" for i in range(W)]
        insert(b, w2)
        wait_until(lambda: "FAULT hang_put" in b.log_text()[mark:], 30, what="B's upload of W2 to be held")
        # 5, 6. A goes on; its retry finds W1 stored (B's marker) and lets the database open again
        mark_a = len(a.log_text())
        put_rules(fa)
        a.wait_ready(60)
        count_after(mark_a, a, rf"database {DB}: kept records .*by the retry", 60)
        time.sleep(1)                                   # the retry's message comes before its confirmation to the log
        # 7. B killed before W2's upload; A takes over
        b.kill9()
        acked = w0 + w1 + w2
        got = set(read_all(a))
        lost = [i for i in acked if i not in got]
        print(f"old={old}: acknowledged {len(acked)}, lost {len(lost)} (W2 lost {sum(1 for i in lost if i.startswith('w2-'))})")
        print("A:", "\n".join(l for l in a.log_text().splitlines() if "quaderno" in l or "local log" in l or "FAULT" in l))
        lost_after_restart = None
        if not lost:
            # durably: the takeover applied W2 from the log; a clean restart of A still has every write
            a.term()
            a.start()
            got = set(read_all(a))
            lost_after_restart = [i for i in acked if i not in got]
        return acked, lost, lost_after_restart
    finally:
        for s in (a, b):
            try:
                s.kill9()
            except Exception:
                pass
        log.stop()


def test_takeover_retry_keeps_writes():
    acked, lost, again = run(old=False)
    assert not lost, f"RULE 8: {len(lost)} of {len(acked)} acknowledged writes lost across the takeover (first: {lost[:5]})"
    assert again == [], f"RULE 8: {len(again)} acknowledged writes missing after a clean restart (first: {again[:5]})"


def test_control_old_retry_loses():
    # the old behaviour (a non-holder's copies, then its retry confirms a copy's position): W2 is dropped from the log
    acked, lost, _ = run(old=True)
    assert lost and all(i.startswith("w2-") for i in lost), f"the control did not lose W2 as expected: lost {lost[:10]}"
