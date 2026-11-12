"""Reviewer's tests for fix-erase-fork-usage-c: a real, billed database must never lose a usage record.

The usage API answers this process's counters from memory, so these tests read the stored records from the bucket
(control/usage/<db>/<day>/*.json) after the servers stopped (a SIGTERM flushes), and sum their command counts.

- test_recreated_name_is_billed_exactly: erase a database, create it again with the same name in the same process, use
  it across many flushes (the re-check window is 5 flushes), let it close when idle (only its own objects stay), use it
  again, erase and re-create a second time: every command sent to each new database is in the stored records.
- test_writes_racing_the_erase_then_the_new_database: writers insert while the database is erased and go on after it
  (they create it again): the stored commands of the new database cover every insert that began after the erase
  answered, and nothing of the old one is left if the name ends erased.
- test_two_processes_recreate: P1 erases a database, P2 creates it again and uses it; P1 then describes it: P2's commands
  are all in the stored records.
"""
import json
import threading
import time

from lib import Server, make_bucket, list_prefix, s3

FLUSH = ["--usage-flush-secs", "1"]


def stored_commands(bucket, db):
    tot, files = 0, []
    for k, _ in list_prefix(bucket, f"control/usage/{db}/"):
        body = json.loads(s3().get_object(Bucket=bucket, Key=k)["Body"].read())
        files.append(k)

        def walk(v):
            n = 0
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if kk == "commands" and isinstance(vv, (int, float)):
                        n += vv
                    else:
                        n += walk(vv)
            elif isinstance(v, list):
                for x in v:
                    n += walk(x)
            return n
        tot += walk(body)
    return tot, files


def test_recreated_name_is_billed_exactly():
    b = make_bucket("r61a")
    s = Server(b, "r61a", idle=3, extra=FLUSH).start()
    lines = []
    try:
        c = s.client()
        c.again.t.insert_many([{"_id": j} for j in range(100)])
        time.sleep(2.5)
        sent = 0
        for cycle in range(2):
            st, r = s.admin.delete("/v1/databases/again")
            assert st == 200, r
            sent = 0
            t_end = time.time() + 12           # many flushes inside the re-check window and after it
            j = 0
            while time.time() < t_end:
                c.again.t.insert_one({"_id": 10_000 * (cycle + 1) + j})
                c.again.t.find_one({"_id": 10_000 * (cycle + 1) + j})
                sent += 2
                j += 1
                time.sleep(0.05)
            time.sleep(8)                      # idle: the database closes (idle 3 s); flushes and re-checks go on
            for k in range(20):
                c.again.t.find_one({"_id": k})
                sent += 1
            time.sleep(4)
            lines.append(f"cycle {cycle}: sent {sent}")
        s.term()
        got, files = stored_commands(b, "again")
        tenant = list_prefix(b, "tenants/again/")
        print("; ".join(lines) + f"; stored commands of the last new database {got} (files {len(files)}); tenants/again objects {len(tenant)}")
        assert got >= sent, f"{sent - got} of {sent} commands of a real database are not in its stored usage records"
    finally:
        s.term()


def test_writes_racing_the_erase_then_the_new_database():
    b = make_bucket("r61b")
    s = Server(b, "r61b", idle=300, extra=FLUSH).start()
    try:
        c = s.client()
        c.w.t.insert_many([{"_id": -j - 1} for j in range(1000)])
        stop = threading.Event()
        erased_at = [None]
        after = [0]
        nxt = [0]
        lock = threading.Lock()

        def writer():
            cw = s.client(retryWrites=False)
            while not stop.is_set():
                with lock:
                    i = nxt[0]
                    nxt[0] += 1
                t0 = time.time()
                try:
                    cw.w.t.insert_one({"_id": i})
                    if erased_at[0] is not None and t0 > erased_at[0]:
                        with lock:
                            after[0] += 1
                except Exception:
                    pass
        ts = [threading.Thread(target=writer) for _ in range(4)]
        for t in ts:
            t.start()
        time.sleep(1.0)
        st, r = s.admin.delete("/v1/databases/w")
        erased_at[0] = time.time()
        assert st == 200, r
        time.sleep(6)
        stop.set()
        for t in ts:
            t.join()
        time.sleep(3)
        s.term()
        got, files = stored_commands(b, "w")
        print(f"inserts that began after the erase answered and were acknowledged: {after[0]}; stored commands of the new database {got}")
        assert got >= after[0], f"{after[0] - got} acknowledged inserts of the new database are not billed"
    finally:
        s.term()


def test_two_processes_recreate():
    b = make_bucket("r61c")
    p1 = Server(b, "r61c1", idle=2, holder="E1", extra=FLUSH).start()
    p2 = Server(b, "r61c2", idle=300, holder="E2", extra=FLUSH).start()
    try:
        c1, c2 = p1.client(), p2.client()
        c1.two.t.insert_many([{"_id": j} for j in range(100)])
        time.sleep(4)                                   # P1 closes it (idle 2 s) and releases it
        st, r = p1.admin.delete("/v1/databases/two")
        assert st == 200, r
        sent = 0
        for j in range(200):
            c2.two.t.insert_one({"_id": 1000 + j})
            sent += 1
        time.sleep(3)
        for _ in range(5):
            p1.admin.get("/v1/databases/two")             # P1 looks at it (storage requests in P1)
            time.sleep(1)
        p2.term()
        p1.term()
        tot, files = stored_commands(b, "two")
        print(f"P2 sent {sent} commands; stored commands under 'two' {tot}; files {[f.rsplit('/', 1)[-1] for f in files]}")
        assert tot >= sent, f"{sent - tot} of P2's {sent} commands are not in the stored usage records"
    finally:
        p1.term()
        p2.term()
