#!/usr/bin/env python3
"""fix-customer-id-plan: the big-speed round's measurement of the defect, before and after, on the same bucket.

The bucket of ctl-tests/test_81 (one 1.5 GiB customer `big` of compressible 256 KiB documents, a 64 MiB customer `rnd`
of random ones, a small customer, moved into our runs by publish-drop), then for each binary a cold server (test_81's
flags, no cgroup) times the queries one after another, with documents examined (explain) and the server's peak RSS.
Usage: bigcust.py --load BUCKET_TAG | bigcust.py BUCKET LABEL BINARY"""
import os, sys, threading, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from lib import Server, make_bucket  # noqa: E402

MIB = int(os.environ.get("BIG_MIB", "1536"))
X = ["--no-sealed-compactor", "--sealed-runs", "1000000", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000",
     "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait", "--memory-limit-mb", "1024"]
ENV = {"FENVIX_TEST_DROP": "1"}


def load():
    b = make_bucket("bigc")
    s = Server(b, "bigcload", idle=600, extra=X, env=ENV).start()
    db = s.client().app
    blob = "y" * (256 * 1024)
    n = MIB * 4
    for k in range(0, n, 16):
        db.checkpoints.insert_many([{"_id": f"big-{j:06d}", "thread_id": "big", "v": j, "b": blob + str(j)} for j in range(k, min(n, k + 16))])
    for k in range(0, 64 * 4, 16):
        db.checkpoints.insert_many([{"_id": f"rnd-{j:06d}", "thread_id": "rnd", "v": j, "b": os.urandom(256 * 1024)} for j in range(k, min(64 * 4, k + 16))])
    db.checkpoints.insert_many([{"_id": f"small-{j}", "thread_id": "small", "v": j} for j in range(10)])
    st, r = s.admin.post("/v1/test/databases/app/publish-drop", {})
    assert st == 200, (st, r)
    s.term()
    print(b)


def rss_mib(pid):
    try:
        for line in open(f"/proc/{pid}/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) // 1024
    except OSError:
        return -1
    return 0


def measure(bucket, label, binary):
    n = MIB * 4
    Q = [
        ("find_one big customer and _id", {"thread_id": "big", "_id": "big-000005"}, {"limit": 1}),
        ("find big customer and _id >= last 3", {"thread_id": "big", "_id": {"$gte": f"big-{n - 3:06d}"}}, {"batchSize": 5}),
        ("find_one rnd customer and _id", {"thread_id": "rnd", "_id": "rnd-000100"}, {"limit": 1}),
        ("find small customer", {"thread_id": "small"}, {}),
        ("find_one small customer", {"thread_id": "small"}, {"limit": 1}),
        ("find_one big customer", {"thread_id": "big"}, {"limit": 1}),
    ]
    for name, f, kw in Q:
        s = Server(bucket, f"bigc{label}", idle=600, extra=X, env=ENV, binary=binary).start()   # cold: a new process per query
        pid, peak, stop = s.proc.pid, [0], [False]

        def watch():
            while not stop[0]:
                r = rss_mib(pid)
                if r < 0:
                    return
                peak[0] = max(peak[0], r)
                time.sleep(0.05)

        threading.Thread(target=watch, daemon=True).start()
        db = s.client().app
        t0 = time.time()
        try:
            cur = db.checkpoints.find(f, {"b": 0})
            if "limit" in kw:
                cur = cur.limit(kw["limit"])
            if "batchSize" in kw:
                cur = cur.batch_size(kw["batchSize"])
            got = [d["v"] for d in cur]
            err = ""
        except Exception as e:
            got, err = [], f" error {type(e).__name__}: {str(e)[:120]}"
        ms = (time.time() - t0) * 1000
        try:
            e = db.command("explain", {"find": "checkpoints", "filter": f, "projection": {"b": 0}, **({"limit": kw["limit"]} if "limit" in kw else {})}, verbosity="executionStats")
            x = e["executionStats"]["totalDocsExamined"]
        except Exception as e2:
            x = f"explain error {str(e2)[:80]}"
        stop[0] = True
        print(f"{label:5} {name:38} {ms:9.1f} ms  examined {x}  returned {got[:5]}{'...' if len(got) > 5 else ''} ({len(got)})  peak RSS {peak[0]} MiB{err}", flush=True)
        s.term()


if __name__ == "__main__":
    if sys.argv[1] == "--load":
        load()
    else:
        measure(sys.argv[1], sys.argv[2], sys.argv[3])
