#!/usr/bin/env python3
"""Can a read return data that is lost by a crash? Writers insert documents with increasing ids; readers poll for the
document each writer is inserting right now. A document a reader saw *before its insert was acknowledged* is a read of
a batch that was staged and maybe not yet durable. After SIGKILL and restart every document a reader saw must be
present. With --durable-reads false (negative control) the readers do see staged data and some of what they saw is lost.

    test_durable_reads.py [--trials 8] [--threads 16] [--flags "--durable-reads false"]
"""
import argparse, json, os, random, signal, subprocess, threading, time

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("BIN", os.path.join(ROOT, "target", "release", "mongo-server"))
OUT = os.path.join(ROOT, "loadtest", "out", "durable-reads"); RES = os.path.join(ROOT, "loadtest", "results", "durable-reads")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
EP = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
PORT = 28101


def start(flags, tag):
    import socket
    with socket.socket() as so:
        if so.connect_ex(("127.0.0.1", PORT)) == 0:
            raise RuntimeError("port in use")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "R", "--lease-ttl-secs", "30", "--endpoint", EP] + flags,
                         stdout=open(os.path.join(OUT, f"{tag}.log"), "wb"), stderr=subprocess.STDOUT)
    for _ in range(300):
        try:
            MongoClient(f"mongodb://127.0.0.1:{PORT}", serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("no start")


def trial(n, threads, flags):
    p = start(flags, f"t{n}")
    uri = f"mongodb://127.0.0.1:{PORT}"
    db = f"dr{n}_{int(time.time())}"
    c = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=3000, maxPoolSize=threads * 2 + 4)
    c[db].c.insert_one({"_id": -1})
    attempt = [0] * threads  # id each writer is inserting now
    acked = [-1] * threads
    seen_early, acked_ids, lock, stop = set(), set(), threading.Lock(), threading.Event()

    def writer(w):
        i = 0
        while not stop.is_set():
            attempt[w] = i
            try:
                c[db].c.insert_one({"_id": w * 1_000_000 + i})
            except Exception:
                return
            with lock: acked_ids.add(w * 1_000_000 + i)
            acked[w] = i
            i += 1

    def reader(w):
        while not stop.is_set():
            i = attempt[w]
            _id = w * 1_000_000 + i
            try:
                d = c[db].c.find_one({"_id": _id})
            except Exception:
                return
            if d is not None and acked[w] < i:  # seen before its insert was acknowledged
                with lock: seen_early.add(_id)
    ts = [threading.Thread(target=writer, args=(w,)) for w in range(threads)] + [threading.Thread(target=reader, args=(w,)) for w in range(threads)]
    [t.start() for t in ts]
    time.sleep(random.uniform(3.0, 5.0))
    p.send_signal(signal.SIGKILL); p.wait()
    stop.set(); [t.join() for t in ts]
    p2 = start([], f"t{n}-recover")
    c2 = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=60000)
    present = {d["_id"] for d in c2[db].c.find({}, {"_id": 1})}
    p2.terminate(); p2.wait(60)
    lost_ack = [i for i in acked_ids if i not in present]
    lost_seen = [i for i in seen_early if i not in present]
    return {"acked": len(acked_ids), "seen_before_ack": len(seen_early), "lost_acked": len(lost_ack), "seen_before_ack_but_lost": len(lost_seen)}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--trials", type=int, default=8); ap.add_argument("--threads", type=int, default=16); ap.add_argument("--flags", default="")
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    rs = []
    for n in range(a.trials):
        r = trial(n, a.threads, a.flags.split()); rs.append(r); print(n, r, flush=True)
    tot = {k: sum(r[k] for r in rs) for k in rs[0]}
    print("TOTAL", a.flags or "(default)", tot)
    json.dump({"flags": a.flags, "trials": rs, "total": tot}, open(os.path.join(RES, f"reads{a.tag}.json"), "w"), indent=1)


main()
