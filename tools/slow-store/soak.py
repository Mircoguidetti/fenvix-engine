"""P-fix-slow-store item 4: the fast write/read workload on a store with S3's errors and tails, and the list of stalls.

    python3 tools/slow-store/soak.py --binary target/release/mongo-server --secs 1800 --out <dir>

Starts MinIO's chaos forwarder (chaosproxy.py: --fail-pct 1, --tail-pct 5, --tail-ms 200 by default), one mongo-server
through it (sealed runs, compactor, derive, zstd: the judge's sealed2-derive environment; FX_WAITS=1000 so that every
command of 1 s or more prints where its time went), then `--procs` client processes of `--threads` threads each. Every
thread works on one of `--dbs` databases: 50% insert_one of a new 1 KB document, 20% update_one $inc of a recent one,
30% find_one of a recent one, back to back. Every operation of 1 s or more is written to stalls.jsonl with its start, end,
kind, database and outcome. At the end every acknowledged insert must read back and every counter must hold at least its
acknowledged increments (rule 8), and the client processes' CPU is reported (the sender must not be the limit).
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import resource
import signal
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "ctl-tests"))
sys.path.insert(0, HERE)


def worker(uri, dbs, threads, secs, out, pid):
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
    c = MongoClient(uri, maxPoolSize=threads + 2, socketTimeoutMS=300000, serverSelectionTimeoutMS=60000)
    stop = time.time() + secs
    lock = threading.Lock()
    stalls = open(os.path.join(out, f"stalls-{pid}.jsonl"), "a", buffering=1)
    acked = {}      # db -> [ids]
    incs = {}       # (db, id) -> acknowledged increments
    errs = {}       # (db, id) -> increments answered with an error (outcome unknown)
    counts = {"insert": 0, "update": 0, "find": 0, "errors": 0}
    lat_max = {"insert": 0.0, "update": 0.0, "find": 0.0}
    per_sec = {}

    def run(t):
        db = dbs[(pid * threads + t) % len(dbs)]
        coll = c[db].t
        rng = random.Random(pid * 1000 + t)
        mine = []
        nxt = (pid * 1000 + t) * 10_000_000
        while time.time() < stop:
            r = rng.random()
            if r < 0.5 or not mine:
                kind, _id = "insert", nxt
                nxt += 1
            elif r < 0.7:
                kind, _id = "update", rng.choice(mine[-200:])
            else:
                kind, _id = "find", rng.choice(mine[-200:])
            t0 = time.time()
            ok, err = True, None
            try:
                if kind == "insert":
                    coll.insert_one({"_id": _id, "n": 0, "pad": "p" * 1000})
                elif kind == "update":
                    coll.update_one({"_id": _id}, {"$inc": {"n": 1}})
                else:
                    coll.find_one({"_id": _id})
            except PyMongoError as e:
                ok, err = False, (getattr(e, "code", None), str(e)[:200])
            t1 = time.time()
            d = t1 - t0
            with lock:
                counts[kind] += 1
                lat_max[kind] = max(lat_max[kind], d)
                sec = int(t1)
                per_sec[sec] = per_sec.get(sec, 0) + 1
                if ok and kind == "insert":
                    acked.setdefault(db, []).append(_id)
                    mine.append(_id)
                elif ok and kind == "update":
                    incs[(db, _id)] = incs.get((db, _id), 0) + 1
                elif not ok:
                    counts["errors"] += 1
                    if kind == "update":
                        errs[(db, _id)] = errs.get((db, _id), 0) + 1
                    if kind == "insert":
                        nxt += 0   # a failed insert's id is never reused
                if d >= 1.0 or not ok:
                    stalls.write(json.dumps({"t0": round(t0, 3), "t1": round(t1, 3), "ms": round(d * 1000), "kind": kind, "db": db, "id": _id, "ok": ok, "err": err, "pid": pid, "thread": t}) + "\n")

    ths = [threading.Thread(target=run, args=(t,)) for t in range(threads)]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    ru = resource.getrusage(resource.RUSAGE_SELF)
    json.dump({"pid": pid, "counts": counts, "lat_max_s": lat_max, "cpu_s": ru.ru_utime + ru.ru_stime, "secs": secs,
               "acked": acked, "incs": [[k[0], k[1], v] for k, v in incs.items()], "errs": [[k[0], k[1], v] for k, v in errs.items()],
               "per_sec": per_sec}, open(os.path.join(out, f"worker-{pid}.json"), "w"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True)
    ap.add_argument("--secs", type=int, default=1800)
    ap.add_argument("--dbs", type=int, default=8)
    ap.add_argument("--procs", type=int, default=3)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--fail-pct", type=float, default=1.0)
    ap.add_argument("--tail-pct", type=float, default=5.0)
    ap.add_argument("--tail-ms", type=int, default=200)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", required=True)
    ap.add_argument("--extra", default="", help="more server flags")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    for k, v in {"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0", "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1",
                 "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_DERIVE": "1", "FENVIX_SEALED_CODEC": "zstd", "FX_WAITS": "1000"}.items():
        os.environ[k] = v
    from lib import Server, make_bucket, MINIO, free_port
    from chaosproxy import ChaosProxy
    host, port = MINIO.split("//")[1].split(":")
    px = ChaosProxy((host, int(port)), 0, a.fail_pct, a.tail_pct, a.tail_ms, os.path.join(a.out, "faults.log"), a.seed)
    bucket = make_bucket("soak")
    sport = free_port()
    srv = Server(bucket, "soak", idle=600, endpoint=px.endpoint, port=sport, binary=a.binary,
                 extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{sport}"] + a.extra.split()).start(wait=False)
    srv.wait_ready(120)
    dbs = [f"soak{i}" for i in range(a.dbs)]
    print(f"server {srv.uri} log {srv.log_path} bucket {bucket}; proxy {px.endpoint}: {a.fail_pct}% 503, {a.tail_pct}% +{a.tail_ms} ms", flush=True)
    # a side probe: every 200 ms one small PUT straight to MinIO and one through the forwarder (no fault injected on its
    # path), each of 500 ms or more recorded: a stall seen by both is the store's (MinIO, the disk); one seen only through
    # the forwarder is the forwarder's; one seen by neither, while clients stall, is the server's
    import boto3
    from botocore.config import Config
    probe_stop = threading.Event()
    px.exclude = "/soak-probe/"
    def probe():
        cfg = Config(retries={"max_attempts": 1}, connect_timeout=30, read_timeout=60)
        cli = {name: boto3.client("s3", endpoint_url=ep, aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1", config=cfg)
               for name, ep in (("direct", MINIO), ("proxy", px.endpoint))}
        f = open(os.path.join(a.out, "store-probe.jsonl"), "a", buffering=1)
        n = 0
        while not probe_stop.is_set():
            for name, cl in cli.items():
                t0 = time.time()
                try:
                    cl.put_object(Bucket=bucket, Key=f"soak-probe/{name}/{n % 50}", Body=b"x" * 512)
                    ok = True
                except Exception:
                    ok = False
                d = time.time() - t0
                if d >= 0.5 or not ok:
                    f.write(json.dumps({"t0": round(t0, 3), "ms": round(d * 1000), "path": name, "ok": ok}) + "\n")
            n += 1
            time.sleep(0.2)
    pt = threading.Thread(target=probe, daemon=True)
    pt.start()
    t_start = time.time()
    ps = [mp.Process(target=worker, args=(srv.uri, dbs, a.threads, a.secs, a.out, p)) for p in range(a.procs)]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    t_end = time.time()
    probe_stop.set()
    # rule 8: every acknowledged insert and increment, read back on the same server through a store without faults
    px.fail_pct = px.tail_pct = 0
    from pymongo import MongoClient
    c = MongoClient(srv.uri)
    missing, short, over, n_ins, n_inc = 0, 0, 0, 0, 0
    cpu = []
    # one sequential scan per database (random reads of a million ids cost thousands of GETs per batch)
    stored = {db: {x["_id"]: x.get("n", 0) for x in c[db].t.find({}, {"n": 1}, batch_size=10000)} for db in dbs}
    for p in range(a.procs):
        w = json.load(open(os.path.join(a.out, f"worker-{p}.json")))
        cpu.append(w["cpu_s"] / a.secs)
        errs = {(d, i): v for d, i, v in w["errs"]}
        for db, ids in w["acked"].items():
            missing += sum(1 for i in ids if i not in stored[db])
            n_ins += len(ids)
        for db, i, v in w["incs"]:
            n_inc += v
            g = stored[db].get(i, -1)
            short += g < v
            over += g > v + errs.get((db, i), 0)
    summary = {"secs": round(t_end - t_start), "acked_inserts": n_ins, "acked_increments": n_inc, "missing_inserts": missing,
               "counters_below_acked": short, "counters_above_acked_plus_errors": over, "client_cpu_share_per_proc": [round(x, 3) for x in cpu],
               "proxy": px.counts, "server_log": srv.log_path, "bucket": bucket}
    json.dump(summary, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    print(json.dumps(summary, indent=1), flush=True)
    srv.proc.send_signal(signal.SIGTERM)
    try:
        srv.proc.wait(60)
    except Exception:
        srv.kill9()
    os.system(f"cp {srv.log_path} {os.path.join(a.out, 'server.log')}")
    px.close()


if __name__ == "__main__":
    main()
