"""Reviewer of L1: are reads of commands charged to background while the profiler writes its entries?

meter::background(db, write_profile) marks the whole database `db` as "in background" (IN_BACKGROUND) while the profiler
task writes a batch. While it is marked, every GET made for `db` in a task slatedb spawns (the block reads of a scan,
counting::request_owner) goes to serverStatus fenvix.backgroundReads and not to the command running at that moment.
The profiler task runs right after each profiled reply, so it overlaps the next command of the benchmark script.

The test (server --store local, small block cache, --idle-secs 600 so no reopen happens during the measure):
  1. loads N documents with an index {c: 1, ts: -1}, restarts the server (cold cache);
  2. warms the database open (one find on another collection-free key) and the writer (one profiled insert);
  3. runs Q sequential cold index scans find({c: k}).sort(ts: -1).limit(50), profiler at 2, distinct k each;
  4. compares: sum of the profiler bytesRead of these finds, growth of backgroundReads, growth of total store getBytes.
  5. control: the same with profiler 0 on distinct k (backgroundReads must not grow: nothing runs in background).
Usage: python3 rev_l1_profiler_bg_test.py BINARY
"""
import os, random, shutil, socket, subprocess, sys, tempfile, time
from pymongo import MongoClient

BIN = sys.argv[1]
N, CLIENTS, Q = 200_000, 4000, 300


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def start(d, port):
    log = open(os.path.join(d, "server.log"), "a")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--store", "local", "--local-dir", os.path.join(d, "data"),
                          "--idle-secs", "600", "--block-cache-mb", "8", "--meta-cache-mb", "64"], stdout=log, stderr=log)
    for _ in range(200):
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close(); break
        except OSError:
            time.sleep(0.1)
    return p, MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=10000)


def st(c):
    f = c.admin.command("serverStatus")["fenvix"]
    return f.get("backgroundReads", {}).get("bytes", 0), f["s3"]["getBytes"], f.get("backgroundReads", {}).get("gets", 0), f["s3"]["get"] + f["s3"]["getRange"]


def main():
    d = tempfile.mkdtemp(prefix="revl1-")
    port = free_port()
    p, c = start(d, port)
    db = c["revl1"]
    rnd = random.Random(1)
    db.items.create_index([("c", 1), ("ts", -1)])
    for b in range(0, N, 2000):
        db.items.insert_many([{"_id": b + i, "c": rnd.randrange(CLIENTS), "ts": b + i, "pad": rnd.randbytes(400).hex()} for i in range(2000)])
    c.close(); p.terminate(); p.wait(60)
    p, c = start(d, port)
    db = c["revl1"]
    try:
        db.items.find_one({"_id": 1})          # the open (reopen) of the database: background by design
        db.command("profile", 2)
        db.other.insert_one({"x": 1})          # the writer (upgrade): background by design
        time.sleep(3)
        keys = list(range(CLIENTS)); rnd.shuffle(keys)
        res = {}
        for label, level, ks in (("profiler 2", 2, keys[:Q]), ("profiler 0", 0, keys[Q:2 * Q]), ("profiler 2 again", 2, keys[2 * Q:3 * Q])):
            db.command("profile", level)
            time.sleep(2)
            bg0, get0, bgn0, n0 = st(c)
            for i, k in enumerate(ks):
                list(db.items.find({"c": k}, comment=f"{label}-{i}").sort("ts", -1).limit(50))
            time.sleep(3)
            bg1, get1, bgn1, n1 = st(c)
            prof = 0
            if level:
                db.command("profile", 0)
                ents = list(db["system.profile"].find({"command.comment": {"$regex": "^" + label + "-"}}))
                prof = sum(e.get("storage", {}).get("data", {}).get("bytesRead", 0) for e in ents)
                pg = sum(e.get("fenvixS3Gets", 0) for e in ents)
                print(f"{label}: profiler GETs sum {pg}, store GETs +{n1 - n0}, background GETs +{bgn1 - bgn0}, shared entries {sum(1 for e in ents if e.get('fenvixS3Shared'))}")
                print(f"{label}: entries {len(ents)} of {len(ks)}")
            res[label] = (prof, bg1 - bg0, get1 - get0)
            print(f"{label}: profiler bytesRead sum {prof}, backgroundReads +{bg1 - bg0}, store getBytes +{get1 - get0}")
        p2, bg2, g2 = res["profiler 2"]
        _, bg0_, g0 = res["profiler 0"]
        print(f"RESULT profiler2: share of store bytes charged to background {bg2 / max(g2,1):.1%}, to the commands {p2 / max(g2,1):.1%}")
        print(f"RESULT profiler0: backgroundReads +{bg0_} (expected 0)")
    finally:
        p.terminate(); p.wait(60)
        print("server log:", os.path.join(d, "server.log"))


if __name__ == "__main__":
    main()
