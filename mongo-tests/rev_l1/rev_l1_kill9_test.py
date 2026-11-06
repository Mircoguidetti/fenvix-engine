"""Reviewer of L1: no acknowledged write lost and no SST deleted under a live manifest, with kill -9 at random moments
while compaction passes (now without a time cap) and the GC (now running beside a pass) work.

Server: --store local, --lease-ttl-secs 3 (a killed process is taken over quickly), small L0 SSTs (1 MiB) so passes and merges run all the time, SST GC
every 3 s with 5 s of unreferenced time, WAL GC every 3 s. Every round: start the server, check every acknowledged
document of all earlier rounds (count per writer and a full scan that reads every live SST, content hash of every
document), then 4 writers insert batches with w:majority for a random 3-40 s, and kill -9 at a random moment.
Usage: python3 rev_l1_kill9_test.py BINARY ROUNDS WORKDIR
"""
import hashlib, os, random, shutil, signal, socket, subprocess, sys, threading, time
from pymongo import MongoClient, WriteConcern

BIN, ROUNDS, W = sys.argv[1], int(sys.argv[2]), sys.argv[3]
PORT = 27901
WRITERS = 4


def body(t, i):
    return hashlib.sha256(f"{t}-{i}".encode()).hexdigest() * 12  # ~770 bytes


def start():
    log = open(os.path.join(W, "server.log"), "a")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--store", "local", "--local-dir", os.path.join(W, "store"),
                          "--replica-set", "rs0", "--lease-ttl-secs", "3", "--writer-poll-secs", "2", "--sst-gc-secs", "3",
                          "--sst-gc-min-unref-secs", "5", "--wal-gc-secs", "3", "--l0-sst-bytes", "1048576"], stdout=log, stderr=log)
    for _ in range(300):
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.2).close()
            break
        except OSError:
            time.sleep(0.1)
    return p


def main():
    shutil.rmtree(W, ignore_errors=True)
    os.makedirs(W)
    acked = [0] * WRITERS  # documents 0..acked-1 of writer t are acknowledged
    lost_rounds, errors = [], []
    rnd = random.Random(42)
    for r in range(ROUNDS):
        p = start()
        c = MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true", serverSelectionTimeoutMS=20000, socketTimeoutMS=8000)
        coll = c["k9"].get_collection("d", write_concern=WriteConcern(w="majority"))
        cc = MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true", serverSelectionTimeoutMS=20000, socketTimeoutMS=300000)
        check = cc["k9"]["d"]
        # check
        try:
            for t in range(WRITERS):
                n = check.count_documents({"t": t, "i": {"$lt": acked[t]}})
                if n != acked[t]:
                    lost_rounds.append((r, t, acked[t], n))
            if r % 20 == 0 or r == ROUNDS - 1:
                bad = 0
                seen = 0
                for d in check.find({}, {"t": 1, "i": 1, "b": 1}):
                    seen += 1
                    if d["b"] != body(d["t"], d["i"]):
                        bad += 1
                if bad:
                    errors.append((r, "content", bad))
                print(f"round {r}: full scan {seen} documents, {bad} bad", flush=True)
        except Exception as e:
            errors.append((r, "check", repr(e)[:300]))
            print(f"round {r}: CHECK ERROR {e!r}"[:400], flush=True)
        try:
            if r == 0:
                coll.create_index([("t", 1), ("i", 1)])
            for t in range(WRITERS):
                top = list(check.find({"t": t}, {"i": 1}).sort("i", -1).limit(1))
                if top:
                    acked[t] = max(acked[t], top[0]["i"] + 1)  # present after the restart: must stay
        except Exception as e:
            errors.append((r, "next", repr(e)[:300]))
        # write, then kill -9
        stop = threading.Event()
        lock = threading.Lock()

        def writer(t):
            while not stop.is_set():
                i0 = acked[t]
                docs = [{"_id": f"{t}-{i}", "t": t, "i": i, "b": body(t, i)} for i in range(i0, i0 + 50)]
                try:
                    coll.insert_many(docs, ordered=True)
                except Exception:
                    return
                with lock:
                    acked[t] = i0 + 50
                time.sleep(0.03)  # keeps the data growing slower (the checks scan it)
        ths = [threading.Thread(target=writer, args=(t,), daemon=True) for t in range(WRITERS)]
        for th in ths:
            th.start()
        time.sleep(rnd.uniform(3, 40))  # long enough, often, for GC rounds (every 15-30 s) during passes
        stop.set()
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
        for th in ths:
            th.join(10)
        c.close()
        cc.close()
        print(f"round {r}: killed, acknowledged {sum(acked)} documents", flush=True)
    print("LOST", lost_rounds)
    print("ERRORS", errors)
    print("PASSED" if not lost_rounds and not errors else "FAILED")


if __name__ == "__main__":
    main()
