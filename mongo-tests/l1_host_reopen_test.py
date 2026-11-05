"""L1: hostInfo of the real machine, and the reads of opening a database are not charged to a command.

Starts mongo-server (--store local, temporary directory, --idle-secs 2 so a database closes between rounds) and checks:
  1. hostInfo: numCores and numCoresAvailableToProcess = CPUs this process can use, memSizeMB = MemTotal of
     /proc/meminfo, memLimitMB <= memSizeMB, numPhysicalCores and numCpuSockets >= 1;
  2. after a database closed (idle), the first find of a document by _id and the same find again:
     - the second (warm) profiler entry reports 0 bytes read from the store;
     - the first (cold) entry reports only its own reads: the filters and indexes of the SSTs it probes and its data block
       (at most COLD_MAX bytes); a find of another key right after is printed (it loads the filters and indexes of the
       other SSTs it probes: the same bytes in every round);
     - serverStatus fenvix.backgroundReads grows (the reopen, the writer's open for the profiler entries).
Usage: python3 l1_host_reopen_test.py [binary path]   (default: ./target/release/mongo-server)
"""
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "target/release/mongo-server")
FAILS = []
IDLE = 2
ROUNDS = 6
# the first find by _id after a reopen loads the bloom filter and the index of every SST it probes (about 1 MB here, 40 MB
# of data) and one data block
COLD_MAX = 4 << 20


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def usable_cpus():
    try:
        n = len(os.sched_getaffinity(0))
    except AttributeError:
        n = os.cpu_count()
    # a cgroup v2 CPU quota lowers it further (Rust's available_parallelism does the same)
    try:
        q, p = open("/sys/fs/cgroup/cpu.max").read().split()
        if q != "max":
            n = min(n, max(1, int(q) // int(p)))
    except (OSError, ValueError):
        pass
    return n


def mem_total_mb():
    for line in open("/proc/meminfo"):
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) // 1024
    return None


def main():
    d = tempfile.mkdtemp(prefix="l1-")
    port = free_port()
    log = open(os.path.join(d, "server.log"), "a")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--store", "local", "--local-dir", os.path.join(d, "data"),
                          "--idle-secs", str(IDLE)], stdout=log, stderr=log)
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=5000)

        # 1. hostInfo
        h = c.admin.command("hostInfo")["system"]
        check(h["numCores"] == usable_cpus(), f"hostInfo numCores {h['numCores']} = usable CPUs {usable_cpus()}")
        check(h["numCoresAvailableToProcess"] == usable_cpus(), f"hostInfo numCoresAvailableToProcess {h['numCoresAvailableToProcess']}")
        check(h["memSizeMB"] == mem_total_mb(), f"hostInfo memSizeMB {h['memSizeMB']} = MemTotal {mem_total_mb()} MB")
        check(0 < h["memLimitMB"] <= h["memSizeMB"], f"hostInfo memLimitMB {h['memLimitMB']} <= memSizeMB")
        check(h["numPhysicalCores"] >= 1 and h["numCpuSockets"] >= 1, "hostInfo physical cores and sockets >= 1")

        # 2. a database with some MB in several SSTs and a WAL, profiler on
        db = c["l1db"]
        db.command("profile", 2)
        rnd = random.Random(7)
        n = 0
        for _ in range(40):
            db.items.insert_many([{"_id": n + i, "pad": rnd.randbytes(1000).hex()} for i in range(1000)])
            n += 1000
        def bg():
            return c.admin.command("serverStatus")["fenvix"]["backgroundReads"]
        bad_warm, bad_cold, bg_grew = [], [], 0
        for r in range(ROUNDS):
            time.sleep(IDLE + 4)  # the database closes (idle)
            before = bg()
            key = rnd.randrange(n)
            tag = f"round{r}"
            db.items.find_one({"_id": key}, comment=tag + "-cold")
            db.items.find_one({"_id": key}, comment=tag + "-warm")
            db.items.find_one({"_id": (key + n // 2) % n}, comment=tag + "-next")
            time.sleep(0.5)
            e = {x["command"]["comment"]: x for x in db["system.profile"].find({"command.comment": {"$regex": "^" + tag}})}
            cold, warm, nxt = e.get(tag + "-cold"), e.get(tag + "-warm"), e.get(tag + "-next")
            if cold is None or warm is None or nxt is None:
                check(False, f"{tag}: profiler entries found")
                continue
            cb = cold.get("storage", {}).get("data", {}).get("bytesRead", 0)
            wb = warm.get("storage", {}).get("data", {}).get("bytesRead", 0)
            nb = nxt.get("storage", {}).get("data", {}).get("bytesRead", 0)
            print(f"     {tag}: cold {cb} bytes, warm {wb} bytes, next key {nb} bytes, background reads +{bg()['bytes'] - before['bytes']} bytes")
            if wb != 0:
                bad_warm.append(wb)
            if cb > COLD_MAX:
                bad_cold.append(cb)
            if bg()["bytes"] > before["bytes"]:
                bg_grew += 1
        check(not bad_warm, f"warm find after a reopen reads 0 bytes in {ROUNDS} rounds (bad: {bad_warm})")
        check(not bad_cold, f"cold find after a reopen reads at most {COLD_MAX} bytes (bad: {bad_cold})")
        check(bg_grew == ROUNDS, f"background reads grew at every reopen ({bg_grew} of {ROUNDS})")
    finally:
        p.terminate()
        p.wait(30)
        shutil.rmtree(d, ignore_errors=True)
    print("FAILED: %d" % len(FAILS) if FAILS else "ALL PASSED")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
