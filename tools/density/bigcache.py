"""Extra to scenario 4 (memory): does the memory of an open database grow with the data it reads? K databases of
SIZE_MB each (1 KB documents), each read in full twice (find {} to the end), engine RSS after each pass. Fresh engine
with the scenario flags (--auth, TLS). Databases are named big0..big{K-1}.
usage: bigcache.py K SIZE_MB"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine
from setup_dbs import admin
from pymongo import MongoClient


def rss(pid):
    with open(f"/proc/{pid}/statm") as f:
        return round(int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20, 1)


def main():
    k, size = int(sys.argv[1]), int(sys.argv[2])
    p = engine.start("bigcache_load")
    c = MongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, username=ROOT_USER, password=ROOT_PW, authSource="admin")
    docs = size * 1000
    for i in range(k):
        st, _ = admin("POST", "/v1/databases", {"name": f"big{i}"})
        if st == 201:
            col = c[f"big{i}"].data
            for b in range(0, docs, 2000):
                col.insert_many([{"_id": j, "pad": "z" * 950} for j in range(b, b + 2000)])
    c.close()
    engine.stop(p, 600)
    p = engine.start("bigcache")
    c = MongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, username=ROOT_USER, password=ROOT_PW, authSource="admin")
    c.admin.command("ping")
    res = {"k": k, "size_mb_per_db": size, "rss_start_mb": rss(p.pid), "passes": []}
    for rnd in range(2):
        for i in range(k):
            t0 = time.time()
            n = sum(1 for _ in c[f"big{i}"].data.find({}, batch_size=5000))
            res["passes"].append({"round": rnd, "db": f"big{i}", "docs": n, "secs": round(time.time() - t0, 2), "rss_mb_after": rss(p.pid)})
            print(res["passes"][-1], flush=True)
    time.sleep(75)  # past --idle-secs: the databases close
    res["rss_after_idle_close_mb"] = rss(p.pid)
    c.close()
    engine.stop(p, 600)
    os.makedirs(os.path.join(RESULTS, "s4_bigcache"), exist_ok=True)
    with open(os.path.join(RESULTS, "s4_bigcache", "bigcache.json"), "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps(res))


if __name__ == "__main__":
    main()
