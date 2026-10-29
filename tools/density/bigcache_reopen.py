"""Follow-up of bigcache.py: after the databases close for idleness the RSS did not go down. Is that memory reused
(allocator keeps it) or lost (it grows again at every reopen)? Fresh engine; CYCLES times: read the K big databases in
full, wait past --idle-secs (they close), record RSS. Uses the big* databases bigcache.py created.
usage: bigcache_reopen.py K CYCLES"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine
from bigcache import rss
from pymongo import MongoClient


def main():
    k, cycles = int(sys.argv[1]), int(sys.argv[2])
    p = engine.start("bigcache_reopen")
    c = MongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, username=ROOT_USER, password=ROOT_PW, authSource="admin")
    c.admin.command("ping")
    res = {"k": k, "rss_start_mb": rss(p.pid), "cycles": []}
    for cy in range(cycles):
        for i in range(k):
            sum(1 for _ in c[f"big{i}"].data.find({}, batch_size=5000))
        after_read = rss(p.pid)
        time.sleep(75)
        res["cycles"].append({"cycle": cy, "rss_after_reading_all_mb": after_read, "rss_after_idle_close_mb": rss(p.pid)})
        print(res["cycles"][-1], flush=True)
    c.close()
    engine.stop(p, 600)
    with open(os.path.join(RESULTS, "s4_bigcache", "bigcache_reopen.json"), "w") as f:
        json.dump(res, f, indent=1)


if __name__ == "__main__":
    main()
