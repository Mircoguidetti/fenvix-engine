#!/usr/bin/env python3
"""Kill 10 index backfills and check that all 10 finish after the restart (results8).

One attempt: a collection of N documents, `createIndex` started in a thread, the server SIGKILLed a random 50-600 ms
later. The attempt counts only if the build was provably unfinished at the kill: the client's `createIndex` failed
and the index was not listed (an unfinished build is hidden from `listIndexes`) right after the restart, before
anything touched the collection. Then the collection is touched (which resumes the build) and the test waits for
the index to be listed, runs `validate` (every document has exactly its entries, no entry points to a missing
document), and compares the number of documents found through the index (`hint`) with the collection count.
Documents are also written during the build (before the kill) so the backfill must cope with concurrent writes.
Output: out/backfill_kill.json
"""
import argparse
import json
import os
import random
import threading
import time

import pymongo

import crash_test as C


def attempt(srv, k, rnd, n_docs, results):
    name = f"bfk{int(time.time()) % 100000}_{k}"
    kind = rnd.choice([[("z", 1)], [("y", 1)], [("w", 1)], [("z", 1), ("y", -1)]])
    cl = C.client()
    db = cl[name]
    for s in range(0, n_docs, 2000):
        db.c.insert_many([{"_id": i, "z": i % 97, "y": f"y{i % 13}", "w": [i % 5, i % 7]} for i in range(s, min(n_docs, s + 2000))])
    out = {"attempt": k, "db": name, "keys": kind, "docs": n_docs}
    res = {}

    def build():
        try:
            C.client()[name].c.create_index(kind, name="bf")
            res["finished"] = True
        except Exception as e:  # the connection dies with the server
            res["finished"] = False
            res["error"] = type(e).__name__

    def writer():
        c2 = C.client()[name].c
        i = n_docs
        while not res.get("stop"):
            try:
                c2.insert_one({"_id": i, "z": i % 97, "y": f"y{i % 13}", "w": [i % 5, i % 7]})
                i += 1
            except Exception:
                return

    bt = threading.Thread(target=build, daemon=True)
    wt = threading.Thread(target=writer, daemon=True)
    bt.start()
    wt.start()
    time.sleep(rnd.uniform(0.05, 0.6))
    listed_before = "bf" in [i["name"] for i in C.client()[name].c.list_indexes()] if res.get("finished") is None else True
    srv.kill()
    res["stop"] = True
    bt.join(10)
    time.sleep(0.3)
    srv.start()
    c3 = C.client()[name]
    # right after the restart, before anything touches the collection's writes: is the index listed?
    listed_after_restart = "bf" in [i["name"] for i in c3.c.list_indexes()]
    out.update({"client_build_finished": res.get("finished"), "listed_before_kill": listed_before, "listed_right_after_restart": listed_after_restart})
    mid_build = (res.get("finished") is False) and not listed_before and not listed_after_restart
    out["killed_mid_build"] = mid_build
    t0 = time.time()
    deadline = t0 + 180
    finished = False
    while time.time() < deadline:
        try:
            if "bf" in [i["name"] for i in c3.c.list_indexes()]:
                v = c3.command("validate", "c")
                if v.get("valid"):
                    finished = True
                    break
        except Exception:
            pass
        time.sleep(0.3)
    out["finished_after_restart"] = finished
    out["secs_to_finish_after_restart"] = round(time.time() - t0, 2)
    if finished:
        total = c3.c.count_documents({})
        via_index = len(list(c3.c.find({}, {"_id": 1}, hint="bf")))
        out.update({"docs_total": total, "docs_via_index": via_index, "index_complete": total == via_index, "validate_valid": True})
    results.append(out)
    return mid_build


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kills", type=int, default=10)
    ap.add_argument("--docs", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=3)
    ap.add_argument("--out", default=os.path.join(C.HERE, "out", "backfill_kill.json"))
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    srv = C.Server()
    srv.start()
    results = []
    counted = 0
    tries = 0
    try:
        while counted < a.kills and tries < a.kills * 4:
            tries += 1
            if attempt(srv, tries, rnd, a.docs, results):
                counted += 1
    finally:
        srv.kill()
    mids = [r for r in results if r["killed_mid_build"]]
    summary = {
        "attempts": len(results),
        "kills_mid_build": len(mids),
        "finished_after_restart": sum(1 for r in mids if r["finished_after_restart"]),
        "index_complete_and_valid": sum(1 for r in mids if r.get("index_complete") and r.get("validate_valid")),
        "max_secs_to_finish": max((r["secs_to_finish_after_restart"] for r in mids), default=None),
        "attempts_not_counted": [r["attempt"] for r in results if not r["killed_mid_build"]],
    }
    json.dump({"summary": summary, "attempts": results}, open(a.out, "w"), indent=1)
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
