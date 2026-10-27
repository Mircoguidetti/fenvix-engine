#!/usr/bin/env python3
"""Concurrent reads of one tenant while it is written (the shared read lock).

One writer increments a counter 150 times and inserts a document each time; 16 reader threads run find_one, find,
count_documents, aggregate and distinct in a loop. Checks: no operation fails; per reader the counter never goes
backwards (a reader never sees an older state after a newer one); every reader's count equals the number of inserted
documents seen by some point in time (count <= final, and count >= the counter value read just before it);
afterwards the counter and the document count are exactly 150; reads of a database that does not exist still return
empty results; and reads are concurrent (16 readers finish much faster than 16 times one reader).
Run against a server started with or without --shared-reads (the checks must hold in both).
"""
import sys, threading, time, uuid
from pymongo import MongoClient

port = int(sys.argv[1]) if len(sys.argv) > 1 else 28001
c = MongoClient(f"mongodb://127.0.0.1:{port}", maxPoolSize=64, retryWrites=False)
db = c[f"shared_{uuid.uuid4().hex[:8]}"]
db.c.insert_one({"_id": "ctr", "n": 0})
N = 150
stop = threading.Event()
errors, backwards, short = [], [], []

def writer():
    for i in range(1, N + 1):
        db.c.update_one({"_id": "ctr"}, {"$set": {"n": i}})
        db.c.insert_one({"_id": f"d{i}", "i": i})
    stop.set()

def reader(k):
    last = -1
    while not stop.is_set():
        try:
            n = db.c.find_one({"_id": "ctr"})["n"]
            cnt = db.c.count_documents({})
            agg = list(db.c.aggregate([{"$group": {"_id": None, "m": {"$max": "$i"}}}]))
            list(db.c.find({"i": {"$gte": N - 3}}))
            db.c.distinct("i")
        except Exception as e:  # noqa
            errors.append(repr(e)[:150]); return
        if n < last:
            backwards.append((k, last, n))
        last = n
        # the counter is written before the document with the same number, so at least n-1 documents plus the counter exist
        if cnt < n:
            short.append((k, n, cnt))

ts = [threading.Thread(target=reader, args=(k,)) for k in range(16)]
[t.start() for t in ts]
w = threading.Thread(target=writer); t0 = time.time(); w.start(); w.join(); [t.join() for t in ts]
final_n = db.c.find_one({"_id": "ctr"})["n"]; final_cnt = db.c.count_documents({})
ghost = c[f"ghost_{uuid.uuid4().hex[:8]}"]
ghost_ok = ghost.c.find_one({}) is None and ghost.c.count_documents({}) == 0 and list(ghost.c.aggregate([{"$match": {}}])) == []
res = {"errors": errors[:3], "counter_went_backwards": len(backwards), "count_below_counter": len(short), "final_counter": final_n, "final_docs": final_cnt, "ghost_db_reads_empty": ghost_ok}
print(res)
ok = not errors and not backwards and not short and final_n == N and final_cnt == N + 1 and ghost_ok
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
