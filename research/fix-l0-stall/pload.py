"""Parallel loader that mimics fc_local.py `load` (500-doc insert_many chunks, chat-message documents, index
{userId:1, ts:-1}, customer key messages=userId) with N parallel clients. Records per-chunk ack latency, per-doc
write rate, timeouts. Usage: pload.py PORT NCLIENTS TOTAL_DOCS OUTJSON [--users 40000] [--chunk 500] [--timeout 30]
"""
import json, random, sys, threading, time, os, argparse
from pymongo import MongoClient
from pymongo.errors import PyMongoError, BulkWriteError
from bson import ObjectId

ap = argparse.ArgumentParser()
ap.add_argument("port", type=int)
ap.add_argument("clients", type=int)
ap.add_argument("total", type=int)
ap.add_argument("out")
ap.add_argument("--users", type=int, default=40000)
ap.add_argument("--chunk", type=int, default=500)
ap.add_argument("--timeout", type=int, default=30)
ap.add_argument("--single", action="store_true", help="also time insert_one acks (per write latency) from one extra client")
a = ap.parse_args()
URI = f"mongodb://127.0.0.1:{a.port}/?directConnection=true"
TEXT = "".join(random.Random(1).choice("abcdefghijklmnopqrstuvwxyz ") for _ in range(4000))


def doc(n):
    r = random.Random(n)
    u = r.randrange(a.users)
    ln = r.randrange(200, 3000)
    off = r.randrange(0, 900)
    return {"_id": ObjectId(n.to_bytes(12, "big")), "userId": u, "ts": 1_700_000_000_000 + n * 37, "role": r.choice(["user", "assistant"]),
            "text": TEXT[off:off + ln], "tokens": ln // 4}


c0 = MongoClient(URI, serverSelectionTimeoutMS=60000)
coll0 = c0["fenvix-log"]["messages"]
coll0.create_index([("userId", 1), ("ts", -1)])
lat, lock = [], threading.Lock()
done = [0]
timeouts = [0]
errors = []
t0 = time.time()
chunks = list(range(0, a.total, a.chunk))
nxt = [0]
stop = [False]


def worker(i):
    c = MongoClient(URI, socketTimeoutMS=a.timeout * 1000, serverSelectionTimeoutMS=60000)
    coll = c["fenvix-log"]["messages"]
    while not stop[0]:
        with lock:
            if nxt[0] >= len(chunks):
                return
            lo = chunks[nxt[0]]
            nxt[0] += 1
        docs = [doc(n) for n in range(lo, min(lo + a.chunk, a.total))]
        s = time.time()
        try:
            coll.insert_many(docs, ordered=False)
        except BulkWriteError as e:
            if not all(x.get("code") == 11000 for x in e.details.get("writeErrors", [])):
                errors.append(str(e)[:300])
        except PyMongoError as e:
            with lock:
                timeouts[0] += 1
                errors.append(f"{time.time()-t0:.1f}s chunk {lo}: {type(e).__name__} {str(e)[:200]}")
            if timeouts[0] >= 3:
                stop[0] = True
            continue
        d = time.time() - s
        with lock:
            lat.append(d)
            done[0] += len(docs)


single_lat = []


def single():
    c = MongoClient(URI, socketTimeoutMS=a.timeout * 1000)
    coll = c["fenvix-log"]["probe"]
    n = 1 << 60
    while not stop[0] and nxt[0] < len(chunks):
        s = time.time()
        try:
            coll.insert_one({"_id": n, "userId": n % 1000, "x": "y" * 200})
        except PyMongoError:
            pass
        single_lat.append(time.time() - s)
        n += 1
        time.sleep(0.01)


ths = [threading.Thread(target=worker, args=(i,)) for i in range(a.clients)]
if a.single:
    ths.append(threading.Thread(target=single))
for t in ths:
    t.start()
last = 0
while any(t.is_alive() for t in ths):
    time.sleep(5)
    el = time.time() - t0
    print(f"{el:6.0f}s docs {done[0]:>9,} rate {(done[0]-last)/5:8.0f}/s timeouts {timeouts[0]}", flush=True)
    last = done[0]
el = time.time() - t0


def pct(x, p):
    x = sorted(x)
    return round(x[min(len(x) - 1, int(p * len(x)))] * 1000, 2) if x else None


res = {"clients": a.clients, "total": a.total, "done": done[0], "seconds": round(el, 1), "docs_per_s": round(done[0] / el),
       "chunk_ack_ms_p50": pct(lat, .5), "chunk_ack_ms_p99": pct(lat, .99), "chunk_ack_ms_max": pct(lat, 1.0),
       "single_insert_ms_p50": pct(single_lat, .5), "single_insert_ms_p99": pct(single_lat, .99), "single_n": len(single_lat),
       "timeouts": timeouts[0], "errors": errors[:10]}
print(json.dumps(res, indent=1))
json.dump(res, open(a.out, "w"), indent=1)
sys.exit(1 if timeouts[0] or done[0] < a.total else 0)
