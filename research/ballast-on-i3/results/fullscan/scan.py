import sys, time, pymongo
uri, db, coll = sys.argv[1], sys.argv[2], sys.argv[3]
c = pymongo.MongoClient(uri, socketTimeoutMS=0)[db][coll]
t0 = time.time(); n = 0; first = None
for _ in c.find({}, batch_size=5000):
    n += 1
    if first is None: first = time.time() - t0; print(f"first document after {first:.1f} s", flush=True)
    if n % 200000 == 0: print(f"{n} after {time.time()-t0:.1f} s", flush=True)
print(f"FULLSCAN {db}.{coll}: {n} documents, first after {first:.1f} s, total {time.time()-t0:.1f} s", flush=True)
