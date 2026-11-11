import time, pymongo
c = pymongo.MongoClient("mongodb://127.0.0.1:27017", socketTimeoutMS=0)
src = c["app_data_b01"]["ordini"]; dst = c["ctl_wire"]["ordini"]
dst.drop()
t0 = time.time(); buf = []; n = 0
for d in src.find({}, batch_size=5000):
    buf.append(d)
    if len(buf) == 2000:
        dst.insert_many(buf, ordered=False); n += len(buf); buf = []
        if n % 200000 == 0: print(n, round(time.time()-t0,1), flush=True)
if buf: dst.insert_many(buf, ordered=False); n += len(buf)
print(f"LOADED {n} in {time.time()-t0:.1f} s", flush=True)
