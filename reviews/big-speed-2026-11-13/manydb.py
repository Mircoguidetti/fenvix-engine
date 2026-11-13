"""Write ~NMB MiB into each of N databases (one 512 KiB document at a time), then hold; print the server's RSS."""
import sys, os, time, threading
from pymongo import MongoClient
port, n, mb = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
pid = int(sys.argv[4])
c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", maxPoolSize=64)
blob = os.urandom(512 * 1024)
def rss():
    try:
        return int(open(f"/proc/{pid}/status").read().split("VmRSS:")[1].split()[0]) // 1024
    except Exception:
        return -1
def work(lo, hi):
    for i in range(lo, hi):
        db = c[f"agent{i:04d}"]
        for k in range(mb * 2):
            db.t.insert_one({"_id": k, "b": blob})
threads = [threading.Thread(target=work, args=(j * n // 16, (j + 1) * n // 16)) for j in range(16)]
t0 = time.time()
for t in threads: t.start()
peak = 0
while any(t.is_alive() for t in threads):
    r = rss(); peak = max(peak, r)
    if int(time.time()-t0) % 5 == 0:
        print(f"{time.time()-t0:.0f}s rss {r} MiB", flush=True)
    if r < 0:
        print(f"{time.time()-t0:.0f}s server gone (rss unreadable)", flush=True); break
    time.sleep(1)
for _ in range(20):
    r = rss(); peak = max(peak, r); time.sleep(1)
print(f"done in {time.time()-t0:.0f}s: {n} databases x {mb} MiB, server RSS now {rss()} MiB, peak {peak} MiB", flush=True)
