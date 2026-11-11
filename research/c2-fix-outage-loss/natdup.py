"""natdup.py <rundir>: for every lost id, its natural-order number (from the saved WAL/SST bodies) and which other
acknowledged document of its batch holds the same number."""
import json, os, struct, sys, glob
d = sys.argv[1]
res = json.load(open(f"{d}/result.json"))
miss = res["missing_slatedb"] or res["missing_merged"]
mk = {}
for ln in open(f"{d}/markers.txt"):
    p = ln.split(); mk[int(p[0])] = p[3:]
blobs = [open(f, "rb").read() for f in sorted(glob.glob(f"{d}/bodies/*"))]
def seq(i):
    m = mk[i // 500][i % 500].encode()
    for b in blobs:
        p = b.find(m)
        if p >= 0 and b[p+1000:p+1003] == b"\0\0\xfe":
            return struct.unpack(">Q", b[p+1003:p+1011])[0]
batches = sorted(set(i // 500 for i in miss))
shared = 0
for bt in batches:
    seqs = {}
    for off in range(500):
        seqs.setdefault(seq(bt * 500 + off), []).append(bt * 500 + off)
    lost = [i for i in miss if i // 500 == bt]
    ok = [i for i in lost if len(seqs[seq(i)]) > 1]
    shared += len(ok)
    print(f"batch {bt}: lost {len(lost)}, of which share their number with another doc of the batch: {len(ok)}; distinct numbers in batch {len(seqs)}")
print(f"TOTAL lost {len(miss)}, sharing a number {shared}")
