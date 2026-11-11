"""forensic.py <rundir>: for the lost ids, which saved PUT bodies hold them, the trace of those objects (PUT, response,
deletes), the batch's ack time, and the lease/writer events around them."""
import json, os, re, sys, subprocess
d = sys.argv[1]
res = json.load(open(f"{d}/result.json"))
miss = res["missing_slatedb"] or res["missing_merged"]
print("missing", len(miss), "first", miss[:3], "last", miss[-3:])
mk = {}
for ln in open(f"{d}/markers.txt"):
    p = ln.split()
    mk[int(p[0])] = p[3:]
batches = {}
for ln in open(f"{d}/batches.log"):
    batches[int(ln.split()[0])] = ln.strip()
lost_b = sorted(set(i // 500 for i in miss))
trace = open(f"{d}/trace.log").read().splitlines()
saved = {}
for ln in trace:
    m = re.search(r"REQ (c\d+) PUT (\S+) .*saved=(\d+)", ln)
    if m:
        saved[m.group(3)] = (m.group(1), m.group(2), ln)
def holders(marker):
    r = subprocess.run(["grep", "-rl", marker, f"{d}/bodies"], capture_output=True, text=True).stdout.split()
    return sorted(os.path.basename(x) for x in r)
for b in lost_b:
    ids = [i for i in miss if i // 500 == b]
    print(f"\nbatch {b}: lost {len(ids)} ({ids[0]}..{ids[-1]}); {batches.get(b)}")
    for off in sorted({ids[0] % 500, ids[-1] % 500, min(ids[-1] % 500 + 1, 499), 499}):
        h = holders(mk[b][off])
        print(f"  doc {b*500+off} ({'lost' if b*500+off in miss else 'present'}) in bodies: " + ", ".join(f"{x}={saved[x][1].split('?')[0].split('/',2)[-1]}" for x in h if x in saved))
