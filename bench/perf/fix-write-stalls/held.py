"""Cause 3: every write SlateDB held 1 s or more (cook log SLOW_STAGE), with the writer's slow uploads (SLOW_PUT) and
the machine (cpu_mem.jsonl, 5 s samples) around it, and every client write of 1 s or more matched by wall clock.

    python held.py RUN_DIR
"""
import gzip, json, os, re, sys

d = sys.argv[1]
op = lambda p: gzip.open(p, "rt") if p.endswith(".gz") else open(p)
cook = next(p for p in (os.path.join(d, "logs", "cook.log.gz"), os.path.join(d, "cook.log.gz")) if os.path.exists(p))
held, puts = [], []
for line in op(cook):
    m = re.search(r"SLOW_STAGE u=(\d+) db=\S+ ms=(\d+) l0=(\d+)(?: l0_max_held=(\d+))?", line)
    if m:
        held.append((int(m.group(1)) / 1000, int(m.group(2)), int(m.group(3)), m.group(4)))
    m = re.search(r"SLOW_PUT u=(\d+) db=\S+ kind=(\S+) ms=(\d+) bytes=(\d+)", line)
    if m:
        puts.append((int(m.group(1)) / 1000, m.group(2), int(m.group(3)), int(m.group(4))))
cpu = [json.loads(l) for l in open(os.path.join(d, "cpu_mem.jsonl"))]
ops = [json.loads(l) for l in op(next(p for p in (os.path.join(d, "ops.jsonl"), os.path.join(d, "ops.jsonl.gz")) if os.path.exists(p)))]
t0 = min(o["ts"] for o in ops)
slow_writes = [o for o in ops if o.get("op") in ("put", "put_writes") and o["ms"] >= 1000]


def machine(t_end, secs):
    xs = [c for c in cpu if t_end - secs - 5 <= c["t"] <= t_end + 2 and c.get("c")]
    if not xs:
        return "no sample"
    f = lambda k: max(c["c"].get(k, {}).get("cores", 0) for c in xs)
    return f"host busy max {max(c['host_cpu_busy'] for c in xs):.2f}, cook max {f('cook'):.2f} cores, minio max {f('minio'):.2f}, log copies max {max(f('log-0'), f('log-1'), f('log-2')):.2f}"


print(f"{d}: held writes {len(held)}, slow writer uploads {len(puts)} (l0 {sum(p[1]=='l0' for p in puts)}, wal {sum(p[1]=='wal' for p in puts)}), client writes >= 1 s {len(slow_writes)}")
for t, ms, l0, l0max in sorted(held, key=lambda h: -h[1]):
    start = t - ms / 1000
    near = [p for p in puts if start - 5 <= p[0] <= t + 0.5]
    print(f"  held {ms} ms ending at +{t - t0:.0f} s; L0 at the end {l0}, largest while held {l0max}; writer uploads >= 500 ms in it: "
          + (", ".join(f"{k} {m} ms {b >> 20} MiB" for _, k, m, b in near) or "none") + f"; {machine(t, ms / 1000)}")
for o in sorted(slow_writes, key=lambda o: -o["ms"])[:20]:
    end = o["ts"]
    match = [h for h in held if abs(h[0] - end) < 1.5]
    print(f"  client {o['op']} {o['ms']/1000:.2f} s ending at +{end - t0:.0f} s: " + (f"SlateDB held {match[0][1]} ms" if match else "no held write logged at that time"))
