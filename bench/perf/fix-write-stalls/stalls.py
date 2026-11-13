"""Write stalls of one benchmark run (fix-write-stalls): longest single write, writes and saves over 1 s, p50/p99 of the
main operations, stored bytes, and what the cook logged about held writes and slow first publishes.

    python stalls.py RUN_DIR [RUN_DIR...]
"""
import gzip, json, os, re, sys


def opener(p):
    return gzip.open(p, "rt") if p.endswith(".gz") else open(p)


def find(d, name):
    for sub in ("", "logs"):
        for n in (name, name + ".gz"):
            if os.path.exists(os.path.join(d, sub, n)):
                return os.path.join(d, sub, n)
    return None


def pct(xs, q):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q / 100 * (len(xs) - 1))))]


for d in sys.argv[1:]:
    print("==", d)
    ops = []
    p = find(d, "ops.jsonl")
    if p:
        with opener(p) as f:
            for line in f:
                try:
                    ops.append(json.loads(line))
                except ValueError:
                    pass
    writes = [o for o in ops if o.get("op") in ("put", "put_writes")]
    saves = [o for o in ops if o.get("op") == "save_step"]
    if writes:
        w = max(writes, key=lambda o: o["ms"])
        print(f"  single writes {len(writes)}; longest {w['ms']/1000:.2f} s ({w['op']} at ts {w['ts']:.1f}); over 1 s: {sum(o['ms'] >= 1000 for o in writes)}; over 500 ms: {sum(o['ms'] >= 500 for o in writes)}")
        top = sorted(writes, key=lambda o: -o["ms"])[:8]
        t0 = min(o["ts"] for o in ops)
        print("  top writes (s since start, s):", ", ".join(f"{o['ts']-t0:.0f}:{o['ms']/1000:.2f}" for o in top))
    if saves:
        s = max(saves, key=lambda o: o["ms"])
        print(f"  saves {len(saves)}; longest {s['ms']/1000:.2f} s; over 1 s: {sum(o['ms'] >= 1000 for o in saves)}")
    by = {}
    for o in ops:
        k = o.get("op")
        if k == "resume":
            k = f"resume[{o.get('phase')}/{o.get('gap')}]" if o.get("phase") == "write" else f"resume[{o.get('phase')}]"
        by.setdefault(k, []).append(o["ms"])
    keys = ["save_step", "put", "put_writes", "resume[write/none]", "resume[write/short]", "resume[write/long]", "resume[write/first]", "resume[read]", "resume[cold]", "time_travel", "list", "list_limit10", "erase_user"]
    print("  p50/p99 ms: " + "; ".join(f"{k} {pct(by[k],50):.1f}/{pct(by[k],99):.1f} (n={len(by[k])})" for k in keys if k in by))
    sp = os.path.join(d, "summary.json")
    if os.path.exists(sp):
        s = json.load(open(sp))
        b = s.get("bytes", {})
        print(f"  ok {s.get('ok')} wrong_reads {s.get('wrong_reads')} stored/logical {b.get('stored_per_logical')} stored {b.get('stored_bytes') or b.get('stored')}")
    c = find(d, "cook.log")
    if c:
        slow, comp, puts = [], [], []
        with opener(c) as f:
            for line in f:
                m = re.search(r"SLOW_STAGE u=(\d+) db=(\S+) ms=(\d+) l0=(\d+)(?: l0_max_held=(\d+))?", line)
                if m:
                    slow.append((int(m.group(3)), int(m.group(4)), m.group(5)))
                m = re.search(r"SLOW_PUT u=(\d+) db=(\S+) kind=(\S+) ms=(\d+) bytes=(\d+)", line)
                if m:
                    puts.append((m.group(3), int(m.group(4)), int(m.group(5))))
                if "FIRST_PUBLISH" in line or "COMP_START" in line or "L0_RECOGNISE" in line:
                    comp.append(line.strip()[:200])
        if slow:
            print(f"  SlateDB held writes >= 1 s: {len(slow)}; ms/l0 at the end[/largest l0 while held]: " + ", ".join(f"{a}/{b}" + (f"/{c}" if c else "") for a, b, c in sorted(slow, reverse=True)[:12]))
        if puts:
            for kind in sorted({p[0] for p in puts}):
                xs = sorted((p[1] for p in puts if p[0] == kind), reverse=True)
                print(f"  writer uploads >= 500 ms, {kind}: {len(xs)}; longest ms: {xs[:10]}")
        for l in comp[:12]:
            print("   ", l)
