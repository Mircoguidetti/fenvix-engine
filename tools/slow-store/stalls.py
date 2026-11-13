"""P-fix-slow-store item 4: the stalls of a soak run (soak.py), grouped and matched with their causes.

    python3 tools/slow-store/stalls.py <soak out dir> [--min-ms 1000]

An episode is a set of client operations of at least --min-ms whose windows overlap. For each episode: when, how long,
how many operations of which kind on which databases, the server's breakdown of its slowest commands in that window
(FX_WAITS lines: where the command's time went: lock_w waiting for the database's lock, refresh, stage, wal_flush, get,
put, ...), the faults the proxy injected during it, and the server's own log lines in it (SLOW, L0, compaction, stop,
WRITE_DEADLINE, runtime probe).
"""
import collections
import glob
import json
import os
import re
import sys


def load(d, min_ms):
    s = []
    for f in glob.glob(os.path.join(d, "stalls-*.jsonl")):
        for l in open(f):
            x = json.loads(l)
            if x["ms"] >= min_ms or not x["ok"]:
                s.append(x)
    s.sort(key=lambda x: x["t0"])
    return s


def episodes(s):
    out, cur = [], None
    for x in s:
        if cur and x["t0"] <= cur["t1"]:
            cur["ops"].append(x)
            cur["t1"] = max(cur["t1"], x["t1"])
        else:
            cur = {"t0": x["t0"], "t1": x["t1"], "ops": [x]}
            out.append(cur)
    return out


WAITS = re.compile(r"WAITS t=\S+ u=(\S+) ms=(\S+) cmd=(\S+) coll=\S*(.*)")
UNIX = re.compile(r"u=(\d{13}(?:\.\d+)?)|unix_ms=(\d{13})")


def server_lines(path):
    waits, other = [], []
    for l in open(path, errors="replace"):
        m = WAITS.match(l)
        if m:
            u, ms, cmd, rest = float(m.group(1)) / 1000, float(m.group(2)), m.group(3), m.group(4)
            if cmd == "hello":
                continue
            parts = dict(p.split("=", 1) for p in rest.split() if "=" in p)
            waits.append((u, u + ms / 1000, ms, cmd, parts))
            continue
        if any(k in l for k in ("SLOW ", "WRITE_DEADLINE", "stall", "L0", "stop took", "compaction", "lease", "PROBE", "late", "abandon", "reopen", "fenced", "error", "failed")):
            m = UNIX.search(l)
            if m:
                t = float(m.group(1) or m.group(2)) / 1000
                other.append((t, l.strip()[:220]))
    return waits, other


def faults(path):
    out = []
    if os.path.exists(path):
        for l in open(path):
            p = l.split()
            out.append((float(p[0]), p[1], p[2], p[3]))
    return out


def main():
    d = sys.argv[1]
    min_ms = int(sys.argv[sys.argv.index("--min-ms") + 1]) if "--min-ms" in sys.argv else 1000
    s = load(d, min_ms)
    eps = episodes(s)
    waits, other = server_lines(os.path.join(d, "server.log"))
    fl = faults(os.path.join(d, "faults.log"))
    summ = json.load(open(os.path.join(d, "summary.json"))) if os.path.exists(os.path.join(d, "summary.json")) else {}
    t_first = min((x["t0"] for x in s), default=0)
    print(f"{len(s)} operations of {min_ms} ms or more (or failed), {len(eps)} episodes; longest {max((x['ms'] for x in s), default=0)} ms")
    if summ:
        print(f"summary: {json.dumps({k: summ[k] for k in summ if k not in ('server_log', 'bucket')})}")
    for i, e in enumerate(sorted(eps, key=lambda e: -max(x["ms"] for x in e["ops"]))):
        ops = e["ops"]
        kinds = collections.Counter(x["kind"] for x in ops)
        dbs = collections.Counter(x["db"] for x in ops)
        failed = [x for x in ops if not x["ok"]]
        longest = max(x["ms"] for x in ops)
        print(f"\n## episode {i + 1}: {longest} ms longest, {len(ops)} ops ({dict(kinds)}), dbs {dict(dbs)}, "
              f"at +{e['t0'] - t_first:.1f} s for {e['t1'] - e['t0']:.1f} s; failed {len(failed)}"
              + (f" e.g. {failed[0]['err']}" if failed else ""))
        w = [x for x in waits if x[0] <= e["t1"] + 0.5 and x[1] >= e["t0"] - 0.5 and x[2] >= min_ms * 0.8]
        w.sort(key=lambda x: -x[2])
        agg = collections.Counter()
        for x in w:
            for k, v in x[4].items():
                if "/" in v and k not in ("get", "put", "store_other", "read_get", "read_scan"):
                    agg[k] += float(v.split("/")[0])
            agg["rest"] += float(x[4].get("rest", 0))
        if w:
            top = ", ".join(f"{k} {v / len(w):.0f} ms" for k, v in agg.most_common(4))
            print(f"   server: {len(w)} commands of {min_ms * 0.8:.0f} ms or more; average split: {top}")
            x = w[0]
            print(f"   slowest: {x[3]} {x[2]:.0f} ms: " + " ".join(f"{k}={v}" for k, v in x[4].items()))
        f = [x for x in fl if e["t0"] - 1 <= x[0] <= e["t1"]]
        if f:
            c = collections.Counter(x[1] for x in f)
            print(f"   faults injected meanwhile: {dict(c)}")
        o = [x for x in other if e["t0"] - 2 <= x[0] <= e["t1"] + 1]
        for t, l in o[:8]:
            print(f"   log +{t - e['t0']:.1f}s: {l}")


if __name__ == "__main__":
    main()
