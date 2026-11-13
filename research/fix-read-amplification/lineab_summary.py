"""Pool command-cpu's lineprobe.py outputs (bench-big's big line, reads only) per binary and compare them.
Usage: lineab_summary.py DIR BASE_TAG FIX_TAG ROUNDS...   (files DIR/line-3g-{tag}-{round}.json)
Prints per operation: p50, p90, p99 of the call time and the mean of the cook's CPU per call, pooled over the rounds,
and the number of answers that differ from the first base round (every answer is hashed by lineprobe.py)."""
import json
import sys

d, base, fix, rounds = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 2)


ref = json.load(open(f"{d}/line-3g-{base}-{rounds[0]}.json"))["hashes"]
for tag in (base, fix):
    rows = []
    for r in rounds:
        j = json.load(open(f"{d}/line-3g-{tag}-{r}.json"))
        rows += j["rows"]
        diff = sum(1 for k in ref if ref[k] != j["hashes"].get(k))
        print(f"{tag} round {r}: {len(j['hashes'])} answers, {diff} different from {base} round {rounds[0]}")
    for op in ("resume:first", "resume", "time_travel", "list", "list_limit10"):
        ms = [x["ms"] for x in rows if x["op"] == op]
        cpu = [x["cpu_ms"] for x in rows if x["op"] == op]
        print(f"  {tag} {op}: n {len(ms)} p50 {pct(ms, .5)} p90 {pct(ms, .9)} p99 {pct(ms, .99)} ms, cook CPU {sum(cpu) / len(cpu):.2f} ms per call")
