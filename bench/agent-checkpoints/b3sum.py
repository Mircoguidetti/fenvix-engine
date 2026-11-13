"""P-B3: pool the mix rounds of one run by condition and delay: save and NEXT reads, p50/p99 per condition, plus the
per-round numbers. Rounds are named `<condition>-d<delay>-<n>`.

  b3sum.py RUN_DIR [CONDITION ...]
"""
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bigmix import _pct, op_key  # noqa: E402

KEYS = ["save_step", "resume[none]", "resume[read]:next", "time_travel:next", "list_limit10:next", "list:next", "resume[long]", "resume[read]", "list", "list_limit10", "time_travel"]
NEXT = {"resume[none]", "resume[read]:next", "time_travel:next", "list_limit10:next", "list:next"}


def main():
    d = sys.argv[1]
    rows = [json.loads(l) for l in open(os.path.join(d, "ops.jsonl"))]
    pooled = defaultdict(lambda: defaultdict(list))
    per_round = defaultdict(lambda: defaultdict(list))
    for r in rows:
        ph = r.get("phase", "")
        if not ph.startswith("mix:"):
            continue
        name = ph[4:]
        try:
            cond, dl, _ = name.rsplit("-", 2)
        except ValueError:
            continue
        k = op_key(r)
        if not k:
            continue
        if k.startswith("save_step"):
            k = "save_step"
        for key in (k, "next(all)" if k in NEXT else None):
            if key:
                pooled[(cond, dl)][key].append(r["ms"])
                per_round[name][key].append(r["ms"])
    print("pooled by condition and delay: n, p50, p90, p99, max (ms)")
    for (cond, dl) in sorted(pooled):
        print(f"== {cond} {dl}")
        for k in KEYS + ["next(all)"]:
            xs = pooled[(cond, dl)].get(k)
            if xs:
                print(f"  {k:20s} n={len(xs):5d} p50={_pct(xs, .5):8.2f} p90={_pct(xs, .9):8.2f} p99={_pct(xs, .99):8.2f} max={max(xs):8.1f}")
    print("per round: save p50/p99, NEXT(all) p50/p99")
    for name in sorted(per_round, key=lambda n: min(r.get("t0", 0) for r in rows if r.get("phase") == "mix:" + n) if False else n):
        s, n = per_round[name]["save_step"], per_round[name]["next(all)"]
        print(f"  {name:16s} save n={len(s)} {_pct(s, .5)}/{_pct(s, .99)}  next n={len(n)} {_pct(n, .5)}/{_pct(n, .99)}")


if __name__ == "__main__":
    main()
