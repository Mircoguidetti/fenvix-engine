"""Step 9 · two first-next-ops summaries side by side (coordinator, 2026-11-13): ours and another server's (a mongod
holding the same data, or our own at another size), per operation, FIRST and NEXT, p50 and p99.
Two readings of every pair, both printed:
  - DECISIONS 2026-11-12: ours at most 20 ms above the other is WITHIN, more is OVER (the gate's speed tolerance);
  - the benches' ratio rule: ours / theirs under 0.9 FASTER, up to 1.1 EQUAL, above SLOWER.
FIRST is reported, never judged against the goal (a first request may pay S3); NEXT and save are the goal.
A pair is compared only when both sides ran the same label, seed and group size; otherwise the output says so.
Usage: first_next_compare.py OURS_SUMMARY_JSON THEIRS_SUMMARY_JSON [OUT_JSON] [--their-name mongod] [--across-labels]
  exit 0 always when both files are read (it reports; the decision is the reader's)."""
import json
import sys

TOLERANCE_MS = 20.0
OPS = ("resume", "last10", "history", "time_travel", "save")


def ratio_verdict(r):
    return "FASTER" if r < 0.9 else ("EQUAL" if r <= 1.1 else "SLOWER")


def compare(ours, theirs, across_labels=False):
    """across_labels: the two sides ran other labels on purpose (after-writes against before-writes: other cold
    customers of the same database), so only seed and group size must match (aws-local-round: the local W1 printed
    "NOT THE SAME PLAN" for that pair, which is the plan)."""
    keys = ("seed", "per_op") if across_labels else ("label", "seed", "per_op")
    same = all(ours.get(k) == theirs.get(k) for k in keys)
    if not across_labels and ours.get("customers_fingerprint") != theirs.get("customers_fingerprint"):
        same = False   # the same label, seed and size, but other customers (other state files): not a pair
    out = {"same_plan": same, "ours_db": ours.get("db"), "theirs_db": theirs.get("db"), "ops": {}}
    for op in OPS:
        o, t = (ours.get("ops") or {}).get(op) or {}, (theirs.get("ops") or {}).get(op) or {}
        row = {}
        for role in ("first", "next"):
            a, b = o.get(role), t.get(role)
            if not a or not b:
                row[role] = None
                continue
            cell = {"ours_n": a["n"], "theirs_n": b["n"]}
            for q in ("p50_ms", "p99_ms"):
                d = a[q] - b[q]
                cell[q] = {"ours": a[q], "theirs": b[q], "ours_minus_theirs_ms": round(d, 3),
                           "tolerance": "WITHIN" if d <= TOLERANCE_MS else "OVER",
                           "ratio": round(a[q] / max(1e-9, b[q]), 2), "ratio_verdict": ratio_verdict(a[q] / max(1e-9, b[q]))}
            row[role] = cell
        out["ops"][op] = row
    nexts = [c[q]["tolerance"] for op in OPS for c in [out["ops"][op].get("next")] if c for q in ("p50_ms", "p99_ms")]
    out["every_next_within_tolerance"] = bool(nexts) and all(x == "WITHIN" for x in nexts)
    return out


def text(c, their_name):
    lines = [f"ours ({c['ours_db']}) against {their_name} ({c['theirs_db']})" +
             ("" if c["same_plan"] else " - NOT THE SAME PLAN (label, seed or group size differ): read with care")]
    for op in OPS:
        for role in ("first", "next"):
            cell = c["ops"][op].get(role)
            if not cell:
                lines.append(f"{role.upper():5s} {op}: no pair")
                continue
            parts = []
            for q in ("p50_ms", "p99_ms"):
                x = cell[q]
                parts.append(f"{q[:3]} {x['ours']:.2f} vs {x['theirs']:.2f} ms ({x['ours_minus_theirs_ms']:+.2f} ms {x['tolerance']}, "
                             f"x{x['ratio']} {x['ratio_verdict']})")
            lines.append(f"{role.upper():5s} {op}: " + "; ".join(parts) + (" [FIRST: reported, not judged]" if role == "first" else ""))
    lines.append(f"every NEXT and save within {TOLERANCE_MS:.0f} ms of {their_name}: {'YES' if c['every_next_within_tolerance'] else 'NO'}")
    return "\n".join(lines)


def main(argv):
    args = [x for x in argv if not x.startswith("--")]
    name = argv[argv.index("--their-name") + 1] if "--their-name" in argv else "theirs"
    if "--their-name" in argv:
        args.remove(name)
    ours, theirs = json.load(open(args[0])), json.load(open(args[1]))
    c = compare(ours, theirs, across_labels="--across-labels" in argv)
    c["text"] = text(c, name)
    if len(args) > 2:
        json.dump(c, open(args[2], "w"), indent=1)
    print(c["text"])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
