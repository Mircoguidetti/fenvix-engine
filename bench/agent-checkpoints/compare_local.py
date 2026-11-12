"""Reads the local equal-durability results (bench_local.sh) and prints, one operation per line, ours vs PostgreSQL vs
mongod (p50 and p99 in ms), the winner and the ratio ours / best of the others; then the spread across repeats.
It only reads summary.json files: no measurement here.
Usage: compare_local.py RESULTS_DIR TAG   (e.g. results 2026-11-11)"""
import json
import os
import sys

OPS = [("save one step", "save_step"), ("resume, no gap", "resume[write/none]"), ("resume, short gap", "resume[write/short]"),
       ("resume, long gap", "resume[write/long]"), ("resume, cold (after a restart)", "resume[cold]"),
       ("resume, read phase", "resume[read]"), ("time travel", "time_travel"), ("list whole history", "list"),
       ("list last 10", "list_limit10"), ("erase one end user", "erase_user"),
       # total review R1 point 3: the next reads of the same thread right after its cold resume
       ("NEXT after cold: resume", "resume[cold]:next"), ("NEXT after cold: list last 10", "list_limit10:next"),
       ("NEXT after cold: list whole history", "list:next"), ("NEXT after cold: time travel", "time_travel:next")]
SYS = [("ours", "fenvix"), ("Postgres", "postgres"), ("mongod", "mongo")]


def load(d):
    out = {}
    for _, k in SYS:
        f = os.path.join(d, k, "summary.json")
        if os.path.exists(f):
            out[k] = json.load(open(f))
    return out


def fmt(x):
    return "n/a" if x is None else (f"{x:.1f}" if x >= 10 else f"{x:.2f}")


def op_line(name, key, s):
    parts, vals = [], {}
    for label, k in SYS:
        v = s.get(k, {}).get("latency_ms", {}).get(key)
        vals[k] = v
        parts.append(f"{label} {fmt(v['p50']) if v else 'n/a'} / {fmt(v['p99']) if v else 'n/a'}")
    res = []
    for q in ("p50", "p99"):
        have = {k: v[q] for k, v in vals.items() if v and v.get(q) is not None}
        if len(have) < 2:
            continue
        win = min(have, key=have.get)
        others = [have[k] for k in have if k != "fenvix"]
        r = have["fenvix"] / min(others) if "fenvix" in have and others and min(others) > 0 else None
        res.append(f"{q} winner {dict(SYS_NAME)[win]}, ours/best other {r:.2f}" if r else f"{q} winner {dict(SYS_NAME)[win]}")
    n = vals.get("fenvix", {}) or {}
    return f"{name}: {'; '.join(parts)} ms (p50 / p99; n ours {n.get('n', 'n/a')}). " + "; ".join(res)


SYS_NAME = [(k, label) for label, k in SYS]


def bytes_line(s):
    v = {k: s.get(k, {}).get("bytes", {}).get("stored_per_logical") for _, k in SYS}
    have = {k: x for k, x in v.items() if x}
    if not have:
        return "stored bytes per logical byte: n/a"
    win = min(have, key=have.get)
    others = [have[k] for k in have if k != "fenvix"]
    r = have.get("fenvix") / min(others) if "fenvix" in have and others else None
    return (f"stored bytes per logical byte: ours {fmt(v['fenvix']) if v['fenvix'] else 'n/a'}, Postgres "
            f"{fmt(v['postgres']) if v['postgres'] else 'n/a'}, mongod {fmt(v['mongo']) if v['mongo'] else 'n/a'}; winner "
            f"{dict(SYS_NAME)[win]}" + (f", ours/best other {r:.2f}" if r else "") + ours_split(s))


def ours_split(s):
    """Our bytes split (review F9): the WAL share, compaction 2's fx/ share, SlateDB's compacted/ share, the settle."""
    b = s.get("fenvix", {}).get("bytes", {})
    st = s.get("fenvix", {}).get("settle") or {}
    if "wal_share" not in b:
        return ""
    return (f"; ours: WAL {b['wal_share']:.1%}, fx/ {b['fx_share']:.1%}, compacted/ {b['compacted_share']:.1%}; settle "
            f"{st.get('seconds')} s, capped {st.get('capped')}, compaction 2 published {st.get('compaction2_published')}")


def worst(runs):
    """The worst ratio ours / best other over every operation, percentile and run given."""
    w = None
    for tag, s in runs:
        for name, key in OPS:
            for q in ("p50", "p99"):
                a = s.get("fenvix", {}).get("latency_ms", {}).get(key, {})
                o = [s.get(k, {}).get("latency_ms", {}).get(key, {}).get(q) for k in ("postgres", "mongo")]
                o = [x for x in o if x]
                if a and a.get(q) and o and min(o) > 0:
                    r = a[q] / min(o)
                    if w is None or r > w[0]:
                        w = (r, tag, name, q, a[q], min(o))
        b = s.get("fenvix", {}).get("bytes", {}).get("stored_per_logical")
        o = [s.get(k, {}).get("bytes", {}).get("stored_per_logical") for k in ("postgres", "mongo")]
        o = [x for x in o if x]
        if b and o and (w is None or b / min(o) > w[0]):
            w = (b / min(o), tag, "stored bytes per logical byte", "-", b, min(o))
    return w


def main():
    rd, tag = sys.argv[1], sys.argv[2]
    sets = [(f"equal r{k}", os.path.join(rd, f"local-{tag}-equal-r{k}")) for k in (1, 2, 3)] + \
           [("equal --workers 8", os.path.join(rd, f"local-{tag}-equal-w8")), ("single node", os.path.join(rd, f"local-{tag}-single"))]
    loaded = [(t, load(d)) for t, d in sets if os.path.isdir(d)]
    for t, s in loaded:
        print(f"== {t}")
        for name, key in OPS:
            print(op_line(name, key, s))
        print(bytes_line(s))
        print("ok: " + ", ".join(f"{k} {s[k]['ok']} (wrong reads {s[k].get('wrong_reads')})" for _, k in SYS if k in s))
    reps = [(t, s) for t, s in loaded if t.startswith("equal r")]
    if len(reps) > 1:
        print("== spread across the equal-durability repeats (min-max of p50 / min-max of p99, ms)")
        for name, key in OPS:
            parts = []
            for label, k in SYS:
                vs = [s.get(k, {}).get("latency_ms", {}).get(key) for _, s in reps]
                vs = [v for v in vs if v]
                if vs:
                    parts.append(f"{label} {fmt(min(v['p50'] for v in vs))}-{fmt(max(v['p50'] for v in vs))} / "
                                 f"{fmt(min(v['p99'] for v in vs))}-{fmt(max(v['p99'] for v in vs))}")
            print(f"{name}: " + "; ".join(parts))
    deciding = [(t, s) for t, s in loaded if t != "single node"]
    w = worst(deciding)
    if w:
        print(f"== worst for us over the deciding runs: {w[2]} {w[3]} ({w[1]}): ours {fmt(w[4])} vs best other {fmt(w[5])}, ratio {w[0]:.2f}")


if __name__ == "__main__":
    main()
