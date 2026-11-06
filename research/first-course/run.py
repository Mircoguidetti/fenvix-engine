"""Run the first-course measurement on the three traces and write results/<dataset>.json and results/summary.txt.

Usage: python3 run.py [irc lsapp cdnow]
"""
import json
import os
import sys

import numpy as np

import first_course as fc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "return-prediction"))
import loaders  # noqa: E402
import sim  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CONF = {  # loader, min users for a per-group database (same floors as return-prediction)
    "irc": (loaders.irc, 150),
    "lsapp": (loaders.lsapp, 40),
    "cdnow": (loaders.cdnow, 10**9),
}
# Reference model (fenvix-brain sessions/2026-11-06-d1c-warm-spare/cost.py, unchanged): cold reads/s per profile,
# GET_PER_COLD = 4, 0.0004 $ per 1,000 GETs, 730 h per month.
COLD_GET_USD = {p: c * 4.0 * 730 * 3600 * 0.0004 / 1000 for p, c in (("quiet", 5), ("medium", 20))}


def one(name, users, ts):
    db = sim.Database(name, loaders.encode(users), ts, warmup_days=28)
    return fc.measure(db)


def pct(x):
    return "n/a" if x is None else f"{100 * x:.1f}%"


def summary_lines(name, r):
    c = r["cold"]
    o = r["openings"]
    L = [f"== {name}: users {r['users']}, eval days {r['eval_days']}, session starts {r['starts_eval']}, "
         f"cold (miss 0.2% LRU) {r['cold_starts']} (LRU catches {pct(r['lru_catches'])})"]
    L.append(f"  cold starts: new {pct(c['new'])}; within " +
             ", ".join(f"{T}d {pct(c[f'within_{T}d'])}" for T in fc.TS_DAYS) +
             f"; silent >30d {pct(c['silent_over_30d'])}; within 30d of returning only {pct(c['within_30d_of_returning'])}")
    L.append("  buckets: " + ", ".join(f"{k[7:]} {pct(v)}" for k, v in c.items() if k.startswith("bucket_")))
    a = r["all_starts"]
    L.append(f"  all starts (LRU hits included): new {pct(a['new'])}, within 30d {pct(a['within_30d'])}")
    L.append(f"  new share by period: first 28 eval days {pct(r['new_by_period']['first_28d'])}, "
             f"last 28 {pct(r['new_by_period']['last_28d'])}")
    L.append(f"  known users: mean {o['mean_known']:.0f}, last day {o['known_last_day']}")
    for T in fc.TS_DAYS:
        x = o[f"{T}d"]
        s = x["scale_1tb"]
        L.append(f"  T={T}d: openings mean {x['mean_held']:.0f} (max {x['max_held']}), share of known "
                 f"{pct(x['share_of_known'])} (first day {pct(x['share_first_day'])}, last day {pct(x['share_last_day'])}), "
                 f"trace footprint {x['footprint_mb']:.1f} MB; 1 TB: " +
                 "; ".join(f"{k}/user {v['users']} users -> {v['footprint_gb']:.1f} GB {'fits' if v['fits_29gb'] else 'NO'}"
                           for k, v in s.items()))
    for T, sp in r["spread"].items():
        if sp.get("covered_users"):
            L.append(f"  spread T={T}: {sp['covered_starts']} covered starts by {sp['covered_users']} users "
                     f"(of {sp['returning_users_with_cold_start']} returning users with a cold start); top 1% users "
                     f"{pct(sp['top1pct_share'])}, top 10% {pct(sp['top10pct_share'])}; half of covered starts by "
                     f"{sp['users_for_half']} users ({pct(sp['users_for_half_share'])}); median per user "
                     f"{sp['median_starts_per_covered_user']:.0f}")
    L.append("  dollars (reference cold-read GETs, quiet %.1f $ / medium %.1f $ per 1 TB customer per month): " %
             (COLD_GET_USD["quiet"], COLD_GET_USD["medium"]) +
             "; ".join(f"T={T}d saves {c[f'within_{T}d'] * COLD_GET_USD['quiet']:.1f} / "
                       f"{c[f'within_{T}d'] * COLD_GET_USD['medium']:.1f} $" for T in fc.TS_DAYS) +
             f"; idea 2 saves {c['new'] * COLD_GET_USD['quiet']:.1f} / {c['new'] * COLD_GET_USD['medium']:.1f} $")
    return L


def main():
    names = sys.argv[1:] or list(CONF)
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    for name in names:
        loader, min_group = CONF[name]
        g, u, t = loader()
        out = {"dataset": name, "pooled": one(name, u, t), "groups": {}}
        print("\n".join(summary_lines(name, out["pooled"])), flush=True)
        groups = np.unique(g)
        if len(groups) > 1:
            for grp in groups:
                m = g == grp
                if len(np.unique(u[m])) < min_group:
                    continue
                out["groups"][str(grp)] = one(f"{name}/{grp}", u[m], t[m])
        with open(os.path.join(HERE, "results", f"{name}.json"), "w") as f:
            json.dump(out, f, indent=1, sort_keys=True)
    # summary over every result file present
    lines = []
    for name in CONF:
        p = os.path.join(HERE, "results", f"{name}.json")
        if not os.path.exists(p):
            continue
        out = json.load(open(p))
        lines += summary_lines(name, out["pooled"])
        if out["groups"]:
            gs = out["groups"]
            w30 = sorted((v["cold"]["within_30d"], k) for k, v in gs.items() if v["cold"])
            nw = sorted((v["cold"]["new"], k) for k, v in gs.items() if v["cold"])
            sh = sorted((v["openings"]["30d"]["share_of_known"], k) for k, v in gs.items() if v["openings"].get("30d"))
            lines.append(f"  per-group databases ({len(gs)}): within 30d " +
                         ", ".join(f"{k} {pct(x)}" for x, k in w30))
            lines.append(f"  per-group new share: " + ", ".join(f"{k} {pct(x)}" for x, k in nw))
            lines.append(f"  per-group 30d openings share of known: " + ", ".join(f"{k} {pct(x)}" for x, k in sh))
            lines.append(f"  per-group medians: within 30d {pct(float(np.median([x for x, _ in w30])))}, "
                         f"new {pct(float(np.median([x for x, _ in nw])))}, "
                         f"30d share of known {pct(float(np.median([x for x, _ in sh])))}")
        lines.append("")
    with open(os.path.join(HERE, "results", "summary.txt"), "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
