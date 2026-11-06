"""Print a plain-text summary of results/*.json (writes results/summary.txt too)."""
import glob
import json
import os

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PRED = ("p1_yesterday", "p2_last7", "p3_weekday", "p4_gaphist", "p5_lr", "p5_gbm", "oracle")
QS = ("0.02", "0.05", "0.08", "0.15", "0.2")
SIMPLE = ("p1_yesterday", "p2_last7", "p3_weekday", "p4_gaphist")
# reference model (D1c cost.py, unchanged): cold-read GETs per 1 TB customer per month
GET_QUIET, GET_MEDIUM = 21.0, 84.0
DISK_FREE_GB = 118 - 44 * 1.30 / 0.85     # m8gd.large NVMe minus maps at 1 TB under choice (b), D1c inputs


def pct(x):
    return "n/a" if x is None else f"{100 * x:.1f}%"


def best(r, pop, q, preds):
    c = [(r[f"{pop}|{p}|{q}"]["h"], p) for p in preds if f"{pop}|{p}|{q}" in r and r[f"{pop}|{p}|{q}"]["h"] is not None]
    return max(c) if c else (None, None)


def main():
    out = []
    p = out.append
    for fp in sorted(glob.glob(os.path.join(HERE, "results", "*.json"))):
        d = json.load(open(fp))
        r = d["pooled"]["results"]
        de = d["pooled"]["describe"]
        p(f"=== {d['dataset']} (pooled: one database) ===")
        p(f"  users {de['users']}, accesses {de['events']}, sessions {de['sessions']}, days {de['days']}, nights simulated "
          f"{de['eval_nights']}, night boundary {de['boundary_hour_utc']}:00 UTC, known users per night {de['mean_known_users']:.0f}, "
          f"daily active {de['mean_daily_active']:.0f} ({pct(de['daily_active_share_of_known'])} of known)")
        for pop in ("known", "active28"):
            p(f"  population {pop}: h per predictor at q = 2/5/8/15/20%")
            for pr in PRED:
                if f"{pop}|{pr}|0.08" not in r:
                    continue
                p(f"    {pr:13s} " + "  ".join(pct(r[f'{pop}|{pr}|{q}']['h']) for q in QS))
        k8 = {pr: r.get(f"known|{pr}|0.08") for pr in PRED}
        p("  at q = 8% (known users):")
        for pr, x in k8.items():
            if x is None:
                continue
            p(f"    {pr:13s} h {pct(x['h'])} (nights p10-p90 {pct(x['night_p10'])}-{pct(x['night_p90'])}); "
              f"first-of-day per user {pct(x['h_first_of_day'])}; incl. brand-new users {pct(x['h_incl_new_users'])}; "
              f"after an LRU of 0.2% of users {pct(x['h_after_lru_0.002'])} (the LRU already catches {pct(x['lru_0.002_catches'])}); "
              f"after 1% LRU {pct(x['h_after_lru_0.01'])} (catches {pct(x['lru_0.01_catches'])}); "
              f"selected users' share of past accesses {pct(x['byte_share'])}")
        hb, pb = best(r, "known", "0.08", PRED[:-1])
        if hb is not None:
            p(f"  best at q 8%: {pb} {pct(hb)} -> saved cold-read GETs per 1 TB per month {hb * GET_QUIET:.1f} $ quiet, "
              f"{hb * GET_MEDIUM:.1f} $ medium (reference 21 and 84 $)")
        if d["groups"]:
            rows = []
            for g, gd in d["groups"].items():
                gr = gd["results"]
                h8, pb = best(gr, "known", "0.08", SIMPLE + ("p5_lr", "p5_gbm"))
                h2 = gr.get("known|p2_last7|0.08", {}).get("h")
                o8 = gr.get("known|oracle|0.08", {}).get("h")
                rows.append((h8, g, pb, h2, o8, gd["describe"]["users"], gd["describe"]["daily_active_share_of_known"]))
            rows = [x for x in rows if x[0] is not None]
            rows.sort(reverse=True)
            hs = np.array([x[0] for x in rows])
            p(f"  spread across {len(rows)} groups (each its own database), best predictor at q 8%: min {pct(hs.min())}, "
              f"median {pct(float(np.median(hs)))}, max {pct(hs.max())}; groups at 35% or more: {(hs >= 0.35).sum()}, "
              f"under 15%: {(hs < 0.15).sum()}")
            for h8, g, pb, h2, o8, nu, da in rows:
                p(f"    {g:28s} users {nu:6d} daily-active {pct(da):>6s}  best {pct(h8):>6s} ({pb})  p2 {pct(h2):>6s}  oracle {pct(o8)}")
        p("")
    p("Disk quota at 1 TB with the prompt's rule (users x average bytes per user = q x 1 TB): "
      + ", ".join(f"q {float(q)*100:.0f}% {float(q)*1024:.0f} GB" for q in QS)
      + f"; free NVMe on m8gd.large beside the maps: {DISK_FREE_GB:.0f} GB")
    text = "\n".join(out)
    print(text)
    open(os.path.join(HERE, "results", "summary.txt"), "w").write(text + "\n")


if __name__ == "__main__":
    main()
