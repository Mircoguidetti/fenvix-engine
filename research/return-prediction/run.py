"""Run the return-prediction simulation on one dataset and write results/<dataset>.json.

Usage: python3 run.py lsapp|irc|cdnow|rees46_m24 [--no-learned]
Each dataset is simulated as one database (all groups together) and, where it has groups (apps, channels),
also one database per group with enough users.
"""
import json
import os
import sys
import time

import numpy as np

import loaders
import sim

HERE = os.path.dirname(os.path.abspath(__file__))
CONF = {
    # name: (loader, warm-up days, min users for a group database, min users for the learned model in a group)
    "lsapp": (loaders.lsapp, 28, 40, 10**9),
    "irc": (loaders.irc, 28, 150, 2000),
    "cdnow": (loaders.cdnow, 28, 10**9, 10**9),
    "rees46_m24": (loaders.rees46_m24, 7, 10**9, 10**9),
}


def one(name, users, ts, warmup, learned):
    t0 = time.time()
    db = sim.Database(name, loaders.encode(users), ts, warmup_days=warmup)
    d = sim.describe(db)
    r = sim.run(db, learned=learned)
    d["seconds"] = round(time.time() - t0, 1)
    print(f"  {name}: users {d['users']} sessions {d['sessions']} nights {d['eval_nights']} "
          f"daily-active/known {d['daily_active_share_of_known']:.3f} ({d['seconds']} s)", flush=True)
    for p in sim.PREDICTORS + ("oracle",):
        k = f"known|{p}|0.08"
        if k in r:
            print(f"     q8% {p:13s} h {r[k]['h']:.3f}  first-of-day {r[k]['h_first_of_day']:.3f}  "
                  f"byte share {r[k]['byte_share'] if r[k]['byte_share'] is None else round(r[k]['byte_share'], 3)}  "
                  f"after LRU 0.2% {r[k]['h_after_lru_0.002']:.3f} (LRU catches {r[k]['lru_0.002_catches']:.3f})",
                  flush=True)
    return dict(describe=d, results=r)


def main():
    name = sys.argv[1]
    learned = "--no-learned" not in sys.argv
    loader, warmup, min_group, min_learned = CONF[name]
    g, u, t = loader()
    print(f"{name}: {len(t)} accesses loaded", flush=True)
    out = {"dataset": name, "pooled": one(name, u, t, warmup, learned), "groups": {}}
    groups, counts = np.unique(g, return_counts=True)
    if len(groups) > 1:
        for grp in groups:
            m = g == grp
            nu = len(np.unique(u[m]))
            if nu < min_group:
                continue
            out["groups"][str(grp)] = one(f"{name}/{grp}", u[m], t[m], warmup, learned and nu >= min_learned)
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    with open(os.path.join(HERE, "results", f"{name}.json"), "w") as f:
        json.dump(out, f, indent=1, sort_keys=True, default=float)


if __name__ == "__main__":
    main()
