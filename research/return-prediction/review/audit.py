"""Reviewer audit of the return-prediction study (c95028b). Runs sim.run unchanged, with three additions:

1. Baselines added as extra predictors: b_random (a fresh random q% of known users every night) and
   b_most_active_ever (most sessions ever before the boundary; ties by recency).
2. Byte budget (--bytes): instead of q% of USERS, select users in score order until their past accesses reach q% of
   the past accesses of all known users (accesses = the study's own proxy for bytes, `byte_share`). This is what an
   82 GB (8% of 1 TB) disk quota actually holds; the study's quota assumes the selected users are average-sized.
3. Dollars with brand-new users counted as misses: saved share of cold reads = hits after the 0.2% LRU divided by
   (cold reads of known users + sessions of brand-new users), times the reference 21 / 84 $ per 1 TB per month.

Usage: python3 review/audit.py <dataset> [--bytes] [--no-learned]   (needs the raw data, see ../README.md)
Writes review/results/<dataset>[_bytes].json and prints the q = 8% lines.
"""
import json
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import loaders  # noqa: E402
import sim  # noqa: E402
from run import CONF  # noqa: E402

GET_QUIET, GET_MEDIUM = 21.0, 84.0      # summarize.py, reference model D1c (unchanged)

_orig_scores = sim.simple_scores
_rng = np.random.default_rng(12345)
_cur = {}


def scores_with_baselines(f):
    s = _orig_scores(f)
    s["b_random"] = (_rng.random(len(f["c1"])), f["last"], f["call"])
    s["b_most_active_ever"] = (f["call"], f["last"], f["ev"])
    _cur["ev"] = f["ev"]
    return s


_orig_top_k = sim.top_k


def top_k_bytes(idx, keys, k):
    """Same ranking as sim.top_k, but the quota is in accesses (bytes proxy), not in users. k is ignored except to
    recover q = k / len(idx)."""
    if k <= 0:
        return idx[:0]
    q = k / len(idx)
    order = np.lexsort(tuple(kk[idx] for kk in reversed(keys)))[::-1]
    ranked = idx[order]
    ev = _cur["ev"][ranked]
    budget = q * _cur["ev"][idx].sum()
    n = int(np.searchsorted(np.cumsum(ev), budget, side="right"))
    return ranked[:max(n, 0)]


def main():
    name = sys.argv[1]
    by_bytes = "--bytes" in sys.argv
    learned = "--no-learned" not in sys.argv
    sim.simple_scores = scores_with_baselines
    if by_bytes:
        sim.top_k = top_k_bytes
    loader, warmup, _, _ = CONF[name]
    g, u, t = loader()
    db = sim.Database(name, loaders.encode(u), t, warmup_days=warmup)
    r = sim.run(db, learned=learned, populations=("known",))
    out = {}
    for key, x in r.items():
        cold_known = x["sessions"] * (1 - x["lru_0.002_catches"]) if x["lru_0.002_catches"] is not None else None
        hits_cold = x["h_after_lru_0.002"] * cold_known if cold_known else 0.0
        # brand-new users' sessions: almost all cold (first session has infinite LRU distance) -> all counted as misses
        h_cold_incl_new = hits_cold / (cold_known + x["new_user_sessions"]) if cold_known else None
        x["h_after_lru_0.002_incl_new_users"] = h_cold_incl_new
        out[key] = x
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    fn = f"{name}{'_bytes' if by_bytes else ''}.json"
    with open(os.path.join(HERE, "results", fn), "w") as f:
        json.dump(out, f, indent=1, sort_keys=True, default=float)
    print(f"{name} {'BYTE budget' if by_bytes else 'USER budget'} q = 8% (known users)")
    for key in sorted(out):
        pop, p, q = key.split("|")
        if q != "0.08" or p == "oracle":
            continue
        x = out[key]
        hc = x["h_after_lru_0.002_incl_new_users"]
        print(f"  {p:20s} h {100*x['h']:5.1f}%  incl new users {100*x['h_incl_new_users']:5.1f}%  "
              f"byte_share {100*(x['byte_share'] or 0):5.1f}%  mean selected users {x['mean_k']:8.1f}  "
              f"cold h incl new {100*hc:5.1f}% -> {hc*GET_QUIET:5.1f} $ quiet {hc*GET_MEDIUM:5.1f} $ medium")


if __name__ == "__main__":
    main()
