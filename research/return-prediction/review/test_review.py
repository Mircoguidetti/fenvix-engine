"""Reviewer tests for c95028b. Run from research/return-prediction: python3 -m pytest -q review/test_review.py

test_no_future_leak_* PASS: they are the leakage checks (they found nothing).
test_defect_* FAIL on c95028b: each one is a number in the delivery that is wrong; the assert message says by how much.
"""
import json
import os
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)
import sim  # noqa: E402
import summarize  # noqa: E402

DAY = sim.DAY


def trace(days=45, seed=3):
    rng = np.random.default_rng(seed)
    u, t = [], []
    for d in range(days):
        for i in range(30):                         # habitual users
            if rng.random() < 0.8:
                u.append(i); t.append(d * DAY + 9 * 3600 + int(rng.integers(0, 7200)))
        for j in range(30, 600):                    # occasional users, some with a weekday habit
            p = 0.4 if (j % 7 == d % 7 and j < 200) else 0.03
            if rng.random() < p:
                u.append(j); t.append(d * DAY + int(rng.integers(0, DAY)))
        for j in range(5):                          # brand-new users every day
            u.append(1000 + d * 10 + j); t.append(d * DAY + int(rng.integers(0, DAY)))
    return np.array(u), np.array(t)


def selections(u, t, upto_night, learned):
    """Selected user sets per (night, predictor, q) for nights < upto_night."""
    rec = {}
    orig = sim.top_k
    state = {}

    orig_features = sim.Database.features

    def features(self, n):
        state["n"] = n
        return orig_features(self, n)

    def top_k(idx, keys, k):
        sel = orig(idx, keys, k)
        rec.setdefault(state["n"], []).append(tuple(sorted(sel.tolist())))
        return sel

    sim.top_k, sim.Database.features = top_k, features
    try:
        db = sim.Database("t", u, t, warmup_days=28, hour=0)
        sim.run(db, learned=learned, retrain_every=3, populations=("known", "active28"))
    finally:
        sim.top_k, sim.Database.features = orig, orig_features
    return {n: v for n, v in rec.items() if n < upto_night}


@pytest.mark.parametrize("learned", [False, True])
def test_no_future_leak_selections(learned):
    # selections of nights 28..36 must not change when everything from day 38 on is deleted
    # (all predictors, including logistic regression and GBM, all q, both populations)
    u, t = trace()
    cut = 38
    full = selections(u, t, cut - 1, learned)
    m = t < cut * DAY
    short = selections(u[m], t[m], cut - 1, learned)
    assert full.keys() == short.keys() and len(full) >= 8
    for n in full:
        assert full[n] == short[n], f"night {n}: selection depends on data after night {cut}"


def test_no_future_leak_known_population():
    # q is taken over users seen before the boundary only; a user first seen on day n is not a candidate
    u, t = trace()
    db = sim.Database("t", u, t, warmup_days=28, hour=0)
    T, known, _ = db.features(30)
    newcomers = np.unique(u[(t >= T)])
    newcomers = newcomers[newcomers >= 1000]
    first = {x: t[u == x].min() for x in newcomers}
    for x in newcomers:
        assert known[x] == (first[x] < T)


def _res(name):
    return json.load(open(os.path.join(STUDY, "results", f"{name}.json")))["pooled"]["results"]


def test_defect_disk_quota_ignores_who_is_selected():
    # summarize.py sizes the q = 8% fill as 8% of 1 TB (82 GB): it assumes the selected users are average-sized.
    # Its own byte_share (the selected users' share of past accesses, the study's bytes proxy) says otherwise.
    r = _res("irc")["known|p5_lr|0.08"]
    claimed_gb = 0.08 * 1024
    implied_gb = r["byte_share"] * 1024
    assert implied_gb < 1.25 * claimed_gb, (
        f"IRC p5_lr q 8%: selected users own {100*r['byte_share']:.1f}% of past accesses -> ~{implied_gb:.0f} GB "
        f"at 1 TB, not {claimed_gb:.0f} GB")


def test_defect_headline_h_excludes_brand_new_users():
    # The brief: users who appear for the first time on day N+1 must count as misses. The headline h (summarize.best,
    # the CONTINUE line and the dollars) divides by session starts of KNOWN users only.
    r = _res("irc")
    h_head, p = summarize.best(r, "known", "0.08", summarize.PRED[:-1])
    h_honest = r[f"known|{p}|0.08"]["h_incl_new_users"]
    assert abs(h_head - h_honest) < 0.02, (
        f"IRC {p} q 8%: headline h {100*h_head:.1f}% vs {100*h_honest:.1f}% with brand-new users as misses")


def test_defect_dollars_use_h_not_cold_reads():
    # summarize.py multiplies the reference COLD-read GET cost (21 / 84 $) by h over ALL session starts, including
    # those the existing 0.2% LRU already serves and excluding brand-new users' cold reads.
    r = _res("irc")["known|p5_lr|0.08"]
    claimed = r["h"] * summarize.GET_QUIET
    cold_known = r["sessions"] * (1 - r["lru_0.002_catches"])
    h_cold = r["h_after_lru_0.002"] * cold_known / (cold_known + r["new_user_sessions"])
    honest = h_cold * summarize.GET_QUIET
    assert abs(claimed - honest) < 1.0, f"IRC quiet: {claimed:.1f} $ claimed vs {honest:.1f} $ on cold reads incl. new users"
