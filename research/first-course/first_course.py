"""First-course trace test (Fable olix-transport ideas 1 and 2).

For every cold session start (an access after at least 30 minutes of silence that misses the 0.2% user-level LRU
counter), after a 28-day history warm-up, classify the user by how long ago they were last seen:
never seen before (idea 2: an existence filter answers with no GET), or last seen within 1, 3, 7, 14, 30 days,
or silent longer (idea 1: an opening of about 32 KB kept for T days answers the first query locally).

Also measures, for each T, the openings held on an average day (distinct users seen in the last T days), their share
of all known users, the footprint at 32 KB per opening, and how concentrated the covered starts are across users.

Reuses the session rule, the night boundary and the LRU distance of ../return-prediction/sim.py unchanged.
"""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "return-prediction"))
import sim  # noqa: E402

DAY = sim.DAY
TS_DAYS = (1, 3, 7, 14, 30)
LRU_SHARE = 0.002            # the RAM counter at 1 TB, same as return-prediction
OPENING_KB = 32              # prompt: assume 32 KB per opening
GIB = 1024 ** 3
TIB = 1024 ** 4
PER_USER_SIZES = {"64 KB": 64 * 1024, "512 KB": 512 * 1024, "4 MB": 4 * 1024 ** 2}
BUDGET_GB = 29.0             # a quarter of the 118 GB NVMe of an m8gd.large (DECLARED, AWS instance table)


def previous_end(db):
    """For each session start: end time of the same user's previous session (-1 if none)."""
    o = np.lexsort((db.st, db.su))
    prev = np.full(len(db.su), -1, np.int64)
    same = db.su[o][1:] == db.su[o][:-1]
    prev[o[1:]] = np.where(same, db.send[o][:-1], -1)
    return prev


def classify(db, lru_share=LRU_SHARE):
    """Returns (mask of evaluated cold starts, silence in seconds per start or -1 for never seen, cold mask)."""
    prev = previous_end(db)
    silence = np.where(prev >= 0, db.st - prev, -1)
    in_eval = (db.sday >= db.warmup) & (db.sday < db.n_days)
    cold = ~(db.slru < lru_share * db.U)
    return in_eval & cold, silence, in_eval


def shares(silence_sel):
    """Share of starts by never-seen users, by bucket and cumulative within T days."""
    n = len(silence_sel)
    if n == 0:
        return None
    new = silence_sel < 0
    out = dict(starts=int(n), new=float(new.mean()))
    lo = 0
    for T in TS_DAYS:
        within = (~new) & (silence_sel <= T * DAY)
        out[f"within_{T}d"] = float(within.mean())
        out[f"bucket_{lo}_{T}d"] = float(((~new) & (silence_sel > lo * DAY) & (silence_sel <= T * DAY)).mean())
        lo = T
    out["silent_over_30d"] = float(((~new) & (silence_sel > 30 * DAY)).mean())
    ret = ~new
    out["within_30d_of_returning"] = float((silence_sel[ret] <= 30 * DAY).mean()) if ret.any() else None
    return out


def openings(db):
    """For each evaluated day boundary T: known users, and distinct users seen in the last t days for each t."""
    o = np.argsort(db.ts_all, kind="stable")
    u_all, t_all = db.users_all[o], db.ts_all[o]
    last = np.full(db.U, -1, np.int64)
    pos = 0
    rows = []
    for n in db.nights():
        T = db.origin + n * DAY
        end = np.searchsorted(t_all, T, side="left")
        if end > pos:
            np.maximum.at(last, u_all[pos:end], t_all[pos:end])
            pos = end
        known = int((last >= 0).sum())
        row = dict(day=n, known=known)
        for t in TS_DAYS:
            row[t] = int(((last >= 0) & (last >= T - t * DAY)).sum())
        rows.append(row)
    out = dict(days=len(rows))
    if not rows:
        return out
    known = np.array([r["known"] for r in rows], float)
    out["mean_known"] = float(known.mean())
    out["known_last_day"] = int(known[-1])
    for t in TS_DAYS:
        held = np.array([r[t] for r in rows], float)
        share = held / np.maximum(known, 1)
        out[f"{t}d"] = dict(
            mean_held=float(held.mean()), max_held=int(held.max()),
            share_of_known=float(share.mean()), share_first_day=float(share[0]), share_last_day=float(share[-1]),
            footprint_mb=float(held.mean() * OPENING_KB / 1024))
        out[f"{t}d"]["scale_1tb"] = {
            name: dict(users=int(TIB // size),
                       footprint_gb=float(out[f"{t}d"]["share_of_known"] * (TIB // size) * OPENING_KB * 1024 / GIB),
                       fits_29gb=bool(out[f"{t}d"]["share_of_known"] * (TIB // size) * OPENING_KB * 1024 / GIB
                                      <= BUDGET_GB))
            for name, size in PER_USER_SIZES.items()}
    return out


def spread(db, cold_eval, silence, T):
    """How the cold starts covered by an opening kept T days are spread across users."""
    cov = cold_eval & (silence >= 0) & (silence <= T * DAY)
    per_user = np.bincount(db.su[cov], minlength=db.U)
    per_user = np.sort(per_user[per_user > 0])[::-1]
    cold_users = len(np.unique(db.su[cold_eval]))
    ret_users = len(np.unique(db.su[cold_eval & (silence >= 0)]))
    if len(per_user) == 0:
        return dict(covered_users=0)
    tot = per_user.sum()
    top = lambda f: float(per_user[: max(1, int(np.ceil(f * len(per_user))))].sum() / tot)
    cum = np.cumsum(per_user) / tot
    return dict(covered_starts=int(tot), covered_users=int(len(per_user)),
                users_with_cold_start=int(cold_users), returning_users_with_cold_start=int(ret_users),
                top1pct_share=top(0.01), top10pct_share=top(0.10),
                users_for_half=int(np.searchsorted(cum, 0.5) + 1),
                users_for_half_share=float((np.searchsorted(cum, 0.5) + 1) / len(per_user)),
                median_starts_per_covered_user=float(np.median(per_user)))


def new_share_by_period(db, cold_eval, silence):
    """New-user share of cold starts in the first and the last 28 evaluated days (a short trace labels long-silent
    users as new early on; the late window is the less biased one)."""
    first = db.warmup
    last = db.n_days
    out = {}
    for name, (a, b) in (("first_28d", (first, first + 28)), ("last_28d", (last - 28, last))):
        m = cold_eval & (db.sday >= a) & (db.sday < b)
        out[name] = float((silence[m] < 0).mean()) if m.any() else None
    return out


def measure(db):
    cold_eval, silence, in_eval = classify(db)
    all_starts = in_eval
    res = dict(
        users=int(db.U), sessions=int(len(db.su)), eval_days=len(db.nights()),
        starts_eval=int(all_starts.sum()), cold_starts=int(cold_eval.sum()),
        lru_catches=float(1 - cold_eval.sum() / all_starts.sum()) if all_starts.sum() else None,
        cold=shares(silence[cold_eval]),
        all_starts=shares(silence[all_starts]),
        openings=openings(db),
        spread={f"{T}d": spread(db, cold_eval, silence, T) for T in (7, 30)},
        new_by_period=new_share_by_period(db, cold_eval, silence),
    )
    return res
