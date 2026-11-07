"""Point 3: while a draft on one customer is open for L seconds, how often does someone else write that customer?

Human traces stand in for "the rest of the world" of one customer: every event of a user is a write by or about
that user. See METHOD.md. Loaders follow `research/return-prediction/loaders.py` (same files, same md5).

Usage: python3 collisions.py L1 L2 ...   -> results/collisions.json
"""
import calendar
import glob
import json
import os
import re
import sys
import time

import numpy as np

DATA = os.environ.get("DR_DATA", "/home/user/data")
DAY = 86400
SEED = 7


# ---------------------------------------------------------------- loaders: (user codes, times in seconds)

LINE = re.compile(r"^\[(\d\d):(\d\d)\] (?:<([^>]+)>|\s\*\s(\S+))")
BOT = re.compile(r"bot|^meetingology$|^lubotu|^ubot|^ubottu")


def _norm_nick(n):
    n = n.lower().lstrip("@+")
    n = re.sub(r"[|`^].*$", "", n)
    return n.rstrip("_-0123456789") or n


def load_irc(root=None):
    """Ubuntu IRC, 11 channels pooled; one message or /me action = one write by that nick. Minute timestamps."""
    root = root or os.path.join(DATA, "irc")
    u, t = [], []
    for ch_dir in sorted(glob.glob(os.path.join(root, "*"))):
        for fp in sorted(glob.glob(os.path.join(ch_dir, "*.txt"))):
            day = calendar.timegm(time.strptime(os.path.basename(fp)[:10], "%Y-%m-%d"))
            with open(fp, errors="replace") as f:
                for line in f:
                    m = LINE.match(line)
                    if not m:
                        continue
                    nick = _norm_nick(m.group(3) or m.group(4))
                    if not nick or BOT.search(nick):
                        continue
                    u.append(nick)
                    t.append(day + int(m.group(1)) * 3600 + int(m.group(2)) * 60)
    return np.array(u, dtype=object), np.array(t, dtype=np.float64), 60.0


def load_lsapp(path=None):
    """LSApp: every app event of one phone is a write on that user. Second timestamps."""
    path = path or os.path.join(DATA, "lsapp", "lsapp.tsv")
    u, t = [], []
    with open(path) as f:
        f.readline()
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 5:
                continue
            u.append(p[0])
            t.append(calendar.timegm(time.strptime(p[2], "%Y-%m-%d %H:%M:%S")))
    return np.array(u, dtype=object), np.array(t, dtype=np.float64), 1.0


def load_cdnow(path=None):
    """CDNOW: one purchase row = one write by that customer. Day timestamps."""
    path = path or glob.glob(os.path.join(DATA, "cdnow_pkg", "*", "lifetimes", "datasets", "CDNOW_master.txt"))[0]
    u, t = [], []
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 4 or not p[1].isdigit():
                continue
            u.append(p[0])
            t.append(calendar.timegm(time.strptime(p[1], "%Y%m%d")))
    return np.array(u, dtype=object), np.array(t, dtype=np.float64), float(DAY)


LOADERS = {"irc": load_irc, "lsapp": load_lsapp, "cdnow": load_cdnow}


# ---------------------------------------------------------------- the measure

def prepare(users, times, resolution, seed=SEED):
    """Spread every event uniformly inside its clock tick, sort by (user, time)."""
    rng = np.random.default_rng(seed)
    _, codes = np.unique(users.astype(str), return_inverse=True)
    t = times + rng.uniform(0, resolution, size=len(times))
    order = np.lexsort((t, codes))
    return codes[order].astype(np.int64), t[order]


def collision_probs(codes, t, Ls, samples_per_day=50, seed=SEED, session_gap=1800.0):
    """For each L: uniform-moment, event-anchored and session-start-anchored probability that the same customer
    has another event in (t, t+L]."""
    rng = np.random.default_rng(seed + 1)
    t0 = np.floor(t.min() / DAY) * DAY
    span = t.max() - t0 + 10 * DAY
    key = codes * span + (t - t0)          # one sorted axis: user blocks, time inside
    # active (customer, day) pairs
    day = np.floor((t - t0) / DAY).astype(np.int64)
    pairs = np.unique(codes * 100000 + day)
    pu, pd = pairs // 100000, pairs % 100000
    # uniform moments in each active day
    m_u = np.repeat(pu, samples_per_day)
    m_t = np.repeat(pd * DAY, samples_per_day) + rng.uniform(0, DAY, size=len(m_u))
    mkey = m_u * span + m_t
    nxt = np.searchsorted(key, mkey, side="right")
    valid = nxt < len(key)
    nxt_u = np.where(valid, codes[np.minimum(nxt, len(key) - 1)], -1)
    nxt_dt = np.where(valid & (nxt_u == m_u), key[np.minimum(nxt, len(key) - 1)] - mkey, np.inf)
    # event anchor: next event of the same user
    same = codes[1:] == codes[:-1]
    gap_next = np.where(same, t[1:] - t[:-1], np.inf)
    gap_next = np.append(gap_next, np.inf)
    gap_prev = np.insert(np.where(same, t[1:] - t[:-1], np.inf), 0, np.inf)
    sess = gap_prev >= session_gap
    out = {"events": int(len(t)), "users": int(len(np.unique(codes))), "active_user_days": int(len(pairs)),
           "sessions": int(sess.sum()), "by_L": {}}
    for L in Ls:
        out["by_L"][str(L)] = {
            "uniform_moment": round(100.0 * float(np.mean(nxt_dt <= L)), 2),
            "event_anchor": round(100.0 * float(np.mean(gap_next <= L)), 2),
            "session_start_anchor": round(100.0 * float(np.mean(gap_next[sess] <= L)), 2),
        }
    return out


def main():
    Ls = [float(x) for x in sys.argv[1:]] or [60.0, 120.0, 300.0, 1200.0]
    res = {}
    for name, loader in LOADERS.items():
        users, times, resol = loader()
        codes, t = prepare(users, times, resol)
        res[name] = collision_probs(codes, t, Ls)
        res[name]["clock_resolution_s"] = resol
        print(name, json.dumps(res[name]["by_L"]), file=sys.stderr)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "collisions.json")
    json.dump({"L_seconds": Ls, "datasets": res}, open(out, "w"), indent=1)


if __name__ == "__main__":
    main()
