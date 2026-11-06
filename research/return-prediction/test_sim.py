"""Sanity tests for sim.py on synthetic traces. Run: python3 -m pytest -q test_sim.py (or python3 test_sim.py)."""
import numpy as np

import sim

DAY = sim.DAY


def trace(daily_users, random_users, p_random, days=70, seed=1):
    rng = np.random.default_rng(seed)
    u, t = [], []
    for d in range(days):
        for i in range(daily_users):           # come every day at 09:00 plus a few minutes
            u.append(i); t.append(d * DAY + 9 * 3600 + int(rng.integers(0, 600)))
        act = rng.random(random_users) < p_random
        for j in np.flatnonzero(act):           # memoryless users: same chance every day
            u.append(daily_users + j); t.append(d * DAY + int(rng.integers(0, DAY)))
    # everyone shows up on day 0 so that all users are known from the start
    for i in range(daily_users + random_users):
        u.append(i); t.append(int(rng.integers(0, 3 * 3600)))
    return np.array(u), np.array(t)


def test_daily_users_are_found():
    # 50 daily users out of 1,000: at q = 5% the daily users fill the quota and carry all their sessions
    u, t = trace(50, 950, 0.02)
    db = sim.Database("t", u, t, warmup_days=28, hour=0)
    r = sim.run(db, learned=False, populations=("known",))
    daily_share = 50 / (50 + 950 * 0.02)
    for p in ("p2_last7", "p3_weekday", "p4_gaphist"):
        h = r[f"known|{p}|0.05"]["h"]
        assert abs(h - daily_share) < 0.05, (p, h, daily_share)
    # "seen yesterday" ties daily users with random users seen yesterday; recency breaks the tie, so it finds fewer
    assert 0.3 < r["known|p1_yesterday|0.05"]["h"] < daily_share


def test_memoryless_users_give_h_near_q():
    # nobody has a habit: any ranking selects users whose next-day chance is the same as everyone else's
    u, t = trace(0, 5000, 0.05, days=50)
    db = sim.Database("t", u, t, warmup_days=28, hour=0)
    r = sim.run(db, learned=False, populations=("known",))
    for q in (0.08, 0.20):
        for p in ("p1_yesterday", "p2_last7", "p4_gaphist"):
            h = r[f"known|{p}|{q}"]["h"]
            assert abs(h - q) < 0.03, (p, q, h)


def test_no_future_leak():
    # changing what happens after night n must not change the selection of night n
    u, t = trace(20, 500, 0.05, days=40)
    db1 = sim.Database("a", u, t, warmup_days=28, hour=0)
    cut = t < 35 * DAY
    db2 = sim.Database("b", u[cut], t[cut], warmup_days=28, hour=0)
    for n in (29, 32):
        _, k1, f1 = db1.features(n)
        _, k2, f2 = db2.features(n)
        for key in ("c1", "c7", "wk", "hazard"):
            assert np.allclose(f1[key][: len(f2[key])], f2[key]), key


def test_sessions_split_on_30_minutes():
    u = np.array([0, 0, 0, 0])
    t = np.array([0, 1799, 3598, 3598 + 1800])
    su, st, n, end = sim.sessionize(u, t)
    assert list(st) == [0, 3598 + 1800] and list(n) == [3, 1] and list(end) == [3598, 3598 + 1800]


def test_lru_distance():
    # user 0 at t=0; users 1 and 2 in between; user 0 again at t=10000: two other users seen in between
    su = np.array([0, 1, 2, 1, 0]); st = np.array([0, 2000, 4000, 6000, 10000]); end = st.copy()
    d = sim.lru_user_distance(su, st, end)
    assert np.isinf(d[0]) and d[3] == 1 and d[4] == 2, d


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
