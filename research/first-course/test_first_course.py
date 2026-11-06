"""Tests on synthetic traces where the answer is known. Run: python3 -m pytest -q test_first_course.py
(or python3 test_first_course.py)."""
import numpy as np

import first_course as fc
import sim

DAY = sim.DAY


def db_of(u, t, warmup=28):
    return sim.Database("t", np.array(u), np.array(t), warmup_days=warmup, hour=0)


def test_all_users_daily():
    # 300 users, each once a day around 09:00 for 60 days: every cold start is a user seen within 1 day
    u, t = [], []
    for d in range(60):
        for i in range(300):
            u.append(i); t.append(d * DAY + 9 * 3600 + i)
    r = fc.measure(db_of(u, t))
    c = r["cold"]
    assert r["cold_starts"] == r["starts_eval"] == 300 * 31, r["cold_starts"]   # days 28..58; day 59 is incomplete
    assert c["new"] == 0.0 and c["within_1d"] == 1.0 and c["within_30d"] == 1.0 and c["silent_over_30d"] == 0.0
    for T in fc.TS_DAYS:            # every known user was seen in the last day: openings = all known users
        assert r["openings"][f"{T}d"]["mean_held"] == 300
        assert r["openings"][f"{T}d"]["share_of_known"] == 1.0
    sp = r["spread"]["30d"]         # perfectly even spread
    assert sp["covered_users"] == 300 and abs(sp["top10pct_share"] - 0.10) < 1e-9


def test_all_users_new():
    # 50 brand-new users a day, each seen once: every cold start is a never-seen user, no opening ever helps
    u, t = [], []
    for d in range(60):
        for i in range(50):
            u.append(d * 50 + i); t.append(d * DAY + 3600 + i * 60)
    r = fc.measure(db_of(u, t))
    c = r["cold"]
    assert r["cold_starts"] == 50 * 31
    assert c["new"] == 1.0 and c["within_30d"] == 0.0 and c["silent_over_30d"] == 0.0
    o = r["openings"]
    nights = range(28, 59)
    assert o["1d"]["mean_held"] == 50
    assert abs(o["30d"]["mean_held"] - np.mean([50 * min(n, 30) for n in nights])) < 1e-9
    # known grows by 50 a day: on night n there are 50 n known users, 50 of them seen in the last day
    assert abs(o["1d"]["share_of_known"] - np.mean([1 / n for n in nights])) < 1e-9
    assert r["spread"]["30d"]["covered_users"] == 0


def test_users_return_only_after_60_days():
    # 120 users, user i visits on day i % 60 and then every 60 days: no return falls within 30 days
    u, t = [], []
    for i in range(120):
        for d in range(i % 60, 200, 60):
            u.append(i); t.append(d * DAY + 12 * 3600 + i)
    r = fc.measure(db_of(u, t))
    c = r["cold"]
    assert r["cold_starts"] > 0
    assert c["within_30d"] == 0.0
    assert abs(c["new"] + c["silent_over_30d"] - 1.0) < 1e-12
    assert c["silent_over_30d"] > 0.5
    # openings at 30 days hold only the users who visited in the last 30 days: about half of the known users
    assert 0.3 < r["openings"]["30d"]["share_of_known"] < 0.7


def test_lru_hits_are_not_cold():
    # one user back after 40 minutes with nobody else in between: the counter already holds it, not a cold start
    u = [0, 0, 1]
    t = [29 * DAY + 3600, 29 * DAY + 3600 + 2400, 29 * DAY + 7200]
    # user 0 was also seen on day 5 (silent 24 days); user 5 frames the trace so that day 29 is evaluated
    # users 10..19 are seen on day 10, so user 0 is no longer in the counter on day 29
    u = [5, 0] + list(range(10, 20)) + u + [5]; t = [0, 5 * DAY] + [10 * DAY] * 10 + t + [31 * DAY]
    r = fc.measure(db_of(u, t))
    assert r["starts_eval"] == 3 and r["cold_starts"] == 2   # user 0 back after 24 days, and user 1 (new)
    assert r["cold"]["new"] == 0.5 and r["cold"]["silent_over_30d"] == 0.0 and r["cold"]["within_30d"] == 0.5


def test_scale_to_1tb_fits():
    # the 1 TB scaling: footprint = share of known x (1 TiB / size) x 32 KiB, against 29 GB
    rng = np.random.default_rng(2)
    u, t = [], []
    for d in range(60):                     # 1,000 users, half of them active every day, half once at the start
        for i in range(500):
            u.append(i); t.append(d * DAY + 9 * 3600 + int(rng.integers(0, 600)))
    for i in range(500, 1000):
        u.append(i); t.append(3600 + i)
    r = fc.measure(db_of(u, t))
    s = r["openings"]["1d"]
    assert s["share_of_known"] == 0.5
    sc = s["scale_1tb"]
    assert sc["512 KB"]["users"] == 2 * 1024 ** 2
    assert abs(sc["512 KB"]["footprint_gb"] - 32.0) < 1e-9 and not sc["512 KB"]["fits_29gb"]
    assert abs(sc["4 MB"]["footprint_gb"] - 4.0) < 1e-9 and sc["4 MB"]["fits_29gb"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
