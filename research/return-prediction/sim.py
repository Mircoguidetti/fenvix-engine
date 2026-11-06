"""Return prediction simulator (Fable olix-impossible idea C).

Every night N, using only accesses before the night boundary T, rank the users of one database and select the top q%.
On the next 24 hours, measure the share of "first accesses" (session starts: an access after at least 30 minutes of
silence from the same user) that belong to selected users: the hit share h.

Input: one database = arrays (user codes 0..U-1, timestamps in seconds, sorted by time or not).
Everything here is deterministic except the training subsample of the learned model (fixed seed).
"""
import math
import numpy as np

DAY = 86400
SESSION_GAP = 1800          # 30 minutes of silence starts a new session (prompt)
QS = (0.02, 0.05, 0.08, 0.15, 0.20)
PREDICTORS = ("p1_yesterday", "p2_last7", "p3_weekday", "p4_gaphist", "p5_lr", "p5_gbm")


LRU_SHARES = (0.002, 0.01)    # cache that already exists, in share of users: RAM counter at 1 TB ~0.2% (see README)


def lru_user_distance(su, st, send):
    """For each session start, the number of distinct OTHER users whose latest access falls between this user's
    previous session end and this start (user-level LRU stack distance). inf for a user's first session.
    Approximation: a user's mark moves to its session end when the session starts, so users with a session
    still open at that moment are not counted; this makes the existing cache look stronger (conservative for C)."""
    times = np.unique(np.concatenate([st, send]))
    i_start = np.searchsorted(times, st)
    i_end = np.searchsorted(times, send)
    n = len(times)
    tree = np.zeros(n + 1, np.int64)

    def add(i, v):
        i += 1
        while i <= n:
            tree[i] += v
            i += i & -i

    def prefix(i):            # sum over positions 0..i-1
        s = 0
        while i > 0:
            s += tree[i]
            i -= i & -i
        return s

    last = {}
    dist = np.full(len(su), np.inf)
    for k in range(len(su)):
        u = int(su[k])
        p = last.get(u)
        if p is not None:
            dist[k] = prefix(int(i_start[k])) - prefix(p + 1)
            add(p, -1)
        e = int(i_end[k])
        add(e, 1)
        last[u] = e
    return dist


def boundary_hour(ts):
    """UTC hour with the fewest accesses: the night fill happens there (the quietest hour)."""
    h = np.bincount(((ts % DAY) // 3600).astype(np.int64), minlength=24)
    return int(np.argmin(h))


def sessionize(users, ts):
    """Return (users, ts, n_events) of session starts, sorted by time; n_events = accesses in that session."""
    o = np.lexsort((ts, users))
    u, t = users[o], ts[o]
    new = np.ones(len(u), bool)
    new[1:] = (u[1:] != u[:-1]) | (t[1:] - t[:-1] >= SESSION_GAP)
    idx = np.flatnonzero(new)
    n_ev = np.diff(np.append(idx, len(u)))
    su, st = u[idx], t[idx]
    send = t[np.append(idx[1:], len(u)) - 1]
    o2 = np.argsort(st, kind="stable")
    return su[o2], st[o2], n_ev[o2], send[o2]


class Database:
    def __init__(self, name, users, ts, warmup_days=28, hour=None):
        users = np.asarray(users, np.int64)
        ts = np.asarray(ts, np.int64)
        self.name = name
        self.U = int(users.max()) + 1
        self.hour = boundary_hour(ts) if hour is None else hour
        day0 = (ts.min() - self.hour * 3600) // DAY
        self.origin = day0 * DAY + self.hour * 3600            # first boundary at or before the first access
        self.su, self.st, self.sev, self.send = sessionize(users, ts)
        self.slru = lru_user_distance(self.su, self.st, self.send)
        self.sday = (self.st - self.origin) // DAY             # day index of each session start
        # gap to the previous session of the same user (-1 for a user's first session)
        o = np.lexsort((self.st, self.su))
        g = np.full(len(self.su), -1, np.int64)
        same = self.su[o][1:] == self.su[o][:-1]
        g_o = np.where(same, self.st[o][1:] - self.st[o][:-1], -1)
        g[o[1:]] = g_o
        self.sgap = g
        self.first_ts = np.full(self.U, np.iinfo(np.int64).max)
        np.minimum.at(self.first_ts, users, ts)
        self.n_days = int((ts.max() - self.origin) // DAY)     # last complete day index is n_days - 1
        self.warmup = warmup_days
        self.n_events = len(ts)
        # (user, day) activity pairs
        ud = np.unique(self.su * 100000 + self.sday)
        self.ud_user, self.ud_day = ud // 100000, ud % 100000
        # event counts per session, cumulated per user over time: proxy for the user's bytes
        self.users_all = users
        self.ts_all = ts

    def nights(self):
        return range(self.warmup, self.n_days)

    # ------------------------------------------------------------------ features for night n (boundary T)
    def features(self, n):
        T = self.origin + n * DAY
        U = self.U
        before = self.st < T
        su, st, sday, sgap, sev = self.su[before], self.st[before], self.sday[before], self.sgap[before], self.sev[before]
        known = self.first_ts < T
        last = np.full(U, -1, np.int64)
        np.maximum.at(last, su, st)
        elapsed = np.where(last >= 0, T - last, 10**9).astype(np.float64)

        def cnt(days):
            m = st >= T - days * DAY
            return np.bincount(su[m], minlength=U).astype(np.float64)

        c1, c7, c28 = cnt(1), cnt(7), cnt(28)
        call = np.bincount(su, minlength=U).astype(np.float64)
        ev = np.bincount(su, weights=sev, minlength=U)          # accesses so far (data-size proxy)
        udm = self.ud_day < n
        uu, dd = self.ud_user[udm], self.ud_day[udm]
        a7 = np.bincount(uu[dd >= n - 7], minlength=U).astype(np.float64)
        a28 = np.bincount(uu[dd >= n - 28], minlength=U).astype(np.float64)
        aall = np.bincount(uu, minlength=U).astype(np.float64)
        wk = np.zeros(U)
        for k in (1, 2, 3, 4):
            wk += np.bincount(uu[dd == n - 7 * k], minlength=U)
        weeks_seen = np.clip(np.ceil((T - self.first_ts) / (7 * DAY)), 0, 4)  # how many same-weekdays the user could have shown
        wk_frac = np.where(weeks_seen > 0, wk / np.maximum(weeks_seen, 1), 0.0)
        # gap histogram hazard: P(next session within 24 h | silent for `elapsed`)
        gm = sgap >= 0
        gu, gg = su[gm], sgap[gm].astype(np.float64)
        e_g = elapsed[gu]
        hit = np.bincount(gu, weights=((gg > e_g) & (gg <= e_g + DAY)).astype(np.float64), minlength=U)
        surv = np.bincount(gu, weights=(gg > e_g).astype(np.float64), minlength=U)
        # pooled prior over all users: closed gaps plus the open (censored) gap of every known user, so that users
        # who never came back count in the denominator (without them one-off visitors look like fast returners)
        allg = np.sort(gg)
        openg = np.sort(elapsed[known])
        n_gt_e = (len(allg) - np.searchsorted(allg, elapsed, side="right")
                  + len(openg) - np.searchsorted(openg, elapsed, side="right"))
        n_gt_e1 = len(allg) - np.searchsorted(allg, elapsed + DAY, side="right")
        n_hit = (len(allg) - np.searchsorted(allg, elapsed, side="right")) - n_gt_e1
        pooled = (n_hit + 0.5) / (n_gt_e + 1.0)
        K = 2.0
        hazard = (hit + K * pooled) / (surv + 1.0 + K)          # +1: the open gap is censored (> elapsed, no hit yet)
        age = np.where(known, (T - self.first_ts) / DAY, 0.0)
        feats = dict(c1=c1, c7=c7, c28=c28, call=call, a7=a7, a28=a28, aall=aall, wk=wk, wk_frac=wk_frac,
                     hazard=hazard, pooled=pooled, elapsed=elapsed, age=age, ev=ev, last=last.astype(np.float64))
        return T, known, feats

    def outcome(self, n, lru_share=None):
        """sessions per user on day n (the 24 h after boundary n). With lru_share, only the sessions whose first
        read misses a user-level LRU cache holding that share of all users (the cache that already exists)."""
        m = self.sday == n
        if lru_share is not None:
            m &= ~(self.slru < lru_share * self.U)
        return np.bincount(self.su[m], minlength=self.U).astype(np.float64)


FEATURE_NAMES = ("c1", "c7", "c28", "call", "a7", "a28", "aall", "wk", "wk_frac", "hazard", "pooled", "elapsed", "age")


def design(feats, idx):
    cols = []
    for k in FEATURE_NAMES:
        x = feats[k][idx]
        if k in ("c1", "c7", "c28", "call", "a7", "a28", "aall", "age"):
            x = np.log1p(x)
        if k == "elapsed":
            x = np.log1p(np.minimum(x, 400 * DAY) / 3600.0)
        cols.append(x)
    return np.column_stack(cols)


def simple_scores(feats):
    """(primary, tiebreak1, tiebreak2) per predictor; higher is better. Ties broken by recency last."""
    f = feats
    return {
        "p1_yesterday": (f["c1"], f["last"], f["call"]),
        "p2_last7": (f["c7"], f["a7"], f["last"]),
        "p3_weekday": (f["wk_frac"] * (f["wk"] > 0), f["a28"], f["last"]),
        "p4_gaphist": (f["hazard"], f["last"], f["call"]),
    }


def top_k(idx, keys, k):
    """idx: candidate user ids; keys: tuple of arrays over all users (primary first). Returns k selected ids."""
    if k <= 0:
        return idx[:0]
    order = np.lexsort(tuple(kk[idx] for kk in reversed(keys)))   # ascending by primary, then tiebreaks
    return idx[order[::-1][:k]]


def run(db, learned=True, retrain_every=7, max_train_rows=300000, seed=0, populations=("known", "active28")):
    """Simulate every night after warm-up. Returns dict with session-weighted totals per population/predictor/q."""
    rng = np.random.default_rng(seed)
    res = {}
    hist_X, hist_y = [], []
    lr = gbm = None
    last_fit = -10**9
    for n in db.nights():
        T, known, f = db.features(n)
        y = db.outcome(n)
        known_idx = np.flatnonzero(known)
        if len(known_idx) == 0:
            continue
        X = design(f, known_idx)
        if learned and len(hist_X) >= 7 and (n - last_fit >= retrain_every or lr is None):
            Xtr = np.vstack(hist_X)
            ytr = np.concatenate(hist_y)
            if len(ytr) > max_train_rows:
                s = rng.choice(len(ytr), max_train_rows, replace=False)
                Xtr, ytr = Xtr[s], ytr[s]
            if (ytr > 0).any() and (ytr == 0).any():
                from sklearn.linear_model import LogisticRegression
                from sklearn.preprocessing import StandardScaler
                from sklearn.pipeline import make_pipeline
                from sklearn.ensemble import HistGradientBoostingRegressor
                lr = make_pipeline(StandardScaler(), LogisticRegression(max_iter=300, C=1.0))
                lr.fit(Xtr, (ytr > 0).astype(int), logisticregression__sample_weight=np.maximum(ytr, 1.0))
                gbm = HistGradientBoostingRegressor(loss="poisson", max_iter=150, learning_rate=0.08,
                                                    max_leaf_nodes=31, random_state=seed)
                gbm.fit(Xtr, ytr)
                last_fit = n
        scores = simple_scores(f)
        if lr is not None:
            p_lr = np.zeros(db.U); p_lr[known_idx] = lr.predict_proba(X)[:, 1]
            p_gb = np.zeros(db.U); p_gb[known_idx] = gbm.predict(X)
            scores["p5_lr"] = (p_lr, f["last"], f["call"])
            scores["p5_gbm"] = (p_gb, f["last"], f["call"])
        hist_X.append(X)
        hist_y.append(y[known_idx])
        new_sessions = float(y[~known].sum())
        first_of_day = (y > 0).astype(np.float64)
        ycold = {c: db.outcome(n, c) for c in LRU_SHARES}
        for pop in populations:
            cand = known_idx if pop == "known" else known_idx[f["c28"][known_idx] > 0]
            if len(cand) == 0:
                continue
            tot = float(y[cand].sum()) if pop == "known" else float(y[known_idx].sum())
            tot_first = float(first_of_day[cand].sum()) if pop == "known" else float(first_of_day[known_idx].sum())
            ev_tot = float(f["ev"][known_idx].sum())
            for q in QS:
                k = int(math.ceil(q * len(cand)))
                oracle = np.sort(y[cand])[::-1][:k].sum()
                for p, keys in list(scores.items()) + [("oracle", None)]:
                    if p == "oracle":
                        hits, hits_first, evsel = oracle, min(k, (y[cand] > 0).sum()), np.nan
                        sel = cand[np.argsort(-y[cand], kind="stable")[:k]]
                    else:
                        sel = top_k(cand, keys, k)
                        hits = float(y[sel].sum())
                        hits_first = float(first_of_day[sel].sum())
                        evsel = float(f["ev"][sel].sum())
                    r = res.setdefault((pop, p, q), dict(hits=0.0, tot=0.0, hits_first=0.0, tot_first=0.0, new=0.0,
                                                         ev_sel=0.0, ev_tot=0.0, nights=0, per_night=[], k=[],
                                                         cold_hits={c: 0.0 for c in LRU_SHARES},
                                                         cold_tot={c: 0.0 for c in LRU_SHARES}))
                    for c in LRU_SHARES:
                        r["cold_hits"][c] += float(ycold[c][sel].sum())
                        r["cold_tot"][c] += float(ycold[c][known_idx].sum())
                    r["hits"] += hits; r["tot"] += tot
                    r["hits_first"] += hits_first; r["tot_first"] += tot_first
                    r["new"] += new_sessions
                    r["ev_sel"] += 0.0 if np.isnan(evsel) else evsel
                    r["ev_tot"] += ev_tot
                    r["nights"] += 1
                    r["per_night"].append(hits / tot if tot else np.nan)
                    r["k"].append(k)
    out = {}
    for (pop, p, q), r in res.items():
        pn = np.array(r["per_night"], float)
        pn = pn[~np.isnan(pn)]
        out[f"{pop}|{p}|{q}"] = dict(
            h=r["hits"] / r["tot"] if r["tot"] else None,
            h_first_of_day=r["hits_first"] / r["tot_first"] if r["tot_first"] else None,
            h_incl_new_users=r["hits"] / (r["tot"] + r["new"]) if r["tot"] + r["new"] else None,
            byte_share=(r["ev_sel"] / r["ev_tot"]) if r["ev_tot"] and p != "oracle" else None,
            nights=r["nights"], night_p10=float(np.percentile(pn, 10)) if len(pn) else None,
            night_p90=float(np.percentile(pn, 90)) if len(pn) else None,
            mean_k=float(np.mean(r["k"])), sessions=r["tot"], new_user_sessions=r["new"])
        for c in LRU_SHARES:
            # h on the first reads that miss an existing user-level LRU holding share c of users
            out[f"{pop}|{p}|{q}"][f"h_after_lru_{c}"] = (r["cold_hits"][c] / r["cold_tot"][c]) if r["cold_tot"][c] else None
            out[f"{pop}|{p}|{q}"][f"lru_{c}_catches"] = 1 - r["cold_tot"][c] / r["tot"] if r["tot"] else None
    return out


def describe(db):
    """Habit statistics: users, days, daily active share, sessions per day."""
    days = db.n_days
    act = []
    for n in db.nights():
        T = db.origin + n * DAY
        known = (db.first_ts < T).sum()
        dau = len(np.unique(db.su[db.sday == n]))
        act.append((known, dau))
    known = np.array([a for a, _ in act], float)
    dau = np.array([b for _, b in act], float)
    return dict(users=int(db.U), events=int(db.n_events), sessions=int(len(db.su)), days=int(days),
                eval_nights=len(act), boundary_hour_utc=db.hour,
                mean_known_users=float(known.mean()) if len(known) else None,
                mean_daily_active=float(dau.mean()) if len(dau) else None,
                daily_active_share_of_known=float((dau / np.maximum(known, 1)).mean()) if len(dau) else None)
