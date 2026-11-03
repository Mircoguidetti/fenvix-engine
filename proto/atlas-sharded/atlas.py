#!/usr/bin/env python3
"""M19 · Atlas Core sharded (regole DICHIARATE, shard per shard) contro Fenvix con quaderni accesi al minuto.

Modello e soglie in proto/atlas-sharded/SOGLIE.md, scritti prima di simulare. Dollari al mese (730 ore).
Riusa proto/elastica (M17): curve, sim.fenvix (camerieri), sim._prefix.
"""
import math
import os
import sys

import numpy as np

QUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(QUI, "..", "elastica"))
import sim  # noqa: E402  (M17)

H_MESE = 730.0
GIORNO = 1440

# ---------------- Atlas Core (DICHIARATO, mongodb.com/pricing 29/09/2026, us-east-1, cluster da 3 nodi) ----------
#            nome   vCPU  $/ora  disco max GB  disco compreso GB (docs "Amazon Web Services")
TAGLIE = [("M30", 2, 0.54, 512, 40),
          ("M40", 4, 1.04, 1024, 80),
          ("M50", 8, 2.00, 4096, 160),
          ("M60", 16, 3.95, 4096, 320),
          ("M80", 32, 7.30, 4096, 750),
          ("M140", 48, 10.99, 4096, 1024)]
IDX = {t[0]: i for i, t in enumerate(TAGLIE)}
VC = np.array([t[1] for t in TAGLIE], float)
PR = np.array([t[2] for t in TAGLIE], float)
DMAX = np.array([t[3] for t in TAGLIE], float)
DINC = np.array([t[4] for t in TAGLIE], float)
MONGO_R_US = 55e-6
GB_FISICI_PER_GB = 0.8
MARGINE_DISCO = 1.25
TETTO_DIM = 0.65  # CPU dello shard più carico al picco sulla taglia di dimensionamento (B6)


def p999(x):
    return float(np.percentile(x, 99.9))


# ---------------- curve e profili ----------------
def forma(c, bersaglio):
    """curva^k con k tale che p99,9/media = bersaglio (bisezione sul logaritmo di k)."""
    c = np.asarray(c, float) / c.mean()
    c = np.maximum(c, 0)

    def r(k):
        y = c ** k
        return p999(y) / y.mean()

    lo, hi = 0.01, 40.0
    if r(hi) < bersaglio:
        k = hi
    elif r(lo) > bersaglio:
        k = lo
    else:
        for _ in range(60):
            mid = math.sqrt(lo * hi)
            if r(mid) < bersaglio:
                lo = mid
            else:
                hi = mid
        k = math.sqrt(lo * hi)
    y = c ** k
    return y / y.mean(), k


def ripeti(c, giorni):
    n = giorni * GIORNO
    return np.resize(c, n)


def eventi(n, giorni_inizio=(45, 136, 227, 318), durata_g=3, rampa_h=6, fattore=10.0):
    m = np.ones(n)
    r = rampa_h * 60
    for g in giorni_inizio:
        a = g * GIORNO
        b = a + durata_g * GIORNO
        for t in range(a, min(b, n)):
            dentro = min(t - a, b - 1 - t)
            m[t] = 1 + (fattore - 1) * min(1.0, dentro / r)
    return m


def profilo(c, nome, forma_vera=False, scala=1.0, tb=None):
    """ritorna (scritture/s per minuto, TB logici, k usato)."""
    if nome in ("A", "C"):
        if forma_vera:
            y, k = c / c.mean(), 1.0
        else:
            y, k = forma(c, 5.0)
        y = ripeti(y, 28)
        picco = 40000 if nome == "A" else 80000
        w = y / p999(y) * picco * scala
        tbs = (2.0 if nome == "A" else 5.0) if tb is None else tb
    elif nome == "B":
        y, k = forma(c, 1.5)
        y = ripeti(y, 365)
        y = y * eventi(len(y))
        w = y / p999(y) * 40000 * scala
        tbs = 2.0 if tb is None else tb
    else:
        raise ValueError(nome)
    return w, tbs, k


# ---------------- Atlas ----------------
def n_shard(L_picco, tb, taglia_dim="M60"):
    i = IDX[taglia_dim]
    disco_tot = tb * 1000 * GB_FISICI_PER_GB * MARGINE_DISCO
    s = 1
    while True:
        quote = quote_shard(s)
        if L_picco * quote.max() / s <= TETTO_DIM * VC[i] and disco_tot / s <= 4096:
            return s
        s += 1


def quote_shard(s, squilibrio=0.10):
    if s == 1:
        return np.ones(1)
    q = np.linspace(1 - squilibrio, 1 + squilibrio, s)
    return q / q.mean()


def pavimento(disco_shard):
    for i in range(len(TAGLIE)):
        if DMAX[i] >= disco_shard:
            return i
    raise ValueError(disco_shard)


def atlas(L, tb, variante, taglia_dim="M60", predittiva=True, squilibrio=0.10, tmax=None, conta_da=GIORNO):
    """L: core del primario per minuto per tutto il cluster. variante: 'i' | 'ii' | 'iii'."""
    i_dim = IDX[taglia_dim]
    if tmax is None:
        tmax = min(len(TAGLIE) - 1, i_dim + 1)
    S = n_shard(p999(L), tb, taglia_dim)
    q = quote_shard(S, squilibrio)
    disco = tb * 1000 * GB_FISICI_PER_GB * MARGINE_DISCO / S
    tmin = pavimento(disco)
    n = len(L)
    Ls = np.outer(q / S, L)  # S x n
    tiers = np.zeros((S, n), int)
    over = np.zeros(S)
    if variante == "iii":
        tiers[:] = i_dim
        for s in range(S):
            over[s] = (Ls[s, conta_da:] > VC[i_dim]).sum()
    else:
        # taglia "necessaria" guardando avanti (predittiva perfetta, M17)
        need = np.zeros((S, n), int)
        need60 = np.zeros((S, n), int)
        from numpy.lib.stride_tricks import sliding_window_view
        for s in range(S):
            p = sim._prefix(np.concatenate([Ls[s], np.full(60, Ls[s, -1])]))
            f10 = (p[10:10 + n] - p[:n]) / 10
            f60 = (p[60:60 + n] - p[:n]) / 60
            nd = np.full(n, tmax)
            for i in range(tmax, tmin - 1, -1):
                nd[(f10 <= 0.90 * VC[i]) & (f60 <= 0.75 * VC[i])] = i
            need[s] = nd
            pad = np.concatenate([nd, np.full(60, nd[-1])])
            need60[s] = sliding_window_view(pad, 61).max(axis=1)[:n]
        giu = variante == "i"
        tier = [int(need[s, 0]) if giu else tmin for s in range(S)]
        last_up10 = [-10 ** 9] * S
        last_up60 = [-10 ** 9] * S
        last_pred = [-10 ** 9] * S
        last_down = [0] * S
        pu = np.zeros((S, n + 1))
        Lsl = Ls.tolist()
        needl = need.tolist()
        need60l = need60.tolist()
        vcl = VC.tolist()
        for t in range(n):
            for s in range(S):
                tr = tier[s]
                if predittiva and needl[s][t] > tr and t - last_pred[s] >= 10:
                    tr = min(needl[s][t], tr + 2)
                    last_pred[s] = last_up10[s] = last_up60[s] = t
                if t >= 10:
                    ps = pu[s]
                    m10 = (ps[t] - ps[t - 10]) / 10
                    m60 = (ps[t] - ps[t - 60]) / 60 if t >= 60 else 0.0
                    m240 = (ps[t] - ps[t - 240]) / 240 if t >= 240 else 1.0
                    if tr < tmax and m10 > 0.90 and t - last_up10[s] >= 10:
                        tr += 1
                        last_up10[s] = last_up60[s] = t
                    elif tr < tmax and m60 > 0.75 and t - last_up60[s] >= 60 and t - last_up10[s] >= 10:
                        tr += 1
                        last_up10[s] = last_up60[s] = t
                    elif (giu and tr > tmin and t - last_down[s] >= GIORNO and t >= 240 and m10 < 0.45
                          and m240 < 0.45 and not (predittiva and need60l[s][t] >= tr)):
                        tr -= 1
                        last_down[s] = t
                tier[s] = tr
            mx = max(tier)
            for s in range(S):
                if tier[s] < mx - 2:
                    tier[s] = mx - 2
            for s in range(S):
                tr = tier[s]
                u = Lsl[s][t] / vcl[tr]
                pu[s, t + 1] = pu[s, t] + (u if u < 1.0 else 1.0)
                tiers[s, t] = tr
                if u > 1.0 and t >= conta_da:
                    over[s] += 1
    tt = tiers[:, conta_da:]
    calcolo = PR[tt].mean(axis=1).sum() * H_MESE
    gb_extra = (3 * np.maximum(0.0, disco - DINC[tt])).mean(axis=1).sum()  # GB-mese oltre il compreso
    ore = {}
    for i in range(len(TAGLIE)):
        h = float((tt == i).mean(axis=1).mean() * H_MESE)
        if h > 0.05:
            ore[TAGLIE[i][0]] = round(h, 1)
    alta = float((tt >= i_dim).mean())  # quota di tempo sulla taglia di dimensionamento o sopra
    alta_max = float((tt == tt.max()).mean())
    return dict(S=S, disco_shard=disco, pavimento=TAGLIE[tmin][0], calcolo=calcolo, gb_extra=gb_extra,
                spazio=(gb_extra * 0.10, gb_extra * 0.25), backup=(100 * tb, 160 * tb),
                ore_per_taglia_per_shard=ore, quota_taglia_alta=alta, quota_taglia_max=alta_max,
                minuti_sovraccarico_per_shard=float(over.mean() / max(1, (n - conta_da)) * 30 * GIORNO),
                taglia_dim=taglia_dim)


def tot_atlas(a):
    lo = a["calcolo"] + a["spazio"][0] + a["backup"][0]
    hi = a["calcolo"] + a["spazio"][1] + a["backup"][1]
    return lo, hi, (lo + hi) / 2


# ---------------- Fenvix ----------------
QUADERNO = 451.0


def noi(w, tb, mongo_w_us, conta_da=GIORNO, rapporti=(0.81, 0.36)):
    """ritorna dict con voci per il caso migliore e peggiore (vedi SOGLIE.md)."""
    w = np.asarray(w, float)
    r = w  # letture = scritture
    wc = w[conta_da:]
    media = float(wc.mean())
    out = {}
    cam = {}
    for rap in rapporti:
        F = r * (MONGO_R_US / 0.93) + w * (mongo_w_us / rap)
        cam[rap] = sim.fenvix(F[conta_da:])["costo"]
    quad = {}
    for tetto in (42000, 20000):
        quad[tetto] = float(np.maximum(1, np.ceil(wc / (tetto * 0.8))).mean() * QUADERNO)
    quad_picco = {tetto: math.ceil(p999(wc) / (tetto * 0.8)) * QUADERNO for tetto in (42000, 20000)}
    s3 = 24.7 * tb + 0.0121 * media
    lav = 45.8 * tb + 0.0052 * media
    tz = 39.0 * media / 1000
    resto = 80.0
    for nome, rap, tetto, disp in (("migliore", rapporti[0], 42000, 185), ("peggiore", rapporti[1], 20000, 317)):
        voci = dict(camerieri=cam[rap], quaderni=quad[tetto], dispensa=disp * tb, s3=s3, lavaggio=lav, tra_zone=tz,
                    resto=resto)
        costo = sum(voci.values())
        out[nome] = dict(voci=voci, costo=costo, prezzo=costo / 0.55, quaderni_picco=quad_picco[tetto])
    out["centro"] = dict(costo=(out["migliore"]["costo"] + out["peggiore"]["costo"]) / 2)
    out["centro"]["prezzo"] = out["centro"]["costo"] / 0.55
    out["media_scritture"] = media
    out["picco_scritture"] = p999(wc)
    return out


def confronto(w, tb, mongo_w_us, varianti=("i", "i_reat", "ii", "iii"), taglia_dim="M60", squilibrio=0.10,
              con_noi=True):
    L = w * MONGO_R_US + w * mongo_w_us
    res = dict(atlas={})
    for v in varianti:
        if v == "i_reat":
            a = atlas(L, tb, "i", taglia_dim, predittiva=False, squilibrio=squilibrio)
        else:
            a = atlas(L, tb, v, taglia_dim, squilibrio=squilibrio)
        a["totale"] = tot_atlas(a)
        res["atlas"][v] = a
    if con_noi:
        f = noi(w, tb, mongo_w_us)
        res["noi"] = f
        for v, a in res["atlas"].items():
            lo, hi, c = a["totale"]
            a["rapporto"] = dict(centro=f["centro"]["prezzo"] / c, migliore=f["migliore"]["prezzo"] / hi,
                                 peggiore=f["peggiore"]["prezzo"] / lo)
    return res


if __name__ == "__main__":
    t = np.arange(14 * GIORNO)
    c = 1 + 0.8 * np.sin(2 * np.pi * t / GIORNO) ** 8 + 0.05 * np.random.default_rng(1).standard_normal(len(t))
    for prof in "AB":
        w, tb, k = profilo(c, prof)
        import time
        t0 = time.time()
        r = confronto(w, tb, 300e-6)
        print(prof, "k", round(k, 2), "media", round(w.mean()), "secondi", round(time.time() - t0, 1))
        for v, a in r["atlas"].items():
            print(" ", v, a["S"], a["pavimento"], a["ore_per_taglia_per_shard"], [round(x) for x in a["totale"]],
                  {k2: round(x, 2) for k2, x in a["rapporto"].items()}, round(a["minuti_sovraccarico_per_shard"], 1))
        print("  noi", {k2: round(r["noi"][k2]["prezzo"]) for k2 in ("migliore", "peggiore", "centro")},
              r["noi"]["peggiore"]["voci"])
