#!/usr/bin/env python3
"""M17 · bolletta elastica: Infinite (regole DICHIARATE) contro camerieri Fenvix, minuto per minuto.

Modello e soglie in proto/elastica/SOGLIE.md (scritti prima di simulare). Tutti i dollari sono al mese (730 ore).
"""
import math
import numpy as np

H_MESE = 730.0

# ---------------- Infinite (DICHIARATO, pagina prezzi MongoDB 29/09/2026, us-east-1, per HA shard) ----------------
TAGLIE = [  # nome, vCPU, $/ora per HA shard (primario + standby)
    ("M10", 2, 0.088),
    ("M20", 2, 0.227),
    ("M30", 2, 0.677),
    ("M40", 4, 1.354),
    ("M50", 8, 2.709),
    ("M60", 16, 5.417),
]
MONGO_R_US, MONGO_W_US = 55e-6, 300e-6  # B1, STIMATO


def _prefix(x):
    p = np.zeros(len(x) + 1)
    np.cumsum(x, out=p[1:])
    return p


def infinite(L, predittiva, tmin=2, fatt_ora=False):
    """L: core occupati sul primario per minuto. Ritorna dict con costo calcolo al mese e statistiche.
    tmin: indice della taglia minima (2 = M30)."""
    n = len(L)
    vc = np.array([t[1] for t in TAGLIE], float)
    pr = np.array([t[2] for t in TAGLIE], float)
    # taglia minima con CPU <= 75% per ogni minuto
    need = np.full(n, len(TAGLIE) - 1)
    for i in range(len(TAGLIE) - 1, tmin - 1, -1):
        need[L <= 0.75 * vc[i]] = i
    need = np.maximum(need, tmin)
    # massimo dei prossimi 60 minuti (per la predittiva)
    if predittiva:
        from numpy.lib.stride_tricks import sliding_window_view
        pad = np.concatenate([need, np.full(60, need[-1])])
        need60 = sliding_window_view(pad, 61).max(axis=1)[:n]
    tier = int(need[0]) if predittiva else max(tmin, int(need[0]))
    util = np.zeros(n)
    tiers = np.zeros(n, int)
    last_up10 = last_up60 = -10**9
    last_down = 0  # appena "creato": nessuna discesa nelle prime 24 ore
    over = 0
    over_m60 = 0
    pu = np.zeros(n + 1)
    for t in range(n):
        if predittiva and need60[t] > tier:
            tier = int(need60[t])
            last_up10 = last_up60 = t
        # regole reattive guardano il passato (fino al minuto t-1)
        if t >= 10:
            m10 = (pu[t] - pu[t - 10]) / 10
            m60 = (pu[t] - pu[t - 60]) / 60 if t >= 60 else 0
            m240 = (pu[t] - pu[t - 240]) / 240 if t >= 240 else 1
            if tier < len(TAGLIE) - 1 and m10 > 0.90 and t - last_up10 >= 10:
                tier += 1
                last_up10 = t
                last_up60 = t
            elif tier < len(TAGLIE) - 1 and m60 > 0.75 and t - last_up60 >= 60 and t - last_up10 >= 10:
                tier += 1
                last_up10 = t
                last_up60 = t
            elif (tier > tmin and t - last_down >= 1440 and t >= 240 and m10 < 0.45 and m240 < 0.45
                  and not (predittiva and need60[t] >= tier)):
                tier -= 1
                last_down = t
        u = L[t] / vc[tier]
        util[t] = min(u, 1.0)
        pu[t + 1] = pu[t] + util[t]
        tiers[t] = tier
        if u > 1.0:
            over += 1
        if L[t] > vc[-1]:
            over_m60 += 1
    if fatt_ora:
        nh = math.ceil(n / 60)
        tt = np.concatenate([tiers, np.full(nh * 60 - n, tiers[-1])]).reshape(nh, 60).max(axis=1)
        costo_ora = pr[tt].sum()
        costo = costo_ora / nh * H_MESE
    else:
        costo = pr[tiers].mean() * H_MESE
    dist = {TAGLIE[i][0]: round(float((tiers == i).mean()), 3) for i in range(len(TAGLIE)) if (tiers == i).any()}
    return dict(costo=costo, standby=costo / 2, over_min=over, over_m60_min=over_m60, util_media=float(util.mean()),
                taglie=dist, n=n)


# ---------------- Fenvix (camerieri) ----------------
TIPI = {"c7g.large": (2, 0.0775), "c7g.xlarge": (4, 0.155)}  # DICHIARATO (NUMBERS.md, DoiT)


def fenvix(F, avvio_s=60, margine=0.25, tipo="c7g.large", minimo=3, calma_min=10):
    """F: core occupati per minuto (CPU del nostro motore). Passi da 10 s, carico costante nel minuto."""
    vcpu, prezzo = TIPI[tipo]
    S = 6  # passi per minuto
    f = np.repeat(F, S)
    n = len(f)
    d = int(round(avvio_s / 10))
    calma = calma_min * S
    pronti = minimo
    in_arrivo = []  # passi a cui diventano pronti
    storia_voluti = []
    acceso = np.zeros(n)
    capac = np.zeros(n)
    over = 0
    ops_over = 0.0
    pf = _prefix(f)
    for t in range(n):
        # chi arriva ora
        arrivati = [x for x in in_arrivo if x <= t]
        if arrivati:
            pronti += len(arrivati)
            in_arrivo = [x for x in in_arrivo if x > t]
        oss = (pf[t] - pf[max(0, t - S)]) / max(1, min(S, t)) if t > 0 else f[0]
        voluti = max(minimo, math.ceil(oss * (1 + margine) / vcpu - 1e-9))
        storia_voluti.append(voluti)
        totali = pronti + len(in_arrivo)
        if voluti > totali:
            for _ in range(voluti - totali):
                in_arrivo.append(t + d) if d > 0 else None
            if d == 0:
                pronti += voluti - totali
        else:
            tetto = max(storia_voluti[-calma:]) if t >= calma else totali
            if totali > tetto:
                togli = totali - tetto
                # prima quelli non ancora pronti, poi i pronti
                while togli and in_arrivo:
                    in_arrivo.pop()
                    togli -= 1
                pronti -= togli
        acceso[t] = pronti + len(in_arrivo)
        capac[t] = pronti * vcpu
        if f[t] > capac[t]:
            over += 1
            ops_over += f[t] - capac[t]
    costo = acceso.mean() * prezzo * H_MESE
    return dict(costo=costo, camerieri_medi=float(acceso.mean()), camerieri_max=int(acceso.max()),
                over_min=over / S, quota_cpu_oltre=float(ops_over / max(f.sum(), 1e-9)), n_min=n / S)


# ---------------- voci fisse (STIMATO, B1 e revisori; vedi SOGLIE.md) ----------------
FISSE = {
    "a": dict(quaderno=226, dispensa=317, s3=82, lavaggio=56, tra_zone=155, portiere=0, resto=65),
    "b": dict(quaderno=451, dispensa=317, s3=336, lavaggio=112, tra_zone=970, portiere=0, resto=65),
}
PORTIERE_B1 = {"a": 500, "b": 970}
PROFILI = {"a": (20000, 0.8), "b": (50000, 0.5)}  # media operazioni al secondo, quota letture
SPAZIO_INF = 345.0
QUADERNO_TETTO = 20000  # record al secondo per quaderno (STIMATO)
QUADERNO_EXTRA = 451


def scala(curva, profilo):
    media, qr = PROFILI[profilo]
    ops = curva / curva.mean() * media
    return ops * qr, ops * (1 - qr)


def bolletta(curva, profilo, avvio_s=60, margine=0.25, w_rapporto=0.36, tipo="c7g.large", tmin=2,
             fatt_ora=False, portiere=False):
    r, w = scala(curva, profilo)
    L = r * MONGO_R_US + w * MONGO_W_US
    ip = infinite(L, True, tmin, fatt_ora)
    ir = infinite(L, False, tmin, fatt_ora)
    F = r * (MONGO_R_US / 0.93) + w * (MONGO_W_US / w_rapporto)
    fe = fenvix(F, avvio_s, margine, tipo)
    fisse = dict(FISSE[profilo])
    picco_w = float(w.max())
    nq = math.ceil(picco_w / QUADERNO_TETTO)
    if nq > 1:
        fisse["quaderno"] += (nq - 1) * QUADERNO_EXTRA
    if portiere:
        fisse["portiere"] = PORTIERE_B1[profilo]
    fis = sum(fisse.values())
    # pagare il picco: flotta fissa di camerieri grande quanto il picco (con margine) tutto il mese
    vcpu, prezzo = TIPI[tipo]
    flotta_picco = max(3, math.ceil(F.max() * (1 + margine) / vcpu)) * prezzo * H_MESE
    # costo "ideale" della sola CPU usata (vCPU frazionarie al prezzo per vCPU)
    ideale_fe = F.mean() * prezzo / vcpu * H_MESE
    ideale_inf_primario = L.mean() * (TAGLIE[-1][2] / 2 / TAGLIE[-1][1]) * H_MESE
    return dict(
        profilo=profilo, picco_su_media=float(curva.max() / curva.mean()),
        inf_pred=ip, inf_reat=ir, fen=fe, fisse=fisse, fisse_tot=fis, quaderni=nq,
        tot_inf_pred=ip["costo"] + SPAZIO_INF, tot_inf_reat=ir["costo"] + SPAZIO_INF,
        tot_fen=fe["costo"] + fis, flotta_picco=flotta_picco, ideale_fe=ideale_fe,
        ideale_inf_primario=ideale_inf_primario,
    )


if __name__ == "__main__":
    # prova veloce su una curva sinusoidale di 7 giorni
    t = np.arange(7 * 1440)
    c = 1 + 0.6 * np.sin(2 * np.pi * t / 1440) + 0.05 * np.random.default_rng(1).standard_normal(len(t))
    for p in "ab":
        b = bolletta(c, p)
        print(p, round(b["tot_inf_pred"]), round(b["tot_inf_reat"]), round(b["tot_fen"]), b["inf_pred"]["taglie"],
              round(b["fen"]["costo"]), b["fen"]["over_min"], b["inf_pred"]["over_min"])
