"""M25 · simulatore delle tre politiche del bancone sulle conversazioni di fonti.py.

Politiche (SOGLIE.md):
  a  riscaldamento del gruppo a ogni segnale che lo rivela (e a ogni richiesta che lo tocca)
  b  dati condivisi sempre sul bancone
  c  segnale esplicito all'inizio della conversazione per il gruppo del compito (conv['main'])
Una politica è una stringa con le lettere accese, es. "", "a", "b", "c", "ab", "cb", "abc".

Freddezza (indipendente dalla politica):
  dato di un cliente: prima lettura dell'oggetto nella conversazione, se non scritto prima;
  dato condiviso: nessuno lo ha letto o scritto nell'ultima ora nello stesso database.
"""
import math
import os
import sys
from collections import defaultdict

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "riscaldamento-utente"))
import sim  # noqa: E402  (M24: latenze dello scenario medio di M15)

X_SHARED = 3600.0


def stream(convs):
    """Tutti gli eventi di un database in ordine di tempo, con l'indice della conversazione."""
    out = []
    for ci, c in enumerate(convs):
        if c["main"] is not None:
            out.append((c["start"], 0, ci, "c", None, c["main"]))
        for (t, k, o, g) in c["ev"]:
            out.append((t, 1, ci, k, o, g))
    out.sort(key=lambda e: (e[0], e[1]))
    return out


def simulate(convs, policy, lat, ttl=1800.0, warm_lat=None, shared_mode="db"):
    """Ritorna i conteggi per la politica. convs: conversazioni dello stesso database."""
    ev = stream(convs)
    last_shared = {}
    seen = set()          # oggetti dei clienti già sul bancone (letti o scritti nella conversazione)
    state = {}            # gruppo -> [pronto, scade, inizio, utile]
    n = defaultdict(int)
    waits = []
    resid = []
    wl = warm_lat if warm_lat is not None else lat.warm(50000) / 1e3
    wi = [0]

    def warm(g, t):
        s = state.get(g)
        if s is not None and s[1] >= t:
            s[1] = max(s[1], t + ttl)
            return
        if s is not None:
            resid.append(s[1] - s[2])
            n["utili"] += s[3]
        L = wl[wi[0] % len(wl)]
        wi[0] += 1
        n["riscaldamenti"] += 1
        state[g] = [t + L, t + ttl, t, 0]

    mains = [c["main"] for c in convs]
    ends = [max([c["start"]] + [e[0] for e in c["ev"]]) for c in convs]

    def touch(g, t):
        s = state.get(g)
        if s is not None and s[1] >= t:
            s[1] = max(s[1], t + ttl)

    for (t, _, ci, k, o, g) in ev:
        # ogni richiesta della conversazione tiene vivo il gruppo del compito e quello che tocca
        if k != "c":
            if mains[ci] is not None:
                touch(mains[ci], t)
            if g is not None:
                touch(g, t)
        if k == "c":
            if "c" in policy:
                # segnale esplicito: "lavoro sul cliente X" vale finché il compito è aperto
                warm(g, t)
                state[g][1] = max(state[g][1], ends[ci] + ttl)
            continue
        if k == "s":
            if "a" in policy and g is not None:
                warm(g, t)
            continue
        if k == "w":
            if g is None:
                last_shared[(ci, o) if shared_mode == "conv" else o] = t
            else:
                seen.add(o)
            continue
        # lettura
        n["letture"] += 1
        if g is None:
            ko = (ci, o) if shared_mode == "conv" else o
            lt = last_shared.get(ko)
            cold = lt is None or t - lt > X_SHARED
            last_shared[ko] = t
            if cold:
                n["fredde"] += 1
                n["fredde_condivise"] += 1
                if "b" in policy:
                    n["servite"] += 1
                    n["servite_condivise"] += 1
                else:
                    n["su_s3"] += 1
            continue
        cold = o not in seen
        seen.add(o)
        s = state.get(g)
        live = s is not None and s[1] >= t
        if cold:
            n["fredde"] += 1
            n["fredde_proprie"] += 1
            if live:
                s[3] = 1
                if s[0] <= t:
                    n["servite"] += 1
                    n["servite_proprie"] += 1
                else:
                    n["in_attesa"] += 1
                    waits.append((s[0] - t) * 1e3)
            else:
                n["su_s3"] += 1
        # la richiesta tiene vivo il gruppo (anche senza politica a: il cameriere lo ha già)
        if live:
            s[1] = max(s[1], t + ttl)
    for g, s in state.items():
        resid.append(s[1] - s[2])
        n["utili"] += s[3]
    r = dict(n)
    r["_attese_ms"] = waits
    r["_permanenze_s"] = resid
    return r


def merge(results):
    out = defaultdict(float)
    waits, resid = [], []
    for r in results:
        for k, v in r.items():
            if k == "_attese_ms":
                waits += v
            elif k == "_permanenze_s":
                resid += v
            else:
                out[k] += v
    out = dict(out)
    out["_attese_ms"] = waits
    out["_permanenze_s"] = resid
    return out


def summarize(r, lat, cold_share_trace=None):
    f = r.get("fredde", 0)
    pct = lambda x: round(100 * x, 1)
    o = {
        "letture": int(r.get("letture", 0)),
        "fredde": int(f),
        "fredde_condivise_quota": pct(r.get("fredde_condivise", 0) / f) if f else None,
        "servite": pct(r.get("servite", 0) / f) if f else None,
        "in_attesa": pct(r.get("in_attesa", 0) / f) if f else None,
        "su_s3": pct(r.get("su_s3", 0) / f) if f else None,
        "servite_proprie": pct(r.get("servite_proprie", 0) / r["fredde_proprie"]) if r.get("fredde_proprie") else None,
        "servite_condivise": pct(r.get("servite_condivise", 0) / r["fredde_condivise"]) if r.get("fredde_condivise") else None,
        "riscaldamenti": int(r.get("riscaldamenti", 0)),
        "risc_per_fredda": round(r.get("riscaldamenti", 0) / f, 3) if f else None,
        "inutili": pct(1 - r.get("utili", 0) / r["riscaldamenti"]) if r.get("riscaldamenti") else None,
        "permanenza_media_s": round(float(np.mean(r["_permanenze_s"])), 0) if r["_permanenze_s"] else None,
        "attesa_media_ms": round(float(np.mean(r["_attese_ms"])), 1) if r["_attese_ms"] else None,
    }
    if f:
        sv = r.get("servite", 0) / f
        pd = r.get("in_attesa", 0) / f
        o["p50_p99_p999_profiloB_ms"] = [round(x, 2) for x in sim.mix_latency(lat, 0.10, sv, pd, r["_attese_ms"])]
        if cold_share_trace is not None:
            o["p50_p99_p999_traccia_ms"] = [round(x, 2) for x in
                                           sim.mix_latency(lat, cold_share_trace, sv, pd, r["_attese_ms"])]
    return o


POLICIES = ("", "a", "b", "c", "ab", "cb", "abc")


NIC_BPS = 1.25e9   # m7gd.large "fino a 12,5 Gbps" (DICHIARATO AWS), circa 1,25 GB/s al massimo


def warm_latencies(hist_bytes, med, p99, n=50000, seed=21):
    """Caricamento di una storia di H byte: k GET a intervallo in parallelo, k = max(4, H / 8 MB) fino a 64,
    ognuna con la sua latenza (scenario di M15) più i suoi byte a 85 MB/s; mai sotto H / 1,25 GB/s."""
    k = int(min(64, max(4, math.ceil(hist_bytes / 8e6))))
    rng = np.random.default_rng(seed)
    g = sim.lognorm(rng, med, p99, n * k).reshape(n, k) + hist_bytes / k / 85e6 * 1e3
    x = g.max(axis=1)
    return np.maximum(x, hist_bytes / NIC_BPS * 1e3) / 1e3


def run_source(convs, k=4, hist_bytes=2e6, ttl=1800.0, policies=POLICIES, nvme=False, shared_mode="db",
               med=25.0, p99=100.0):
    by_db = defaultdict(list)
    for c in convs:
        by_db[c["db"]].append(c)
    out = {}
    wl = warm_latencies(hist_bytes, med, p99)
    for pol in policies:
        lat = sim.Lat(k=k, hist_bytes=hist_bytes, nvme=nvme, med=med, p99=p99)
        res = [simulate(v, pol, lat, ttl=ttl, warm_lat=wl, shared_mode=shared_mode) for v in by_db.values()]
        m = merge(res)
        cs = m.get("fredde", 0) / m["letture"] if m.get("letture") else None
        out[pol or "nessuna"] = summarize(m, lat, cs)
    out["_caricamento_ms_mediana_p99"] = [round(float(np.median(wl)) * 1e3, 1),
                                          round(float(np.percentile(wl, 99)) * 1e3, 1)]
    return out


def steps_summary(convs):
    x = np.array([s for c in convs for s in c.get("passi", []) if 0 < s < 3600])
    if not len(x):
        return None
    return {"n": int(len(x)), "p10_s": round(float(np.percentile(x, 10)), 2),
            "mediana_s": round(float(np.median(x)), 2), "media_s": round(float(x.mean()), 2),
            "p90_s": round(float(np.percentile(x, 90)), 2)}


def first_read_delay(convs):
    """Quanto tempo passa tra l'inizio della conversazione / il primo segnale del gruppo e la prima lettura
    fredda di un suo dato (secondi). Dice quanto margine ha il riscaldamento."""
    a, c = [], []
    for cv in convs:
        sig = {}
        seen = set()
        for (t, k, o, g) in cv["ev"]:
            if g is None:
                continue
            if k == "s" and g not in sig:
                sig[g] = t
            if k == "r" and o not in seen:
                seen.add(o)
                if g == cv["main"]:
                    c.append(t - cv["start"])
                if g in sig:
                    a.append(t - sig[g])
                break
    f = lambda v: {"n": len(v), "mediana_s": round(float(np.median(v)), 2),
                   "p10_s": round(float(np.percentile(v, 10)), 2),
                   "sotto_0_2s": round(100 * float(np.mean(np.array(v) < 0.2)), 1)} if v else None
    return {"da_inizio": f(c), "da_primo_segnale": f(a)}
