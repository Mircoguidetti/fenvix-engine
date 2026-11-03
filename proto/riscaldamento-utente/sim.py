"""M24 · simulatore del riscaldamento per utente.

Un evento è una tupla (t, chi, oggetto, padrone, tipo):
  t       secondi (float)
  chi     l'utente che fa la richiesta (stringa) o None
  oggetto chiave dell'oggetto letto o scritto, None per un segnale puro (lettura di un indice
          sempre caldo, inizio di una sessione con login)
  padrone gruppo a cui appartiene l'oggetto (dati raggruppati per utente), None se condiviso
  tipo    'r' lettura, 'w' scrittura, 's' segnale puro

Tre passi, tutti in ordine di traccia:
  1. freddezza di ogni lettura (a tempo X ore o bancone LRU)
  2. preceduta? per w in W: lettore (la persona che legge), stretta (il padrone), larga (il gruppo)
  3. riscaldamento simulato (stretta e larga) e anticipo ingenuo per successore (M15)
"""
import math
import random
from collections import OrderedDict, defaultdict

import numpy as np

W = (1, 5, 30, 300)


# ---------------------------------------------------------------- freddezza

def coldness(ev, mode, param, warm_frac=0.2):
    """-> lista per evento: None (non misurata / non lettura), True fredda, False calda.

    mode 'tempo': param = X secondi; misurate solo le letture con t >= t0 + X.
    mode 'lru':   param = capacità in oggetti; il primo warm_frac delle letture non si misura.
    """
    out = [None] * len(ev)
    if not ev:
        return out
    if mode == "tempo":
        last = {}
        t0 = ev[0][0]
        for i, (t, u, o, g, k) in enumerate(ev):
            if o is None:
                continue
            if k == "r" and t >= t0 + param:
                lt = last.get(o)
                out[i] = (lt is None) or (t - lt > param)
            last[o] = t
    else:
        cap = max(1, int(param))
        lru = OrderedDict()
        nread = sum(1 for e in ev if e[4] == "r" and e[2] is not None)
        skip = int(nread * warm_frac)
        seen = 0
        for i, (t, u, o, g, k) in enumerate(ev):
            if o is None:
                continue
            if k == "r":
                seen += 1
                hit = o in lru
                if seen > skip:
                    out[i] = not hit
            if o in lru:
                lru.move_to_end(o)
            else:
                lru[o] = 1
                if len(lru) > cap:
                    lru.popitem(last=False)
    return out


# ---------------------------------------------------------------- preceduta

def preceded(ev, cold, jitter_same_second=False, seed=1):
    """Per ogni lettura fredda: l'ultima richiesta prima (stesso lettore / padrone / gruppo) è entro w?

    Ritorna dict con, per ogni misura, la lista delle distanze (inf se mai).
    Con jitter_same_second (ore al secondo) una distanza 0 diventa uniforme in (0, 1).
    """
    rnd = random.Random(seed)
    last_req = {}     # ultima richiesta della persona
    last_grp = {}     # ultima richiesta che tocca il gruppo (o fatta dal padrone)
    d = {"lettore": [], "stretta": [], "larga": [], "condivisi": 0, "fredde": 0}

    def gap(t, lt):
        if lt is None:
            return math.inf
        x = t - lt
        if jitter_same_second and x == 0:
            x = rnd.random()
        return x

    for i, (t, u, o, g, k) in enumerate(ev):
        if cold[i]:
            d["fredde"] += 1
            d["lettore"].append(gap(t, last_req.get(u)) if u is not None else math.inf)
            if g is None:
                d["condivisi"] += 1
                d["stretta"].append(math.inf)
                d["larga"].append(math.inf)
            else:
                d["stretta"].append(gap(t, last_req.get(g)))
                d["larga"].append(gap(t, last_grp.get(g)))
        if u is not None:
            last_req[u] = t
            last_grp[u] = t
        if g is not None:
            last_grp[g] = t
    return d


def share(gaps, w):
    if not gaps:
        return float("nan")
    return sum(1 for x in gaps if x <= w) / len(gaps)


# ---------------------------------------------------------------- latenze

def lognorm(rng, med, p99, n):
    s = math.log(p99 / med) / 2.3263
    return med * np.exp(rng.standard_normal(n) * s)


class Lat:
    """Scenario medio di M15: GET S3 mediana 25 ms, p99 100 ms, più byte a 85 MB/s."""

    def __init__(self, k=4, hist_bytes=2_000_000, seed=7, med=25.0, p99=100.0, nvme=False):
        self.rng = np.random.default_rng(seed)
        self.k, self.h, self.med, self.p99, self.nvme = k, hist_bytes, med, p99, nvme

    def hot(self, n):
        if self.nvme:
            return lognorm(self.rng, 0.4, 1.5, n)
        return np.full(n, 0.05)

    def cold_find(self, n):
        """k GET in parallelo (una per pila), 8 KB ciascuna; nel 10% dei casi un secondo giro."""
        if n == 0:
            return np.zeros(0)
        g = lognorm(self.rng, self.med, self.p99, n * self.k).reshape(n, self.k) + 8e3 / 85e6 * 1e3
        x = g.max(axis=1)
        second = self.rng.random(n) < 0.1
        x[second] += lognorm(self.rng, self.med, self.p99, int(second.sum()))
        return x

    def warm(self, n):
        """Caricamento della storia: k GET a intervallo con H/k byte ciascuna."""
        if n == 0:
            return np.zeros(0)
        per = self.h / self.k / 85e6 * 1e3
        g = lognorm(self.rng, self.med, self.p99, n * self.k).reshape(n, self.k) + per
        return g.max(axis=1)


# ---------------------------------------------------------------- riscaldamento

def warm_sim(ev, cold, lat, ttl=1800.0, which="stretta", jitter_same_second=False, seed=3,
             group_size=None):
    """Simula il riscaldamento per gruppo. Ritorna statistiche e i campioni di latenza.

    which 'stretta': il gruppo g si scalda a ogni richiesta fatta da g.
    which 'larga':   anche a ogni richiesta (di chiunque) che tocca un oggetto di g.
    which 'lettore': il gruppo è la persona che legge (Twitter: il frontend). Una lettura fredda
                     dell'oggetto o è servita se o è stato toccato dal lettore prima del riscaldamento.
    group_size: funzione (gruppo, t) -> numero di oggetti del gruppo in quel momento (per lo spazio).
    """
    rnd = random.Random(seed)
    state = {}        # gruppo -> [pronto_a, scade_a, inizio, utile, n_oggetti]
    n_warm = 0
    useful = 0
    resid = []
    sizes = []
    pending_wait = []
    served = 0
    pend = 0
    miss = 0
    touched = defaultdict(set) if which == "lettore" else None
    warm_lat = lat.warm(200000)
    wi = 0

    def close(g, s):
        nonlocal useful
        resid.append(s[1] - s[2])
        if s[3]:
            useful += 1

    def signal(g, t, tt):
        nonlocal n_warm, wi
        s = state.get(g)
        if s is not None and s[1] >= t:
            s[1] = max(s[1], t + ttl)
            return
        if s is not None:
            close(g, s)
        L = warm_lat[wi % len(warm_lat)] / 1e3
        wi += 1
        n_warm += 1
        nobj = group_size(g, t) if group_size else 0
        sizes.append(nobj)
        state[g] = [tt + L, t + ttl, t, False, nobj, set(touched[g]) if touched is not None else None]

    prev_t = {}
    for i, (t, u, o, g, k) in enumerate(ev):
        c = cold[i]
        if c:
            grp = u if which == "lettore" else g
            s = state.get(grp) if grp is not None else None
            ok = s is not None and s[1] >= t
            if ok and which == "lettore":
                ok = o in s[5]
            if ok:
                # quando è partito davvero il segnale: con l'ora al secondo, distanza a caso
                ready = s[0]
                if jitter_same_second and s[2] == t:
                    ready = t - rnd.random() + (s[0] - s[2])
                if ready <= t:
                    served += 1
                    s[3] = True
                else:
                    pend += 1
                    s[3] = True
                    pending_wait.append((ready - t) * 1e3)
            else:
                miss += 1
        # segnali di questa richiesta
        if u is not None:
            if which == "lettore":
                signal(u, t, t)
                if o is not None:
                    touched[u].add(o)
            else:
                signal(u, t, t)
        if which == "larga" and g is not None and g != u:
            signal(g, t, t)
    for g, s in state.items():
        close(g, s)
    nc = served + pend + miss
    return {
        "fredde": nc,
        "servite": served / nc if nc else float("nan"),
        "in_attesa": pend / nc if nc else float("nan"),
        "su_s3": miss / nc if nc else float("nan"),
        "riscaldamenti": n_warm,
        "risc_per_fredda": n_warm / nc if nc else float("nan"),
        "inutili": 1 - useful / n_warm if n_warm else float("nan"),
        "permanenza_media_s": float(np.mean(resid)) if resid else float("nan"),
        "oggetti_medi_per_risc": float(np.mean(sizes)) if sizes else float("nan"),
        "_attese_ms": pending_wait,
    }


def naive_successor(ev, cold, lat, ttl=1800.0):
    """Anticipo senza utente (M15): dopo la lettura di o si anticipa l'oggetto che l'ultima volta
    è venuto dopo o nel flusso anonimo. 1 GET per anticipo; tenuto ttl secondi."""
    succ = {}
    pref = {}     # oggetto -> (pronto, scade, usato)
    prev = None
    last = {}
    n_pref = used = served = pend = miss = 0
    L = lat.cold_find(100000) / 1e3
    li = 0
    waits = []
    for i, (t, u, o, g, k) in enumerate(ev):
        if o is None:
            continue
        if cold[i]:
            p = pref.get(o)
            if p and p[1] >= t:
                if not p[2]:
                    used += 1
                    p[2] = True
                if p[0] <= t:
                    served += 1
                else:
                    pend += 1
                    waits.append((p[0] - t) * 1e3)
            else:
                miss += 1
        last[o] = t
        if k == "r":
            if prev is not None and prev != o:
                succ[prev] = o
            nx = succ.get(o)
            hot = nx is not None and nx in last and t - last[nx] <= ttl
            if nx is not None and not hot and (nx not in pref or pref[nx][1] < t):
                n_pref += 1
                pref[nx] = [t + L[li % len(L)], t + ttl, False]
                li += 1
            prev = o
    nc = served + pend + miss
    return {"fredde": nc, "servite": served / nc if nc else float("nan"),
            "in_attesa": pend / nc if nc else float("nan"),
            "anticipi": n_pref, "anticipi_per_fredda": n_pref / nc if nc else float("nan"),
            "precisione": used / n_pref if n_pref else float("nan"), "_attese_ms": waits}


# ---------------------------------------------------------------- p99

def mix_latency(lat, cold_share, served, pend_share, pending_waits, n=400000):
    """Campioni di latenza di una miscela: (1 - cold_share) calde, e delle fredde la quota servita
    dal bancone, quella in attesa (con le attese misurate, ma mai oltre la loro GET) e il resto su S3."""
    rng = lat.rng
    nc = int(round(n * cold_share))
    nh = n - nc
    parts = [lat.hot(nh)]
    if nc:
        k_srv = int(round(nc * served))
        k_pend = min(int(round(nc * pend_share)), nc - k_srv) if pending_waits else 0
        k_miss = nc - k_srv - k_pend
        parts.append(lat.hot(k_srv))
        if k_pend:
            w = np.array(pending_waits)
            pw = rng.choice(w, k_pend)
            own = lat.cold_find(k_pend)
            parts.append(np.minimum(pw, own) + 0.05)
        parts.append(lat.cold_find(k_miss))
    x = np.concatenate(parts)
    return float(np.percentile(x, 50)), float(np.percentile(x, 99)), float(np.percentile(x, 99.9))
