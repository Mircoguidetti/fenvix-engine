"""M24 · corse. Uso: python corri.py <fonte> <uscita.json>   (fonte: tau2, twitter, gh, ecom)"""
import glob
import json
import math
import os
import sys

import numpy as np

import fonti
import sim

TAU2 = os.environ.get("TAU2", "/tmp/tau2")
TW = os.environ.get("TW", "/tmp/twtrace/samples/2020Mar")
HERE = os.path.dirname(os.path.abspath(__file__))


def pct(x):
    return round(100 * x, 1) if x == x else None


def evaluate(ev, cold_defs, jitter=False, k=4, gsize=None, whiches=("stretta", "larga"), naive=True,
             ttls=(300, 1800, 7200), lettore_warm=False):
    out = {"eventi": len(ev), "letture": sum(1 for e in ev if e[4] == "r")}
    for name, (mode, param) in cold_defs.items():
        cold = sim.coldness(ev, mode, param)
        nr = sum(1 for c in cold if c is not None)
        nc = sum(1 for c in cold if c)
        r = {"letture_misurate": nr, "fredde": nc, "quota_fredde": pct(nc / nr) if nr else None}
        p = sim.preceded(ev, cold, jitter_same_second=jitter)
        r["fredde_su_dati_condivisi"] = pct(p["condivisi"] / nc) if nc else None
        r["solo_dati_propri_preceduta_stretta"] = {str(w): pct(sim.share(p["stretta_propri"], w)) for w in sim.W}
        for m in ("lettore", "stretta", "larga"):
            r[f"preceduta_{m}"] = {str(w): pct(sim.share(p[m], w)) for w in sim.W}
        for which in (list(whiches) + (["lettore"] if lettore_warm else [])):
            for ttl in ttls:
                for h, hb in (("2MB", 2e6), ("20MB", 20e6)):
                    if h == "20MB" and ttl != 1800:
                        continue
                    lat = sim.Lat(k=k, hist_bytes=hb)
                    s = sim.warm_sim(ev, cold, lat, ttl=ttl, which=which, jitter_same_second=jitter,
                                     group_size=gsize)
                    waits = s.pop("_attese_ms")
                    ow = s.pop("_attese_proprie_ms")
                    s["p50_p99_p999_profiloB_solo_propri_ms"] = [round(x, 2) for x in sim.mix_latency(
                        lat, 0.10, s["servite_proprie"] if s["fredde_proprie"] else 0.0,
                        s["in_attesa_proprie"] if s["fredde_proprie"] else 0.0, ow)]
                    s["attesa_media_ms"] = round(float(np.mean(waits)), 1) if waits else None
                    trace_mix = sim.mix_latency(lat, nc / nr if nr else 0, s["servite"], s["in_attesa"], waits)
                    prof_b = sim.mix_latency(lat, 0.10, s["servite"], s["in_attesa"], waits)
                    s["p50_p99_p999_traccia_ms"] = [round(x, 2) for x in trace_mix]
                    s["p50_p99_p999_profiloB_ms"] = [round(x, 2) for x in prof_b]
                    for kk in ("servite", "in_attesa", "su_s3", "inutili", "servite_proprie", "in_attesa_proprie"):
                        s[kk] = pct(s[kk])
                    r[f"risc_{which}_ttl{ttl}_{h}"] = s
        lat = sim.Lat(k=k)
        base_b = sim.mix_latency(lat, 0.10, 0.0, 0.0, [])
        base_t = sim.mix_latency(lat, nc / nr if nr else 0, 0.0, 0.0, [])
        r["senza_risc_p50_p99_p999_traccia_ms"] = [round(x, 2) for x in base_t]
        r["senza_risc_p50_p99_p999_profiloB_ms"] = [round(x, 2) for x in base_b]
        if naive:
            nv = sim.naive_successor(ev, cold, lat)
            waits = nv.pop("_attese_ms")
            nv["p50_p99_p999_profiloB_ms"] = [round(x, 2) for x in
                                             sim.mix_latency(lat, 0.10, nv["servite"], nv["in_attesa"], waits)]
            for kk in ("servite", "in_attesa", "precisione"):
                nv[kk] = pct(nv[kk])
            r["ingenuo_successore"] = nv
        out[name] = r
        print(name, json.dumps({kk: r[kk] for kk in r if kk.startswith("preceduta") or kk == "quota_fredde"}),
              flush=True)
    return out


def run_tau2():
    res = {}
    files = sorted(glob.glob(f"{TAU2}/data/tau2/results/final/*_4trials.json"))
    files = [f for f in files if "_retail_" in f or "_airline_" in f]
    for login in (False, True):
        evs = []
        gs = {}
        st_all = []
        off = 0.0
        for f in files:
            dom = "retail" if "_retail_" in f else "airline"
            ev, st = fonti.tau2_events(f, f"{TAU2}/data/tau2/domains", login=login)
            gfun, n = fonti.tau2_group_size(f"{TAU2}/data/tau2/domains", dom)
            st["file"] = os.path.basename(f)
            st_all.append(st)
            # ogni file è un database a sé: lo mettiamo in fila nel tempo, chiavi separate
            tag = os.path.basename(f)[:40]
            t0 = ev[0][0] if ev else 0
            for (t, u, o, g, k) in ev:
                evs.append((t - t0 + off, f"{tag}|{u}" if u else None, f"{tag}|{o}" if o else None,
                            f"{tag}|{g}" if g else None, k))
            for u, c in n.items():
                gs[f"{tag}|{u}"] = c
            off = (evs[-1][0] + 86400) if evs else off
        nobj = 0
        for f in files:
            dom = "retail" if "_retail_" in f else "airline"
            db = json.load(open(f"{TAU2}/data/tau2/domains/{dom}/db.json"))
            nobj += sum(len(v) for v in db.values())
        defs = {"bancone10": ("lru", 0.10 * nobj), "bancone1": ("lru", 0.01 * nobj)}
        key = "login" if login else "identificazione"
        res[key] = evaluate(evs, defs, k=4, gsize=lambda g, t: gs.get(g, 1))
        res[key]["file"] = st_all
    return res


def run_twitter():
    res = {}
    for c, defs in (("cluster011", None), ("cluster024", None), ("cluster025", None),
                    ("cluster010", "lungo"), ("cluster050", "lungo"), ("cluster045", "lungo")):
        ev = fonti.twitter_events(f"{TW}/{c}")
        nobj = len({e[2] for e in ev})
        d = {"bancone10": ("lru", 0.10 * nobj), "bancone1": ("lru", 0.01 * nobj)}
        if defs == "lungo":
            d.update({"tempo1h": ("tempo", 3600), "tempo6h": ("tempo", 6 * 3600), "tempo24h": ("tempo", 86400)})
        # quanto è grande il "gruppo" di un client: oggetti toccati dal client / oggetti totali
        per = {}
        for e in ev:
            per.setdefault(e[1], set()).add(e[2])
        sz = sorted(len(v) / nobj for v in per.values())
        r = evaluate(ev, d, k=4, whiches=(), lettore_warm=True, ttls=(1800,))
        r["client"] = len(per)
        r["gruppo_client_quota_dati_mediana"] = pct(sz[len(sz) // 2])
        r["gruppo_client_quota_dati_pesata_letture"] = pct(
            sum(len(per[e[1]]) for e in ev if e[4] == "r") / max(1, sum(1 for e in ev if e[4] == "r")) / nobj)
        res[c] = r
        print(c, "client", len(per), flush=True)
    return res


def run_gh(with_bots=False):
    path = f"{HERE}/tracce/gh/gh-eventi.tsv.gz"
    import hashlib
    mod = json.load(open(f"{HERE}/ci/run.json")).get("campione_mod", 20)

    def samp(login):
        return int(hashlib.md5(login.lower().encode()).hexdigest()[:8], 16) % mod == 0

    ev = fonti.gh_events(path, sampled_owner=samp, with_bots=with_bots)
    # dimensione del gruppo: oggetti distinti del padrone visti nella traccia fino a t (approssimato: totale)
    objs = {}
    for e in ev:
        if e[2] is not None:
            objs.setdefault(e[3], set()).add(e[2])
    n = {g: len(v) for g, v in objs.items()}
    nobj = sum(n.values())
    defs = {"tempo1h": ("tempo", 3600), "tempo6h": ("tempo", 6 * 3600), "tempo24h": ("tempo", 86400),
            "bancone10": ("lru", 0.10 * nobj), "bancone1": ("lru", 0.01 * nobj)}
    r = evaluate(ev, defs, jitter=True, k=4, gsize=lambda g, t: n.get(g, 1))
    r["padroni"] = len(n)
    r["oggetti"] = nobj
    return r


def run_ecom():
    path = f"{HERE}/tracce/ecom/ecom-eventi.tsv.gz"
    ev, info = fonti.rees46_events(path)
    n = info.pop("_gsize")
    nobj = info["oggetti"]
    defs = {"tempo1h": ("tempo", 3600), "tempo6h": ("tempo", 6 * 3600), "tempo24h": ("tempo", 86400),
            "bancone10": ("lru", 0.10 * nobj), "bancone1": ("lru", 0.01 * nobj)}
    r = evaluate(ev, defs, jitter=True, k=7, gsize=lambda g, t: n.get(g, 1))
    r["info"] = info
    return r


if __name__ == "__main__":
    src, outp = sys.argv[1], sys.argv[2]
    fn = {"tau2": run_tau2, "twitter": run_twitter, "gh": run_gh, "ecom": run_ecom,
          "gh_bot": lambda: run_gh(True)}[src]
    res = fn()
    json.dump(res, open(outp, "w"), indent=1)
