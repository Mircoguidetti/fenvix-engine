#!/usr/bin/env python3
"""M19: lancia atlas.confronto su tutte le curve di M17, profili A/B/C, CPU 300 e 600 µs, varianti e scala.
Scrive risultati/risultati.json e risultati/riassunto.txt. Uso: python3 corri.py [veloce]"""
import json
import os
import sys
import time
from multiprocessing import Pool

import numpy as np

import atlas

sys.path.insert(0, os.path.join(atlas.QUI, "..", "elastica"))
import analizza  # noqa: E402  (M17)

QUI = atlas.QUI
DB = ("me", "tw")  # famiglie "da database" (M17)


def curve():
    serie = analizza.carica(["prova", "finale"])
    return analizza.curve(serie)


CS = None


def init():
    global CS
    CS = curve()


def lavoro(job):
    nome, prof, us, opz = job
    c = CS[nome]
    w, tb, k = atlas.profilo(c, prof, forma_vera=opz.get("forma_vera", False), scala=opz.get("scala", 1.0),
                             tb=opz.get("tb"))
    kw = {}
    if "taglia_dim" in opz:
        kw["taglia_dim"] = opz["taglia_dim"]
    if "squilibrio" in opz:
        kw["squilibrio"] = opz["squilibrio"]
    if "pav_min" in opz:
        kw["pav_min"] = opz["pav_min"]
    if "varianti" in opz:
        kw["varianti"] = tuple(opz["varianti"])
    r = atlas.confronto(w, tb, us, con_noi=False, **kw)
    # la nostra bolletta non dipende dalle varianti di Atlas: si calcola una volta e si tiene da parte
    kn = f"{nome}|{prof}|{int(us * 1e6)}|{opz.get('forma_vera', False)}|{opz.get('scala', 1.0)}|{opz.get('tb')}"
    fn = f"{QUI}/risultati/noi/" + kn.replace("|", "_") + ".json"
    if os.path.exists(fn):
        f = json.load(open(fn))
    else:
        f = atlas.noi(w, tb, us)
        json.dump(f, open(fn, "w"), default=float)
    r["noi"] = f
    for a in r["atlas"].values():
        lo, hi, c = a["totale"]
        a["rapporto"] = dict(centro=f["centro"]["prezzo"] / c, migliore=f["migliore"]["prezzo"] / hi,
                             peggiore=f["peggiore"]["prezzo"] / lo)
    r["k"] = k
    r["tb"] = tb
    return job, r


def chiave(job):
    nome, prof, us, opz = job
    o = ",".join(f"{a}={b}" for a, b in sorted(opz.items()) if a != "varianti")
    return f"{nome}|{prof}|{int(us * 1e6)}|{o}"


def jobs(cs, veloce=False):
    J = []
    nomi = list(cs)
    db = [n for n in nomi if n[:2] in DB]
    if veloce:
        nomi = nomi[:2] + [n for n in nomi if n.startswith(("al", "te"))]
        db = db[:2]
    for us in (300e-6, 600e-6):
        for n in nomi:
            for prof in ("A", "B", "C"):
                J.append((n, prof, us, {}))
            J.append((n, "A", us, {"forma_vera": True}))
        for n in db:
            for prof in ("A", "B", "C"):
                for td in ("M50", "M80"):
                    J.append((n, prof, us, {"taglia_dim": td}))
                J.append((n, prof, us, {"squilibrio": 0.0}))
                for pm in ("M50", "M60"):
                    J.append((n, prof, us, {"pav_min": pm, "varianti": ["i"]}))
            for sc in (0.25, 0.5, 2.0, 4.0, 8.0):
                J.append((n, "A", us, {"scala": sc, "varianti": ["i", "iii"]}))
                J.append((n, "A", us, {"scala": sc, "tb": 2.0 * sc * 1.25 if sc != 1 else 2.0,
                                       "varianti": ["i", "iii"]}))
    # ordina per durata: B prima
    J.sort(key=lambda j: (j[1] != "B", -j[3].get("scala", 1)))
    return J


def main():
    veloce = "veloce" in sys.argv
    cs = curve()
    J = jobs(cs, veloce)
    print(len(J), "lavori", flush=True)
    t0 = time.time()
    out = {}
    os.makedirs(f"{QUI}/risultati/noi", exist_ok=True)
    os.makedirs(f"{QUI}/risultati/lavori", exist_ok=True)

    def fl(job):
        return f"{QUI}/risultati/lavori/" + chiave(job).replace("|", "_").replace(",", "_") + ".json"

    fatti = [j for j in J if os.path.exists(fl(j))]
    for j in fatti:
        out[chiave(j)] = json.load(open(fl(j)))
    J = [j for j in J if not os.path.exists(fl(j))]
    print(len(fatti), "già fatti,", len(J), "da fare", flush=True)
    with Pool(2, initializer=init) as p:
        for i, (job, r) in enumerate(p.imap_unordered(lavoro, J)):
            out[chiave(job)] = r
            json.dump(r, open(fl(job), "w"), default=float)
            if i % 20 == 0:
                print(i, round(time.time() - t0), chiave(job), flush=True)
    os.makedirs(f"{QUI}/risultati", exist_ok=True)
    json.dump(out, open(f"{QUI}/risultati/risultati{'_veloce' if veloce else ''}.json", "w"), default=float)
    print("fatto", round(time.time() - t0))


if __name__ == "__main__":
    main()
