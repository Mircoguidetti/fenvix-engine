"""Corre il generatore del catalogo e l'analisi; scrive i risultati in risultati/."""
import argparse, json, sys, time
sys.path.insert(0, __import__("os").path.dirname(__file__))
import catalogo, analizza

ap = argparse.ArgumentParser()
ap.add_argument("--nome", default="base")
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--card", type=float, default=1.0)
ap.add_argument("--zipf", type=float, default=1.0)
ap.add_argument("--uniq", type=float, default=0.0)
ap.add_argument("--durata", type=int, default=catalogo.DURATA)
a = ap.parse_args()

combos = [(ttl, w) for ttl in (0, 60) for w in (300, 600, 3600)]
an = analizza.Analyzer(combos)
t0 = time.time(); n = 0
for ev in catalogo.generate(seed=a.seed, card_mult=a.card, z_mult=a.zipf, uniq_add=a.uniq, durata=a.durata):
    an.feed(ev); n += 1
res = an.result()
meta = dict(vars(a), eventi=n, secondi_cpu=round(time.time() - t0, 1))
json.dump({"meta": meta, "risultati": res}, open(f"risultati/catalogo-{a.nome}.json", "w"), indent=1)
print(meta)
print(analizza.fmt(res, f"catalogo {a.nome}"))
