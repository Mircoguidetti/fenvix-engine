"""Analisi delle tracce pubbliche di Twitter (twitter/cache-trace, campioni 2020Mar, 1 milione di righe per cluster).

Ogni riga: timestamp, chiave anonimizzata, dimensione chiave, dimensione valore, client, operazione, TTL.
- "collezione" = lo spazio dei nomi della chiave (il prefisso fino all'ultimo ':'; la chiave intera
  senza ':' finisce nella collezione "_");
- lettura = get/gets (tutte letture puntuali);
- scrittura = set/add/replace/cas/append/prepend/delete/incr/decr.
Con --riempimenti, un set/add sulla stessa chiave entro 5 s da un get non conta come scrittura
(in una cache "a lato" quel set è il riempimento dopo un mancato, non un cambio del dato).
"""
import argparse, glob, json, os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
import analizza

READ = {"get", "gets"}
FILL = {"set", "add"}

ap = argparse.ArgumentParser()
ap.add_argument("cartella")
ap.add_argument("--riempimenti", action="store_true")
ap.add_argument("--nome", default="twitter")
a = ap.parse_args()

combos = [(0, 600), (10, 600), (60, 600)]
tutti = {}
t0 = time.time()
for f in sorted(glob.glob(os.path.join(a.cartella, "cluster*"))):
    an = analizza.Analyzer(combos)
    last_get = {}
    span = 0
    with open(f, errors="replace") as fh:
        for line in fh:
            p = line.rstrip("\n").split(",")
            if len(p) < 7:
                continue
            # la chiave può contenere virgole: i campi fissi sono i primi 1 e gli ultimi 5
            ts = int(p[0]); op = p[-2]; key = ",".join(p[1:-5])
            i = key.rfind(":")
            coll = key[:i] if i > 0 else "_"
            span = ts
            if op in READ:
                if a.riempimenti:
                    last_get[key] = ts
                an.feed((ts, "tw", coll, "point", key, [(coll, "_id", key)]))
            else:
                if a.riempimenti and op in FILL:
                    lg = last_get.get(key)
                    if lg is not None and ts - lg <= 5:
                        continue
                an.feed((ts, "tw", coll, "write", None, [(coll, "ALL"), (coll, "_id", key)]))
    res = an.result()
    for r in res:
        r.pop("per_app", None)
        r["durata_s"] = span
    tutti[os.path.basename(f)] = res
    r0, r10, r1 = res
    print(f"{os.path.basename(f)} durata={span}s letture={r0['letture']} scritture={r0['scritture']} "
          f"| senza cache app: coll={r0['collezione']:.1%} int={r0['intervallo']:.1%} "
          f"| cache app 10s: coll={r10['collezione']:.1%} int={r10['intervallo']:.1%} | 60s: coll={r1['collezione']:.1%} "
          f"int={r1['intervallo']:.1%} tetto={r1['tetto']:.1%}", flush=True)
json.dump(tutti, open(f"risultati/{a.nome}.json", "w"), indent=1)
print("secondi", round(time.time() - t0))
