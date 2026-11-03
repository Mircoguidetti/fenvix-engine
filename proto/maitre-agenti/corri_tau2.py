"""Corre la misura su tau2-bench. Uso: python corri_tau2.py <cartella results/final> <uscita.json>"""
import glob, json, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import maitre, tau2_eventi

COMBOS = [(60, 600), (60, 300), (60, 3600), (0, 600)]
root, outp = sys.argv[1], sys.argv[2]
files = sorted(f for f in glob.glob(os.path.join(root, "*.json"))
               if any(f"_{d}_default_" in f or f"_{d}_base_" in f for d in ("airline", "retail", "telecom")))
res = {"file": [os.path.basename(f) for f in files], "ambiti": {}}
txt = []
for scope in ("condiviso", "clienti", "compito", "paralleli", "sessione"):
    allev, allst = [], {}
    per_file = {}
    for f in files:
        ev, st, dom = tau2_eventi.events(f, scope)
        allev += ev
        allst[os.path.basename(f)] = st
        per_file[os.path.basename(f)] = maitre.run(ev, [(60, 600)])[0]
    r = maitre.run(allev, COMBOS)
    res["ambiti"][scope] = {"insieme": r, "per_file": per_file, "conteggi": allst}
    txt.append(maitre.fmt(r, f"tau2 ambito={scope} (12 file insieme, ognuno il suo database)"))
    for fn, x in per_file.items():
        txt.append(f"   {fn[:60]:60s} al_db={x['letture_al_db']:5d} coll={x['collezione']:.1%} int={x['intervallo']:.1%} tetto={x['tetto']:.1%} +param={x['parametri_in_piu']:.1%}")
json.dump(res, open(outp, "w"), indent=1)
open(outp.replace(".json", ".txt"), "w").write("\n".join(txt) + "\n")
print("\n".join(txt))
