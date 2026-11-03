"""Stampa in breve un risultato di pg_rami.py: mediana [min–max] di ogni misura."""
import json, sys
for f in sys.argv[1:]:
    r = json.load(open(f))
    print("==", f, r["conf"], r["n"], "rami", r.get("letture"), "letture", r.get("scritture"), "scritture")
    def p(k, v):
        if isinstance(v, dict) and "mediana" in v:
            print(f"  {k}: mediana {v['mediana']:.2f} [min {v['min']:.2f}, p90 {v['p90']:.2f}, max {v['max']:.2f}]")
        elif isinstance(v, dict):
            for kk, vv in v.items():
                p(f"{k}.{kk}", vv)
        elif isinstance(v, float):
            print(f"  {k}: {v:.2f}")
    for k, v in r.items():
        if k not in ("creazione_grezzi", "config"):
            p(k, v)
