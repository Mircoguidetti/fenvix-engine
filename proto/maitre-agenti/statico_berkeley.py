"""Il conto "senza tempo" dello studio di Berkeley: su K tentativi della stessa domanda, quante
query (e quanti pezzi) sono distinte sul totale. Nessuna cache, nessun orologio.
Uso: python statico_berkeley.py <all_trajectories> <uscita.txt>"""
import os, sys, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dab_eventi
root, outp = sys.argv[1], sys.argv[2]
groups = collections.defaultdict(list)
for model, ds, q, run, f in dab_eventi.all_runs(root):
    for c in dab_eventi.load_run(f):
        if c["tool"] != "query_db":
            continue
        a = c["args"]
        qq = a.get("query") if isinstance(a.get("query"), str) else str(a.get("query"))
        qk, sh, subs, coll, kind = dab_eventi.keys(str(a.get("db_name")), qq)
        groups[(model, ds, q)].append((qk, sh, subs))
tot = dq = ds_ = 0
stot = sdist = 0
fr = []
for k, L in groups.items():
    n = len(L)
    d = len({x[0] for x in L})
    tot += n; dq += d; ds_ += len({x[1] for x in L})
    allsub = [s for x in L for s in x[2]]
    stot += len(allsub); sdist += len(set(allsub))
    fr.append(d / n)
fr.sort()
lines = [
    f"gruppi (modello, domanda): {len(groups)}",
    f"query totali {tot}, distinte esatte {dq} ({dq/tot:.1%}), forme distinte {ds_} ({ds_/tot:.1%})",
    f"pezzi (sotto-SELECT, tabella+filtro, prefissi di pipeline) totali {stot}, distinti {sdist} ({(sdist/stot if stot else 0):.1%})",
    f"quota distinte per gruppo: mediana {fr[len(fr)//2]:.1%}, 10° percentile {fr[len(fr)//10]:.1%}, 90° {fr[9*len(fr)//10]:.1%}",
]
open(outp, "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
