"""Corre la misura su DAB. Uso: python corri_dab.py <cartella all_trajectories> <uscita.json>"""
import json, os, sys, collections, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import maitre, dab_eventi

COMBOS = [(60, 600), (60, 300), (60, 3600), (0, 600)]
root, outp = sys.argv[1], sys.argv[2]
t0 = time.time()
runs = dab_eventi.all_runs(root)
rc, st = [], collections.Counter()
for model, ds, q, run, f in runs:
    calls = dab_eventi.load_run(f)
    rc.append((model, ds, q, run, calls))
    st["conversazioni"] += 1
    for c in calls:
        st["chiamate_" + c["tool"]] += 1
        st["riuscite" if c["ok"] else "fallite"] += 1
print("caricate", dict(st), round(time.time() - t0), "s", flush=True)

res = {"conteggi": dict(st), "casi": {}}
txt = []
cases = [("sessione", 10.0, 86400.0, False), ("paralleli", 10.0, 86400.0, False),
         ("condiviso", 10.0, 86400.0, False), ("condiviso", 10.0, 3600.0, False),
         ("paralleli", 3.0, 86400.0, False), ("paralleli", 30.0, 86400.0, False),
         ("paralleli", 10.0, 86400.0, True), ("condiviso", 10.0, 86400.0, True)]
for scope, step, period, only_ok in cases:
    name = f"{scope} passo={step:g}s periodo={period/3600:g}h solo_riuscite={only_ok}"
    ev = dab_eventi.events(rc, scope, step=step, period=period, only_ok=only_ok)
    r = maitre.run(ev, COMBOS)
    # per modello (cache app 60 s, W 10 min)
    per_model = {}
    for m in sorted({e["model"] for e in ev}):
        per_model[m] = maitre.run([e for e in ev if e["model"] == m], [(60, 600), (0, 600)])
    for e in ev:
        e.pop("subs", None)
    res["casi"][name] = {"insieme": r, "per_modello": per_model}
    txt.append(maitre.fmt(r, "DAB " + name))
    for m, x in per_model.items():
        a, b = x
        txt.append(f"   {m:18s} al_db={a['letture_al_db']:6d} tetto_dopo_cache={a['tetto']:.1%} +param={a['parametri_in_piu']:.1%} +sottoq={a['sottoquery_in_piu']:.1%} | senza_cache tetto={b['tetto']:.1%}")
    print("\n".join(txt[-6:]), flush=True)
json.dump(res, open(outp, "w"), indent=1)
open(outp.replace(".json", ".txt"), "w").write(json.dumps(dict(st)) + "\n" + "\n".join(txt) + "\n")
