"""M24 · variante: si riscalda solo dopo k richieste dell'utente in 10 minuti (taglia i riscaldamenti inutili)."""
import json, sys
import fonti, sim, corri
out = {}
srcs = {"ecom": lambda: fonti.rees46_events(f"{corri.HERE}/tracce/ecom/ecom-eventi.tsv.gz")[0]}
for name, f in srcs.items():
    ev = f()
    cold = sim.coldness(ev, "tempo", 3600)
    for k in (1, 2, 3, 5, 10):
        s = sim.warm_sim(ev, cold, sim.Lat(k=7), ttl=1800, which="stretta", jitter_same_second=True, min_req=k)
        s.pop("_attese_ms"); s.pop("_attese_proprie_ms")
        s["risc_per_fredda_propria"] = s["riscaldamenti"] / s["fredde_proprie"]
        out[f"{name}_k{k}"] = s
        print(name, k, round(100*s["servite_proprie"],1), round(s["risc_per_fredda_propria"],2), round(100*s["inutili"],1), flush=True)
json.dump(out, open("risultati/soglia_segnali.json", "w"), indent=1)
