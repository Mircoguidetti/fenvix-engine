#!/usr/bin/env python3
"""M29: riassunto delle prove (risultati/carichi.jsonl e misure.jsonl) in risultati/riassunto.jsonl."""
import json, os
os.chdir(os.path.join(os.path.dirname(__file__), ".."))
car = {}
for l in open("risultati/carichi.jsonl"):
    j = json.loads(l); car[j["etichetta"]] = j
mis = {}
for l in open("risultati/misure.jsonl"):
    j = json.loads(l); L = j["lettore"]
    mis.setdefault(j["etichetta"], []).append(j)
out = open("risultati/riassunto.jsonl", "w")
base_cpu = None
for et, j in car.items():
    r = j["regime"]
    ug = r["utente_byte"] / 2**30
    row = {"et": et, "comp": j.get("compressione"), "blocco_kb": j.get("blocco_kb"), "clienti": j["clienti"], "lavaggio": j["lavaggio"],
           "wa_fisica": round(r["put_sst_byte"] / r["utente_byte"], 2), "put_per_gb": round(r["put_sst"] / ug), "get_per_gb": round(r["get_sst"] / ug),
           "byte_get_per_gb_mb": round(r["get_sst_byte"] / ug / 2**20), "cpu_s_per_gb_utente": round(r["cpu_s"] / ug, 1),
           "pile_media": round(r["pile_media"], 2), "l0_media": round(r["l0_media"], 2), "sr_media": round(r["sr_media"], 2), "pile_p90": r["pile_p90"], "pile_max": r["pile_max"],
           "mbs": round(r["utente_byte"] / r["secondi"] / 2**20, 2)}
    for m in mis.get(et, []):
        L = m["lettore"]; k = f'{L["ra_find"]}:{L["ft_find"]}/{L["ra_scalda"]}:{L["ft_scalda"]}'
        q = m["query"]
        row.setdefault("misure", {})[k] = {
            "pile": m["pile"], "pile_l0": m["pile_l0"], "find_tipica": round(m["find_tipica"], 2),
            **{f"{n}": [round(q[n]["get_media"], 2), round(q[n]["get_per_pila"], 3), q[n]["get_p90"], round(q[n]["byte_media"] / 1024)] for n in q},
            "s3_bancone": {n: {s: round(v["get_media"], 2) for s, v in q[n]["s3_con_bancone"].items()} for n in q},
            "bancone": m["bancone_soglie"], "pile_mb": [round(b / 2**20) for b in m["pile_byte"]],
            "indici_mb": round(m["bancone_indici_byte"] / 2**20, 1), "filtri_mb": round(m["bancone_filtri_byte"] / 2**20, 1)}
    print(json.dumps(row)); out.write(json.dumps(row) + "\n")
