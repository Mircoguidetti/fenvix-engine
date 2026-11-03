#!/usr/bin/env python3
"""Riassunto delle misure vere: amplificazione, CPU per GB, PUT e GET del lavaggio, pile, GET per find."""
import json, sys, collections
car = [json.loads(l) for l in open("risultati/carichi.jsonl")]
mis = {}
for l in open("risultati/misure.jsonl"):
    j = json.loads(l); mis[j["etichetta"]] = j
base = {}
for j in car:
    if not j["lavaggio"]:
        base[(j["disposizione"], j["taglia_byte"])] = j
out = []
for j in car:
    if not j["lavaggio"]:
        continue
    r = j["regime"]; u = r["utente_byte"]
    b = base.get((j["disposizione"], j["taglia_byte"])) or base.get(("Cliente", j["taglia_byte"]))
    l0 = b["regime"]["put_sst_byte"] / b["regime"]["utente_byte"] if b else float("nan")
    comp_w = r["put_sst_byte"] - l0 * u
    cpu_comp = r["cpu_s"] - (b["regime"]["cpu_s"] * u / b["regime"]["utente_byte"] if b else float("nan"))
    m = mis.get(j["etichetta"], {})
    q = m.get("query", {})
    d = dict(et=j["etichetta"], taglia_gb=round(j["taglia_byte"] / 2**30, 2), wa_tot=r["put_sst_byte"] / u, l0=l0,
             wa_lav=comp_w / u, wa_lett=r["get_sst_byte"] / u, put_per_gb=r["put_sst"] / (u / 2**30), get_per_gb=r["get_sst"] / (u / 2**30),
             cpu_s_per_gb_scritto=cpu_comp / (comp_w / 2**30) if comp_w > 0 else float("nan"), mbs_utente=u / 2**20 / r["secondi"],
             pile_media=r["pile_media"], pile_p90=r["pile_p90"], pile_max=r["pile_max"], pile_fine=m.get("pile_media"),
             find=m.get("find_tipica"), q1=q.get("Q1", {}).get("get_media"), q2=q.get("Q2", {}).get("get_media"),
             q3=q.get("Q3", {}).get("get_media"), scalda=q.get("SCALDA", {}).get("get_media"), scalda_p90=q.get("SCALDA", {}).get("get_p90"),
             livelli=j.get("livelli", 0), ritmo=j.get("ritmo_mbs", 0))
    out.append(d)
for d in out:
    print(" ".join(f"{k}={round(v,2) if isinstance(v,float) else v}" for k, v in d.items()))
json.dump(out, open("risultati/riassunto.json", "w"), indent=1)
