#!/usr/bin/env python3
"""Prezzo finale a 20 TB, 4.000 scritture/s: pile del simulatore + correzione misurata (+2,6 pile), GET per find,
costo del lavaggio (costi.py), base di B13 e B1. Tutto STIMATO."""
import json, sys
sys.path.insert(0, "scripts")
import costi as C
CORR = 2.6  # pile vere - pile simulate (MISURATO a 0,25-2 GB: +2,1..+2,9)
INF = C.INFINITE
rows = [json.loads(l) for l in open("risultati/proiezione-20tb.jsonl")]
for j in rows:
    if j["l0_mb"] != 256 or j["wa_lavaggio"] > 1000 or j["modo"] == "grandi":
        continue
    pile = j["pile_media"] + CORR
    peggio = 0.8 * pile + 0.4
    tipico = 0.8 * (j["get_qualsiasi"] + 1.0) + 0.4  # +1 pila grande in più, prudente
    scalda = j["get_tutte_p90"] + CORR
    out = {"modo": j["modo"], "T": j["T"], "wa": round(j["wa_lavaggio"], 1), "pile": round(pile, 1), "find_peggio": round(peggio, 1),
           "find_tipico": round(tipico, 1), "scalda_p90": round(scalda, 1)}
    for rq, (pm, gm) in (("regolate", (10, 16)), ("oggi", (7, 1.2))):
        lav = C.lavaggio(j["wa_lavaggio"], j["wa_lettura"], j["spazio_medio"], 24.5, pm, gm)
        out["lav_" + rq] = round(lav["totale"])
        if rq == "regolate":
            out["lav_dettaglio"] = {k: round(v, 1) for k, v in lav.items()}
        for sc_n, sc in (("dieta", C.SCRITTURE_DIETA), ("oggi", C.SCRITTURE_B1)):
            for g_n, g in (("peggio", peggio), ("tipico", tipico)):
                costo = C.BASE_B13 + sum(sc.values()) + C.BANCONE_INDICI[1] + C.FREDDE_S * g * C.MESE_S * C.GET + lav["totale"]
                p = costo / 0.55
                out[f"{rq}/{sc_n}/{g_n}"] = (round(costo), round(p), round(p / INF["basso"] * 100, 1), round(p / INF["centro"] * 100, 1), round(p / INF["alto"] * 100, 1))
    costo0 = C.BASE_B13 + sum(C.SCRITTURE_DIETA.values()) + C.BANCONE_INDICI[1] + C.FREDDE_S * peggio * C.MESE_S * C.GET
    out["senza_lavaggio_dieta_peggio"] = (round(costo0), round(costo0 / 0.55 / INF["centro"] * 100, 1))
    print(json.dumps(out))
