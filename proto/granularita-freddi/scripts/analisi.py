#!/usr/bin/env python3
"""Riassunto delle misure M21 e conti in soldi sul profilo B di B13.

Legge risultati/misure.jsonl e scrive risultati/riassunto.json (e un testo leggibile su stdout).
Tutte le costanti in soldi vengono da NUMBERS.md o da B13 / revisore B13 (vedi SOGLIE.md).
"""
import json, sys, collections, os

QUI = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(QUI, "..", "risultati")

# --- costanti (etichette nel rapporto)
GET_PREZZO = 0.0004 / 1000          # $ per GET, DICHIARATO (NUMBERS.md, S3 Standard)
SECONDI_MESE = 2_592_000            # 30 giorni
FREDDE_S = 35                       # letture fredde al secondo, profilo B (STIMATO, B13)
COSTO_B13_CONDIVISA = 630           # $ al mese, cucina condivisa, con 1,3 GET per lettura fredda (STIMATO, B13)
GET_B13 = 48                        # $ al mese di GET dentro i 630 (STIMATO, B13)
COSTO_B13_DEDICATI = (1130, 1280)   # camerieri dedicati (STIMATO, B13)
INFINITE = {"basso": 6500, "centro": 7440, "alto": 8900}   # $ al mese (STIMATO su prezzi DICHIARATI, B13 e revisore)
MARGINE = 0.45
SOGLIA = 0.47
NVME_GB_MESE = (0.119 - 0.091) * 730 / 118   # $ per GB di NVMe al mese: m7gd.large meno m7g.large, 118 GB (STIMATO da prezzi DICHIARATI in NUMBERS.md)
COPIE_BANCONE = (1, 3)              # in quanti camerieri sta il bancone di un database (1 = una zona, 3 = tre zone)
TB_PROFILO_B = 20                   # TB logici
# Indici dei blocchi e filtri crescono con i byte logici (i blocchi si contano prima della compressione, i filtri per
# chiave), non con i byte compressi: il rapporto misurato si applica ai 20 TB logici.
SCATOLE_PER_TB_LOGICO = 1.0
MIX = {"Q1": 0.6, "Q2": 0.2, "Q3": 0.2}

righe = [json.loads(l) for l in open(os.path.join(R, "misure.jsonl")) if l.strip()]
stato = {}
disco = {}
ris = collections.defaultdict(dict)
for r in righe:
    e = r.get("etichetta")
    if r.get("tipo") == "stato":
        stato[e] = r
    elif r.get("tipo") == "disco":
        disco[e] = r["byte"]
    elif r.get("tipo") == "riassunto":
        key = (r["bancone"], r["ritardo_ms"])
        ris[e].setdefault(key, {})[r["q"]] = r


def costo_get(g):
    return FREDDE_S * SECONDI_MESE * g * GET_PREZZO


out = {}
for e in ris:
    st = stato.get(e, {})
    meta_ratio = (st.get("byte_indici_blocchi", 0) + st.get("byte_filtri", 0)) / max(1, st.get("byte_scatole", 1))
    idx_ratio = st.get("byte_indici_blocchi", 0) / max(1, st.get("byte_scatole", 1))
    filt_ratio = st.get("byte_filtri", 0) / max(1, st.get("byte_scatole", 1))
    d = {"stato": {k: st.get(k) for k in ("l0", "sorted_run", "scatole", "byte_scatole", "byte_indici_blocchi", "byte_filtri")},
         "meta_su_dati": meta_ratio, "indici_su_dati": idx_ratio, "filtri_su_dati": filt_ratio}
    for (banc, rit), qs in sorted(ris[e].items()):
        k = f"{banc}" + (f"_ritardo{rit}" if rit else "")
        d[k] = {q: {"get": qs[q]["get_media"], "get_p90": qs[q]["get_p90"], "get_max": qs[q]["get_max"],
                    "kb": qs[q]["byte_media"] / 1024, "docs": qs[q]["docs_media"],
                    "giri": qs[q].get("giri_p50", 0), "ms_p50": qs[q]["ms_p50"]} for q in qs}
        if all(q in qs for q in MIX):
            d[k]["mix"] = {"get": sum(MIX[q] * qs[q]["get_media"] for q in MIX),
                           "kb": sum(MIX[q] * qs[q]["byte_media"] for q in MIX) / 1024,
                           "giri": sum(MIX[q] * qs[q].get("giri_p50", 0) for q in MIX)}
    # soldi sul profilo B con il bancone S1 (indici e filtri sempre sul bancone)
    if "S1" in d and "mix" in d["S1"]:
        g = d["S1"]["mix"]["get"]
        g_peggio = max(d["S1"][q]["get"] for q in MIX)
        meta_gb = meta_ratio * TB_PROFILO_B * 1000 * SCATOLE_PER_TB_LOGICO
        banc = [meta_gb * NVME_GB_MESE * c for c in COPIE_BANCONE]
        base = COSTO_B13_CONDIVISA - GET_B13
        costi = {"get_mix": costo_get(g), "get_peggiore": costo_get(g_peggio),
                 "bancone_meta_gb_20TB": meta_gb, "bancone_extra": banc,
                 "costo_condivisa": [base + costo_get(g) + b for b in banc],
                 "costo_condivisa_peggiore": base + costo_get(g_peggio) + banc[1],
                 "costo_dedicati": [COSTO_B13_DEDICATI[0] - GET_B13 + costo_get(g) + banc[0],
                                    COSTO_B13_DEDICATI[1] - GET_B13 + costo_get(g) + banc[1]]}
        costi["prezzo_condivisa"] = [c / (1 - MARGINE) for c in costi["costo_condivisa"]]
        costi["prezzo_dedicati"] = [c / (1 - MARGINE) for c in costi["costo_dedicati"]]
        costi["su_infinite_condivisa"] = {k: [p / v for p in costi["prezzo_condivisa"]] for k, v in INFINITE.items()}
        costi["su_infinite_dedicati"] = {k: [p / v for p in costi["prezzo_dedicati"]] for k, v in INFINITE.items()}
        costi["su_infinite_peggiore_condivisa_centro"] = costi["costo_condivisa_peggiore"] / (1 - MARGINE) / INFINITE["centro"]
        d["soldi_B"] = costi
    out[e] = d

# GET per find alla soglia del 47% (cucina condivisa, Infinite al centro, bancone come chat-base-4)
base = COSTO_B13_CONDIVISA - GET_B13
g47 = (SOGLIA * INFINITE["centro"] * (1 - MARGINE) - base) / costo_get(1)
out["_costanti"] = {"costo_1_get_per_find_al_mese": costo_get(1), "nvme_gb_mese": NVME_GB_MESE,
                    "get_per_find_alla_soglia_47_senza_bancone": g47}
json.dump(out, open(os.path.join(R, "riassunto.json"), "w"), indent=1)

for e, d in out.items():
    if e.startswith("_"):
        continue
    s = d["stato"]
    print(f"\n== {e}: L0 {s['l0']}, sorted run {s['sorted_run']}, scatole {s['scatole']}, dati {s['byte_scatole']/1e9:.2f} GB, "
          f"indici dei blocchi {100*d['indici_su_dati']:.2f}%, filtri {100*d['filtri_su_dati']:.2f}% dei dati")
    for k in ("S0", "S1", "S1_ritardo20"):
        if k in d:
            parts = [f"{q} {d[k][q]['get']:.1f} GET (p90 {d[k][q]['get_p90']}, {d[k][q]['kb']:.0f} KB, {d[k][q]['docs']:.1f} doc"
                     + (f", {d[k][q]['giri']:.1f} giri" if k.endswith('20') else "") + ")" for q in ("Q1", "Q2", "Q3") if q in d[k]]
            mix = d[k].get("mix")
            print(f"  {k}: " + "; ".join(parts) + (f"; MIX {mix['get']:.1f} GET" if mix else ""))
    if "soldi_B" in d:
        c = d["soldi_B"]
        print(f"  soldi B: GET ${c['get_mix']:.0f}/mese (peggiore query ${c['get_peggiore']:.0f}); bancone in più "
              f"{c['bancone_meta_gb_20TB']:.0f} GB = ${c['bancone_extra'][0]:.0f}-{c['bancone_extra'][1]:.0f}; "
              f"costo condivisa ${c['costo_condivisa'][0]:.0f}-{c['costo_condivisa'][1]:.0f}, prezzo ${c['prezzo_condivisa'][0]:.0f}-{c['prezzo_condivisa'][1]:.0f}, "
              f"su Infinite centro {100*c['su_infinite_condivisa']['centro'][0]:.0f}-{100*c['su_infinite_condivisa']['centro'][1]:.0f}% "
              f"(forchetta {100*c['su_infinite_condivisa']['alto'][0]:.0f}-{100*c['su_infinite_condivisa']['basso'][1]:.0f}%); "
              f"dedicati prezzo ${c['prezzo_dedicati'][0]:.0f}-{c['prezzo_dedicati'][1]:.0f} = {100*c['su_infinite_dedicati']['centro'][0]:.0f}-{100*c['su_infinite_dedicati']['centro'][1]:.0f}% "
              f"(peggio {100*c['su_infinite_dedicati']['basso'][1]:.0f}%)")
print(json.dumps(out["_costanti"]))
