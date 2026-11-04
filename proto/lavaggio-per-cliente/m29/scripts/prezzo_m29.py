#!/usr/bin/env python3
"""M29: prezzo a 20 TB, 4.000 scritture/s, con i numeri nuovi. Voci di M26 (scripts/costi.py) e del revisore di M26.
Tutto STIMATO. Ingressi misurati da M29 in risultati/riassunto.jsonl (passati a mano qui sotto, con l'etichetta).
uso: python3 m29/scripts/prezzo_m29.py  (dalla cartella proto/lavaggio-per-cliente)"""
import json, sys
sys.path.insert(0, "scripts")
import costi as C

P = json.load(open("m29/risultati/ingressi-prezzo.json"))  # scritto a mano dopo le misure, con la fonte di ogni numero
INF = C.INFINITE["centro"]
GET_FIND = C.FREDDE_S * C.MESE_S * C.GET  # $ al mese per 1 GET per find
NVME = 0.17  # $/GB-mese di bancone (M21)
WA, WA_L, SPAZIO = 29.4, 30.6, 1.14  # pile a gradini T = 4 a 20 TB (simulatore M26, rifatto dal revisore)
PILE = 6.43 + P["correzione_pile"]  # pile medie a 20 TB
FUORI_200 = 2.71 + 1  # pile fuori da 200 GB di bancone (revisore, pile_sul_bancone.py) + 1 di correzione


def find(k, pile, q2=2.0):
    """find peggiore: 60% Q1 e 20% Q3 costano k GET per pila, Q2 costa q2 GET."""
    return 0.8 * k * pile + 0.2 * q2


def lavaggio(r, cpu_extra_s_gb, put_mb=10, get_mb=16, r_spazio=2.0):
    """r = rapporto di compressione dei byte del lavaggio (1 = niente); cpu_extra = secondi di CPU in più per GB logico
    scritto dal lavaggio; r_spazio = rapporto per lo spazio su S3 (B13 e M26 contano 2)."""
    w_gb = WA * C.U_GB
    r_gb = WA_L * C.U_GB
    put = w_gb / r * 1024 / put_mb * C.PUT
    get = r_gb / r * 1024 / get_mb * C.GET
    vcpu = w_gb * (24.5 + cpu_extra_s_gb) / C.MESE_S
    cpu = vcpu * 1.5 * C.VCPU_SPOT * 730
    spazio = (SPAZIO - 1) * 20480 / r_spazio * C.S3_GB
    return {"put": put, "get": get, "cpu": cpu, "spazio": spazio, "vcpu": vcpu, "totale": put + get + cpu + spazio}


def costo(g_find, r, cpu_extra, bancone_gb=0.0, copie=1, put_mb=10, r_spazio=2.0):
    lav = lavaggio(r, cpu_extra, put_mb, 16, r_spazio)
    # la base di B13 conta 20 TB compressi 2 volte su S3: con un altro rapporto lo spazio cambia
    spazio_delta = (20480 / r_spazio - C.DATI_S3_GB) * C.S3_GB
    banc = bancone_gb / r * NVME * copie
    c = C.BASE_B13 + spazio_delta + sum(C.SCRITTURE_DIETA.values()) + C.BANCONE_INDICI[1] + GET_FIND * g_find + lav["totale"] + banc
    return c, lav, banc, spazio_delta


def riga(nome, g_find, r, cpu_extra, bancone_gb=0.0, copie=1, put_mb=10, r_spazio=2.0):
    c, lav, banc, sd = costo(g_find, r, cpu_extra, bancone_gb, copie, put_mb, r_spazio)
    p = c / 0.55
    j = {"caso": nome, "find_peggiore": round(g_find, 2), "rapporto": r, "costo": round(c), "prezzo": round(p),
         "pct_centro": round(p / INF * 100, 1), "pct_forchetta": [round(p / C.INFINITE["alto"] * 100, 1), round(p / C.INFINITE["basso"] * 100, 1)],
         "lavaggio": {k: round(v, 1) for k, v in lav.items()}, "bancone_pile": round(banc, 1), "spazio_delta": round(sd, 1), "get_find": round(GET_FIND * g_find, 1)}
    print(json.dumps(j))
    return j


rows = []
k26, kreg, k_s3, q2b = P["k_lettore_m26"], P["k_lettore_regolato"], P["k_s3_con_bancone"], P["q2_con_bancone"]
F_BANC = find(k_s3, FUORI_200, q2b)
rows.append(riga("lettore M26, senza bancone, senza compressione", find(k26, PILE), 1.0, 0))
rows.append(riga("lettore regolato, senza bancone, senza compressione", find(kreg, PILE), 1.0, 0))
rows.append(riga("regolato + pile recenti sul bancone (200 GB, 1 copia)", F_BANC, 1.0, 0, 200))
rows.append(riga("regolato + bancone (3 copie)", F_BANC, 1.0, 0, 200, 3))
for cod in ("lz4", "zstd1"):
    r, cx = P[f"rapporto_{cod}"], P[f"cpu_extra_{cod}"]
    rows.append(riga(f"regolato + bancone + {cod} (rapporto misurato anche per lo spazio)", F_BANC, r, cx, 200, 1, 10, r))
    rows.append(riga(f"regolato + bancone + {cod}, rapporto 2 (prudente)", F_BANC, 2.0, cx, 200, 1, 10, 2.0))
    rows.append(riga(f"regolato + bancone + {cod} misurato + parti PUT da 64 MB", F_BANC, r, cx, 200, 1, 64, r))
json.dump(rows, open("m29/risultati/prezzo-20tb.json", "w"), indent=1)
