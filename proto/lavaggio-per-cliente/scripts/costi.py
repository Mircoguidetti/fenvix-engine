#!/usr/bin/env python3
"""M26 · costo al mese del lavaggio a 20 TB con 4.000 scritture/s e prezzo al cliente contro Infinite.

Tutto STIMATO, su prezzi DICHIARATI (NUMBERS.md, B1, B13) e su misure locali (CPU per GB, taglia delle richieste).
uso: costi.py CPU_S_PER_GB PUT_MB GET_MB
"""
import json, sys

MESE_S = 2_628_000  # 730 h
U_BS = 4000 * 1.9e3  # byte dell'utente al secondo (B1: 1,9 KB per scrittura)
U_GB = U_BS * MESE_S / 1e9  # GB dell'utente al mese
PUT = 0.005 / 1000
GET = 0.0004 / 1000
VCPU_SPOT = 0.0192  # $/vCPU-h: c7g.xlarge spot di B1 ($56 al mese per 4 vCPU)
VCPU_OD = 0.03875  # $/vCPU-h: c7g.large on demand $0,0775/h (NUMBERS.md)
S3_GB = 0.023
DATI_S3_GB = 10_240  # 20 TB logici compressi 2 volte (B13)
FREDDE_S = 35  # find fredde al secondo (profilo B di B13)

# base senza lavaggio e senza GET delle find (STIMATO)
BASE_B13 = 630 - 48  # cucina condivisa B13, profilo B, senza le sue GET
SCRITTURE_B1 = {  # voci delle scritture a 4.000/s dal revisore B1, profilo (a)
    "quaderno": 226, "tra_zone": 412, "cpu_scritture": 120, "portiere_scritture": 195, "scarico": 18, "punto_nel_tempo": 29,
}
SCRITTURE_DIETA = dict(SCRITTURE_B1, tra_zone=165, portiere_scritture=0)  # M13 (-60% byte tra zone), M14 driver diretti
BANCONE_INDICI = (25, 105)  # M21, blocchi da 64 KB
INFINITE = {"basso": 6000 + 1978, "centro": 6450 + 1978, "alto": 6900 + 3954}  # spazio 20-23 TB a $0,30 + M50/M60 Gen2


def lavaggio(wa, wa_lett, spazio, cpu_s_gb, put_mb, get_mb, testa=1.5, spot=True):
    w_gb = wa * U_GB
    r_gb = wa_lett * U_GB
    put = w_gb * 1024 / put_mb * PUT
    get = r_gb * 1024 / get_mb * GET
    vcpu = w_gb * cpu_s_gb / MESE_S
    cpu = vcpu * testa * (VCPU_SPOT if spot else VCPU_OD) * 730
    sp = max(0.0, spazio - 1) * DATI_S3_GB * S3_GB
    return {"put": put, "get": get, "cpu": cpu, "vcpu_medi": vcpu, "spazio": sp, "totale": put + get + cpu + sp, "scritti_tb_mese": w_gb / 1000}


def main():
    cpu_s_gb = float(sys.argv[1]); put_mb = float(sys.argv[2]); get_mb = float(sys.argv[3])
    righe = [json.loads(l) for l in open("risultati/proiezione-20tb.jsonl")]
    for j in righe:
        if j["wa_lavaggio"] > 1000:
            j["costo"] = None
        else:
            j["costo"] = {}
            for nome, (pm, gm) in {"misurate": (put_mb, get_mb), "grandi": (10, 16)}.items():
                j["costo"][nome] = lavaggio(j["wa_lavaggio"], j["wa_lettura"], j["spazio_medio"], cpu_s_gb, pm, gm)
        out = {"l0_mb": j["l0_mb"], "modo": j["modo"], "consolida": j["consolida"], "T": j["T"], "wa": j["wa_lavaggio"],
               "pile": j["pile_media"], "pile_max": j["pile_max"], "find_tutte": j["find_tipica_tutte"], "find_qualsiasi": j["find_tipica_qualsiasi"],
               "scalda_p90": j["get_tutte_p90"]}
        if j["costo"]:
            for sc_nome, sc in (("b1", SCRITTURE_B1), ("dieta", SCRITTURE_DIETA)):
                for g_nome, g in (("peggio", j["find_tipica_tutte"]), ("tipico", j["find_tipica_qualsiasi"])):
                    for rq in ("misurate", "grandi"):
                        lav = j["costo"][rq]["totale"]
                        for bi, b in zip(("lo", "hi"), BANCONE_INDICI):
                            costo = BASE_B13 + sum(sc.values()) + b + FREDDE_S * g * MESE_S / 0.99 * GET * 0.99 + lav
                            prezzo = costo / 0.55
                            for inf_n, inf in INFINITE.items():
                                out[f"{sc_nome}/{g_nome}/{rq}/{bi}/{inf_n}"] = round(prezzo / inf * 100, 1)
            out["lav_misurate"] = {k: round(v, 1) for k, v in j["costo"]["misurate"].items()}
            out["lav_grandi"] = {k: round(v, 1) for k, v in j["costo"]["grandi"].items()}
        print(json.dumps(out))


if __name__ == "__main__":
    main()
