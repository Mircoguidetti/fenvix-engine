#!/usr/bin/env python3
"""M26 · GET per find fredda a 20 TB, dalle pile simulate.

Regola misurata (M21 e M26): con i dati raggruppati una find costa una GET per ogni pila dove il cliente c'è; i filtri
per prefisso saltano le pile dove non c'è. Qui: per ogni fotografia delle pile durante il regime (finestre di tempo
[a, c] in tempo-dati), la probabilità che un cliente con ritmo r scritture/s abbia dati in una pila è
1 - exp(-r * sovrapposizione([a, c], [T - D, T - dormienza])).

Clienti: N clienti con attività zipf (s = 0,7) come nel carico misurato, 4.000 scritture al secondo in tutto.
Classi: (1) cliente qualsiasi (media sulla popolazione), (2) cliente dormiente da 7 giorni, (3) il cliente più
attivo dell'1% (presente quasi ovunque = caso peggiore = "GET = pile").
"""
import json, math, subprocess, sys, random

SIM = "scripts/sim.py"


def rates(N, s=0.7, tot=4000.0, k=400):
    """Campione di k ritmi della popolazione (quantili) e ritmo dell'1% più attivo."""
    # H(N, s) ~ somma r^-s
    H = sum((r + 1) ** -s for r in range(min(N, 100000))) + (
        ((N ** (1 - s)) - (100000 ** (1 - s))) / (1 - s) if N > 100000 else 0)
    qs = [int(N * (i + 0.5) / k) + 1 for i in range(k)]
    pop = [tot * (q ** -s) / H for q in qs]
    top1 = tot * ((N * 0.005) ** -s) / H
    return pop, top1


def presenza(snap, T, D, r, dorm):
    lo, hi = T - D, T - dorm
    g = 0.0
    for a, c, _ in snap:
        ov = min(c, hi) - max(a, lo)
        if ov > 0:
            g += 1 - math.exp(-r * ov)
    return g


def main():
    cfgs = json.loads(sys.argv[1])
    N = int(float(sys.argv[2])) if len(sys.argv) > 2 else 10_000_000
    pop, top1 = rates(N)
    for cfg in cfgs:
        cmd = ["python3", SIM, "--ovh", "1.33", "--snaps"] + [str(x) for x in cfg]
        j = json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout)
        snaps = j.pop("snaps")
        D = j["D"]
        T = j["t_dati_fine"]
        # le fotografie sono prese in tempi diversi: T di ogni fotografia = c massimo
        res = {"tutte": [], "qualsiasi": [], "dormiente7": [], "attivo1": []}
        for sn in snaps:
            Tn = max(c for _, c, _ in sn)
            res["tutte"].append(len(sn))
            res["qualsiasi"].append(sum(presenza(sn, Tn, D, max(r, 1e-12), 0) for r in pop) / len(pop))
            res["dormiente7"].append(sum(presenza(sn, Tn, D, r, 7 * 86400) for r in pop) / len(pop))
            res["attivo1"].append(presenza(sn, Tn, D, top1, 0))
        out = {k: v for k, v in j.items()}
        for k, v in res.items():
            v.sort()
            out["get_" + k] = sum(v) / len(v)
            out["get_" + k + "_p90"] = v[int(0.9 * (len(v) - 1))]
        # find tipica = 0,6 Q1 + 0,2 Q2 (2 GET: biglietto e documento) + 0,2 Q3, con Q1 = Q3 = pile dove il cliente c'è
        for k in ("qualsiasi", "dormiente7", "attivo1", "tutte"):
            out["find_tipica_" + k] = 0.8 * out["get_" + k] + 0.4
        out["N"] = N
        out["giorni_D"] = D / 86400
        print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
