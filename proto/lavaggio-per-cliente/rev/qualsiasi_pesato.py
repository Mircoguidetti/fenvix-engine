"""Revisore M26: cliente qualsiasi pesato per letture. M26 fa la media uniforme sui clienti; se un cliente legge
in proporzione a quanto scrive (attività zipf uguale per letture e scritture), la find media pesa i clienti attivi."""
import json, subprocess, sys, math
sys.path.insert(0, "scripts")
from proietta import rates, presenza
N = int(float(sys.argv[1]))
cfg = ["--taglia-gb", 20480, "--w-mbs", 7.6, "--l0-mb", 256, "--modo", "livelli", "--T", 4]
j = json.loads(subprocess.run(["python3", "scripts/sim.py", "--ovh", "1.33", "--snaps"] + [str(x) for x in cfg], capture_output=True, text=True, check=True).stdout)
pop, top1 = rates(N)
D = j["D"]; un = []; pe = []
for sn in j["snaps"]:
    Tn = max(c for _, c, _ in sn)
    g = [presenza(sn, Tn, D, r, 0) for r in pop]
    un.append(sum(g) / len(g)); pe.append(sum(x * r for x, r in zip(g, pop)) / sum(pop))
u = sum(un) / len(un); p = sum(pe) / len(pe)
print(json.dumps({"N": N, "pile_uniforme": round(u, 2), "pile_pesato_letture": round(p, 2), "pile_tutte": round(j["pile_media"], 2),
                  "find_uniforme_M26": round(0.8 * (u + 1) + 0.4, 2), "find_pesato": round(0.8 * (p + 1) + 0.4, 2), "find_peggio_M26": round(0.8 * (j["pile_media"] + 2.6) + 0.4, 2)}))
