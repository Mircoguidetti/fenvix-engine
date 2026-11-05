#!/usr/bin/env python3
"""Revisore M26: se le pile piccole e recenti stanno sul bancone (NVMe dei camerieri, spinte dal lavaggio come da
DECISIONS 2026-11-02), quante pile restano da leggere su S3? Usa le fotografie del simulatore di M26 (sim.py --snaps).
Per ogni budget B (GB logici x ovh), si tengono sul bancone le pile più piccole finché ci stanno; GET peggiore = pile
rimaste fuori. Uscita: media sulle fotografie."""
import json, subprocess, sys
cfgs = json.loads(sys.argv[1])
budgets = [0, 50, 100, 200, 400, 800, 1600]
for cfg in cfgs:
    j = json.loads(subprocess.run(["python3", "scripts/sim.py", "--ovh", "1.33", "--snaps"] + [str(x) for x in cfg],
                                  capture_output=True, text=True, check=True).stdout)
    snaps = j["snaps"]
    out = {"cfg": " ".join(str(x) for x in cfg), "wa": round(j["wa_lavaggio"], 1), "pile": round(j["pile_media"], 2)}
    for b in budgets:
        fuori = []
        for sn in snaps:
            sizes = sorted(s for _, _, s in sn)
            acc, k = 0.0, 0
            for s in sizes:
                if acc + s <= b * 2**30:
                    acc += s; k += 1
                else:
                    break
            fuori.append(len(sizes) - k)
        fuori.sort()
        out[f"fuori_{b}GB"] = round(sum(fuori) / len(fuori), 2)
        out[f"fuori_{b}GB_p90"] = fuori[int(0.9 * (len(fuori) - 1))]
    print(json.dumps(out), flush=True)
