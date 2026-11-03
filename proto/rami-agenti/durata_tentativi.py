"""M22: quanto dura un tentativo di un agente, sulle tracce con l'ora di tau2-bench (le stesse 12 di M18).

Per ogni conversazione (un tentativo): durata dalla prima all'ultima chiamata a uno strumento che tocca il database e
dall'inizio alla fine della simulazione; pause tra due chiamate consecutive (quanto il ramo resta fermo ad aspettare il
modello). Uso: python3 durata_tentativi.py <cartella data/tau2/results/final> risultati/durata-tau2.json
"""
import glob
import json
import os
import statistics
import sys
from datetime import datetime


def ts(s):
    return datetime.fromisoformat(s).timestamp()


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def summary(xs):
    return {"n": len(xs), "mediana": statistics.median(xs), "p10": pct(xs, 0.1), "p25": pct(xs, 0.25), "p75": pct(xs, 0.75),
            "p90": pct(xs, 0.9), "max": max(xs), "media": statistics.fmean(xs)}


def main():
    src, out = sys.argv[1], sys.argv[2]
    files = [f for f in sorted(glob.glob(os.path.join(src, "*.json")))
             if ("_default_" in f or "_base_" in f) and "workflow" not in os.path.basename(f)]
    sim_dur, tool_span, gaps, max_gap, calls = [], [], [], [], []
    per_file = {}
    for f in files:
        d = json.load(open(f))
        fd = []
        for s in d["simulations"]:
            if s.get("start_time") and s.get("end_time"):
                sim_dur.append(ts(s["end_time"]) - ts(s["start_time"]))
                fd.append(sim_dur[-1])
            t = [ts(m["timestamp"]) for m in s["messages"] if m.get("role") == "tool" and m.get("timestamp")]
            if len(t) >= 1:
                calls.append(len(t))
            if len(t) >= 2:
                tool_span.append(t[-1] - t[0])
                g = [b - a for a, b in zip(t, t[1:])]
                gaps.extend(g)
                max_gap.append(max(g))
        per_file[os.path.basename(f)] = summary(fd) if fd else None
    res = {"file": [os.path.basename(f) for f in files],
           "durata_simulazione_s": summary(sim_dur),
           "durata_prima_ultima_chiamata_s": summary(tool_span),
           "chiamate_per_tentativo": summary(calls),
           "pausa_tra_chiamate_s": summary(gaps),
           "pausa_piu_lunga_per_tentativo_s": summary(max_gap),
           "quota_tentativi_sotto_90s": sum(1 for x in sim_dur if x < 90) / len(sim_dur),
           "quota_pause_sopra_60s": sum(1 for x in gaps if x > 60) / len(gaps),
           "per_file_durata_simulazione_s": per_file}
    json.dump(res, open(out, "w"), indent=1)
    for k, v in res.items():
        if k not in ("file", "per_file_durata_simulazione_s"):
            print(k, {a: round(b, 1) for a, b in v.items()} if isinstance(v, dict) else round(v, 3))
    for k, v in per_file.items():
        print(" ", k, v and round(v["mediana"], 1))


if __name__ == "__main__":
    main()
