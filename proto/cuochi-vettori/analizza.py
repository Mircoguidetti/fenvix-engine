"""M20: legge risultati.jsonl e dà, per ogni configurazione, la più veloce a richiamo >= 0,90 e >= 0,95,
poi i rapporti R90/R95 (noi contro Lucene). Uso: python3 analizza.py <risultati.jsonl> [riuso.jsonl]"""
import json, sys
from collections import defaultdict


def key(r):
    if r["engine"].startswith("lucene"):
        return (r["engine"], f'{r["config"]} {r["segments"]} seg' + (" +ricalcolo" if r["rescore"] else ""), r["filter"])
    return (r["engine"], r["config"], r["filter"])


def main():
    rows = [json.loads(l) for l in open(sys.argv[1])]
    groups = defaultdict(list)
    for r in rows:
        groups[key(r)].append(r)
    best = {}
    print("== migliore per configurazione (ricerche per secondo di CPU, un thread) ==")
    for k in sorted(groups):
        g = groups[k]
        out = []
        for t in (0.90, 0.95):
            ok = [x for x in g if x["recall10"] >= t]
            b = max(ok, key=lambda x: x["qps_cpu"]) if ok else None
            best[(k, t)] = b
            out.append(f"@{t}: " + (f'{b["qps_cpu"]:.0f}/s (richiamo {b["recall10"]:.3f}, {b["params"]})' if b else "non arriva"))
        top = max(x["recall10"] for x in g)
        print(f"{k[0]} | {k[1]} | filtro={k[2]} | max richiamo {top:.3f} | " + " | ".join(out))

    def pick(pred, t, filt):
        c = [(k, best[(k, t)]) for k in groups if pred(k) and k[2] == filt and best[(k, t)]]
        return max(c, key=lambda x: x[1]["qps_cpu"]) if c else (None, None)

    lucene8 = lambda k: k[0].startswith("lucene") and " 1 seg" not in k[1]
    lucene1 = lambda k: k[0].startswith("lucene") and " 1 seg" in k[1]
    ours = lambda k: not k[0].startswith("lucene") and "8 pezzi" not in k[1]
    ours8 = lambda k: not k[0].startswith("lucene") and "8 pezzi" in k[1]
    print("\n== rapporti ==")
    for filt in (False, True):
        for t in (0.90, 0.95):
            l8k, l8 = pick(lucene8, t, filt)
            l1k, l1 = pick(lucene1, t, filt)
            ok, o = pick(ours, t, filt)
            o8k, o8 = pick(ours8, t, filt)
            s = f"filtro={filt} richiamo>={t}: "
            if l8: s += f"Lucene più segmenti {l8['qps_cpu']:.0f} ({l8k[1]}); "
            if l1: s += f"Lucene 1 segmento {l1['qps_cpu']:.0f} ({l1k[1]}); "
            if o: s += f"noi {o['qps_cpu']:.0f} ({ok[0]} {ok[1]}); "
            if o8: s += f"noi a 8 pezzi {o8['qps_cpu']:.0f}; "
            if l8 and l1: s += f"leva1 in Lucene {l1['qps_cpu'] / l8['qps_cpu']:.2f}x; "
            if l8 and o: s += f"R contro più segmenti {o['qps_cpu'] / l8['qps_cpu']:.2f}x; "
            if l1 and o: s += f"R contro 1 segmento {o['qps_cpu'] / l1['qps_cpu']:.2f}x; "
            if l8 and o8: s += f"leva2 da sola (8 pezzi contro 8 segmenti) {o8['qps_cpu'] / l8['qps_cpu']:.2f}x; "
            print(s)
    # per famiglia di quantizzazione
    print("\n== stessa famiglia (senza filtro, richiamo >= 0,95) ==")
    fam = {
        "int8": (lambda k: k[0].startswith("lucene") and k[1].startswith("int8"),
                 lambda k: not k[0].startswith("lucene") and ("i8" in k[1] or "SQ8" in k[1]) and "8 pezzi" not in k[1]),
        "binario": (lambda k: k[0].startswith("lucene") and k[1].startswith("bin"),
                    lambda k: not k[0].startswith("lucene") and "b1" in k[1]),
    }
    for name, (lp, op) in fam.items():
        for segpred, lab in ((lambda k: " 1 seg" not in k[1], "più segmenti"), (lambda k: " 1 seg" in k[1], "1 segmento")):
            lk, l = pick(lambda k: lp(k) and segpred(k), 0.95, False)
            okk, o = pick(op, 0.95, False)
            if l and o:
                print(f"{name}: Lucene {lab} {l['qps_cpu']:.0f} ({lk[1]}), noi {o['qps_cpu']:.0f} ({okk[1]}), {o['qps_cpu'] / l['qps_cpu']:.2f}x")
            else:
                print(f"{name}: Lucene {lab} {'%.0f' % l['qps_cpu'] if l else 'non arriva'}, noi {'%.0f' % o['qps_cpu'] if o else 'non arriva'}")
    if len(sys.argv) > 2:
        print("\n== leva 3 (riuso) ==")
        rr = [json.loads(l) for l in open(sys.argv[2])]
        for seq in ("095", "090"):
            for scen in ("tutte", "dopo cache app"):
                g = [x for x in rr if x["seq"] == seq and x["scenario"] == scen]
                base = [x for x in g if x["variant"] == "senza riuso"]
                b = base[0] if base else None
                lim = min(0.95, b["recall10"] - 0.005) if b else 0.95
                cand = [x for x in g if x["variant"] == "riuso" and x["recall10"] >= lim]
                c = max(cand, key=lambda x: x["speedup_cpu"]) if cand else None
                s = f"sequenza coseno 0,{seq[1:]} {scen}: senza riuso {b['qps_cpu']:.0f}/s richiamo {b['recall10']:.3f}" if b else "?"
                if c:
                    s += (f"; con riuso {c['qps_cpu']:.0f}/s richiamo {c['recall10']:.3f}, {c['speedup_cpu']:.2f}x "
                          f"(tau {c['tau']}, C {c['C']}, vicine {c['n_vicina']}, uguali {c['n_uguale']}, su {c['n']})")
                else:
                    s += "; nessuna variante con richiamo vicino a quello senza riuso"
                print(s)


if __name__ == "__main__":
    main()
