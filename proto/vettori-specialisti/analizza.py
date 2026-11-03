"""M23: per ogni job, la configurazione più veloce a richiamo >= 0,90 e >= 0,95 (con e senza filtro), e il rapporto
noi / avversario dentro lo stesso job (stessa macchina).
Uso: python3 analizza.py <risultati.jsonl> [altri risultati.jsonl ...]
"""
import json, sys
from collections import defaultdict

NOI = ("noi (faiss)", "noi (usearch)")


def main():
    rows = []
    for p in sys.argv[1:]:
        try:
            rows += [json.loads(l) for l in open(p) if l.strip()]
        except FileNotFoundError:
            print("manca", p)
    jobs = defaultdict(list)
    for r in rows:
        jobs[r.get("job", "?")].append(r)
    summary = []
    for job, jr in sorted(jobs.items()):
        print(f"\n===== job {job} =====")
        g = defaultdict(list)
        for r in jr:
            g[(r["engine"], r["config"], r["filter"])].append(r)
        best = {}
        for k in sorted(g, key=str):
            out = []
            for t in (0.90, 0.95):
                ok = [x for x in g[k] if x["recall10"] >= t]
                b = max(ok, key=lambda x: x["qps_cpu_net"]) if ok else None
                best[(k, t)] = b
                if b:
                    s = f'@{t}: {b["qps_cpu"]:.0f}/s lordo'
                    if abs(b["qps_cpu_net"] - b["qps_cpu"]) > 0.5:
                        s += f', {b["qps_cpu_net"]:.0f}/s senza protocollo (vuota {b.get("ovh_ms", 0):.3f} ms)'
                    s += f' (richiamo {b["recall10"]:.3f}, {b["params"]})'
                else:
                    s = f"@{t}: non arriva"
                out.append(s)
            top = max(x["recall10"] for x in g[k])
            print(f"{k[0]} | {k[1]} | filtro={k[2]} | max richiamo {top:.3f} | " + " | ".join(out))
        print("-- rapporti nello stesso job (noi faiss / avversario; senza protocollo, e lordo) --")
        for filt in (False, True):
            for t in (0.90, 0.95):
                ref = best.get((("noi (faiss)", "HNSW SQ8+ricalcolo, 1 pezzo", filt), t))
                if not ref:
                    continue
                for k in sorted(g, key=str):
                    if k[0] in NOI or k[2] != filt:
                        continue
                    b = best.get((k, t))
                    if not b:
                        print(f"filtro={filt} @{t}: {k[0]} {k[1]}: non arriva (noi {ref['qps_cpu']:.0f}/s)")
                        continue
                    rn = ref["qps_cpu"] / b["qps_cpu_net"]
                    rl = ref["qps_cpu"] / b["qps_cpu"]
                    print(f"filtro={filt} @{t}: noi {ref['qps_cpu']:.0f}/s contro {k[0]} {k[1]} "
                          f"{b['qps_cpu_net']:.0f}/s -> {rn:.2f}x (lordo {rl:.2f}x)")
                    summary.append((filt, t, k[0], k[1], job, rn, rl, ref["qps_cpu"], b["qps_cpu_net"]))
                # le altre nostre (usearch) contro il riferimento
                for k in sorted(g, key=str):
                    if k[0] == "noi (usearch)" and k[2] == filt and best.get((k, t)):
                        b = best[(k, t)]
                        print(f"filtro={filt} @{t}: noi usearch {k[1]} {b['qps_cpu']:.0f}/s = "
                              f"{b['qps_cpu'] / ref['qps_cpu']:.2f} volte il riferimento faiss")
    if summary:
        print("\n===== tutti gli avversari, rapporto noi/avversario (sotto 1 = l'avversario usa meno CPU) =====")
        for filt in (False, True):
            for t in (0.95, 0.90):
                sel = sorted([s for s in summary if s[0] == filt and s[1] == t], key=lambda s: s[5])
                for s in sel:
                    print(f"filtro={filt} @{t}: {s[5]:.2f}x (lordo {s[6]:.2f}x) {s[2]} {s[3]} [job {s[4]}: noi {s[7]:.0f}/s, "
                          f"loro {s[8]:.0f}/s]")
    builds = []
    for p in sys.argv[1:]:
        try:
            builds += [json.loads(l) for l in open(p.replace(".jsonl", "-build.jsonl"))]
        except FileNotFoundError:
            pass
    if builds:
        print("\n===== costruzione e protocollo =====")
        for b in builds:
            print(json.dumps({k: v for k, v in b.items() if k not in ("plan", "plan_filter")}))


if __name__ == "__main__":
    main()
