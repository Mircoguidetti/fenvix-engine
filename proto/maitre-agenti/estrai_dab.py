"""Estrae dalle tracce pubbliche di DAB (Data Agent Benchmark, UC Berkeley) solo le
chiamate degli agenti al database (query_db, list_db), in un JSONL compresso.

Uso: python estrai_dab.py <cartella_estratta> <uscita.jsonl.gz>
Ogni riga: modello/percorso, dataset, query, run, ordine, start, end, tool, db_name,
query (testo), successo, lunghezza della risposta (anteprima, tagliata a 10.000 dal banco).
"""
import gzip, json, os, sys, re
from collections import Counter

root, out = sys.argv[1], sys.argv[2]
stats = Counter()
with gzip.open(out, "wt", encoding="utf-8") as fo:
    for dp, dn, fn in os.walk(root):
        dn.sort()
        if "tool_calls.jsonl" not in fn:
            continue
        rel = os.path.relpath(dp, root)
        stats["file"] += 1
        m_ds = re.search(r"query_([^/]+)", rel)
        m_q = re.search(r"/query(\d+)(/|$)", "/" + rel)
        m_r = re.search(r"run_(\d+)", rel)
        with open(os.path.join(dp, "tool_calls.jsonl"), encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                try:
                    e = json.loads(line)
                except Exception:
                    stats["righe_rotte"] += 1
                    continue
                tn = e.get("tool_name")
                stats["tool:" + str(tn)] += 1
                if tn not in ("query_db", "list_db"):
                    continue
                a = e.get("args") or {}
                res = e.get("result") or {}
                fo.write(json.dumps({
                    "path": rel, "dataset": m_ds.group(1) if m_ds else None,
                    "q": int(m_q.group(1)) if m_q else None,
                    "run": int(m_r.group(1)) if m_r else None,
                    "i": i, "start": e.get("start"), "end": e.get("end"),
                    "time": e.get("time"), "tool": tn,
                    "db_name": a.get("db_name"), "query": a.get("query"),
                    "ok": res.get("success"), "res_len": len(res.get("preview") or ""),
                }) + "\n")
                stats["estratte"] += 1
print(json.dumps(stats, indent=1))
