"""Summary of the fx_derive bench (results/bench.jsonl) against the competitors (data-prep/results/competitors.json).
Writes results/summary.txt and results/summary.json. Every number here is MEASURED (bytes, GETs, CPU); the latency is
behind a simulated S3 (25 ms median, sigma 0.6, 90 MB/s)."""
import json, os, sys
H = os.path.dirname(os.path.abspath(__file__))
rows = [json.loads(l) for l in open(os.path.join(H, "results/bench.jsonl"))]
comp = json.load(open(os.path.join(H, "data-prep/results/competitors.json")))
cells = comp["cells"] if isinstance(comp, dict) and "cells" in comp else comp
def cmap():
    out = {}
    it = cells.items() if isinstance(cells, dict) else [(c.get("cell"), c) for c in cells]
    for k, v in it:
        out[k] = v
    return out
C = cmap()
def get(c, *names):
    for n in names:
        if n in c and c[n] is not None:
            return c[n]
    return None
by = {}
for r in rows:
    cell = os.path.basename(r["dump"]).replace(".fxd", "")
    cfg = f'{r["codec"]}-{"derived" if r["derive"] else "plain"}' + ("" if r.get("prefetch", True) else "-noprefetch")
    by.setdefault(cell, {})[cfg] = r
lines, js = [], {}
for cell in sorted(by):
    c = C.get(cell, {})
    pg = get(c, "postgres_default_bytes_per_thread")
    pgd = get(c, "postgres_delta_bytes_per_thread")
    mgu = get(c, "mongo_default_uncompressed_per_thread")
    mgd = get(c, "mongo_default_disk_per_thread")
    lines.append(f"== {cell}: threads {next(iter(by[cell].values()))['threads']}, mean checkpoints {get(c, 'mean_checkpoints_per_thread', 'mean_checkpoints')}")
    lines.append(f"   competitors per thread: Postgres saver as shipped {pg}, with DeltaChannel {pgd}, Mongo saver uncompressed {mgu}, on disk {mgd}")
    js[cell] = {"competitors": c, "ours": {}}
    for cfg, r in sorted(by[cell].items()):
        b = r["stored_bytes_per_thread"]
        e = {"bytes_per_thread": b, "vs_pg_default": pg / b if pg else None, "vs_pg_delta": pgd / b if pgd else None,
             "vs_mongo_uncompressed": mgu / b if mgu else None, "wrong": r["wrong"], "checked": r["checked"],
             "latest_cold": r["latest_cold"], "old_cold": r["old_cold"], "latest_warm": r["latest_warm"], "old_warm": r["old_warm"],
             "reseal_cpu_s_per_saver_gb": r["reseal_cpu_s_per_saver_gb"], "encoded": r["encoded"], "fallbacks": r["fallbacks"],
             "passes": r.get("reseal_passes")}
        js[cell]["ours"][cfg] = e
        f = lambda x: f"{x:.1f}" if x else "-"
        lines.append(f"   {cfg:22s} {b/1e3:9.1f} KB/thread  x{f(e['vs_pg_default'])} vs PG, x{f(e['vs_pg_delta'])} vs PG DeltaChannel, x{f(e['vs_mongo_uncompressed'])} vs Mongo"
                     f" | wrong {r['wrong']}/{r['checked']} | cold latest GETs {r['latest_cold']['gets_mean']:.2f} trips {r['latest_cold']['trips_mean']:.2f} p50 {r['latest_cold']['ms_p50']:.0f} ms"
                     f" | cold old GETs {r['old_cold']['gets_mean']:.2f} trips {r['old_cold']['trips_mean']:.2f} p50 {r['old_cold']['ms_p50']:.0f} ms | warm p50 {r['latest_warm']['ms_p50']:.2f} ms"
                     f" | reseal CPU {r['reseal_cpu_s_per_saver_gb']:.1f} s/GB")
open(os.path.join(H, "results/summary.txt"), "w").write("\n".join(lines) + "\n")
json.dump(js, open(os.path.join(H, "results/summary.json"), "w"), indent=1)
print("\n".join(lines))
