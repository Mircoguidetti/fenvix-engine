"""P-bench-big (2026-11-12): the verdict per operation, ours against the best of mongod and Postgres, from the three
summaries of `lines.py run LINE --big`.

  python compare_big.py RESULTS_DIR      (RESULTS_DIR/{postgres,mongo,fenvix}/summary.json) -> RESULTS_DIR/verdict.{json,txt}

Rule, written before the first measured run: for each operation and each of p50 and p99, ratio = ours / best of the
other two (the lower of the two values). FASTER below 0.9, EQUAL from 0.9 to 1.1, SLOWER above 1.1. The absolute gap
in ms is given too (DECISIONS 2026-11-12: within 20 ms a customer does not notice).
Comparisons:
  "no delay":   ours round A against the others' round A (every line after its own cold restart);
  "20 ms delay": ours round B (MinIO behind s3delay, 20 ms per response) against the others' round B (no delay applies
                to them: round B is a second cold sample of the same mix on new threads).
Resources per op (means per call, from the probes): ours S3 GETs and bytes, block cache and sealed-run hits, cook and
MinIO CPU; mongod and Postgres disk reads (blkio) and CPU of the primary.
"""
from __future__ import annotations

import json
import os
import sys

# P-big-speed: every op FIRST (the customer's first request after idle) and NEXT (the same customer right after):
# resume[long] is a write task's first request, resume[none] its next; a read task's next requests are "<op>:next".
OPS = ["resume[long]", "resume[none]", "resume[read]", "resume[read]:next", "list", "list:next", "list_limit10",
       "list_limit10:next", "time_travel", "time_travel:next", "save_step[old]", "save_step[none]", "save_step"]
TARGETS = ("postgres", "mongo", "fenvix")


def verdict(ratio):
    if ratio is None:
        return "UNKNOWN"
    return "FASTER" if ratio < 0.9 else ("EQUAL" if ratio <= 1.1 else "SLOWER")


def res_line(t: str, d: dict) -> dict:
    m = d.get("res_mean") or {}
    n = d.get("n") or 1

    def s(prefix):
        return sum(v for k, v in m.items() if k.startswith(prefix))
    out = {"cpu_ms_all_members": round(s("cpu_ns.") / 1e6, 3)}
    for k, v in m.items():
        if k.startswith("cpu_ns."):
            out[f"cpu_ms.{k[7:]}"] = round(v / 1e6, 3)
        elif k.startswith(("rd_ops.", "rd_bytes.")):
            out[k] = v
    if t == "fenvix":
        out["s3_gets"] = round(m.get("srv.s3.get", 0) + m.get("srv.s3.getRange", 0), 3)
        out["s3_get_bytes"] = m.get("srv.s3.getBytes", 0)
        out["s3_puts"] = m.get("srv.s3.put", 0)
        out["block_cache_hits"], out["block_cache_misses"] = m.get("srv.cache.hits", 0), m.get("srv.cache.misses", 0)
        out["sealed_block_hits"], out["sealed_run_gets"] = m.get("srv.sealedRuns.blockHits", 0), m.get("srv.sealedRuns.runGets", 0)
        out["background_gets"] = m.get("srv.backgroundReads.gets", 0)
        p50 = d.get("res_p50") or {}
        out["s3_gets_p50"] = (p50.get("srv.s3.get") or 0) + (p50.get("srv.s3.getRange") or 0)
    out["n"] = n
    return out


def main() -> None:
    d = sys.argv[1]
    S = {t: json.load(open(os.path.join(d, t, "summary.json"))) for t in TARGETS}
    out = {"rule": "ratio = ours / best of mongod and Postgres; FASTER < 0.9 <= EQUAL <= 1.1 < SLOWER",
           "ok": {t: S[t].get("ok") for t in TARGETS}, "wrong_reads": {t: S[t].get("wrong_reads") for t in TARGETS},
           "sizes": {}, "comparisons": {}, "resources": {}, "cache": {}}
    for t in TARGETS:
        b = S[t]["bytes"]
        out["sizes"][t] = {"logical": b.get("logical_total"), "stored": b.get("stored"), "stored_from": b.get("stored_from"),
                           "stored_per_logical": b.get("stored_per_logical")}
    lines = []
    for cmp_name, rnd in (("no delay", "A"), ("20 ms delay on our MinIO", "B")):
        rows = {}
        for op in OPS:
            L = {t: (S[t].get("latency_ms") or {}).get(rnd, {}).get(op) for t in TARGETS}
            if not all(L.values()):
                rows[op] = {"missing": [t for t in TARGETS if not L[t]]}
                continue
            r = {"n": {t: L[t]["n"] for t in TARGETS}}
            for q in ("p50", "p99"):
                best_t = min(("postgres", "mongo"), key=lambda t: L[t][q])
                best = L[best_t][q]
                ours = L["fenvix"][q]
                ratio = round(ours / best, 2) if best else None
                r[q] = {"ours": ours, "postgres": L["postgres"][q], "mongo": L["mongo"][q], "best": best_t,
                        "ratio": ratio, "gap_ms": round(ours - best, 2), "verdict": verdict(ratio)}
            rows[op] = r
            lines.append(f"{cmp_name:26s} {op:18s} p50 ours {r['p50']['ours']:9.2f} pg {r['p50']['postgres']:9.2f} mg {r['p50']['mongo']:9.2f}"
                         f" x{r['p50']['ratio']:<6} {r['p50']['verdict']:6s} | p99 ours {r['p99']['ours']:9.2f} pg {r['p99']['postgres']:9.2f}"
                         f" mg {r['p99']['mongo']:9.2f} x{r['p99']['ratio']:<6} {r['p99']['verdict']}   n={r['n']}")
        out["comparisons"][cmp_name] = rows
    for t in TARGETS:
        out["resources"][t] = {rnd: {op: res_line(t, v) for op, v in (S[t].get("latency_ms") or {}).get(rnd, {}).items()}
                               for rnd in ("A", "B")}
        out["cache"][t] = {r["round"]: r.get("cache") for r in S[t]["mix"]["rounds"]}
    json.dump(out, open(os.path.join(d, "verdict.json"), "w"), indent=1)
    txt = "\n".join([out["rule"], f"ok {out['ok']} wrong_reads {out['wrong_reads']}",
                     json.dumps(out["sizes"])] + lines)
    open(os.path.join(d, "verdict.txt"), "w").write(txt + "\n")
    print(txt)


if __name__ == "__main__":
    main()
