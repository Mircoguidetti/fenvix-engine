"""firstnext.py's FIRST/NEXT lines, with each system's summary taken from a given results dir (P-command-cpu: our line
and Postgres from 2026-11-13, mongod from 2026-11-12 because the mongod line did not fit the disk twice on 2026-11-13).
  python firstnext_mixed.py fenvix=DIR/fenvix postgres=DIR/postgres mongo=DIR2/mongo [base=DIR/fenvix-base]"""
import json, sys
args = dict(a.split("=", 1) for a in sys.argv[1:])
S = {k: json.load(open(v + "/summary.json"))["latency_ms"] for k, v in args.items()}
PAIRS = [("resume (write task)", "resume[long]", "resume[none]"), ("resume (read task)", "resume[read]", "resume[read]:next"),
         ("list whole history", "list", "list:next"), ("list last 10", "list_limit10", "list_limit10:next"),
         ("time travel", "time_travel", "time_travel:next"), ("save one step", "save_step[old]", "save_step[none]")]
v = lambda r: "FASTER" if r < 0.9 else ("EQUAL" if r <= 1.1 else "SLOWER")
for rnd, name in (("A", "no delay"), ("B", "20 ms delay on our MinIO")):
    print(f"== round {rnd} ({name})")
    for label, first, nxt in PAIRS:
        for which, op in (("FIRST", first), ("NEXT", nxt)):
            L = {t: S[t][rnd][op] for t in S}
            parts = []
            for q in ("p50", "p99"):
                bt = min(("postgres", "mongo"), key=lambda t: L[t][q])
                o, b = L["fenvix"][q], L[bt][q]
                extra = f" (base {L['base'][q]:.1f})" if "base" in L else ""
                parts.append(f"{q} ours {o:.1f}{extra} pg {L['postgres'][q]:.1f} mongo {L['mongo'][q]:.1f} -> x{o/b:.2f} {v(o/b)}")
            cpu = (L["fenvix"].get("res_p50") or {}).get("cpu_ns.cook", 0) / 1e6
            cb = (L["base"].get("res_p50") or {}).get("cpu_ns.cook", 0) / 1e6 if "base" in L else None
            print(f"{label:20s} {which:5s} " + " | ".join(parts) + f" | cook cpu p50 {cpu:.2f} ms" + (f" (base {cb:.2f})" if cb is not None else ""))
