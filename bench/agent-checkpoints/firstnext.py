"""P-big-speed: the report's FIRST / NEXT lines from the three summaries (same rule as compare_big.py).

  python firstnext.py RESULTS_DIR   -> RESULTS_DIR/firstnext.txt

FIRST = the customer's first request after idle: resume[long] (a write task's resume), resume[read], list,
list_limit10, time_travel (a read task's first read). NEXT = the same customer right after: resume[none],
<op>:next. Saves: save_step[old] (the saves of the first turn, right after the FIRST resume) and save_step[none].
"""
import json
import os
import sys

PAIRS = [("resume (write task)", "resume[long]", "resume[none]"), ("resume (read task)", "resume[read]", "resume[read]:next"),
         ("list whole history", "list", "list:next"), ("list last 10", "list_limit10", "list_limit10:next"),
         ("time travel", "time_travel", "time_travel:next"), ("save one step", "save_step[old]", "save_step[none]")]


def verdict(r):
    return "FASTER" if r < 0.9 else ("EQUAL" if r <= 1.1 else "SLOWER")


def main():
    d = sys.argv[1]
    S = {t: json.load(open(os.path.join(d, t, "summary.json"))) for t in ("postgres", "mongo", "fenvix")}
    out = []
    for cmp, rnd in (("no delay", "A"), ("20 ms delay", "B")):
        out.append(f"== {cmp} (ours round {rnd} against the others' round {rnd})")
        for name, first, nxt in PAIRS:
            for which, op in (("FIRST", first), ("NEXT", nxt)):
                L = {t: S[t]["latency_ms"][rnd].get(op) for t in S}
                if not all(L.values()):
                    out.append(f"{name:20s} {which:5s} missing {[t for t in L if not L[t]]}")
                    continue
                parts = []
                for q in ("p50", "p99"):
                    best_t = min(("postgres", "mongo"), key=lambda t: L[t][q])
                    b, o = L[best_t][q], L["fenvix"][q]
                    parts.append(f"{q} ours {o:.1f} best {b:.1f} ({best_t}) x{o / b:.2f} {verdict(o / b)} gap {o - b:+.1f} ms")
                out.append(f"{name:20s} {which:5s} [{op}] n={L['fenvix']['n']}: " + " | ".join(parts))
    txt = "\n".join(out)
    open(os.path.join(d, "firstnext.txt"), "w").write(txt + "\n")
    print(txt)


if __name__ == "__main__":
    main()
