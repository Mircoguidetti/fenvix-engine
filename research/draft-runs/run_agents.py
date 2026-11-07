"""Points 1 and 2 on every agent source -> results/agents.json, results/agent_runs.tsv.gz.

Usage: python3 run_agents.py   (env: DR_DATA, TAU2, BOARD, TAUB, DAB, SWE; see README.md)
"""
import gzip
import json
import os
import sys

import agent_runs as ar
import docs_model as dm

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def pct(xs, q):
    s = sorted(xs)
    if not s:
        return None
    i = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[i]


def share(rows, f):
    return round(100.0 * sum(1 for r in rows if f(r)) / len(rows), 1) if rows else None


def summarize(rows):
    """rows: measured runs (all, writing or not)."""
    w = [r for r in rows if r["n_writes"] > 0]
    out = {"runs": len(rows), "writing_runs": len(w), "writing_share": share(rows, lambda r: r["n_writes"] > 0)}
    if not w:
        return out
    L = [r["length"] for r in w if r.get("length") is not None]
    Lc = [r["length_capped"] for r in w if r.get("length_capped") is not None]
    T = [r["span_txn"] for r in w if r.get("span_txn") is not None]
    out.update({
        "length_median_s": round(pct(L, 0.5), 1) if L else None,
        "length_p90_s": round(pct(L, 0.9), 1) if L else None,
        "length_p10_s": round(pct(L, 0.1), 1) if L else None,
        "over_60s": share(w, lambda r: (r.get("length") or 0) > 60),
        "over_60s_capped": share(w, lambda r: (r.get("length_capped") or 0) > 60),
        "length_capped_median_s": round(pct(Lc, 0.5), 1) if Lc else None,
        "length_capped_p90_s": round(pct(Lc, 0.9), 1) if Lc else None,
        "length_agent_only_median_s": (round(pct([r["length_agent_only"] for r in w if r.get("length_agent_only")
                                                  is not None], 0.5) or 0, 1)),
        "over_60s_agent_only": share(w, lambda r: (r.get("length_agent_only") or 0) > 60),
        "span_txn_median_s": round(pct(T, 0.5), 1) if T else None,
        "span_txn_p90_s": round(pct(T, 0.9), 1) if T else None,
        "span_txn_over_60s": share(w, lambda r: (r.get("span_txn") or 0) > 60),
        "writes_median": pct([r["n_writes"] for r in w], 0.5),
        "writes_mean": round(sum(r["n_writes"] for r in w) / len(w), 2),
        "writes_1": share(w, lambda r: r["n_writes"] == 1),
        "writes_2plus": share(w, lambda r: r["n_writes"] >= 2),
        "docs_median": pct([r["docs_written"] for r in w], 0.5),
        "single_customer": share(w, lambda r: r["n_customers"] == 1),
        "several_customers": share(w, lambda r: r["n_customers"] >= 2),
        "no_customer_resolved": share(w, lambda r: r["n_customers"] == 0),
        "touches_shared_doc": share(w, lambda r: bool(r["shared_written"])),
        "single_customer_no_shared": share(w, lambda r: r["n_customers"] == 1 and not r["shared_written"]),
        "ryw_doc": share(w, lambda r: r["ryw_doc"]),
        "ryw_customer": share(w, lambda r: r["ryw_cust"]),
        "multi_doc_cross_calls": share(w, lambda r: r["multi_doc_cross_calls"]),
        "multi_doc_within_call": share(w, lambda r: r["multi_doc_within_call"]),
        "ryw_or_multi": share(w, lambda r: r["ryw_doc"] or r["multi_doc_cross_calls"]),
        "ryw_or_multi_incl_within": share(w, lambda r: r["ryw_doc"] or r["multi_doc_cross_calls"]
                                         or r["multi_doc_within_call"]),
        "span_between_writes_median_s_2plus": (round(pct([r["span_first_write_last_write"] for r in w
                                                          if r["n_writes"] >= 2 and r.get("span_first_write_last_write")
                                                          is not None], 0.5) or 0, 1)
                                               if any(r["n_writes"] >= 2 for r in w) else None),
        "span_between_writes_over_60s": share(w, lambda r: (r.get("span_first_write_last_write") or 0) > 60),
        "neither": share(w, lambda r: not (r["ryw_doc"] or r["multi_doc_cross_calls"])),
        "owner_fallback_runs": share(w, lambda r: r.get("owner_fallbacks", 0) > 0),
    })
    return out


def main():
    os.makedirs(OUT, exist_ok=True)
    dbs = {d: dm.DB(d) for d in ("retail", "airline", "telecom", "banking_knowledge")}
    rows = []
    core = []
    for src, files in (("tau2_paper", ar.tau2_paper_files()), ("tau2_board", ar.tau2_board_files())):
        for model, dom, f in files:
            for r in ar.tau2_runs(src, model, dom, ar.tau2_sims(f)):
                m = ar.measure(r, dbs[dom])
                m.update({k: r[k] for k in ("src", "dom", "model", "id")})
                rows.append(m)
                m2 = ar.measure(r, dbs[dom], exclude=ar.LOG_WRITES)
                m2.update({k: r[k] for k in ("dom", "model", "id")})
                m2["src"] = src + "_core"
                core.append(m2)
        print(src, "done", file=sys.stderr)
    a_steps, u_steps = ar.paper_step_distributions()
    sm = ar.StepModel(a_steps, u_steps)
    det = {}
    for deterministic in (False, True):
        for r in ar.taubench_runs(sm, deterministic=deterministic):
            m = ar.measure(r, dbs[r["dom"]])
            m.update({k: r[k] for k in ("src", "dom", "model", "id")})
            if deterministic:
                m["src"] = "taubench_median_steps"
                det.setdefault("x", []).append(m)
            else:
                rows.append(m)
    swe = ar.swe_runs()
    dab = ar.dab_summary()
    res = {"step_model": {"agent_step_median_s": round(ar.StepModel.median(sm.a), 2),
                          "user_step_median_s": round(ar.StepModel.median(sm.u), 2),
                          "agent_steps": len(sm.a), "user_steps": len(sm.u)},
           "dab": dab, "by_source": {}, "by_source_domain": {}, "by_model_board": {}}
    groups = {
        "tau2_paper": [r for r in rows if r["src"] == "tau2_paper"],
        "tau2_board": [r for r in rows if r["src"] == "tau2_board"],
        "taubench": [r for r in rows if r["src"] == "taubench"],
        "taubench_median_steps": det.get("x", []),
        "swe": swe,
    }
    groups["customer_service_all"] = groups["tau2_paper"] + groups["tau2_board"] + groups["taubench"]
    groups["customer_service_clocked"] = groups["tau2_paper"] + groups["tau2_board"]
    groups["tau2_board_core"] = [r for r in core if r["src"] == "tau2_board_core"]
    groups["customer_service_all_core"] = core + groups["taubench"]
    groups["board_banking_core"] = [r for r in core if r["src"] == "tau2_board_core"
                                    and r["dom"] == "banking_knowledge"]
    for k, v in groups.items():
        res["by_source"][k] = summarize(v)
    for src in ("tau2_paper", "tau2_board", "taubench"):
        for dom in sorted({r["dom"] for r in groups[src]}):
            res["by_source_domain"][f"{src}|{dom}"] = summarize([r for r in groups[src] if r["dom"] == dom])
    for model in sorted({r["model"] for r in groups["tau2_board"]}):
        res["by_model_board"][model] = summarize([r for r in groups["tau2_board"] if r["model"] == model])
    # write tools that most often make a run multi-customer or touch shared docs
    multi = {}
    for r in groups["customer_service_all"]:
        if r["n_customers"] >= 2:
            multi.setdefault(r["dom"], 0)
            multi[r["dom"]] += 1
    res["several_customer_runs_by_domain"] = multi
    json.dump(res, open(f"{OUT}/agents.json", "w"), indent=1)
    with gzip.open(f"{OUT}/agent_runs.tsv.gz", "wt") as f:
        cols = ["src", "dom", "model", "id", "n_writes", "docs_written", "n_customers", "shared_written",
                "ryw_doc", "ryw_cust", "multi_doc_cross_calls", "multi_doc_within_call", "length",
                "length_capped", "length_agent_only", "span_txn", "span_first_write_last_write", "t_first_write_from_start"]
        f.write("\t".join(cols) + "\n")
        for r in rows + swe:
            vals = []
            for c in cols:
                v = r.get(c)
                if isinstance(v, float):
                    v = round(v, 2)
                if isinstance(v, list):
                    v = len(v)
                vals.append("" if v is None else str(v))
            f.write("\t".join(vals) + "\n")
    print(json.dumps(res["by_source"], indent=1))


if __name__ == "__main__":
    main()
