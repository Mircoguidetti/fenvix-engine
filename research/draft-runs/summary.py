"""results/summary.txt from results/agents.json and results/collisions.json, with the gate verdicts and point 4."""
import json
import os

R = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def main():
    a = json.load(open(f"{R}/agents.json"))
    c = json.load(open(f"{R}/collisions.json"))
    S = a["by_source"]
    out = []
    p = out.append
    p("POINT 1 and 2, by source (writing runs only unless said)")
    keys = ["runs", "writing_runs", "writing_share", "length_median_s", "length_p90_s", "length_capped_median_s",
            "length_capped_p90_s", "over_60s", "length_agent_only_median_s", "over_60s_agent_only",
            "span_txn_median_s", "span_txn_over_60s", "writes_mean", "writes_2plus", "single_customer",
            "several_customers", "touches_shared_doc", "ryw_doc", "ryw_customer", "multi_doc_cross_calls",
            "multi_doc_within_call", "ryw_or_multi", "ryw_or_multi_incl_within", "neither",
            "span_between_writes_median_s_2plus", "span_between_writes_over_60s", "owner_fallback_runs"]
    for src, v in S.items():
        p(f"\n[{src}]")
        for k in keys:
            if k in v:
                p(f"  {k}: {v[k]}")
    p("\nBy source and domain: over_60s / single_customer / touches_shared / ryw_doc / multi_cross / ryw_or_multi")
    for k, v in a["by_source_domain"].items():
        p(f"  {k}: {v.get('writing_runs')} writing; {v.get('over_60s')} / {v.get('single_customer')} / "
          f"{v.get('touches_shared_doc')} / {v.get('ryw_doc')} / {v.get('multi_doc_cross_calls')} / "
          f"{v.get('ryw_or_multi')}; median length {v.get('length_median_s')} s")
    p("\n2026 leaderboard by model: median length (capped) / over_60s / ryw_doc / ryw_or_multi")
    for k, v in a["by_model_board"].items():
        p(f"  {k}: {v.get('length_capped_median_s')} s / {v.get('over_60s')} / {v.get('ryw_doc')} / "
          f"{v.get('ryw_or_multi')}")
    d = a["dab"]
    p(f"\nDAB: {d['runs']} runs, {d['queries']} queries, {d['write_queries']} writing queries in "
      f"{d['writing_runs']} runs (all temporary or scratch tables)")
    p(f"Step model (tau2 paper, MEASURED): {a['step_model']}")

    p("\nPOINT 3: collision probability (%) by L (uniform moment / event anchor / session-start anchor)")
    for ds, v in c["datasets"].items():
        p(f"  {ds}: {v['users']} users, {v['events']} events, {v['active_user_days']} active user-days, "
          f"clock {v['clock_resolution_s']} s")
        for L, x in v["by_L"].items():
            p(f"    L={L} s: {x['uniform_moment']} / {x['event_anchor']} / {x['session_start_anchor']}")

    cs = S["customer_service_all"]
    p("\nGATES (pool: tau2 paper + tau2 2026 leaderboard + tau-bench, customer-service agents)")
    g1c = cs["over_60s"] >= 30 and cs["single_customer"] >= 80
    g1d = cs["over_60s"] < 10 or cs["several_customers"] > 40
    p(f"  1. over 60 s {cs['over_60s']}%, single customer {cs['single_customer']}%, several "
      f"{cs['several_customers']}% -> {'CONTINUE' if g1c else 'DROP' if g1d else 'MIXED'}")
    x = cs["ryw_or_multi"]
    p(f"  2. RYW or multi-document {x}% (RYW alone {cs['ryw_doc']}%) -> "
      f"{'CONTINUE' if x >= 30 else 'DROP' if x < 10 else 'MIXED'}")
    Lm = str(min(c["L_seconds"], key=lambda L: abs(L - cs["length_median_s"])))
    irc = c["datasets"]["irc"]["by_L"][Lm]
    p(f"  3. chat at the median length L={Lm} s: uniform moment {irc['uniform_moment']}% -> "
      f"{'CONTINUE' if irc['uniform_moment'] < 10 else 'DROP' if irc['uniform_moment'] > 30 else 'MIXED'}; "
      f"session-start anchor {irc['session_start_anchor']}%")

    p("\nPOINT 4: 100 customer-service runs of the median length")
    no_write = 100 - cs["writing_share"]
    need = cs["writing_share"] * x / 100
    pause = cs["writing_share"] - need
    p(f"  no write at all: {no_write:.1f}; write but pause-and-ask is enough: {pause:.1f}; need a draft: {need:.1f}")
    for ds in ("irc", "lsapp", "cdnow"):
        v = c["datasets"][ds]["by_L"][Lm]
        for anchor in ("uniform_moment", "session_start_anchor"):
            conf = need * v[anchor] / 100
            p(f"  {ds} {anchor}: conflict and re-run {conf:.1f}, approved in one click {need - conf:.1f}")
    open(f"{R}/summary.txt", "w").write("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
