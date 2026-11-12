"""Cost per million agent steps and cost per year of kept history, from a bench.py summary and prices.json.

Every printed number is labelled: MEASURED comes from the summary (stored bytes per checkpoint, S3 PUTs per checkpoint),
DECLARED from prices.json (with its URL), ESTIMATED is the arithmetic shown, UNKNOWN when a price is missing.

Model (all ESTIMATED, assumptions printed):
  storage for 1 M steps kept 1 year  = stored_bytes_per_checkpoint * 1e6 / 1e9 GB * $/GB-month * 12
  requests per 1 M steps             = Aurora Standard and DocumentDB Standard: write I/Os = ceil(stored bytes per
                                       checkpoint / 4 KiB) per step (Aurora counts writes in 4 KiB units; DocumentDB's
                                       unit is an ASSUMPTION; reads assumed served from cache);
                                       Fenvix: S3 PUTs per checkpoint (MEASURED on this run) * $/1000
  compute per 1 M steps              = instance $/h * 1e6 / (steps_per_second * 3600), at the stated sustained rate
  one year of history at rate R      = storage grows linearly; month m holds m/12 of the year's bytes:
                                       $/GB-month * GB_per_year * (1+2+...+12)/12, plus instance $/h * 8760

Usage: python cost.py results/local-.../postgres/summary.json --option aurora_standard [--rate 20] [--sps 200]
       python cost.py --all results/local-.../   (every target dir x every price option of the same target kind)
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os

HERE = os.path.dirname(os.path.abspath(__file__))


# options charged per million I/O (DECLARED in prices.json): Aurora Standard and, since DECISIONS 2026-11-11 ("Amazon
# DocumentDB joins the comparison"), DocumentDB Standard. Their I/O-Optimized options charge none.
IO_PRICED = ("aurora_standard", "documentdb_standard")


def v(p: dict | None):
    return None if not p else p.get("value")


def fmt(x, nd=4):
    return "UNKNOWN" if x is None else f"${x:,.{nd}f}"


def cost(summary: dict, opt_name: str, opt: dict, rate: float, sps: float) -> dict:
    b = summary["bytes"]
    per_ckpt = b.get("stored_per_checkpoint")
    lines, out = [], {"option": opt_name, "target": summary["target"]}
    lines.append(f"== {opt_name} on {summary['target']} ({summary.get('label') or 'no label'}); {summary['note']}")
    if per_ckpt is None:
        lines.append("stored bytes per checkpoint: UNKNOWN (the target reported no stored size)")
        out["lines"] = lines
        return out
    lines.append(f"stored bytes per checkpoint: {per_ckpt:,.0f} (MEASURED, {b['stored_from']}; "
                 f"{b['stored']:,} B / {b['checkpoints']:,} checkpoints)")
    gb_per_m = per_ckpt * 1e6 / 1e9
    st = v(opt.get("storage_gb_month"))
    s_year = None if st is None else gb_per_m * st * 12
    lines.append(f"storage, 1 M steps kept 1 year: {gb_per_m:,.2f} GB x {fmt(st, 3)}/GB-month x 12 = {fmt(s_year)} (ESTIMATED; price {opt.get('storage_gb_month', {}).get('label')})")
    req = 0.0
    if opt_name in IO_PRICED:
        ios = math.ceil(per_ckpt / 4096)
        req = ios * v(opt["io_per_million"])
        unit = "" if opt_name == "aurora_standard" else (" (ASSUMPTION: DocumentDB's I/O unit taken as Aurora's 4 KiB; "
                                                         "the AWS round measures DocumentDB's I/O)")
        lines.append(f"I/O, 1 M steps: {ios} write I/Os per step (ceil({per_ckpt:,.0f}/4096), ESTIMATED{unit}) x {fmt(v(opt['io_per_million']), 2)}/M = {fmt(req)}")
    elif summary["target"] == "fenvix" and summary.get("s3_put_requests_during_write") is not None:
        puts = summary["s3_put_requests_during_write"] / b["checkpoints"]
        req = puts * 1e6 / 1000 * v(opt["put_per_1000"])
        lines.append(f"S3 PUT, 1 M steps: {puts:.3f} PUT per step (MEASURED this run; depends on write rate and flush interval) x 1e6 / 1000 x {fmt(v(opt['put_per_1000']), 4)} = {fmt(req)}")
    ih = v(opt.get("instance_hour"))
    comp = None if ih is None else ih * 1e6 / (sps * 3600)
    lines.append(f"compute, 1 M steps at {sps:g} steps/s sustained (ASSUMPTION): {fmt(ih, 3)}/h x {1e6 / (sps * 3600):,.2f} h = {fmt(comp)}")
    per_m = None if s_year is None or comp is None else s_year + req + comp
    lines.append(f"=> cost per 1 M steps (incl. 1 year of keeping them): {fmt(per_m)} (ESTIMATED)")
    gb_year = per_ckpt * rate * 3600 * 24 * 365 / 1e9
    hist = None if st is None else st * gb_year * sum(range(1, 13)) / 12
    year = None if hist is None or ih is None else hist + ih * 8760
    lines.append(f"one year of history at {rate:g} steps/s average (ASSUMPTION): {gb_year:,.1f} GB written; storage "
                 f"{fmt(st, 3)} x {gb_year:,.1f} x 78/12 = {fmt(hist, 2)}; + instance {fmt(ih, 3)} x 8760 h => {fmt(year, 2)} (ESTIMATED)")
    out.update({"gb_per_million_steps": gb_per_m, "storage_1y_per_million": s_year, "requests_per_million": req,
                "compute_per_million": comp, "per_million_steps": per_m, "history_gb_per_year": gb_year,
                "history_storage_year": hist, "year_total": year, "lines": lines})
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("summary", nargs="?", help="a summary.json")
    ap.add_argument("--all", help="a results directory with one sub-directory per target")
    ap.add_argument("--option", help="price option name in prices.json (default: every option of the target kind)")
    ap.add_argument("--prices", default=os.path.join(HERE, "prices.json"))
    ap.add_argument("--rate", type=float, default=20, help="average steps/s of the app over a year (assumption)")
    ap.add_argument("--sps", type=float, default=200, help="sustained steps/s one instance serves (assumption)")
    ap.add_argument("--out", help="write the results as JSON here")
    a = ap.parse_args()
    prices = json.load(open(a.prices))["options"]
    # LangGraph targets only (mastra-* summaries have no per-checkpoint storage model)
    files = [f for t in ("fenvix", "mongo", "postgres") for f in glob.glob(os.path.join(a.all, t, "summary.json"))] if a.all else [a.summary]
    res = []
    for f in files:
        s = json.load(open(f))
        settled = os.path.join(os.path.dirname(f), "s3_after_idle_close.json")
        if s["target"] == "fenvix" and os.path.exists(settled):
            # the live listing still holds WAL objects that GC removes later; use the bucket after the tenant's idle close
            tot = json.load(open(settled))["total"]
            s["bytes"].update(stored=tot, stored_per_checkpoint=round(tot / s["bytes"]["checkpoints"], 1),
                              stored_from="S3 objects under tenants/app_demo/ after the idle close (WAL garbage-collected)")
        names = [a.option] if a.option else [n for n, o in prices.items() if o["target"] == s["target"]]
        for n in names:
            r = cost(s, n, prices[n], a.rate, a.sps)
            print("\n".join(r["lines"]) + "\n")
            res.append(r)
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
